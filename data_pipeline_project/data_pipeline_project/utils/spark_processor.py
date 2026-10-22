"""
utils/spark_processor.py
─────────────────────────
REAL Apache Spark processing for the Big Data pipeline.

Replaces the simulated Python-loop scaling with genuine distributed
computation on a local Spark cluster (single-node by default; swap
master URL to a real cluster for production).

Spark handles:
  • Distributed data loading (partitioned reads)
  • Parallel deduplication
  • Vectorised fill of missing values
  • Multi-stage aggregations (category / region / month)
  • Single-file output merge

Install:  pip install pyspark
"""

import glob
import logging
import os
import shutil
from typing import Any, Dict

log = logging.getLogger(__name__)


# ── Spark session factory ─────────────────────────────────────────────────────

def _get_spark_session(app_name: str = "EcommerceBigDataPipeline"):
    """
    Create (or reuse) a SparkSession.
    master="local[*]" uses all CPU cores on this machine — real parallelism.
    Change master to "spark://<host>:7077" for a standalone Spark cluster,
    or "yarn" / "k8s://..." for Hadoop / Kubernetes deployments.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                           # use ALL available CPU cores
        .config("spark.sql.shuffle.partitions", "8")  # tuned for single-node
        .config("spark.executor.memory",        "1g")
        .config("spark.driver.memory",          "1g")
        .config("spark.sql.adaptive.enabled",   "true")  # AQE — Spark 3+
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def is_spark_available() -> bool:
    """Return True if PySpark is installed and importable."""
    try:
        from pyspark.sql import SparkSession  # noqa: F401
        return True
    except ImportError:
        return False


# ── Main processing entry point ───────────────────────────────────────────────

def spark_process(input_csv: str, output_csv: str) -> Dict[str, Any]:
    """
    Full distributed ETL on the staged CSV using Apache Spark.

    Pipeline (all steps run inside the Spark engine):
      1. Read CSV into a distributed DataFrame (auto-partitioned)
      2. Drop duplicate order_ids (Spark shuffle dedup)
      3. Fill missing / invalid values (vectorised Spark operations)
      4. Count filled values
      5. Parallel aggregations — category sales, region sales, monthly sales
      6. Write cleaned DataFrame back to a single CSV via coalesce(1)

    Args:
        input_csv:  Path to the staged input CSV.
        output_csv: Destination path for the cleaned output CSV.

    Returns:
        dict with keys: cleaned_records, removed_duplicates, missing_filled,
                        summary {category_sales, region_sales, monthly_sales},
                        engine (always "Apache Spark")
    """
    from pyspark.sql import functions as F

    log.info("[Spark] Initialising Spark session …")
    spark = _get_spark_session()

    # ── 1. Distributed read ───────────────────────────────────────────────────
    log.info(f"[Spark] Reading {input_csv} …")
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    total_before = df.count()
    log.info(f"[Spark] Loaded {total_before:,} rows across {df.rdd.getNumPartitions()} partitions")

    # ── 2. Deduplication (Spark shuffle + hash) ───────────────────────────────
    df = df.dropDuplicates(["order_id"])
    total_after_dedup = df.count()
    n_dups = total_before - total_after_dedup
    log.info(f"[Spark] Dedup: removed {n_dups:,} duplicate rows")

    # ── 3. Fill missing values (vectorised column operations) ─────────────────
    df = df.withColumn(
        "sales",
        F.when(
            F.col("sales").isNull() | (F.col("sales").cast("double") <= 0),
            F.lit(99.99)
        ).otherwise(F.col("sales").cast("double"))
    )
    df = df.withColumn(
        "quantity",
        F.when(
            F.col("quantity").isNull() | (F.col("quantity").cast("int") <= 0),
            F.lit(1)
        ).otherwise(F.col("quantity").cast("int"))
    )
    df = df.withColumn(
        "region",
        F.when(
            F.col("region").isNull() | (F.trim(F.col("region")) == ""),
            F.lit("Unknown")
        ).otherwise(F.col("region"))
    )
    df = df.withColumn(
        "category",
        F.when(
            F.col("category").isNull() | (F.trim(F.col("category")) == ""),
            F.lit("Other")
        ).otherwise(F.col("category"))
    )
    df = df.withColumn(
        "date",
        F.when(
            F.col("date").isNull() | (F.trim(F.col("date")) == ""),
            F.lit(str(__import__("datetime").date.today()))
        ).otherwise(F.col("date"))
    )

    cleaned_count = df.count()

    # Approximate filled count (rows where any of the four fields was defaulted)
    n_filled = df.filter(
        (F.col("sales")    == 99.99)  |
        (F.col("quantity") == 1)      |
        (F.col("region")   == "Unknown") |
        (F.col("category") == "Other")
    ).count()

    log.info(f"[Spark] After cleaning: {cleaned_count:,} rows, ~{n_filled:,} defaults applied")

    # ── 4. Parallel aggregations ──────────────────────────────────────────────
    # Spark executes these as separate stages; on a real cluster they run
    # across distributed executors.

    # Category sales
    cat_df = (
        df.groupBy("category")
          .agg(F.round(F.sum("sales"), 2).alias("total"))
          .orderBy(F.desc("total"))
    )
    category_sales = {r["category"]: float(r["total"]) for r in cat_df.collect()}

    # Region sales
    reg_df = (
        df.groupBy("region")
          .agg(F.round(F.sum("sales"), 2).alias("total"))
          .orderBy(F.desc("total"))
    )
    region_sales = {r["region"]: float(r["total"]) for r in reg_df.collect()}

    # Monthly sales (YYYY-MM bucket)
    month_df = (
        df.withColumn("month", F.substring(F.col("date"), 1, 7))
          .filter(F.length("month") == 7)
          .groupBy("month")
          .agg(F.round(F.sum("sales"), 2).alias("total"))
          .orderBy("month")
    )
    monthly_sales = {r["month"]: float(r["total"]) for r in month_df.collect()}

    log.info(
        f"[Spark] Aggregations complete — "
        f"{len(category_sales)} categories, "
        f"{len(region_sales)} regions, "
        f"{len(monthly_sales)} months"
    )

    # ── 5. Write output (coalesce to single partition for downstream tasks) ───
    tmp_dir = output_csv + "_spark_tmp"
    df.coalesce(1).write.csv(tmp_dir, header=True, mode="overwrite")

    # Spark writes part-*.csv inside the tmp dir — move to final path
    parts = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if parts:
        os.makedirs(os.path.dirname(os.path.abspath(output_csv)), exist_ok=True)
        shutil.copy(parts[0], output_csv)
        log.info(f"[Spark] Output written → {output_csv}")
    shutil.rmtree(tmp_dir, ignore_errors=True)

    return {
        "cleaned_records":    cleaned_count,
        "removed_duplicates": n_dups,
        "missing_filled":     n_filled,
        "summary": {
            "category_sales": category_sales,
            "region_sales":   region_sales,
            "monthly_sales":  monthly_sales,
        },
        "engine": "Apache Spark",
        "partitions_used": df.rdd.getNumPartitions(),
    }


def spark_get_record_count(csv_path: str) -> int:
    """Quick distributed row count — useful for reporting."""
    if not is_spark_available():
        return 0
    spark = _get_spark_session()
    return spark.read.csv(csv_path, header=True).count()
