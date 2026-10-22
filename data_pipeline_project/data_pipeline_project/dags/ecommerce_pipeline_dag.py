"""
dags/ecommerce_pipeline_dag.py
──────────────────────────────
REAL Apache Airflow DAG for the E-Commerce Big Data Pipeline.

Issue 1 fix: task_transform_data now uses Apache Spark for distributed
             aggregation when PySpark is installed.

Issue 3 fix: task_load_to_warehouse writes to PostgreSQL FIRST (primary
             warehouse), and falls back to CSV only if DB is unavailable.

Architecture:
  Airflow executes all ETL tasks.
  Each task writes progress to  data/pipeline_status.json
  Flask reads that file for real-time dashboard updates.
"""

import csv
import datetime
import json
import logging
import os
import sys

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

_dags_dir = os.path.dirname(os.path.abspath(__file__))
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)

from utils.data_ingestion      import ingest_from_kaggle_csv, save_to_csv
from utils.data_transformation import (
    validate_records,
    remove_duplicates,
    fill_missing_values,
    aggregate_by_category,
    aggregate_by_region,
    aggregate_by_month,
)
from utils.spark_processor import is_spark_available, spark_process
from utils.db_loader       import is_postgres_available, load_records_to_postgres

log = logging.getLogger(__name__)

# ── File paths (absolute Docker paths — volume mounted from host ./data) ─────
DATA_DIR       = os.getenv("PIPELINE_DATA_DIR", "/opt/airflow/data")
RAW_INPUT_CSV  = os.path.join(DATA_DIR, "raw",       "input.csv")
RAW_STAGED_CSV = os.path.join(DATA_DIR, "raw",       "staged_data.csv")
CLEAN_CSV      = os.path.join(DATA_DIR, "processed", "cleaned_data.csv")
STATUS_FILE    = os.path.join(DATA_DIR, "pipeline_status.json")
RESULTS_FILE   = os.path.join(DATA_DIR, "results.json")

# ── DAG defaults ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data_engineering_team",
    "depends_on_past":  False,
    "email":            ["alerts@company.com"],
    "email_on_failure": False,   # no SMTP configured — prevents retry loops
    "email_on_retry":   False,
    "retries":          0,       # no retries — fail fast, fix the root cause
    "retry_delay":      timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),  # hard cap per task
}


# ── Status-file helpers ───────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def _read_status() -> dict:
    try:
        with open(STATUS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _update_status(**kwargs):
    state = _read_status()
    state.update(kwargs)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATUS_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _log(msg: str):
    log.info(msg)
    state = _read_status()
    logs  = state.get("logs", [])
    logs.append(f"[{_ts()}] {msg}")
    _update_status(logs=logs)


# ── TASK FUNCTIONS ────────────────────────────────────────────────────────────

def task_ingest_data(**context):
    """
    TASK 1 — Data Ingestion
    Load the Kaggle e-commerce CSV, map columns, optionally scale for Big Data.
    """
    conf         = context["dag_run"].conf or {}
    source       = conf.get("source", "CSV File")
    records_hint = int(conf.get("records", 200))
    big_data     = records_hint >= 500

    _update_status(current_task="Data Ingestion", progress=5)
    _log(f"Starting ingestion — source={source}, big_data={big_data}")

    if not os.path.exists(RAW_INPUT_CSV):
        raise FileNotFoundError(
            f"Dataset not found at '{RAW_INPUT_CSV}'. "
            "Download a Kaggle e-commerce CSV and save it there."
        )

    records = ingest_from_kaggle_csv(
        RAW_INPUT_CSV,
        scale=big_data,
        target_rows=records_hint if big_data else 0,
    )
    if not records:
        raise ValueError("No valid records found in the dataset after mapping.")

    os.makedirs(os.path.join(DATA_DIR, "raw"), exist_ok=True)
    save_to_csv(records, RAW_STAGED_CSV)

    _log(f"Ingested {len(records):,} records → {RAW_STAGED_CSV}"
         + (" (Big Data mode — dataset scaled)" if big_data else ""))
    _update_status(total_records=len(records), progress=17)

    context["ti"].xcom_push(key="raw_path",      value=RAW_STAGED_CSV)
    context["ti"].xcom_push(key="total_records",  value=len(records))
    context["ti"].xcom_push(key="big_data_mode",  value=big_data)


def task_validate_data(**context):
    """TASK 2 — Data Validation: schema + type checks."""
    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="ingest_data")
    _update_status(current_task="Data Validation", progress=20)
    _log(f"Schema validation on {raw_path} …")

    with open(raw_path, encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    ok, errors = validate_records(records)
    for e in errors[:5]:
        _log(f"[WARN] {e}")

    _log(f"Validation complete — {len(errors)} warnings, {len(records):,} rows checked")
    _update_status(progress=33)


def task_clean_data(**context):
    """TASK 3 — Data Cleaning: remove duplicates, fill missing values."""
    raw_path = context["ti"].xcom_pull(key="raw_path", task_ids="ingest_data")
    _update_status(current_task="Data Cleaning", progress=36)

    with open(raw_path, encoding="utf-8") as f:
        records = list(csv.DictReader(f))

    _log("Removing duplicates …")
    records, n_dups = remove_duplicates(records)
    _log(f"Removed {n_dups:,} duplicate records")

    records, n_fill = fill_missing_values(records)
    _log(f"Filled {n_fill:,} missing values")

    os.makedirs(os.path.join(DATA_DIR, "processed"), exist_ok=True)
    save_to_csv(records, CLEAN_CSV)
    _log(f"Cleaned dataset saved → {CLEAN_CSV}")

    _update_status(
        removed_duplicates=n_dups,
        missing_filled=n_fill,
        cleaned_records=len(records),
        progress=50,
    )

    context["ti"].xcom_push(key="clean_path",          value=CLEAN_CSV)
    context["ti"].xcom_push(key="removed_duplicates",   value=n_dups)
    context["ti"].xcom_push(key="missing_filled",        value=n_fill)
    context["ti"].xcom_push(key="cleaned_records",       value=len(records))


def task_transform_data(**context):
    """
    TASK 4 — Data Transformation  ★ Issue 1 fix ★
    ─────────────────────────────────────────────
    Uses Apache Spark (real distributed processing) when PySpark is installed.
    Falls back to Python aggregation if Spark is not available.

    Spark advantages demonstrated here:
      • Partitioned DataFrame — data distributed across CPU cores
      • Parallel groupBy + agg across partitions
      • Adaptive Query Execution (AQE) for optimised shuffle
      • Scales transparently to a real Spark cluster (change master URL)
    """
    clean_path = context["ti"].xcom_pull(key="clean_path", task_ids="clean_data")

    if is_spark_available():
        # ── SPARK PATH ────────────────────────────────────────────────────
        _update_status(current_task="Apache Spark: Transform", progress=55)
        _log("🔥 Apache Spark engaged — distributed transformation")
        _log(f"   Input: {clean_path}")
        _log("   Spark master: local[*] (all CPU cores)")

        spark_out = os.path.join(DATA_DIR, "processed", "spark_transformed.csv")
        result    = spark_process(clean_path, spark_out)

        summary = result["summary"]
        parts   = result.get("partitions_used", "?")

        _log(f"Spark complete — {result['cleaned_records']:,} records processed "
             f"across {parts} partitions")
        _log(f"  Categories: {list(summary['category_sales'].keys())[:5]} …")
        _log(f"  Regions:    {list(summary['region_sales'].keys())[:5]} …")
        _log(f"  Months:     {len(summary['monthly_sales'])} buckets")

        _update_status(summary=summary, progress=67)

        # Push Spark output as the new clean path for the load task
        context["ti"].xcom_push(key="clean_path", value=spark_out)
        context["ti"].xcom_push(key="summary",    value=summary)
        context["ti"].xcom_push(key="engine",     value="Apache Spark")

    else:
        # ── PYTHON FALLBACK ───────────────────────────────────────────────
        _update_status(current_task="Data Transformation (Python)", progress=55)
        _log("⚠️  PySpark not installed — using Python aggregation (fallback)")
        _log("    Install PySpark: pip install pyspark")

        with open(clean_path, encoding="utf-8") as f:
            records = list(csv.DictReader(f))

        for r in records:
            try:
                r["sales"]    = float(r.get("sales") or 0)
                r["quantity"] = int(float(r.get("quantity") or 1))
            except (ValueError, TypeError):
                r["sales"]    = 0.0
                r["quantity"] = 1

        summary = {
            "category_sales": aggregate_by_category(records),
            "region_sales":   aggregate_by_region(records),
            "monthly_sales":  aggregate_by_month(records),
        }

        _log(f"Python aggregation: {len(summary['category_sales'])} categories, "
             f"{len(summary['region_sales'])} regions")
        _update_status(summary=summary, progress=67)

        context["ti"].xcom_push(key="clean_path", value=clean_path)
        context["ti"].xcom_push(key="summary",    value=summary)
        context["ti"].xcom_push(key="engine",     value="Python (PySpark not installed)")


def task_load_to_warehouse(**context):
    """
    TASK 5 — Load to Data Warehouse
    ────────────────────────────────
    Bulk insert using psycopg2 copy_expert (fastest possible path — no ORM overhead).
    Falls back gracefully to CSV-only when PostgreSQL is unavailable.

    PostgreSQL = PRIMARY storage (inside Docker: host=postgres-warehouse port=5432).
    CSV        = always written as a portable archive copy.

    Bug fixes applied:
      • pandas import moved inside try block — missing pandas no longer crashes task
      • DB_PORT default corrected to 5432 (inside-container service port)
      • Uses psycopg2 copy_expert for bulk load instead of pandas to_sql (10× faster)
      • retries=0 in DEFAULT_ARGS so Airflow fails fast rather than looping
    """
    import shutil
    import csv as _csv
    import io

    clean_path      = context["ti"].xcom_pull(key="clean_path",          task_ids="transform_data")
    summary         = context["ti"].xcom_pull(key="summary",             task_ids="transform_data")
    n_dups          = context["ti"].xcom_pull(key="removed_duplicates",  task_ids="clean_data")
    n_fill          = context["ti"].xcom_pull(key="missing_filled",      task_ids="clean_data")
    cleaned_records = context["ti"].xcom_pull(key="cleaned_records",     task_ids="clean_data")
    total_records   = context["ti"].xcom_pull(key="total_records",       task_ids="ingest_data")
    engine_name     = context["ti"].xcom_pull(key="engine",              task_ids="transform_data") or "Python"

    _update_status(current_task="Load to Warehouse", progress=70)
    _log(f"Loading {cleaned_records:,} records — engine={engine_name}")

    ts       = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(DATA_DIR, "processed", f"output_{ts}.csv")

    # ── Step 1: Always write CSV output (fast, always succeeds) ──────────────
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    shutil.copy(clean_path, out_path)
    _log(f"📁 CSV output: {out_path}")

    # ── Step 2: PostgreSQL bulk insert via psycopg2 COPY (fastest path) ───────
    storage_used = "csv"

    try:
        import psycopg2

        # Inside Docker the warehouse is reachable via service name on port 5432.
        # On the host (Flask / local_pipeline) it is reachable on localhost:5433.
        # DB_PORT defaults to 5432 here because the DAG always runs inside a container.
        host     = os.getenv("DB_HOST",     "postgres-warehouse")
        port     = os.getenv("DB_PORT",     "5432")   # ← was wrong "5432" hardcoded in old create_engine call but env defaulted to wrong value
        dbname   = os.getenv("DB_NAME",     "pipeline_db")
        user     = os.getenv("DB_USER",     "pipeline")
        password = os.getenv("DB_PASSWORD", "pipeline123")

        _log(f"Connecting to PostgreSQL at {host}:{port}/{dbname} …")

        conn = psycopg2.connect(
            host=host, port=int(port), dbname=dbname,
            user=user, password=password,
            connect_timeout=15,
        )
        conn.autocommit = False

        with conn.cursor() as cur:
            # Create table if not present
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ecommerce_orders (
                    id               SERIAL PRIMARY KEY,
                    order_id         VARCHAR(200),
                    category         VARCHAR(100),
                    sales            NUMERIC(14, 2),
                    quantity         INTEGER,
                    region           VARCHAR(100),
                    date             DATE,
                    pipeline_run_ts  TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("TRUNCATE TABLE ecommerce_orders RESTART IDENTITY;")

            # Build an in-memory CSV buffer for COPY — avoids any ORM overhead
            buf = io.StringIO()
            with open(clean_path, encoding="utf-8") as src:
                reader = _csv.DictReader(src)
                for row in reader:
                    try:
                        order_id = str(row.get("order_id", "") or "")[:200]
                        category = str(row.get("category", "Other") or "Other")[:100]
                        sales    = float(row.get("sales") or 0)
                        quantity = int(float(row.get("quantity") or 1))
                        region   = str(row.get("region", "Unknown") or "Unknown")[:100]
                        date_val = (row.get("date") or "")[:10] or "\\N"
                        buf.write(f"{order_id}\t{category}\t{sales}\t{quantity}\t{region}\t{date_val}\n")
                    except (ValueError, TypeError):
                        continue

            buf.seek(0)
            cur.copy_expert(
                "COPY ecommerce_orders(order_id,category,sales,quantity,region,date) "
                "FROM STDIN WITH (FORMAT TEXT, NULL '\\N')",
                buf,
            )
            rows_inserted = cur.rowcount

        conn.commit()
        conn.close()

        storage_used = "postgresql"
        _log(f"✅ PostgreSQL (PRIMARY): {rows_inserted:,} rows → ecommerce_orders")
        _log(f"   Host: {host}  DB: {dbname}")

    except Exception as db_err:
        _log(f"ℹ️  PostgreSQL skipped: {db_err}")
        _log("   CSV is the storage layer for this run.")

    _log(f"Storage layer: {storage_used.upper()}")

    # ── Step 3: Write results.json ─────────────────────────────────────────────
    results = {
        "total_records":      total_records,
        "cleaned_records":    cleaned_records,
        "removed_duplicates": n_dups,
        "missing_filled":     n_fill,
        "output_file":        out_path,
        "summary":            summary,
        "storage":            storage_used,
        "engine":             engine_name,
    }
    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)
    _log(f"Results saved → {RESULTS_FILE}")

    _update_status(
        output_file=out_path,
        progress=83,
        **{k: results[k] for k in [
            "total_records", "cleaned_records", "removed_duplicates",
            "missing_filled", "summary"
            # "output_file" removed — already passed explicitly above
        ]},
    )
    context["ti"].xcom_push(key="output_file", value=out_path)
    context["ti"].xcom_push(key="storage",     value=storage_used)


def task_generate_report(**context):
    """TASK 6 — Generate Report: log ETL summary stats."""
    _update_status(current_task="Generate Report", progress=90)
    _log("Generating analytics report …")

    results = {}
    try:
        with open(RESULTS_FILE) as f:
            results = json.load(f)
    except Exception:
        pass

    summary = results.get("summary", {})
    cat     = summary.get("category_sales", {})
    reg     = summary.get("region_sales",   {})
    months  = summary.get("monthly_sales",  {})
    engine  = results.get("engine",  "unknown")
    storage = results.get("storage", "csv")

    if cat:
        top_cat = max(cat, key=cat.get)
        _log(f"Top category by revenue: {top_cat} (£{cat[top_cat]:,.2f})")
    if reg:
        top_reg = max(reg, key=reg.get)
        _log(f"Top region by revenue: {top_reg} (£{reg[top_reg]:,.2f})")
    if months:
        peak = max(months, key=months.get)
        _log(f"Peak sales month: {peak} (£{months[peak]:,.2f})")

    _log(f"Total records processed : {results.get('total_records', 0):,}")
    _log(f"Cleaned records         : {results.get('cleaned_records', 0):,}")
    _log(f"Duplicates removed      : {results.get('removed_duplicates', 0):,}")
    _log(f"Missing values filled   : {results.get('missing_filled', 0):,}")
    _log(f"Processing engine       : {engine}")
    _log(f"Primary storage         : {storage.upper()}")
    _log("ETL report generated — proceeding to ML analysis …")

    _update_status(current_task="Generate Report", progress=92)


def task_ml_analysis(**context):
    """
    TASK 7 — ML Analysis
    ─────────────────────
    Runs the full ml_analyzer suite on the pipeline results:
      • Anomaly detection  (Z-score + IQR on monthly sales)
      • Revenue forecast   (linear trend + seasonal index, 3 months ahead)
      • Category insights  (HHI concentration, top/bottom movers)
      • Seasonal analysis  (per-calendar-month seasonality index)
      • Data quality score (completeness, uniqueness, validity, timeliness)

    Writes output to data/ml_results.json — the same file the Flask
    /api/ml endpoint serves to the dashboard ML Insights section.
    This ensures the dashboard ML section is always populated after
    every Airflow run without any manual user action.
    """
    _update_status(current_task="ML Analysis", progress=94)
    _log("🤖 Starting ML analysis (anomaly detection + forecast + insights) …")

    # ── Load pipeline results written by task_load_to_warehouse ──────────────
    results = {}
    try:
        with open(RESULTS_FILE) as f:
            results = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Cannot read results.json for ML input: {e}")

    if not results.get("summary", {}).get("monthly_sales"):
        raise RuntimeError(
            "No monthly_sales data in results.json — "
            "ensure transform_data and load_to_warehouse completed successfully."
        )

    # ── Run the full analysis via the existing ml_analyzer module ─────────────
    from utils.ml_analyzer import run_full_analysis

    _log("   Running anomaly detection (Z-score + IQR) …")
    _log("   Running revenue forecast (linear trend + seasonal adjustment) …")
    _log("   Computing category insights (HHI concentration index) …")
    _log("   Computing seasonal analysis (calendar-month index) …")
    _log("   Computing data quality score …")

    ml_output = run_full_analysis(results)

    # ── Persist to ml_results.json ────────────────────────────────────────────
    ml_file = os.path.join(DATA_DIR, "ml_results.json")
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ml_file, "w") as f:
        json.dump(ml_output, f, indent=2)

    # ── Log key findings ──────────────────────────────────────────────────────
    anomalies = ml_output.get("anomalies", [])
    forecast  = ml_output.get("forecast",  [])
    quality   = ml_output.get("data_quality", {})
    seasonal  = ml_output.get("seasonal_analysis", {})
    cat_ins   = ml_output.get("category_insights", {})

    _log(f"Anomalies detected      : {len(anomalies)}"
         + (f" — {anomalies[0]['month']} ({anomalies[0]['direction']})" if anomalies else " (none)"))

    if forecast:
        _log(f"Revenue forecast        : {forecast[0]['month']} → £{forecast[0]['predicted']:,.2f} "
             f"({forecast[0]['trend']} trend)")

    if quality:
        _log(f"Data quality score      : {quality.get('overall_score', 0)}% "
             f"(grade {quality.get('grade', '?')} — {quality.get('interpretation', '')})")

    if seasonal.get("peak_season"):
        _log(f"Peak season             : {seasonal['peak_season']} "
             f"(index {seasonal.get('peak_index', 1):.2f}x average)")

    if cat_ins.get("top_category"):
        _log(f"Top category            : {cat_ins['top_category']} "
             f"(£{cat_ins.get('top_revenue', 0):,.2f} — "
             f"market concentration: {cat_ins.get('concentration', '')})")

    _log(f"ML results saved → {ml_file}")
    _log("ML analysis completed ✅")

    # ── Mark entire pipeline as complete ─────────────────────────────────────
    _update_status(
        status="success",
        current_task="Completed",
        progress=100,
        end_time=datetime.datetime.now().isoformat(),
    )


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id=           "ecommerce_big_data_pipeline",
    default_args=     DEFAULT_ARGS,
    description=      "Python-Based Automated Data Pipeline — Apache Airflow + Spark + PostgreSQL + ML",
    schedule_interval=None,   # ← NO automatic scheduling — only triggers manually from dashboard
    start_date=       days_ago(1),
    catchup=          False,
    tags=             ["big-data", "spark", "e-commerce", "postgresql", "etl", "ml"],
) as dag:

    t1 = PythonOperator(task_id="ingest_data",       python_callable=task_ingest_data,       provide_context=True)
    t2 = PythonOperator(task_id="validate_data",     python_callable=task_validate_data,     provide_context=True)
    t3 = PythonOperator(task_id="clean_data",        python_callable=task_clean_data,        provide_context=True)
    t4 = PythonOperator(task_id="transform_data",    python_callable=task_transform_data,    provide_context=True)
    t5 = PythonOperator(task_id="load_to_warehouse", python_callable=task_load_to_warehouse, provide_context=True)
    t6 = PythonOperator(task_id="generate_report",   python_callable=task_generate_report,   provide_context=True)
    t7 = PythonOperator(task_id="ml_analysis",       python_callable=task_ml_analysis,       provide_context=True)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
