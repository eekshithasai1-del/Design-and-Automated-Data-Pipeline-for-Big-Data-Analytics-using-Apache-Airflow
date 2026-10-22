"""
utils/local_pipeline.py
────────────────────────
Standalone ETL runner — executes the full pipeline in-process
when Apache Airflow is not reachable (Issue 2 fix).

Flask calls run_in_background() which spawns a daemon thread,
so /api/run_pipeline returns immediately while the pipeline runs.

The same pipeline_status.json contract is followed, so the UI
polls and updates exactly as it would with Airflow.

Architecture:
    Flask detects Airflow is down
        └── calls local_pipeline.run_in_background(source, records)
                └── daemon thread: full ETL runs in-process
                        ├── Spark (if installed) for clean + transform
                        └── Pure Python fallback if PySpark missing
"""

import csv
import datetime
import json
import logging
import os
import shutil
import threading
from typing import Any, Dict

log = logging.getLogger(__name__)

STATUS_FILE   = "data/pipeline_status.json"
RESULTS_FILE  = "data/results.json"
RAW_INPUT_CSV = "data/raw/input.csv"
STAGED_CSV    = "data/raw/staged_data.csv"
CLEAN_CSV     = "data/processed/cleaned_data.csv"


# ── Internal helpers (mirror the DAG helpers) ─────────────────────────────────

def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def _read_status() -> dict:
    try:
        with open(STATUS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _update_status(**kwargs):
    os.makedirs("data", exist_ok=True)
    state = _read_status()
    state.update(kwargs)
    with open(STATUS_FILE, "w") as f:
        json.dump(state, f, indent=2)


def _log(msg: str):
    """Write to both Python logger and the status JSON."""
    log.info(msg)
    state  = _read_status()
    logs   = state.get("logs", [])
    logs.append(f"[{_ts()}] {msg}")
    _update_status(logs=logs)


# ── Full ETL run (executed inside a daemon thread) ────────────────────────────

def _run(source: str, records_hint: int):
    """
    Execute all 6 pipeline stages in-process.
    Mirrors the Airflow DAG task sequence exactly.
    """
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

    # records=0 means "full dataset, no scaling"  (sent by selScale=0)
    # records>=500 means "scale to that many rows" (Big Data mode)
    big_data_mode = records_hint >= 500

    # ── STAGE 1 — Ingest ─────────────────────────────────────────────────────
    _update_status(current_task="Data Ingestion", progress=5)
    _log(f"[LOCAL MODE] Stage 1: Data Ingestion — source={source}")

    if not os.path.exists(RAW_INPUT_CSV):
        raise FileNotFoundError(
            f"Dataset not found at '{RAW_INPUT_CSV}'. "
            "Download a Kaggle e-commerce CSV and save it there."
        )

    records = ingest_from_kaggle_csv(
        RAW_INPUT_CSV,
        scale=big_data_mode,
        target_rows=records_hint if big_data_mode else 0,
    )
    if not records:
        raise ValueError("No valid records found in dataset.")

    os.makedirs("data/raw",       exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    save_to_csv(records, STAGED_CSV)

    total_records = len(records)
    _log(f"Ingested {total_records:,} records → {STAGED_CSV}"
         + (" (Big Data scaled)" if big_data_mode else ""))
    _update_status(total_records=total_records, progress=17)

    # ── STAGE 2 — Validate ───────────────────────────────────────────────────
    _update_status(current_task="Data Validation", progress=20)
    _log("Stage 2: Schema validation …")
    ok, errors = validate_records(records)
    for e in errors[:5]:
        _log(f"[WARN] {e}")
    _log(f"Validation complete — {len(errors)} warnings")
    _update_status(progress=33)

    # ── STAGES 3 + 4 — Clean + Transform (Spark preferred) ───────────────────
    if is_spark_available():
        # ── SPARK PATH — real distributed processing ──────────────────────
        _update_status(current_task="Spark: Clean + Transform", progress=36)
        _log("🔥 Stage 3+4: Apache Spark — distributed clean & transform")
        _log(f"   Spark master: local[*] (all CPU cores)")

        result = spark_process(STAGED_CSV, CLEAN_CSV)

        n_dups   = result["removed_duplicates"]
        n_fill   = result["missing_filled"]
        cleaned  = result["cleaned_records"]
        summary  = result["summary"]
        parts    = result.get("partitions_used", "?")

        _log(f"Spark completed: {cleaned:,} records, {n_dups:,} dups removed, "
             f"{n_fill:,} defaults applied — {parts} Spark partitions used")

    else:
        # ── PYTHON FALLBACK ───────────────────────────────────────────────
        _update_status(current_task="Data Cleaning", progress=36)
        _log("⚠️  PySpark not installed. Run: pip install pyspark")
        _log("Stage 3: Cleaning with Python (fallback) …")

        records, n_dups = remove_duplicates(records)
        _log(f"Removed {n_dups:,} duplicates")

        records, n_fill = fill_missing_values(records)
        _log(f"Filled {n_fill:,} missing values")

        save_to_csv(records, CLEAN_CSV)
        cleaned = len(records)

        _update_status(current_task="Data Transformation", progress=55)
        _log("Stage 4: Aggregating with Python (fallback) …")
        summary = {
            "category_sales": aggregate_by_category(records),
            "region_sales":   aggregate_by_region(records),
            "monthly_sales":  aggregate_by_month(records),
        }

    _update_status(
        removed_duplicates=n_dups,
        missing_filled=n_fill,
        cleaned_records=cleaned,
        summary=summary,
        progress=67,
    )

    # ── STAGE 5 — Load ───────────────────────────────────────────────────────
    _update_status(current_task="Load to Warehouse", progress=70)
    _log("Stage 5: Loading to data warehouse …")

    ts       = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = f"data/processed/output_{ts}.csv"
    shutil.copy(CLEAN_CSV, out_path)

    # ── PostgreSQL is PRIMARY; CSV is fallback (Issue 3 fix) ─────────────────
    db_loaded = 0
    if is_postgres_available():
        try:
            db_loaded = load_records_to_postgres(CLEAN_CSV)
            _log(f"✅ PostgreSQL (PRIMARY WAREHOUSE): {db_loaded:,} rows loaded into ecommerce_orders")
        except Exception as db_err:
            _log(f"[WARN] PostgreSQL insert failed: {db_err}")
            _log("       Data is still persisted in CSV fallback.")
    else:
        _log("ℹ️  PostgreSQL not reachable — CSV is the storage layer.")
        _log("    Ensure Docker is running: docker-compose up -d postgres-warehouse")

    _log(f"Output CSV: {out_path}")

    results: Dict[str, Any] = {
        "total_records":      total_records,
        "cleaned_records":    cleaned,
        "removed_duplicates": n_dups,
        "missing_filled":     n_fill,
        "output_file":        out_path,
        "summary":            summary,
    }
    with open(RESULTS_FILE, "w") as f:
        json.dump(results, f, indent=2)

    _update_status(
        output_file=out_path,
        progress=83,
        **{k: results[k] for k in [
            "total_records", "cleaned_records",
            "removed_duplicates", "missing_filled",
            "output_file", "summary"
        ]},
    )

    # ── STAGE 6 — Report ─────────────────────────────────────────────────────
    _update_status(current_task="Generate Report", progress=90)
    _log("Stage 6: Generating analytics report …")

    cat    = summary.get("category_sales", {})
    reg    = summary.get("region_sales",   {})
    months = summary.get("monthly_sales",  {})

    if cat:
        top_cat = max(cat, key=cat.get)
        _log(f"Top category: {top_cat} (£{cat[top_cat]:,.2f})")
    if reg:
        top_reg = max(reg, key=reg.get)
        _log(f"Top region: {top_reg} (£{reg[top_reg]:,.2f})")
    if months:
        peak = max(months, key=months.get)
        _log(f"Peak month: {peak} (£{months[peak]:,.2f})")

    _log(f"Total records processed: {total_records:,}")
    _log(f"Duplicates removed: {n_dups:,}")
    _log(f"Missing values filled: {n_fill:,}")
    _log("Pipeline completed successfully ✅ [LOCAL MODE — Airflow bypassed]")

    _update_status(
        status="success",
        current_task="Completed",
        progress=100,
        end_time=datetime.datetime.now().isoformat(),
    )


def _safe_run(source: str, records_hint: int):
    """Wrapper that catches any exception and marks pipeline as failed."""
    try:
        _run(source, records_hint)
    except Exception as exc:
        log.exception("[local_pipeline] Unhandled error")
        _log(f"[ERROR] {exc}")
        _update_status(
            status="failed",
            current_task="Failed",
            end_time=datetime.datetime.now().isoformat(),
        )


# ── Public API ────────────────────────────────────────────────────────────────

def run_in_background(source: str, records_hint: int) -> threading.Thread:
    """
    Spawn a daemon thread that runs the full ETL pipeline.
    Returns immediately — Flask does not block.
    """
    t = threading.Thread(
        target=_safe_run,
        args=(source, records_hint),
        name="local-pipeline",
        daemon=True,
    )
    t.start()
    log.info(f"[local_pipeline] Background thread started (tid={t.ident})")
    return t
