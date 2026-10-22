"""
utils/data_ingestion.py
────────────────────────
Ingestion helpers — reads from REAL files/sources.
No random data generation; all data comes from the actual dataset.
"""

import csv
import json
import os
from typing import List, Dict, Any

from utils.dataset_mapper import map_dataset, scale_to_big_data


# ── Public ingestion functions ────────────────────────────────────────────────

def ingest_from_kaggle_csv(path: str, scale: bool = False, target_rows: int = 100_000) -> List[Dict[str, Any]]:
    """
    Load and map a Kaggle e-commerce CSV to the standard pipeline schema.
    Optionally scale to simulate big data.
    """
    records = map_dataset(path)
    if scale:
        records = scale_to_big_data(records, target_rows)
    return records


def ingest_from_csv(path: str) -> List[Dict[str, Any]]:
    """
    Load a CSV that is ALREADY in pipeline schema format
    (order_id, category, sales, quantity, region, date).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def ingest_from_json(path: str) -> List[Dict[str, Any]]:
    """Load records from a JSON list file."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON must contain a top-level list")
    return data


def save_to_csv(records: List[Dict[str, Any]], path: str) -> None:
    """Write a list of dicts to CSV."""
    if not records:
        raise ValueError("No records to save")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=records[0].keys())
        w.writeheader()
        w.writerows(records)
    print(f"[ingest] Saved {len(records)} records → {path}")


def get_dataset_info(path: str) -> Dict[str, Any]:
    """Return basic metadata about a CSV dataset."""
    if not os.path.exists(path):
        return {"exists": False, "path": path}
    size_mb = os.path.getsize(path) / (1024 * 1024)
    with open(path, encoding="utf-8", errors="replace") as f:
        row_count = sum(1 for _ in f) - 1   # subtract header
    return {
        "exists":    True,
        "path":      path,
        "rows":      row_count,
        "size_mb":   round(size_mb, 2),
    }
