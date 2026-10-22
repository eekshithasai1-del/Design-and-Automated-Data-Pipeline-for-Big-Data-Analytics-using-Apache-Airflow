"""
utils/data_transformation.py
──────────────────────────────
Cleaning, validation, and aggregation helpers for the ETL pipeline.
All functions are pure — no side effects, no random data generation.
"""

from typing import List, Dict, Any, Tuple
import datetime


# ── Validation ────────────────────────────────────────────────────────────────

REQUIRED_FIELDS = {"order_id", "category", "quantity", "date"}

def validate_records(records: List[Dict[str, Any]]) -> Tuple[bool, List[str]]:
    """
    Schema + type validation.
    Returns (passed: bool, errors: List[str]).
    Reports only the first 10 errors to keep logs readable.
    """
    errors = []
    for i, r in enumerate(records):
        missing = REQUIRED_FIELDS - set(r.keys())
        if missing:
            errors.append(f"Row {i}: missing fields {missing}")
        if errors and len(errors) >= 10:
            errors.append("… (further errors omitted)")
            break
    return len(errors) == 0, errors


# ── Cleaning ──────────────────────────────────────────────────────────────────

def remove_duplicates(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """Remove rows with duplicate order_id. Returns (cleaned, n_removed)."""
    seen, cleaned = set(), []
    for r in records:
        key = r.get("order_id", "")
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(r)
    return cleaned, len(records) - len(cleaned)


def fill_missing_values(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Replace null / empty values with safe defaults.
    Returns (records, n_filled).
    """
    filled = 0
    for r in records:
        # Sales: replace nulls / zero with median-ish default
        if not r.get("sales") or float(r.get("sales") or 0) <= 0:
            r["sales"] = 99.99    # conservative default for missing revenue
            filled += 1
        # Region
        if not r.get("region") or str(r.get("region")).strip() in ("", "nan", "None"):
            r["region"] = "Unknown"
            filled += 1
        # Quantity
        if not r.get("quantity") or int(float(r.get("quantity") or 0)) <= 0:
            r["quantity"] = 1
            filled += 1
        # Category
        if not r.get("category") or str(r.get("category")).strip() in ("", "nan", "None"):
            r["category"] = "Other"
            filled += 1
        # Date — default to today if missing
        if not r.get("date") or str(r.get("date")).strip() in ("", "nan", "None"):
            r["date"] = datetime.date.today().isoformat()
            filled += 1
    return records, filled


# ── Aggregations ──────────────────────────────────────────────────────────────

def aggregate_by_category(records: List[Dict[str, Any]]) -> Dict[str, float]:
    """Sum sales per product category."""
    result: Dict[str, float] = {}
    for r in records:
        cat = str(r.get("category") or "Other").strip()
        result[cat] = round(result.get(cat, 0.0) + float(r.get("sales") or 0), 2)
    # Sort descending by revenue
    return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))


def aggregate_by_region(records: List[Dict[str, Any]]) -> Dict[str, float]:
    """Sum sales per region."""
    result: Dict[str, float] = {}
    for r in records:
        reg = str(r.get("region") or "Unknown").strip()
        result[reg] = round(result.get(reg, 0.0) + float(r.get("sales") or 0), 2)
    return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))


def aggregate_by_month(records: List[Dict[str, Any]]) -> Dict[str, float]:
    """Sum sales per month (YYYY-MM), chronologically sorted."""
    result: Dict[str, float] = {}
    for r in records:
        date_str = str(r.get("date") or "")
        month = date_str[:7]
        if month and len(month) == 7:
            result[month] = round(result.get(month, 0.0) + float(r.get("sales") or 0), 2)
    return dict(sorted(result.items()))


# ── Full pipeline chain ───────────────────────────────────────────────────────

def run_all_transformations(records: List[Dict[str, Any]]):
    """
    Execute the full transformation chain.
    Returns (cleaned_records, n_dups, n_filled, summary_dict).
    """
    ok, errs = validate_records(records)
    if not ok:
        print(f"[transform] Validation warnings ({len(errs)}): {errs[:3]}")

    records, n_dups  = remove_duplicates(records)
    records, n_fill  = fill_missing_values(records)

    summary = {
        "category_sales": aggregate_by_category(records),
        "region_sales":   aggregate_by_region(records),
        "monthly_sales":  aggregate_by_month(records),
    }
    return records, n_dups, n_fill, summary
