"""
utils/dataset_mapper.py
────────────────────────
Maps columns from popular Kaggle e-commerce datasets
to the standard pipeline schema:
    order_id | category | sales | quantity | region | date
"""

import csv
import hashlib
import os
from typing import List, Dict, Any


# ── Column-map profiles ───────────────────────────────────────────────────────

# Profile: UK Online Retail / E-Commerce Data (most popular Kaggle dataset)
# https://www.kaggle.com/datasets/carrie1/ecommerce-data
ONLINE_RETAIL_MAP = {
    "order_id":  "InvoiceNo",
    "quantity":  "Quantity",
    "date":      "InvoiceDate",
    "region":    "Country",
    # sales = UnitPrice * Quantity  (computed below)
    # category derived from Description
}

# Profile: Superstore Sales
# https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
SUPERSTORE_MAP = {
    "order_id":  "Order ID",
    "category":  "Category",
    "sales":     "Sales",
    "quantity":  "Quantity",
    "region":    "Region",
    "date":      "Order Date",
}

# Profile: Brazilian E-Commerce (Olist)
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
OLIST_MAP = {
    "order_id":  "order_id",
    "category":  "product_category_name_english",
    "sales":     "price",
    "quantity":  "order_item_id",   # proxy
    "region":    "customer_state",
    "date":      "order_purchase_timestamp",
}

# ── Auto-detect helpers ───────────────────────────────────────────────────────

def detect_profile(headers: List[str]) -> str:
    """Return the best-matching profile name based on CSV headers."""
    h = {c.strip().lower() for c in headers}

    if "invoiceno" in h or "invoice no" in h:
        return "online_retail"
    if "order id" in h or "sub-category" in h:
        return "superstore"
    if "order_purchase_timestamp" in h:
        return "olist"
    return "generic"


# ── Row-level mappers ─────────────────────────────────────────────────────────

def _map_online_retail_row(row: Dict[str, str], idx: int) -> Dict[str, Any] | None:
    """Map one UK Online Retail row → pipeline schema."""
    try:
        qty   = float(row.get("Quantity", 0) or 0)
        price = float(row.get("UnitPrice", 0) or 0)
        if qty <= 0 or price <= 0:   # skip returns / invalid
            return None

        # Derive a category from the Description (first significant word)
        desc  = (row.get("Description") or "").strip().title()
        cat   = _coarse_category(desc)

        # Convert InvoiceDate "DD-MM-YYYY HH:MM" → "YYYY-MM-DD"
        raw_date = (row.get("InvoiceDate") or "").strip()
        date     = ""
        if raw_date:
            try:
                from datetime import datetime as _dt
                date = _dt.strptime(raw_date[:16], "%d-%m-%Y %H:%M").strftime("%Y-%m-%d")
            except ValueError:
                try:
                    date = _dt.strptime(raw_date[:16], "%m/%d/%Y %H:%M").strftime("%Y-%m-%d")
                except ValueError:
                    date = raw_date[:10]

        return {
            "order_id": f"{row.get('InvoiceNo','ORD').strip()}_{idx}",
            "category": cat,
            "sales":    round(qty * price, 2),
            "quantity": int(qty),
            "region":   (row.get("Country") or "Unknown").strip().title(),
            "date":     date,
        }
    except (ValueError, TypeError):
        return None


def _map_superstore_row(row: Dict[str, str]) -> Dict[str, Any] | None:
    """Map one Superstore row → pipeline schema."""
    try:
        sales = float((row.get("Sales") or "0").replace(",", ""))
        qty   = int(float(row.get("Quantity") or 1))
        raw_date = (row.get("Order Date") or "").strip()
        # Normalise MM/DD/YYYY → YYYY-MM-DD
        if "/" in raw_date:
            parts = raw_date.split("/")
            if len(parts) == 3:
                raw_date = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        return {
            "order_id": (row.get("Order ID") or f"ORD{hashlib.md5(str(row).encode()).hexdigest()[:6]}").strip(),
            "category": (row.get("Category") or "Other").strip(),
            "sales":    round(sales, 2),
            "quantity": qty,
            "region":   (row.get("Region") or "Unknown").strip(),
            "date":     raw_date[:10],
        }
    except (ValueError, TypeError):
        return None


def _map_olist_row(row: Dict[str, str]) -> Dict[str, Any] | None:
    """Map one Olist row → pipeline schema."""
    try:
        price = float(row.get("price") or 0)
        return {
            "order_id": (row.get("order_id") or "").strip(),
            "category": (row.get("product_category_name_english") or "Other").strip().replace("_", " ").title(),
            "sales":    round(price, 2),
            "quantity": int(float(row.get("order_item_id") or 1)),
            "region":   (row.get("customer_state") or "Unknown").strip().upper(),
            "date":     (row.get("order_purchase_timestamp") or "")[:10],
        }
    except (ValueError, TypeError):
        return None


def _map_generic_row(row: Dict[str, str], idx: int) -> Dict[str, Any] | None:
    """
    Best-effort generic mapping.
    Tries common synonyms for each required field.
    """
    def pick(keys, default=""):
        for k in keys:
            if k in row and row[k]:
                return row[k]
        return default

    try:
        sales_raw = pick(["sales","Sales","revenue","Revenue","total","Total","amount","Amount","price","Price"], "0")
        sales     = float(str(sales_raw).replace(",", "") or 0)
        qty_raw   = pick(["quantity","Quantity","qty","Qty","units","Units"], "1")
        qty       = int(float(qty_raw or 1))
        date_raw  = pick(["date","Date","order_date","Order Date","InvoiceDate","timestamp"], "")
        date      = date_raw[:10].replace("/", "-") if date_raw else ""

        return {
            "order_id": pick(["order_id","Order ID","InvoiceNo","id","ID"], f"ORD{idx}"),
            "category": pick(["category","Category","Description","type","Type","department"], "Other"),
            "sales":    round(sales, 2),
            "quantity": qty,
            "region":   pick(["region","Region","Country","country","state","State","area"], "Unknown"),
            "date":     date,
        }
    except (ValueError, TypeError):
        return None


# ── Coarse category classifier (for online retail descriptions) ───────────────

_CATEGORY_KEYWORDS = {
    "Electronics":  ["lamp","light","clock","camera","radio","wire","battery","usb","led","bulb","charger"],
    "Home & Garden":["jar","vase","pot","cushion","candle","frame","basket","bowl","tray","mat","rug","towel","mug","cup","plate","tin"],
    "Clothing":     ["bag","purse","wallet","shirt","dress","scarf","hat","glove","sock","shoe","belt","jewelry","ring","necklace"],
    "Books":        ["book","card","notepad","diary","pen","pencil","calendar","sticker","label","tag","paper"],
    "Food":         ["cake","chocolate","sweet","candy","tea","coffee","jam","biscuit","kitchen","spice","herb"],
    "Toys & Gifts": ["toy","doll","game","puzzle","balloon","decoration","gift","bunting","fairy","xmas","christmas","halloween","easter"],
    "Sports":       ["sport","gym","yoga","fitness","cycle","bike","ball","outdoor","garden","tool"],
}

def _coarse_category(description: str) -> str:
    desc_lower = description.lower()
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in desc_lower for kw in keywords):
            return cat
    return "General"


# ── Public API ────────────────────────────────────────────────────────────────

def map_dataset(csv_path: str) -> List[Dict[str, Any]]:
    """
    Load any supported Kaggle e-commerce CSV and return
    a list of standardised pipeline records.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Dataset not found: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        reader  = csv.DictReader(f)
        headers = reader.fieldnames or []
        profile = detect_profile(headers)
        print(f"[mapper] Detected dataset profile: {profile}")

        records = []
        for i, row in enumerate(reader):
            if profile == "online_retail":
                mapped = _map_online_retail_row(row, i)
            elif profile == "superstore":
                mapped = _map_superstore_row(row)
            elif profile == "olist":
                mapped = _map_olist_row(row)
            else:
                mapped = _map_generic_row(row, i)

            if mapped and mapped.get("sales", 0) > 0:
                records.append(mapped)

    print(f"[mapper] Mapped {len(records)} valid records from {os.path.basename(csv_path)}")
    return records


def scale_to_big_data(records: List[Dict[str, Any]], target_rows: int = 100_000) -> List[Dict[str, Any]]:
    """
    Multiply records until we reach ~target_rows.
    Each copy gets a unique order_id suffix so duplicates are intentional-free.
    This simulates Big Data conditions for analytics demonstration.
    """
    if len(records) == 0:
        return records

    multiplier = max(1, (target_rows + len(records) - 1) // len(records))
    if multiplier == 1:
        return records

    big = []
    for copy_idx in range(multiplier):
        for r in records:
            new_r             = dict(r)
            new_r["order_id"] = f"{r['order_id']}_x{copy_idx}"
            big.append(new_r)
            if len(big) >= target_rows:
                break
        if len(big) >= target_rows:
            break

    print(f"[mapper] Scaled {len(records)} → {len(big)} rows (Big Data mode)")
    return big
