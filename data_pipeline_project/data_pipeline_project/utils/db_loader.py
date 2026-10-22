"""
utils/db_loader.py
───────────────────
PostgreSQL data warehouse loader.

Loads cleaned pipeline records into a PostgreSQL table using PURE psycopg2
(no SQLAlchemy). This removes the SQLAlchemy version conflict entirely:
  - Airflow 2.8.x internally requires sqlalchemy==1.4.49
  - Flask / local_pipeline on the host might have a different version
  - By using psycopg2 directly we are independent of SQLAlchemy entirely

psycopg2 copy_expert (PostgreSQL COPY protocol) is also the fastest possible
bulk-insert method — orders of magnitude faster than SQLAlchemy executemany.

Environment variables (all optional — defaults shown):
    DB_HOST       localhost
    DB_PORT       5433        ← host-exposed port (docker-compose maps 5433→5432)
    DB_NAME       pipeline_db
    DB_USER       pipeline
    DB_PASSWORD   pipeline123

Inside Docker containers the DAG uses:
    DB_HOST       postgres-warehouse   (service name)
    DB_PORT       5432                 (internal container port)
These are set in docker-compose.yml under the airflow-common environment block.
"""

import csv
import io
import os
from typing import Any, Dict, List


# ── Connection helper ─────────────────────────────────────────────────────────

def _connect():
    """
    Return an open psycopg2 connection.
    Reads DB_* env vars so it works both inside Docker (service name / port 5432)
    and on the host (localhost / port 5433).
    """
    import psycopg2  # imported here so missing psycopg2 raises a clean ImportError

    host     = os.getenv("DB_HOST",     "localhost")
    port     = int(os.getenv("DB_PORT", "5433"))
    dbname   = os.getenv("DB_NAME",     "pipeline_db")
    user     = os.getenv("DB_USER",     "pipeline")
    password = os.getenv("DB_PASSWORD", "pipeline123")

    return psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=user, password=password,
        connect_timeout=10,
    )


# ── DDL ───────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
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
"""


# ── Public API ────────────────────────────────────────────────────────────────

def is_postgres_available() -> bool:
    """
    Return True if psycopg2 is installed and the warehouse DB is reachable.
    Uses a 5-second connect timeout so the pipeline does not hang on startup.
    """
    try:
        import psycopg2

        host     = os.getenv("DB_HOST",     "localhost")
        port     = int(os.getenv("DB_PORT", "5432"))
        dbname   = os.getenv("DB_NAME",     "pipeline_db")
        user     = os.getenv("DB_USER",     "postgres")
        password = os.getenv("DB_PASSWORD", "0601")

        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname,
            user=user, password=password,
            connect_timeout=5,
        )
        conn.close()
        return True
    except Exception:
        return False


def load_records_to_postgres(csv_path: str) -> int:
    """
    Read a cleaned CSV and bulk-load all rows into ecommerce_orders via
    psycopg2 copy_expert (PostgreSQL COPY protocol — no ORM, no row-by-row).

    Creates the table if it does not exist.
    Truncates before insert so repeated runs do not accumulate duplicates.

    Returns the number of rows inserted.
    Raises on any error (caller decides whether to fall back to CSV).
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # Build an in-memory tab-delimited buffer for COPY
    buf    = io.StringIO()
    n_rows = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                order_id = str(row.get("order_id", "") or "")[:200].replace("\t", " ")
                category = str(row.get("category", "Other") or "Other")[:100].replace("\t", " ")
                sales    = float(row.get("sales") or 0)
                quantity = int(float(row.get("quantity") or 1))
                region   = str(row.get("region", "Unknown") or "Unknown")[:100].replace("\t", " ")
                date_val = (row.get("date") or "")[:10] or "\\N"
                buf.write(f"{order_id}\t{category}\t{sales}\t{quantity}\t{region}\t{date_val}\n")
                n_rows += 1
            except (ValueError, TypeError):
                continue   # skip malformed rows silently

    if n_rows == 0:
        raise ValueError("CSV is empty or all rows were malformed — nothing to load.")

    buf.seek(0)

    conn = _connect()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
            cur.execute("TRUNCATE TABLE ecommerce_orders RESTART IDENTITY;")
            cur.copy_expert(
                "COPY ecommerce_orders(order_id, category, sales, quantity, region, date) "
                "FROM STDIN WITH (FORMAT TEXT, NULL '\\N')",
                buf,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"[db_loader] Inserted {n_rows:,} rows into ecommerce_orders")
    return n_rows


def get_warehouse_summary() -> Dict[str, Any]:
    """
    Return quick stats from the warehouse table.
    Used by Flask /api/db_status endpoint and the dashboard.
    """
    try:
        conn = _connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                  AS total_rows,
                    COALESCE(SUM(sales), 0)   AS total_sales,
                    COUNT(DISTINCT category)  AS categories,
                    COUNT(DISTINCT region)    AS regions,
                    MIN(date)                 AS earliest_date,
                    MAX(date)                 AS latest_date
                FROM ecommerce_orders
            """)
            cols = [d[0] for d in cur.description]
            row  = cur.fetchone()
        conn.close()
        return dict(zip(cols, row)) if row else {}
    except Exception as e:
        return {"error": str(e)}
