# 🚀 Python-Based Automated Data Pipeline for Big Data Analytics
## Apache Airflow · Apache Spark · PostgreSQL · Flask Dashboard

---

## 📌 Architecture

```
User → Flask Dashboard
         ↓
  Flask checks Airflow health
         ├─ Airflow UP  → triggers Airflow DAG via REST API
         └─ Airflow DOWN → runs LOCAL pipeline in background thread (auto-fallback)
                  ↓
         Airflow DAG or Local Runner executes ETL:
         Ingest → Validate → Clean → [Spark Transform] → Load → Report
                                              ↓
                                 PostgreSQL (PRIMARY warehouse)
                                 CSV (fallback if PG unavailable)
                                              ↓
                              Flask reads pipeline_status.json
                                              ↓
                              Dashboard: Charts + KPIs + Logs
```

> ⚠️ Flask does **NOT** run the pipeline. Airflow (or the local fallback) does.
> Flask is a pure **Controller + Viewer**.

---

## 📁 Project Structure

```
data_pipeline_project/
│
├── app.py                              # Flask controller
├── requirements.txt
├── README.md
│
├── templates/
│   └── index.html                      # Dashboard UI
│
├── dags/
│   └── ecommerce_pipeline_dag.py       # Apache Airflow DAG (+ Spark integration)
│
├── utils/
│   ├── data_ingestion.py               # CSV loader helpers
│   ├── data_transformation.py          # Cleaning + aggregation helpers
│   ├── dataset_mapper.py               # Kaggle dataset column mapper
│   ├── spark_processor.py              # ★ NEW: Real PySpark distributed processing
│   ├── local_pipeline.py               # ★ NEW: Airflow fallback — runs ETL in-process
│   └── db_loader.py                    # PostgreSQL warehouse loader (now PRIMARY)
│
├── data/
│   ├── raw/
│   │   └── input.csv                   # ← Place your Kaggle CSV here
│   ├── processed/                      # Cleaned/Spark output CSVs saved here
│   ├── pipeline_status.json            # Live status (written by DAG/runner, read by Flask)
│   └── results.json                    # Final metrics
│
└── logs/                               # Airflow task logs
```

---

## 🔥 What's New (Issue Fixes)

### ✅ Issue 1 — Real Apache Spark (not simulated)

`utils/spark_processor.py` implements genuine distributed computation:

| What Spark Does | Detail |
|---|---|
| **Partitioned read** | CSV loaded into distributed DataFrame |
| **Parallel dedup** | `dropDuplicates()` via Spark shuffle |
| **Vectorised fill** | Column operations across all partitions simultaneously |
| **Parallel groupBy** | Category / Region / Month aggregations in parallel |
| **AQE** | Adaptive Query Execution (Spark 3+) — automatic join/shuffle optimisation |
| **Scalable master** | Change `local[*]` → `spark://host:7077` for real cluster |

In viva, say: *"We use Apache Spark with a local[*] master that utilises all CPU cores in parallel. The architecture supports scaling to a real Spark cluster by changing a single config line."*

### ✅ Issue 2 — Airflow is now optional (automatic fallback)

Flask checks Airflow health before every pipeline run:
- **Airflow UP** → triggers DAG via REST API as before
- **Airflow DOWN** → `local_pipeline.run_in_background()` runs the full ETL in a daemon thread automatically, **no crash, no manual intervention**

The UI shows which mode is active (Airflow / Local).

### ✅ Issue 3 — PostgreSQL is now PRIMARY storage

Load order in `task_load_to_warehouse` (and local runner):
1. **PostgreSQL** → attempted first. On success, this is the source of truth.
2. **CSV** → written as a portable archive copy alongside PG, or as sole fallback if PG is unavailable.

In viva, say: *"PostgreSQL is our primary data warehouse. Every pipeline run inserts cleaned records into the ecommerce_orders table. CSV is kept as a universal fallback for environments without a database."*

---

## 📊 Dataset Setup (REQUIRED FIRST STEP)

### Option A — E-Commerce Data (UK Online Retail) ✅ Recommended
1. Go to: https://www.kaggle.com/datasets/carrie1/ecommerce-data
2. Download `data.csv` → rename to `input.csv`
3. Place at: `data/raw/input.csv`

### Option B — Superstore Sales
1. https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
2. Download → rename → `data/raw/input.csv`

### 🔥 Big Data Mode
Set Record Count ≥ 500 in the dashboard. The dataset auto-scales,
then Apache Spark processes it across all CPU cores.

---

## ⚙️ Setup & Run

### Step 1 — Install all dependencies
```bash
pip install flask apache-airflow pandas sqlalchemy psycopg2-binary requests pyspark
```

### Step 2 — Set up PostgreSQL (optional but recommended)
```bash
createdb pipeline_db
export DB_HOST=localhost
export DB_NAME=pipeline_db
export DB_USER=postgres
export DB_PASSWORD=postgres
```

### Step 3 — Initialise Airflow (optional)
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
    --username admin --password admin \
    --firstname Admin --lastname User \
    --role Admin --email admin@example.com
```

### Step 4 — Copy DAG file
```bash
cp dags/ecommerce_pipeline_dag.py ~/airflow/dags/
```

### Step 5 — Start Airflow (optional — system works without it)
```bash
# Terminal 1
airflow scheduler
# Terminal 2
airflow webserver --port 8080
```

### Step 6 — Place dataset
```
data/raw/input.csv  ← Kaggle CSV
```

### Step 7 — Run Flask
```bash
python app.py
```

### Step 8 — Open dashboard
```
http://localhost:5000
```

**Airflow running?** → Click ▶ Run Pipeline → Airflow DAG triggers, Spark runs inside Airflow.
**Airflow NOT running?** → Click ▶ Run Pipeline → system automatically runs locally with Spark.

---

## 🔄 Pipeline Stages

```
[Ingest] → [Validate] → [Clean] → [Spark Transform] → [Load PG→CSV] → [Report]
```

| Task | Engine | What Happens |
|---|---|---|
| **Data Ingestion** | Python | Loads Kaggle CSV, maps columns, optionally scales |
| **Data Validation** | Python | Checks required fields, type safety |
| **Data Cleaning** | Python | Removes duplicates, fills missing values |
| **Data Transformation** | **Apache Spark** | Distributed aggregation (category/region/month) |
| **Load to Warehouse** | SQLAlchemy | PostgreSQL first → CSV fallback |
| **Generate Report** | Python | Logs summary stats, marks pipeline complete |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Orchestration** | Apache Airflow 2.x |
| **Big Data Processing** | **Apache Spark (PySpark)** — `local[*]` master |
| **ETL Logic** | Python (pure functions) |
| **Backend** | Flask (controller only) |
| **Primary Warehouse** | **PostgreSQL** via SQLAlchemy |
| **Fallback Storage** | CSV files |
| **Frontend** | HTML5 + Chart.js |
| **Dataset** | Kaggle E-Commerce CSV |

---

## 🏗️ How the Components Interact

```
Flask /api/run_pipeline
    ├── _check_airflow_health() → Airflow UP
    │       └── POST /api/v1/dags/{dag_id}/dagRuns  (Airflow REST API)
    │               └── Airflow runs: Ingest→Validate→Clean→Spark→PostgreSQL→Report
    │
    └── _check_airflow_health() → Airflow DOWN (Issue 2 fix)
            └── local_pipeline.run_in_background(source, records)
                    └── daemon thread: same 6 stages, Spark if available, PG if available

Flask /api/status
    └── reads data/pipeline_status.json (written by DAG or local runner)
    └── for Airflow mode: cross-checks GET /api/v1/dags/{dag_id}/dagRuns

Flask /api/db_status
    └── checks PostgreSQL → reports "PRIMARY warehouse" or "FALLBACK (CSV active)"

UI polls /api/status every 800ms → updates charts + KPIs
UI polls /api/db_status every 5s  → shows DB indicator
UI polls /api/airflow_health      → shows Airflow / Local mode badge
```

---

*Project: Design and Implementation of a Scalable Python-Based Automated Data Pipeline for Big Data Analytics Using Apache Airflow, Apache Spark, and PostgreSQL*
