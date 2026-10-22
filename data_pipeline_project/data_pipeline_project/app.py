"""
app.py  —  Flask Controller
────────────────────────────────────────────────────────────────
Running `python app.py`:
  1. Starts Apache Airflow Scheduler + Webserver as child processes
  2. Starts hourly APScheduler to auto-trigger the pipeline
  3. On Ctrl+C → kills both Airflow processes, then exits

Pipeline execution:
  PRIMARY  — Apache Airflow (triggered via REST API when Airflow is healthy)
  FALLBACK — Local in-process execution (only when Airflow is genuinely unreachable)

No pipeline run is triggered automatically on boot.
The user clicks "Run Pipeline" in the dashboard to start a run.
"""

import atexit
import os
import signal
import subprocess
import sys
import threading
import time

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, render_template, jsonify, request, send_file, make_response
import json
import datetime
import requests
from requests.auth import HTTPBasicAuth

app = Flask(__name__)

# ── File paths ────────────────────────────────────────────────────────────────
_DATA_DIR    = os.getenv("PIPELINE_DATA_DIR", "data")
STATUS_FILE  = os.path.join(_DATA_DIR, "pipeline_status.json")
RESULTS_FILE = os.path.join(_DATA_DIR, "results.json")
ML_FILE      = os.path.join(_DATA_DIR, "ml_results.json")
REPORT_DIR   = os.path.join(_DATA_DIR, "reports")

DAG_ID = "ecommerce_big_data_pipeline"

# ── Airflow config ────────────────────────────────────────────────────────────
AIRFLOW_HOME     = os.getenv("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")
AIRFLOW_AUTH     = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
AIRFLOW_HEADERS  = {"Content-Type": "application/json", "Accept": "application/json"}

DAG_TASKS = [
    {"id": "ingest",    "name": "Data Ingestion",              "duration": "~3s"},
    {"id": "validate",  "name": "Data Validation",             "duration": "~2s"},
    {"id": "clean",     "name": "Data Cleaning (Spark)",       "duration": "~4s"},
    {"id": "transform", "name": "Data Transformation (Spark)", "duration": "~3s"},
    {"id": "load",      "name": "Warehouse Load (PG / CSV)",   "duration": "~2s"},
    {"id": "report",    "name": "Generate Report",             "duration": "~1s"},
    {"id": "ml",        "name": "ML Analysis",                 "duration": "~3s"},
]

# ── Airflow process handles (owned by this Flask process) ─────────────────────
_airflow_procs: list = []   # list[subprocess.Popen]

# ── Scheduler state ───────────────────────────────────────────────────────────
_scheduler = None
_schedule_state = {
    "enabled": False, "interval_min": 60,
    "last_run": None, "next_run": None, "run_count": 0,
}


# ══════════════════════════════════════════════════════════════
#  AIRFLOW LIFECYCLE  (start / stop as child processes)
# ══════════════════════════════════════════════════════════════

def _airflow_cmd(subcmd: str) -> list:
    """Build the airflow CLI command using the current Python executable."""
    return [sys.executable, "-m", "airflow"] + subcmd.split()


def _copy_dag_to_airflow():
    """Copy our DAG file into AIRFLOW_HOME/dags/ so Airflow can find it."""
    dag_src = os.path.join("dags", "ecommerce_pipeline_dag.py")
    dag_dst_dir = os.path.join(AIRFLOW_HOME, "dags")
    os.makedirs(dag_dst_dir, exist_ok=True)
    dag_dst = os.path.join(dag_dst_dir, "ecommerce_pipeline_dag.py")
    if os.path.exists(dag_src):
        import shutil
        shutil.copy2(dag_src, dag_dst)
        print(f"[airflow] DAG copied -> {dag_dst}")
    else:
        print(f"[airflow] WARNING: DAG source not found at {dag_src}")


def _build_airflow_env() -> dict:
    """Return an env dict with all required Airflow variables."""
    env = os.environ.copy()
    env["AIRFLOW_HOME"]                              = AIRFLOW_HOME
    env["AIRFLOW__CORE__LOAD_EXAMPLES"]              = "False"
    env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] = "False"
    env["AIRFLOW__API__AUTH_BACKENDS"] = (
        "airflow.api.auth.backend.basic_auth,"
        "airflow.api.auth.backend.session"
    )
    env["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"]  = "True"
    env["AIRFLOW__WEBSERVER__SECRET_KEY"]       = "dataflow_flask_secret_key"
    # Make utils/ importable inside Airflow tasks
    project_root = os.path.abspath(".")
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    return env


def _init_airflow_db(env: dict):
    """Initialise the Airflow metadata database (idempotent)."""
    print("[airflow] Initialising metadata DB ...")
    result = subprocess.run(
        _airflow_cmd("db migrate"),
        env=env, capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        # Older Airflow versions use 'db init'
        subprocess.run(
            _airflow_cmd("db init"),
            env=env, capture_output=True, timeout=120,
        )

    # Create admin user — safe to call repeatedly (ignores if already exists)
    subprocess.run(
        _airflow_cmd(
            f"users create "
            f"--username {AIRFLOW_USERNAME} "
            f"--password {AIRFLOW_PASSWORD} "
            f"--firstname Admin --lastname User "
            f"--role Admin --email admin@pipeline.local"
        ),
        env=env, capture_output=True, timeout=60,
    )
    print("[airflow] DB ready")


def _start_airflow():
    """
    Spawn Airflow scheduler + webserver as child processes.
    Stored in _airflow_procs — killed together on shutdown.
    This function blocks until both processes are launched.
    """
    global _airflow_procs

    _copy_dag_to_airflow()
    env = _build_airflow_env()
    _init_airflow_db(env)

    os.makedirs("logs", exist_ok=True)
    sched_log   = open("logs/airflow_scheduler.log", "a")
    web_log     = open("logs/airflow_webserver.log", "a")

    print("[airflow] Starting scheduler ...")
    scheduler = subprocess.Popen(
        _airflow_cmd("scheduler"),
        env=env,
        stdout=sched_log,
        stderr=subprocess.STDOUT,
    )

    print("[airflow] Starting webserver on port 8080 ...")
    webserver = subprocess.Popen(
        _airflow_cmd("webserver --port 8080"),
        env=env,
        stdout=web_log,
        stderr=subprocess.STDOUT,
    )

    _airflow_procs = [scheduler, webserver]
    print(
        f"[airflow] Both processes started — "
        f"scheduler PID={scheduler.pid}  webserver PID={webserver.pid}"
    )
    print("[airflow] Logs -> logs/airflow_scheduler.log + logs/airflow_webserver.log")


def _stop_airflow():
    """
    Terminate all Airflow child processes.
    Called on Ctrl+C (via signal handler) and on normal Python exit (via atexit).
    """
    global _airflow_procs
    if not _airflow_procs:
        return
    print("\n[airflow] Stopping Airflow processes ...")
    for proc in _airflow_procs:
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
    _airflow_procs = []
    print("[airflow] All Airflow processes stopped")


def _wait_for_airflow(timeout_s: int = 90) -> bool:
    """
    Block until Airflow /health returns 200 or timeout elapses.
    Returns True if Airflow came up.
    """
    print(f"[airflow] Waiting for webserver (up to {timeout_s}s) ...")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(
                f"{AIRFLOW_BASE_URL}/health",
                timeout=3, auth=AIRFLOW_AUTH,
            )
            if r.status_code == 200:
                data = r.json()
                sched_ok = (data.get("scheduler", {}).get("status") == "healthy"
                            or data.get("status") == "healthy")
                if sched_ok:
                    print("[airflow] Webserver is healthy")
                    return True
        except Exception:
            pass
        time.sleep(4)
    print("[airflow] Timed out — will use LOCAL fallback until Airflow is ready")
    return False


# ══════════════════════════════════════════════════════════════
#  SIGNAL HANDLING  — Ctrl+C kills Airflow + Flask together
# ══════════════════════════════════════════════════════════════

def _shutdown_handler(signum, frame):
    print("\n[app] Shutdown signal — stopping everything ...")
    _stop_scheduler()
    _stop_airflow()
    sys.exit(0)


signal.signal(signal.SIGINT,  _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)
atexit.register(_stop_airflow)   # safety net for programmatic exits


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def _read_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _write_status(data):
    os.makedirs(_DATA_DIR, exist_ok=True)
    with open(STATUS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _initial_state():
    return {
        "status": "idle", "progress": 0, "current_task": "",
        "start_time": None, "end_time": None, "logs": [],
        "total_records": 0, "cleaned_records": 0,
        "removed_duplicates": 0, "missing_filled": 0,
        "output_file": "", "summary": {}, "mode": "idle",
    }


def _translate_path(path):
    """Translate Docker container paths to host paths."""
    if path and path.startswith("/opt/airflow/data"):
        return path.replace("/opt/airflow/data", _DATA_DIR, 1)
    return path or ""


def _check_airflow_health() -> bool:
    try:
        r = requests.get(f"{AIRFLOW_BASE_URL}/health", timeout=5, auth=AIRFLOW_AUTH)
        return r.status_code == 200
    except Exception:
        return False


def _unpause_dag() -> None:
    """Ensure the DAG is unpaused so it can accept trigger calls."""
    try:
        requests.patch(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}",
            json={"is_paused": False},
            headers=AIRFLOW_HEADERS,
            auth=AIRFLOW_AUTH,
            timeout=8,
        )
    except Exception:
        pass  # non-fatal — DAG may already be unpaused


def _trigger_airflow_dag(conf: dict) -> dict:
    """
    Trigger the Airflow DAG via the REST API.

    Fix: run_id is NOT sent in the payload.
    Airflow 2.x auto-generates run_id. Sending it manually causes:
        HTTP 400 {'run_id': ['Unknown field.']}
    which was incorrectly being caught and falling back to LOCAL mode.
    """
    _unpause_dag()

    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"

    # ── CORRECT payload: only conf, no run_id ────────────────────────────────
    payload = {"conf": conf}

    try:
        r = requests.post(
            url, json=payload,
            headers=AIRFLOW_HEADERS, auth=AIRFLOW_AUTH, timeout=15,
        )
        if r.status_code in (200, 201):
            return r.json()
        # Surface the exact error so it can be debugged — do NOT silently fallback
        try:
            detail = r.json()
        except Exception:
            detail = r.text[:300]
        raise RuntimeError(f"Airflow API {r.status_code}: {detail}")
    except requests.exceptions.ConnectionError:
        raise RuntimeError(f"Airflow unreachable at {AIRFLOW_BASE_URL}")


def _get_airflow_state() -> str:
    url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    try:
        r = requests.get(
            url, params={"limit": 1, "order_by": "-execution_date"},
            headers=AIRFLOW_HEADERS, auth=AIRFLOW_AUTH, timeout=8,
        )
        r.raise_for_status()
        runs = r.json().get("dag_runs", [])
        if not runs:
            return "unknown"
        s = runs[0].get("state", "unknown")
        return s if s in ("running", "success", "failed") else "unknown"
    except requests.exceptions.ConnectionError:
        return "unreachable"
    except Exception:
        return "unknown"


def _do_trigger(source: str = "CSV File", records: int = 0):
    """
    Core trigger — called by /api/run_pipeline and the hourly scheduler.

    Architecture:
        1. Airflow is PRIMARY — always try it first.
        2. LOCAL is FALLBACK — only if Airflow is genuinely unreachable
           (connection refused / timeout). An API error (400, 403, 500)
           means Airflow IS reachable but the request has a problem;
           that is NOT a reason to fall back — it should be surfaced.

    records=0    → full dataset (no scaling)
    records>=500 → Big Data scaling mode
    """
    current = _read_json(STATUS_FILE)
    if current.get("status") == "running":
        return False, "already_running"

    ts = datetime.datetime.now()
    display_records = "FULL DATASET" if records == 0 else f"{records:,} rows (scaled)"
    display_bigdata = "True" if records == 0 else f"True (target={records:,})" if records >= 500 else "False"
    state = _initial_state()
    state.update({
        "status":       "running",
        "current_task": "Initialising...",
        "start_time":   ts.isoformat(),
        "logs": [
            f"[{ts.strftime('%H:%M:%S')}] Pipeline requested — "
            f"source={source}, records={display_records}, big_data={display_bigdata}"
        ],
    })
    _write_status(state)

    # ── CASE 1: Airflow is healthy — always try it first ─────────────────────
    if _check_airflow_health():
        try:
            _trigger_airflow_dag({"source": source, "records": records})
            state["mode"]         = "airflow"
            state["current_task"] = "Airflow DAG triggered — running..."
            state["logs"].append(
                f"[{ts.strftime('%H:%M:%S')}] ✅ Airflow DAG triggered (mode=airflow)"
            )
            _write_status(state)
            return True, "airflow"

        except RuntimeError as e:
            err_msg = str(e)
            # ── Airflow reachable but request failed → surface error, no LOCAL ─
            if "unreachable" not in err_msg.lower():
                state["status"]       = "failed"
                state["current_task"] = "Airflow trigger error"
                state["logs"].append(f"[ERROR] Airflow API error: {err_msg}")
                state["logs"].append(
                    "[INFO] Airflow is running but the trigger request failed. "
                    "Check DAG configuration or Airflow logs."
                )
                _write_status(state)
                return False, f"airflow_error: {err_msg}"

            # ── Connection error raised inside trigger → fall through to LOCAL ─
            state["logs"].append(f"[WARN] Airflow unreachable during trigger: {err_msg}")

    # ── CASE 2: Airflow health check failed OR connection error in trigger ────
    # Only reach here when Airflow is genuinely down/starting up.
    state["mode"]         = "local"
    state["current_task"] = "LOCAL mode (Airflow unavailable)..."
    state["logs"].append(
        f"[{ts.strftime('%H:%M:%S')}] ⚠️  Airflow not reachable — LOCAL fallback active"
    )
    _write_status(state)
    from utils.local_pipeline import run_in_background
    run_in_background(source, records)
    return True, "local"


# ══════════════════════════════════════════════════════════════
#  HOURLY SCHEDULER
# ══════════════════════════════════════════════════════════════

def _scheduler_job():
    _schedule_state["last_run"]   = datetime.datetime.now().isoformat()
    _schedule_state["run_count"] += 1
    ok, mode = _do_trigger("CSV File", 0)
    nxt = datetime.datetime.now() + datetime.timedelta(
        minutes=_schedule_state["interval_min"]
    )
    _schedule_state["next_run"] = nxt.isoformat()
    print(f"[scheduler] Auto-run #{_schedule_state['run_count']} — mode={mode} ok={ok}")


def _start_scheduler(interval_min: int = 60):
    global _scheduler
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        return False, "APScheduler not installed — run: pip install apscheduler"
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        _scheduler_job, "interval", minutes=interval_min,
        id="pipeline_auto_run", replace_existing=True,
    )
    _scheduler.start()
    nxt = datetime.datetime.now() + datetime.timedelta(minutes=interval_min)
    _schedule_state.update({
        "enabled": True,
        "interval_min": interval_min,
        "next_run": nxt.isoformat(),
    })
    return True, "ok"


def _stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
    _schedule_state.update({"enabled": False, "next_run": None})


# ══════════════════════════════════════════════════════════════
#  REPORT BUILDER
# ══════════════════════════════════════════════════════════════

def _build_report_html() -> str:
    results   = _read_json(RESULTS_FILE)
    ml        = _read_json(ML_FILE)
    summary   = results.get("summary", {})
    cat       = summary.get("category_sales", {})
    reg       = summary.get("region_sales", {})
    months    = summary.get("monthly_sales", {})
    anomalies = ml.get("anomalies", [])
    forecast  = ml.get("forecast", [])
    gen_at    = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rev = sum(cat.values()) if cat else 0

    def cat_rows():
        if not cat:
            return "<tr><td colspan='3'>No data</td></tr>"
        return "".join(
            f"<tr><td>{k}</td><td>£{v:,.2f}</td>"
            f"<td>{v / total_rev * 100:.1f}%</td></tr>"
            for k, v in cat.items()
        )

    def reg_rows():
        if not reg:
            return "<tr><td colspan='2'>No data</td></tr>"
        return "".join(
            f"<tr><td>{k}</td><td>£{v:,.2f}</td></tr>"
            for k, v in list(reg.items())[:15]
        )

    def month_rows():
        if not months:
            return "<tr><td colspan='2'>No data</td></tr>"
        return "".join(
            f"<tr><td>{k}</td><td>£{v:,.2f}</td></tr>"
            for k, v in months.items()
        )

    def anom_rows():
        if not anomalies:
            return "<tr><td colspan='3' style='color:#00E5A0'>No anomalies — data is clean</td></tr>"
        return "".join(
            f"<tr style='color:#FF6B6B'><td>{a.get('month','')}</td>"
            f"<td>£{a.get('sales', 0):,.2f}</td>"
            f"<td>{a.get('reason','')}</td></tr>"
            for a in anomalies
        )

    def fc_rows():
        if not forecast:
            return "<tr><td colspan='3'>Run ML analysis to generate forecast</td></tr>"
        return "".join(
            f"<tr><td>{f.get('month','')}</td>"
            f"<td>£{f.get('predicted', 0):,.2f}</td>"
            f"<td style='color:#8EAACB'>{f.get('confidence','')}</td></tr>"
            for f in forecast
        )

    quality  = (
        f"{results.get('cleaned_records',0)/max(results.get('total_records',1),1)*100:.1f}%"
        if results.get("total_records", 0) > 0 else "—"
    )
    top_cat  = max(cat, key=cat.get) if cat else "—"
    top_reg  = max(reg, key=reg.get) if reg else "—"
    peak_mth = max(months, key=months.get) if months else "—"

    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>Pipeline Report</title>
<style>
body{{font-family:'Segoe UI',sans-serif;background:#060C18;color:#D8E8F8;margin:0;padding:32px 40px}}
h1{{color:#00C8F0;border-bottom:2px solid #1C2E4A;padding-bottom:14px;margin-bottom:6px}}
h2{{color:#00E5A0;margin-top:30px;font-size:1rem;text-transform:uppercase;letter-spacing:.1em}}
.meta{{color:#8EAACB;font-size:.82rem;margin-bottom:28px}}
.kpi-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:18px 0 28px}}
.kpi{{background:#0C1526;border:1px solid #1C2E4A;border-radius:10px;padding:18px;text-align:center}}
.kpi .val{{font-size:1.7rem;font-weight:700;color:#00C8F0}}
.kpi .lbl{{font-size:.72rem;color:#4D6B8A;margin-top:5px;text-transform:uppercase;letter-spacing:.05em}}
.section{{background:#0C1526;border:1px solid #1C2E4A;border-radius:12px;padding:20px 22px;margin-top:18px}}
table{{width:100%;border-collapse:collapse;margin-top:10px;font-size:.84rem}}
th{{background:#111E34;color:#8EAACB;padding:10px 14px;text-align:left}}
td{{padding:9px 14px;border-bottom:1px solid #1C2E4A}}
.footer{{margin-top:40px;color:#4D6B8A;font-size:.78rem;border-top:1px solid #1C2E4A;padding-top:14px}}
</style></head><body>
<h1>E-Commerce Pipeline Analytics Report</h1>
<div class="meta">Generated: {gen_at} &middot;
Engine: <strong>{results.get('engine','Apache Spark')}</strong> &middot;
Storage: <strong>{results.get('storage','PostgreSQL').upper()}</strong> &middot;
Quality: <strong>{quality}</strong></div>
<div class="kpi-grid">
  <div class="kpi"><div class="val">{results.get('total_records',0):,}</div><div class="lbl">Total Records</div></div>
  <div class="kpi"><div class="val">{results.get('cleaned_records',0):,}</div><div class="lbl">Cleaned</div></div>
  <div class="kpi"><div class="val">{results.get('removed_duplicates',0):,}</div><div class="lbl">Duplicates Removed</div></div>
  <div class="kpi"><div class="val">£{total_rev:,.0f}</div><div class="lbl">Total Revenue</div></div>
</div>
<div class="section"><h2>Sales by Category</h2>
<table><thead><tr><th>Category</th><th>Revenue (£)</th><th>Share</th></tr></thead>
<tbody>{cat_rows()}</tbody></table></div>
<div class="section"><h2>Sales by Region (Top 15)</h2>
<table><thead><tr><th>Region</th><th>Revenue (£)</th></tr></thead>
<tbody>{reg_rows()}</tbody></table></div>
<div class="section"><h2>Monthly Sales Trend</h2>
<table><thead><tr><th>Month</th><th>Revenue (£)</th></tr></thead>
<tbody>{month_rows()}</tbody></table></div>
<div class="section"><h2>ML Anomaly Detection</h2>
<table><thead><tr><th>Month</th><th>Sales (£)</th><th>Reason</th></tr></thead>
<tbody>{anom_rows()}</tbody></table></div>
<div class="section"><h2>Revenue Forecast — Next 3 Months</h2>
<table><thead><tr><th>Month</th><th>Predicted (£)</th><th>Confidence</th></tr></thead>
<tbody>{fc_rows()}</tbody></table></div>
<div class="footer">Python-Based Automated Data Pipeline — Apache Airflow · Spark · PostgreSQL</div>
</body></html>"""


# ══════════════════════════════════════════════════════════════
#  FLASK ROUTES
# ══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/run_pipeline", methods=["POST"])
def api_run_pipeline():
    body    = request.get_json(silent=True) or {}
    source  = body.get("source", "CSV File")
    records = int(body.get("records", 0))
    ok, mode = _do_trigger(source, records)
    if not ok:
        return jsonify({"error": "Pipeline already running"}), 400
    return jsonify({"message": f"Pipeline started ({mode} mode)", "mode": mode})


@app.route("/api/airflow_health")
def api_airflow_health():
    up = _check_airflow_health()
    return jsonify({
        "airflow_available": up,
        "url": AIRFLOW_BASE_URL,
        "managed_by_flask": True,
        "message": (
            "Airflow running (managed by Flask)" if up
            else "Airflow starting up — LOCAL mode active until ready"
        ),
    })


@app.route("/api/status")
def api_status():
    state = _read_json(STATUS_FILE) or _initial_state()

    if state.get("status") == "running" and state.get("mode") == "airflow":
        af = _get_airflow_state()
        if af == "success" and state.get("progress", 0) < 100:
            results = _read_json(RESULTS_FILE)
            if results:
                results["output_file"] = _translate_path(results.get("output_file", ""))
                state.update(results)
            state.update({"status": "success", "progress": 100})
            _write_status(state)
        elif af == "failed":
            state.update({
                "status": "failed",
                "logs": state.get("logs", []) + ["[ERROR] Airflow DAG run failed"],
            })
            _write_status(state)

    if state.get("status") == "success":
        ml = _read_json(ML_FILE)
        if ml:
            state["ml_results"] = ml

    state["schedule"] = _schedule_state
    # Always report engine so dashboard can display it accurately
    if state.get("mode") == "airflow":
        state["engine_display"] = "Apache Airflow"
    elif state.get("mode") == "local":
        state["engine_display"] = "Local (Airflow unavailable)"
    else:
        state["engine_display"] = "Apache Airflow"  # default when idle
    return jsonify(state)


@app.route("/api/reset", methods=["POST"])
def api_reset():
    if _read_json(STATUS_FILE).get("status") == "running":
        return jsonify({"error": "Cannot reset while running"}), 400
    _write_status(_initial_state())
    return jsonify({"message": "Reset OK"})


@app.route("/api/dag_tasks")
def api_dag_tasks():
    return jsonify(DAG_TASKS)


@app.route("/api/download")
def api_download():
    path = _translate_path(_read_json(STATUS_FILE).get("output_file", ""))
    if not path or not os.path.exists(path):
        path = _translate_path(_read_json(RESULTS_FILE).get("output_file", ""))
    if not path or not os.path.exists(path):
        return jsonify({"error": "No output file yet — run the pipeline first."}), 404
    return send_file(path, as_attachment=True)


@app.route("/api/report")
def api_report():
    if not _read_json(RESULTS_FILE):
        return jsonify({"error": "No results yet — run the pipeline first"}), 404
    os.makedirs(REPORT_DIR, exist_ok=True)
    ts   = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    html = _build_report_html()
    path = os.path.join(REPORT_DIR, f"report_{ts}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    resp = make_response(html)
    resp.headers["Content-Type"]        = "text/html; charset=utf-8"
    resp.headers["Content-Disposition"] = f'attachment; filename="report_{ts}.html"'
    return resp


@app.route("/api/schedule")
def api_schedule_status():
    return jsonify(_schedule_state)


@app.route("/api/schedule/start", methods=["POST"])
def api_schedule_start():
    body         = request.get_json(silent=True) or {}
    interval_min = int(body.get("interval_min", 60))
    ok, msg = _start_scheduler(interval_min)
    if not ok:
        return jsonify({"error": msg}), 500
    return jsonify({"message": f"Scheduler active — every {interval_min} min",
                    "state": _schedule_state})


@app.route("/api/schedule/stop", methods=["POST"])
def api_schedule_stop():
    _stop_scheduler()
    return jsonify({"message": "Scheduler stopped", "state": _schedule_state})


@app.route("/api/ml")
def api_ml():
    ml = _read_json(ML_FILE)
    if not ml:
        return jsonify({"error": "No ML results yet"}), 404
    return jsonify(ml)


@app.route("/api/ml/run", methods=["POST"])
def api_ml_run():
    results = _read_json(RESULTS_FILE)
    if not results:
        return jsonify({"error": "No pipeline results — run the pipeline first."}), 404

    def _bg():
        try:
            from utils.ml_analyzer import run_full_analysis
            out = run_full_analysis(results)
            os.makedirs(_DATA_DIR, exist_ok=True)
            with open(ML_FILE, "w") as f:
                json.dump(out, f, indent=2)
        except Exception as e:
            print(f"[ML] Error: {e}")

    threading.Thread(target=_bg, daemon=True, name="ml").start()
    return jsonify({"message": "ML analysis started — poll /api/ml for results"})


@app.route("/api/db_status")
def api_db_status():
    try:
        from utils.db_loader import is_postgres_available, get_warehouse_summary
        if is_postgres_available():
            s = get_warehouse_summary()
            return jsonify({
                "connected": True, "role": "PRIMARY warehouse",
                "message": "PostgreSQL connected",
                "host": os.getenv("DB_HOST", "localhost"),
                "port": os.getenv("DB_PORT", "5432"),
                "database": os.getenv("DB_NAME", "pipeline_db"),
                "summary": {k: str(v) for k, v in s.items()},
            })
        return jsonify({
            "connected": False, "role": "FALLBACK (CSV active)",
            "message": "PostgreSQL not reachable — CSV fallback active",
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "pipeline_db"),
            "summary": {},
        })
    except Exception as e:
        return jsonify({"connected": False, "role": "error",
                        "message": f"DB error: {e}", "summary": {}})


@app.route("/api/dataset_info")
def api_dataset_info():
    input_path = os.path.join(_DATA_DIR, "raw", "input.csv")
    if not os.path.exists(input_path):
        return jsonify({"exists": False, "path": input_path,
                        "message": "Place your Kaggle CSV at data/raw/input.csv"})
    try:
        size_mb = round(os.path.getsize(input_path) / (1024 * 1024), 2)
        with open(input_path, encoding="utf-8", errors="replace") as f:
            row_count = sum(1 for _ in f) - 1
        return jsonify({
            "exists": True, "rows": row_count, "size_mb": size_mb,
            "path": input_path,
            "message": f"{row_count:,} rows · {size_mb} MB",
        })
    except Exception as e:
        return jsonify({"exists": False, "message": str(e)})


# ══════════════════════════════════════════════════════════════
#  BOOT  — `python app.py` is the ONE command to run everything
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # ── Create directories ────────────────────────────────────────────────────
    for d in [
        _DATA_DIR,
        os.path.join(_DATA_DIR, "raw"),
        os.path.join(_DATA_DIR, "processed"),
        REPORT_DIR, "logs", "templates", "utils",
    ]:
        os.makedirs(d, exist_ok=True)

    if not os.path.exists(STATUS_FILE):
        _write_status(_initial_state())

    # ── Start Airflow as child processes ──────────────────────────────────────
    # Set SKIP_AIRFLOW=1 only for fast local testing without Airflow.
    skip_airflow = os.getenv("SKIP_AIRFLOW", "0") == "1"

    if not skip_airflow:
        try:
            # Run in a thread so Flask starts quickly while Airflow boots
            airflow_boot = threading.Thread(
                target=_start_airflow, daemon=True, name="airflow-boot"
            )
            airflow_boot.start()
            print("[boot] Airflow starting in background ...")
            print("[boot] Airflow UI -> http://localhost:8080  (admin / admin)")
        except Exception as e:
            print(f"[boot] Airflow start error: {e} — will use LOCAL mode")
    else:
        print("[boot] SKIP_AIRFLOW=1 — Airflow skipped, LOCAL mode only")

    # ── Start hourly auto-scheduler ───────────────────────────────────────────
    ok, msg = _start_scheduler(interval_min=60)
    if ok:
        print("[boot] Hourly scheduler active — pipeline auto-runs every 60 min")
        print("[boot] No human intervention required after this point")
    else:
        print(f"[boot] Scheduler: {msg}")

    # ── Print startup banner ──────────────────────────────────────────────────
    print()
    print("=" * 62)
    print("  DataFlow E-Commerce Pipeline")
    print("  Dashboard  -> http://localhost:5000")
    print("  Airflow UI -> http://localhost:8080  (admin / admin)")
    print("  Ctrl+C     -> stops Flask + Airflow + Scheduler together")
    print("=" * 62)
    print()

    # use_reloader=False is REQUIRED — the reloader forks the process and the
    # signal handler ends up in the wrong child, leaving Airflow orphaned.
    app.run(debug=False, port=5000, use_reloader=False)
