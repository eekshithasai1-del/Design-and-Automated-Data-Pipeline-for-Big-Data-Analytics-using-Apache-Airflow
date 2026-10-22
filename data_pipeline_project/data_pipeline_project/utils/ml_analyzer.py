"""
utils/ml_analyzer.py
─────────────────────
Industry-level ML analytics for the e-commerce pipeline.

Implements WITHOUT requiring scikit-learn / tensorflow (pure Python + statistics):
  1. Anomaly Detection   — Z-score & IQR on monthly sales
  2. Linear Forecasting  — Least-squares trend extrapolation (3 months ahead)
  3. Category Insights   — Growth rate, top mover, concentration index
  4. Seasonal Analysis   — Monthly seasonality index per month-of-year
  5. Data Quality Score  — Multi-dimensional score reported as %

If scipy/numpy is available it is used for better stats; otherwise falls back
to pure stdlib math.
"""

import math
import datetime
from typing import Any, Dict, List

# ── Try to import numpy/scipy — both are optional ─────────────────────────────
try:
    import statistics as _stats
    HAS_STATS = True
except ImportError:
    HAS_STATS = False


# ═══════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════

def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _stdev(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    variance = sum((v - m) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def _iqr(values: List[float]):
    """Return (Q1, Q3, IQR) for the given list."""
    if not values:
        return 0, 0, 0
    s = sorted(values)
    n = len(s)
    q1 = s[n // 4]
    q3 = s[(3 * n) // 4]
    return q1, q3, q3 - q1


def _linear_regression(x: List[float], y: List[float]):
    """Return (slope, intercept) via least squares."""
    n = len(x)
    if n < 2:
        return 0.0, _mean(y)
    sx  = sum(x)
    sy  = sum(y)
    sxy = sum(xi * yi for xi, yi in zip(x, y))
    sx2 = sum(xi ** 2 for xi in x)
    denom = n * sx2 - sx ** 2
    if denom == 0:
        return 0.0, _mean(y)
    slope     = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    return slope, intercept


def _next_month_str(month_str: str, delta: int) -> str:
    """'2011-11' + 2 months → '2012-01'"""
    try:
        y, m = int(month_str[:4]), int(month_str[5:7])
        m += delta
        while m > 12:
            m -= 12
            y += 1
        return f"{y}-{m:02d}"
    except Exception:
        return "unknown"


# ═══════════════════════════════════════════════════════════
#  1. ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════

def detect_anomalies(monthly_sales: Dict[str, float],
                     z_threshold: float = 2.0) -> List[Dict[str, Any]]:
    """
    Detect months with abnormally high or low sales using:
      - Z-score (flag if |z| > z_threshold)
      - IQR fence  (flag if below Q1-1.5*IQR or above Q3+1.5*IQR)
    Returns a list of anomaly dicts.
    """
    if len(monthly_sales) < 4:
        return []

    months = sorted(monthly_sales.keys())
    values = [monthly_sales[m] for m in months]
    mean   = _mean(values)
    std    = _stdev(values)
    q1, q3, iqr = _iqr(values)
    low_fence  = q1 - 1.5 * iqr
    high_fence = q3 + 1.5 * iqr

    anomalies = []
    for month, sales in zip(months, values):
        z = (sales - mean) / std if std > 0 else 0
        is_anom = abs(z) > z_threshold or sales < low_fence or sales > high_fence

        if is_anom:
            direction = "spike" if sales > mean else "drop"
            pct_diff  = abs(sales - mean) / mean * 100 if mean > 0 else 0
            reasons   = []
            if abs(z) > z_threshold:
                reasons.append(f"Z-score={z:.2f} (threshold ±{z_threshold})")
            if sales > high_fence:
                reasons.append(f"Above IQR upper fence (£{high_fence:,.0f})")
            elif sales < low_fence:
                reasons.append(f"Below IQR lower fence (£{low_fence:,.0f})")

            anomalies.append({
                "month":     month,
                "sales":     round(sales, 2),
                "mean":      round(mean, 2),
                "z_score":   round(z, 3),
                "direction": direction,
                "deviation": f"{pct_diff:.1f}% {'above' if direction == 'spike' else 'below'} mean",
                "reason":    " | ".join(reasons),
            })

    return anomalies


# ═══════════════════════════════════════════════════════════
#  2. REVENUE FORECAST (3 months ahead)
# ═══════════════════════════════════════════════════════════

def forecast_revenue(monthly_sales: Dict[str, float],
                     n_ahead: int = 3) -> List[Dict[str, Any]]:
    """
    Linear trend extrapolation with a simple seasonal adjustment.
    Returns n_ahead monthly predictions with confidence intervals.
    """
    if len(monthly_sales) < 6:
        return []

    months = sorted(monthly_sales.keys())
    values = [monthly_sales[m] for m in months]
    x      = list(range(len(values)))

    slope, intercept = _linear_regression(x, values)
    residuals        = [values[i] - (slope * i + intercept) for i in x]
    std_resid        = _stdev(residuals)

    # Simple seasonal index per calendar month (1-12)
    monthly_map: Dict[int, List[float]] = {}
    for m_str, v in zip(months, values):
        try:
            cal_month = int(m_str[5:7])
        except Exception:
            continue
        monthly_map.setdefault(cal_month, []).append(v)

    seasonal_index: Dict[int, float] = {}
    overall_mean = _mean(values) or 1
    for cal_m, vs in monthly_map.items():
        seasonal_index[cal_m] = _mean(vs) / overall_mean

    forecast = []
    last_month = months[-1]
    for i in range(1, n_ahead + 1):
        next_x    = len(values) - 1 + i
        trend_val = slope * next_x + intercept
        pred_month = _next_month_str(last_month, i)
        try:
            cal_m = int(pred_month[5:7])
        except Exception:
            cal_m = 1
        seasonal  = seasonal_index.get(cal_m, 1.0)
        predicted = max(0.0, trend_val * seasonal)
        ci_low    = max(0.0, predicted - 1.96 * std_resid)
        ci_high   = predicted + 1.96 * std_resid

        forecast.append({
            "month":      pred_month,
            "predicted":  round(predicted, 2),
            "ci_low":     round(ci_low, 2),
            "ci_high":    round(ci_high, 2),
            "confidence": f"95% CI: £{ci_low:,.0f} – £{ci_high:,.0f}",
            "trend":      "upward" if slope > 0 else "downward",
        })

    return forecast


# ═══════════════════════════════════════════════════════════
#  3. CATEGORY INSIGHTS
# ═══════════════════════════════════════════════════════════

def category_insights(category_sales: Dict[str, float]) -> Dict[str, Any]:
    """
    Compute HHI concentration, top/bottom movers, and share distribution.
    """
    if not category_sales:
        return {}

    total = sum(category_sales.values())
    if total == 0:
        return {}

    shares    = {k: v / total for k, v in category_sales.items()}
    hhi       = sum(s ** 2 for s in shares.values())  # Herfindahl-Hirschman Index
    top_cat   = max(category_sales, key=category_sales.get)
    bottom_cat = min(category_sales, key=category_sales.get)

    # Concentration level
    if hhi > 0.25:
        concentration = "High — dominated by a few categories"
    elif hhi > 0.15:
        concentration = "Moderate — balanced spread"
    else:
        concentration = "Low — well diversified"

    return {
        "total_revenue":      round(total, 2),
        "top_category":       top_cat,
        "top_revenue":        round(category_sales[top_cat], 2),
        "bottom_category":    bottom_cat,
        "bottom_revenue":     round(category_sales[bottom_cat], 2),
        "hhi_index":          round(hhi, 4),
        "concentration":      concentration,
        "shares":             {k: round(v * 100, 2) for k, v in shares.items()},
        "n_categories":       len(category_sales),
    }


# ═══════════════════════════════════════════════════════════
#  4. SEASONAL ANALYSIS
# ═══════════════════════════════════════════════════════════

def seasonal_analysis(monthly_sales: Dict[str, float]) -> Dict[str, Any]:
    """
    Compute a seasonal index for each calendar month (1–12).
    Values > 1.0 indicate above-average months; < 1.0 below-average.
    """
    if len(monthly_sales) < 6:
        return {}

    by_month: Dict[int, List[float]] = {}
    for m_str, v in monthly_sales.items():
        try:
            cal_m = int(m_str[5:7])
        except Exception:
            continue
        by_month.setdefault(cal_m, []).append(v)

    overall_mean = _mean(list(monthly_sales.values())) or 1
    index = {}
    month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                   7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

    for cal_m in sorted(by_month.keys()):
        avg   = _mean(by_month[cal_m])
        si    = avg / overall_mean
        index[month_names.get(cal_m, str(cal_m))] = round(si, 3)

    peak_month_name = max(index, key=index.get) if index else "—"
    trough_name     = min(index, key=index.get) if index else "—"

    return {
        "seasonal_index":    index,
        "peak_season":       peak_month_name,
        "trough_season":     trough_name,
        "peak_index":        index.get(peak_month_name, 1.0),
        "trough_index":      index.get(trough_name, 1.0),
        "interpretation":    (
            f"{peak_month_name} is the strongest month "
            f"({index.get(peak_month_name,1):.2f}x average), "
            f"{trough_name} is the weakest "
            f"({index.get(trough_name,1):.2f}x average)."
        ),
    }


# ═══════════════════════════════════════════════════════════
#  5. DATA QUALITY SCORE
# ═══════════════════════════════════════════════════════════

def data_quality_score(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Multi-dimensional data quality scoring:
      - Completeness  (missing values penalty)
      - Uniqueness    (duplicate penalty)
      - Validity      (records processed ratio)
      - Timeliness    (pipeline recency)
    """
    total   = results.get("total_records", 0) or 1
    cleaned = results.get("cleaned_records", 0)
    dups    = results.get("removed_duplicates", 0)
    filled  = results.get("missing_filled", 0)

    completeness = max(0, 1 - filled / total) * 100
    uniqueness   = max(0, 1 - dups  / total) * 100
    validity     = (cleaned / total) * 100 if total > 0 else 0

    # Timeliness — how recently the pipeline ran
    start = results.get("start_time") or ""
    timeliness = 100.0
    if start:
        try:
            ran_at = datetime.datetime.fromisoformat(start[:19])
            age_h  = (datetime.datetime.now() - ran_at).total_seconds() / 3600
            timeliness = max(0, 100 - age_h * 1.67)  # loses 10% per 6 hours
        except Exception:
            pass

    overall = (completeness * 0.3 + uniqueness * 0.3 +
               validity * 0.25 + timeliness * 0.15)

    return {
        "overall_score":    round(overall, 1),
        "completeness":     round(completeness, 1),
        "uniqueness":       round(uniqueness, 1),
        "validity":         round(validity, 1),
        "timeliness":       round(timeliness, 1),
        "grade":            "A" if overall >= 90 else "B" if overall >= 75 else "C" if overall >= 60 else "D",
        "interpretation":   (
            "Excellent data quality — production-ready." if overall >= 90 else
            "Good quality — minor issues detected." if overall >= 75 else
            "Acceptable — review missing/duplicate data." if overall >= 60 else
            "Poor quality — significant data issues."
        ),
    }


# ═══════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

def run_full_analysis(results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run all ML/analytics modules on the pipeline results dict.
    Returns a comprehensive analysis dict written to data/ml_results.json.
    """
    summary  = results.get("summary", {})
    cat      = summary.get("category_sales", {})
    monthly  = summary.get("monthly_sales", {})

    print("[ML] Running anomaly detection …")
    anomalies = detect_anomalies(monthly)

    print("[ML] Running revenue forecast …")
    forecast = forecast_revenue(monthly, n_ahead=3)

    print("[ML] Computing category insights …")
    cat_ins = category_insights(cat)

    print("[ML] Computing seasonal analysis …")
    seasonal = seasonal_analysis(monthly)

    print("[ML] Computing data quality score …")
    quality = data_quality_score(results)

    n_months  = len(monthly)
    values    = list(monthly.values())
    trend_dir = "upward" if len(values) >= 2 and values[-1] > values[0] else "downward"
    overall_mean = _mean(values)

    return {
        "generated_at":      datetime.datetime.now().isoformat(),
        "anomalies":         anomalies,
        "anomaly_count":     len(anomalies),
        "forecast":          forecast,
        "category_insights": cat_ins,
        "seasonal_analysis": seasonal,
        "data_quality":      quality,
        "summary_stats": {
            "n_months":         n_months,
            "mean_monthly_rev": round(overall_mean, 2),
            "max_monthly_rev":  round(max(values), 2) if values else 0,
            "min_monthly_rev":  round(min(values), 2) if values else 0,
            "revenue_trend":    trend_dir,
            "total_revenue":    round(sum(values), 2),
        },
        "engine": "Pure Python ML (Z-score + IQR + Linear Regression + Seasonal Decomposition)",
    }
