#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch latest COT (Futures+Options Combined) from CFTC Socrata (gpe5-46if)
- robust gegen Timeouts/Rate-Limits (Retry + Backoff)
- kleinere Page-Größen (SOC_LIMIT) für weniger Server-Last
- schreibt:
    data/processed/cot_latest_raw.csv
    data/processed/cot.csv              (Alias)
    data/processed/cot_summary.csv
    data/reports/cot_errors.json        (Run-Report)
"""

import os, json, time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- robuste Netz-Settings (per ENV konfigurierbar) ---
SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))    # Sekunden
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 25000))    # kleinere Pages als 50000

API_BASE   = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
DATASET_ID = os.getenv("COT_DATASET_ID", "gpe5-46if")
APP_TOKEN  = os.getenv("CFTC_APP_TOKEN", "")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

# ---------------------------------------------------------------------------

def make_session():
    s = requests.Session()
    retry = Retry(
        total=SOC_RETRIES,
        connect=SOC_RETRIES,
        read=SOC_RETRIES,
        status=SOC_RETRIES,
        backoff_factor=SOC_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    s.headers.update(headers)
    return s

SESSION = make_session()

def sget(path, params):
    url = f"{API_BASE}/{path}"
    r = SESSION.get(url, params=params, timeout=SOC_TIMEOUT)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP {r.status_code} for {url} ; body={r.text[:500]}") from e
    return r.json()

# ---------------------------------------------------------------------------

def get_latest_date():
    rows = sget(f"{DATASET_ID}.json", {
        "$select": "report_date_as_yyyy_mm_dd",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1
    })
    if not rows:
        return None
    return rows[0]["report_date_as_yyyy_mm_dd"]

def fetch_latest_all(latest):
    # Full pull für den jüngsten Report, gepaged
    rows_all, offset = [], 0
    while True:
        params = {
            "$where": "report_date_as_yyyy_mm_dd = @d",
            "@d": latest,
            "$limit": SOC_LIMIT,
            "$offset": offset,
        }
        chunk = sget(f"{DATASET_ID}.json", params)
        if not chunk:
            break
        rows_all.extend(chunk)
        if len(chunk) < SOC_LIMIT:
            break
        offset += SOC_LIMIT
        time.sleep(0.2)
    return pd.DataFrame(rows_all)

# ---------------------------------------------------------------------------

def main():
    report = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": DATASET_ID,
        "latest": None,
        "rows": 0,
        "filtered_mode": "ALL",
        "errors": []
    }

    try:
        latest = get_latest_date()
        report["latest"] = latest
    except Exception as e:
        report["errors"].append({"stage": "latest", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    if not latest:
        report["errors"].append({"stage": "latest", "msg": "no latest date found"})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    try:
        df = fetch_latest_all(latest)
    except Exception as e:
        report["errors"].append({"stage": "fetch", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    # write outputs
    latest_raw = os.path.join(OUT_DIR, "cot_latest_raw.csv")
    df.to_csv(latest_raw, index=False)
    df.to_csv(os.path.join(OUT_DIR, "cot.csv"), index=False)

    # summary (klein)
    def _num(series):
        return pd.to_numeric(series, errors="coerce")

    if not df.empty:
        summary = pd.DataFrame({
            "market": df.get("market_and_exchange_names"),
            "report_date": df.get("report_date_as_yyyy_mm_dd"),
            "oi": _num(df.get("open_interest_all")),
            "ncl": _num(df.get("noncomm_positions_long_all")),
            "ncs": _num(df.get("noncomm_positions_short_all")),
        })
        summary["noncomm_net"] = summary["ncl"] - summary["ncs"]
    else:
        summary = pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"])

    summary.to_csv(os.path.join(OUT_DIR, "cot_summary.csv"), index=False)

    report["rows"] = int(len(df))
    json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)

if __name__ == "__main__":
    main()
