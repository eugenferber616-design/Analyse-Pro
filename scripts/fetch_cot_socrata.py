#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pull latest COT (Futures+Options Combined) from CFTC Socrata and write:
- data/processed/cot_latest_raw.csv  (full latest report, all markets)
- data/processed/cot.csv             (alias for compatibility)
- data/processed/cot_summary.csv     (small summary)
- data/reports/cot_errors.json       (run report)
"""

import os, json, time
import pandas as pd
import requests

DATASET_ID   = os.getenv("COT_DATASET_ID", "gpe5-46if")   # CFTC Futures+Options combined
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
MODE         = os.getenv("COT_MARKETS_MODE", "FILE").upper()  # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

API_BASE = "https://api.cftc.gov/resource"   # <- Socrata base
LIMIT = 50000                                 # page size

def sget(path, params):
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    url = f"{API_BASE}/{path}"
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def get_latest_date():
    # Robust: sort desc and limit 1 (avoids name typos in max(...))
    rows = sget(f"{DATASET_ID}.json", {
        "$select": "report_date_as_yyyy_mm_dd",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 1
    })
    if not rows:
        return None
    return rows[0]["report_date_as_yyyy_mm_dd"]  # "YYYY-MM-DD"

def read_markets(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out

def fetch_latest_all(latest):
    # Pull ALL rows for that latest date, with pagination
    offset = 0
    chunks = []
    while True:
        params = {
            "$where": "report_date_as_yyyy_mm_dd = @d",
            "@d": latest,
            "$limit": LIMIT,
            "$offset": offset
        }
        rows = sget(f"{DATASET_ID}.json", params)
        if not rows:
            break
        chunks.extend(rows)
        if len(rows) < LIMIT:
            break
        offset += LIMIT
        time.sleep(0.15)  # be nice
    return pd.DataFrame(chunks)

def main():
    report = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": DATASET_ID,
        "latest": None,
        "rows": 0,
        "filtered_mode": MODE,
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
        report["errors"].append({"stage": "latest", "msg": "no latest date"})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    try:
        df = fetch_latest_all(latest)
    except Exception as e:
        report["errors"].append({"stage": "fetch", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    # Optional: filter by watchlist only if MODE != ALL
    if MODE != "ALL":
        wl = read_markets(MARKETS_FILE)
        if wl:
            col = "market_and_exchange_names"
            if col in df.columns:
                df = df[df[col].isin(wl)].copy()

    # Write full latest
    latest_raw = os.path.join(OUT_DIR, "cot_latest_raw.csv")
    df.to_csv(latest_raw, index=False)

    # Alias
    df.to_csv(os.path.join(OUT_DIR, "cot.csv"), index=False)

    # Small summary
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
