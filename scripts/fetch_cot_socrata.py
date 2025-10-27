#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, time, math
import io
import pandas as pd
import requests

BASE = "https://public.fmcsa.dot.gov/resource"  # Placeholder (z. B. wird durch domain ersetzt)
DOMAIN = "public.tableau.com"                    # (wird unten nicht genutzt – nur Platzhalter)

DATASET_ID   = os.getenv("COT_DATASET_ID", "gpe5-46if")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
MODE         = os.getenv("COT_MARKETS_MODE", "FILE").upper()  # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

def _socrata(url, params):
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    r = requests.get(url, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def get_latest_date():
    url = f"https://public.tableau.com/resource/{DATASET_ID}.json"
    # Socrata Abfrage: max(report_date_as_yyyy_mm_dd)
    url = f"https://api.cftc.gov/resource/{DATASET_ID}.json"
    res = _socrata(url, {
        "$select": "max(report_date_as_yyyy_mm_dd) as d"
    })
    latest = res[0]["d"]
    return latest  # "YYYY-MM-DD"

def read_markets_from_file(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out

def fetch_all_for_date(latest, limit=50000):
    url = f"https://api.cftc.gov/resource/{DATASET_ID}.json"
    # Spaltenliste kann voll bleiben; wir filtern nur nach Datum
    offset = 0
    chunks = []
    while True:
        params = {
            "$where": "report_date_as_yyyy_mm_dd = @d",
            "@d": latest,
            "$limit": limit,
            "$offset": offset
        }
        rows = _socrata(url, params)
        if not rows:
            break
        chunks.extend(rows)
        if len(rows) < limit:
            break
        offset += limit
        time.sleep(0.2)  # höfliche Pause
    df = pd.DataFrame(chunks)
    return df

def main():
    errors = []
    try:
        latest = get_latest_date()
    except Exception as e:
        errors.append({"stage": "latest", "msg": str(e)})
        latest = None

    if not latest:
        json.dump({"errors": errors}, open(os.path.join(REP_DIR,"cot_errors.json"),"w"), indent=2)
        raise SystemExit(1)

    try:
        df = fetch_all_for_date(latest)
    except Exception as e:
        errors.append({"stage": "fetch", "msg": str(e)})
        df = pd.DataFrame()

    # Optional: Watchlist-Filter NUR wenn MODE != ALL
    if MODE != "ALL":
        markets = read_markets_from_file(MARKETS_FILE)
        if markets:
            df = df[df["market_and_exchange_names"].isin(markets)].copy()

    # Schreiben
    latest_raw = os.path.join(OUT_DIR, "cot_latest_raw.csv")
    df.to_csv(latest_raw, index=False)

    # Alias (Kompatibilität)
    df.to_csv(os.path.join(OUT_DIR, "cot.csv"), index=False)

    # Kleine Summary
    cols_map = {
        "market": "market_and_exchange_names",
        "report_date": "report_date_as_yyyy_mm_dd",
        "oi": "open_interest_all",
        "ncl": "noncomm_positions_long_all",
        "ncs": "noncomm_positions_short_all",
        "noncomm_net": None
    }
    summ = pd.DataFrame()
    if not df.empty:
        summ = pd.DataFrame({
            "market": df.get(cols_map["market"]),
            "report_date": df.get(cols_map["report_date"]),
            "oi": pd.to_numeric(df.get(cols_map["oi"]), errors="coerce"),
            "ncl": pd.to_numeric(df.get(cols_map["ncl"]), errors="coerce"),
            "ncs": pd.to_numeric(df.get(cols_map["ncs"]), errors="coerce"),
        })
        summ["noncomm_net"] = summ["ncl"] - summ["ncs"]

    summ.to_csv(os.path.join(OUT_DIR, "cot_summary.csv"), index=False)

    report = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": DATASET_ID,
        "latest": latest,
        "rows": int(len(df)),
        "filtered_mode": MODE,
        "errors": errors
    }
    json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)

if __name__ == "__main__":
    main()
