#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch ICE BofA OAS (US & Euro) from FRED into data/processed/fred_oas.csv
Columns: date, series_id, value, bucket, region
- Accepts: --api-key <FRED_API_KEY>
- Gracefully handles existing column name 'date_series_id'
"""

import os, sys, argparse, time, json
import pandas as pd
import requests

OUT_CSV = "data/processed/fred_oas.csv"
REP_JSON = "data/reports/fred_errors.json"
os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(REP_JSON), exist_ok=True)

# FRED series map (feel free to extend when you have official Euro OAS series)
SERIES = [
    # US Investment Grade & High Yield (monthly, FRED)
    {"series_id": "BAMLC0A0CM",   "bucket": "IG", "region": "US"},   # ICE BofA US Corp Master OAS
    {"series_id": "BAMLH0A0HYM2", "bucket": "HY", "region": "US"},   # ICE BofA US High Yield OAS

    # Euro area (falls verfügbar; placeholder: versuche EU IG/HY – ggf. liefern sie NAs, dann siehst du es im Report)
    {"series_id": "BEMLCC0A0M",   "bucket": "IG", "region": "EU"},   # ICE BofA Euro IG Corp OAS (falls vorhanden)
    {"series_id": "BEMLH0A0HYM2", "bucket": "HY", "region": "EU"},   # ICE BofA Euro High Yield OAS (falls vorhanden)
]

BASE = "https://api.stlouisfed.org/fred/series/observations"

def fetch_series(series_id: str, api_key: str):
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "1998-01-01"
    }
    r = requests.get(BASE, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    obs = js.get("observations", [])
    rows = []
    for o in obs:
        d = o.get("date")
        v = o.get("value")
        try:
            val = float(v)
        except Exception:
            val = None
        rows.append({"date": d, "series_id": series_id, "value": val})
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", required=True)
    args = ap.parse_args()

    errors = []
    frames = []

    for s in SERIES:
        sid = s["series_id"]
        try:
            data = fetch_series(sid, args.api_key)
            df = pd.DataFrame(data)
            df["bucket"] = s["bucket"]
            df["region"] = s["region"]
            frames.append(df)
            time.sleep(0.2)
        except Exception as e:
            errors.append({"series_id": sid, "error": str(e)})

    if frames:
        df_all = pd.concat(frames, ignore_index=True)

        # ---- Robust: 'date' sicherstellen & bereinigen
        if "date" not in df_all.columns and "date_series_id" in df_all.columns:
            df_all = df_all.rename(columns={"date_series_id": "date"})

        # nur Datumsteil (YYYY-MM-DD)
        df_all["date"] = df_all["date"].astype(str).str.slice(0, 10)

        # sortiere robust, nur vorhandene Spalten verwenden
        sort_cols = [c for c in ["date", "region", "bucket"] if c in df_all.columns]
        if sort_cols:
            df_all = df_all.sort_values(sort_cols)

        df_all.to_csv(OUT_CSV, index=False)
    else:
        # Wenn gar nichts geladen wurde, leere Datei mit Header schreiben
        pd.DataFrame(columns=["date","series_id","value","bucket","region"]).to_csv(OUT_CSV, index=False)

    report = {
        "ts": pd.Timestamp.utcnow().isoformat(),
        "rows": int(sum(len(f) for f in frames)) if frames else 0,
        "errors": errors,
        "file": OUT_CSV
    }
    with open(REP_JSON, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

if __name__ == "__main__":
    main()
