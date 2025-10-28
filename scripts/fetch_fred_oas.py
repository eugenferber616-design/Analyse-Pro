#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, time
from typing import List, Dict
import requests
import pandas as pd
from datetime import datetime

FRED_API = "https://api.stlouisfed.org/fred/series/observations"

# Bewährte OAS-Serien (US & Euro). Wenn eine Serie bei FRED nicht existiert,
# wird sie sauber geloggt und übersprungen.
SERIES: List[Dict[str, str]] = [
    {"series_id": "BAMLC0A0CM",   "bucket": "US_IG"},  # ICE BofA US Corporate Master OAS
    {"series_id": "BAMLH0A0HYM2", "bucket": "US_HY"},  # ICE BofA US High Yield OAS
    # Euro-Buckets (funktionieren bei FRED i.d.R.; falls nicht, wird es geloggt)
    {"series_id": "BEMLC0A0CM",   "bucket": "EU_IG"},  # ICE BofA Euro Corporate OAS
    {"series_id": "BEMLH0A0HYM2", "bucket": "EU_HY"},  # ICE BofA Euro High Yield OAS
]

OUT_CSV   = "data/processed/fred_oas.csv"
ERR_JSON  = "data/reports/fred_errors.json"
MIN_DATE  = "1998-01-01"

def fetch_series(series_id: str, api_key: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": MIN_DATE,
    }
    r = requests.get(FRED_API, params=params, timeout=40)
    r.raise_for_status()
    j = r.json()
    obs = j.get("observations", [])
    if not obs:
        return pd.DataFrame(columns=["date", "series_id", "value"])
    df = pd.DataFrame(obs)
    # Werte in float umwandeln ('.' -> NaN)
    df = df.assign(value=pd.to_numeric(df["value"], errors="coerce"))
    return df[["date", "value"]].assign(series_id=series_id)

def main():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(ERR_JSON), exist_ok=True)

    api_key = os.getenv("FRED_API_KEY", "")
    if not api_key and len(sys.argv) > 1 and sys.argv[1] == "--api-key":
        api_key = sys.argv[2]
    if not api_key:
        print("❌ FRED_API_KEY fehlt")
        sys.exit(1)

    rows = []
    errors = []
    for s in SERIES:
        sid = s["series_id"]
        bucket = s["bucket"]
        try:
            df = fetch_series(sid, api_key)
            if df.empty:
                errors.append({"series_id": sid, "error": "empty"})
                continue
            region = bucket.split("_")[0]  # US/EU
            df["bucket"] = bucket
            df["region"] = region
            rows.append(df)
        except requests.HTTPError as e:
            errors.append({"series_id": sid, "error": f"{e.response.status_code} {e}"})
        except Exception as e:
            errors.append({"series_id": sid, "error": str(e)})

    if rows:
        out = pd.concat(rows, ignore_index=True)
        out.sort_values(["date", "region", "bucket", "series_id"], inplace=True)
        out.to_csv(OUT_CSV, index=False)
        print(f"✅ wrote {OUT_CSV} with {len(out)} rows")
    else:
        # leere Datei trotzdem anlegen (Header), damit Downstream nicht crasht
        pd.DataFrame(columns=["date","value","series_id","bucket","region"]).to_csv(OUT_CSV, index=False)
        print("⚠️ no rows for fred_oas.csv")

    rep = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "rows": sum((len(x) for x in rows), 0) if rows else 0,
        "errors": errors,
        "file": OUT_CSV,
    }
    with open(ERR_JSON, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    if errors:
        print("ℹ️ FRED errors:", json.dumps(errors, indent=2))

if __name__ == "__main__":
    main()
