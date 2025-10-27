#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch ICE BofA OAS time series from FRED and write a tidy CSV.

Output:
- data/processed/fred_oas.csv  (columns: date, series_id, value, bucket, region)
- data/reports/fred_errors.json
Env:
- FRED_API_KEY   (required)
"""

import os, json, sys, time
from datetime import date
import requests
import pandas as pd

FRED = "https://api.stlouisfed.org/fred/series/observations"

# Konfigurierbare Standardliste (kannst du später erweitern)
SERIES = [
    # (series_id, bucket, region)
    ("BAMLC0A0CM",   "IG", "US"),   # US Corporate OAS
    ("BAMLH0A0HYM2", "HY", "US"),   # US High Yield OAS
    ("BEMLEIG",      "IG", "EU"),   # Euro Corporate OAS
    ("BEMLEHY",      "HY", "EU"),   # Euro High Yield OAS
]

def get(key, series_id, start="1999-01-01"):
    params = {
        "series_id": series_id,
        "api_key": key,
        "file_type": "json",
        "observation_start": start,
        "limit": 100000
    }
    r = requests.get(FRED, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text[:200]}")
    return r.json()

def main():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    key = os.getenv("FRED_API_KEY", "").strip()
    if not key:
        print("FRED_API_KEY fehlt – schreibe leere fred_oas.csv.")
        pd.DataFrame(columns=["date","series_id","value","bucket","region"]).to_csv(
            "data/processed/fred_oas.csv", index=False
        )
        json.dump({"error":"missing_api_key"}, open("data/reports/fred_errors.json","w"), indent=2)
        return 0

    rows, errors = [], []
    for sid, bucket, region in SERIES:
        try:
            js = get(key, sid)
            obs = js.get("observations", [])
            for o in obs:
                v = o.get("value", "")
                if v in ("", "."):
                    continue
                rows.append({
                    "date": o.get("date"),
                    "series_id": sid,
                    "value": float(v),
                    "bucket": bucket,
                    "region": region
                })
            time.sleep(0.3)  # sanft gegen Rate-Limits
        except Exception as e:
            errors.append({"series_id": sid, "msg": str(e)})

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values(["series_id","date"]).reset_index(drop=True)

    out = "data/processed/fred_oas.csv"
    df.to_csv(out, index=False)
    print(f"wrote {out} rows={len(df)}")

    with open("data/reports/fred_errors.json","w") as f:
        json.dump({"ts": date.today().isoformat(), "errors": errors}, f, indent=2)
    if errors:
        print("FRED OAS errors:", len(errors))

    return 0

if __name__ == "__main__":
    sys.exit(main())
