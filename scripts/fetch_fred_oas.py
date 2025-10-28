#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pull ICE/BofA OAS Kurven von FRED.
- Verlässlich vorhanden: US_IG (BAMLC0A0CM), US_HY (BAMLH0A0HYM2)
- EU-Kurven sind auf FRED nicht verfügbar -> bewusst NICHT mehr abfragen.
Output:
  data/processed/fred_oas.csv  (date, series_id, value, bucket, region)
  data/reports/fred_errors.json
"""

import os, json, sys, time
from datetime import date
import requests
import pandas as pd

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
BASE = "https://api.stlouisfed.org/fred/series/observations"

# Mapping: (bucket, region) -> FRED series_id
SERIES = [
    ("IG", "US", "BAMLC0A0CM"),     # ICE BofA US Corporate Index OAS (pct)
    ("HY", "US", "BAMLH0A0HYM2"),   # ICE BofA US High Yield Index OAS (pct)
    # ---- Hinweise für später ----
    # EU-Pendants sind auf FRED nicht verfügbar -> via ECB/ICE/EODHD/Yahoo-ETF lösen.
]

def fetch_series(series_id: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "1998-01-01",
    }
    r = requests.get(BASE, params=params, timeout=40)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text[:200]}")
    js = r.json()
    obs = js.get("observations", [])
    rows = []
    for o in obs:
        v = o.get("value", ".")
        if v != ".":
            rows.append((o["date"], float(v)))
    return pd.DataFrame(rows, columns=["date","value"])

def main():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    out_rows = []
    errors = []

    for bucket, region, sid in SERIES:
        try:
            df = fetch_series(sid)
            if not df.empty:
                df["series_id"] = sid
                df["bucket"] = bucket
                df["region"] = region
                out_rows.append(df)
        except Exception as e:
            errors.append({"series_id": sid, "error": str(e)})

    if out_rows:
        out = pd.concat(out_rows, ignore_index=True)
        # konsistente Sortierung / Datentypen
        out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
        out = out[["date","series_id","value","bucket","region"]]
        out.to_csv("data/processed/fred_oas.csv", index=False)
        print(f"FRED OAS rows written: {len(out)}")
    else:
        # leere Datei mit Header (damit Downstream nicht crasht)
        pd.DataFrame(columns=["date","series_id","value","bucket","region"]).to_csv(
            "data/processed/fred_oas.csv", index=False
        )
        print("⚠ no rows for fred_oas.csv")

    rep = {
        "file": "data/processed/fred_oas.csv",
        "errors": errors,
    }
    with open("data/reports/fred_errors.json","w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

if __name__ == "__main__":
    main()
