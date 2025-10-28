#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, time, argparse
from datetime import datetime
import requests
import pandas as pd

OUT = "data/processed/fred_oas.csv"
REP = "data/reports/fred_errors.json"
os.makedirs("data/processed", exist_ok=True)
os.makedirs("data/reports", exist_ok=True)

# --- FRED series (ICE BofA) -----------------------------------------------
# US:
#   IG  = BAMLC0A0CM  (ICE BofA US Corporate Index OAS)
#   HY  = BAMLH0A0HYM2 (ICE BofA US High Yield Index OAS)
# EU (Euro area):
#   IG  = BAMLEMCBPIOAS (ICE BofA Euro Corporate Index OAS)
#   HY  = BAMLHE00EHYIOAS (ICE BofA Euro High Yield Index OAS)
FRED_SERIES = [
    ("BAMLC0A0CM",       "IG", "US"),
    ("BAMLH0A0HYM2",     "HY", "US"),
    ("BAMLEMCBPIOAS",    "IG", "EU"),
    ("BAMLHE00EHYIOAS",  "HY", "EU"),
]

def fred_get(series_id, api_key=None, retries=3, timeout=30):
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "observation_start": "1990-01-01",
        "file_type": "json",
    }
    if api_key:
        params["api_key"] = api_key
    err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            err = str(e); time.sleep(1.2*(i+1))
    raise RuntimeError(f"FRED fetch failed for {series_id}: {err}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", default=os.getenv("FRED_API_KEY",""))
    args = ap.parse_args()

    rows, errors = [], []
    for sid, bucket, region in FRED_SERIES:
        try:
            js = fred_get(sid, api_key=args.api_key)
            obs = js.get("observations", [])
            for o in obs:
                v = o.get("value")
                if v in (".", None, ""): 
                    continue
                rows.append({
                    "date": o["date"],
                    "series_id": sid,
                    "value": float(v),
                    "bucket": bucket,
                    "region": region,
                })
        except Exception as e:
            errors.append({"series_id": sid, "msg": str(e)})

    df = pd.DataFrame(rows).sort_values(["date","region","bucket"])
    df.to_csv(OUT, index=False)

    rep = {"ts": datetime.utcnow().isoformat()+"Z",
           "rows": int(len(df)),
           "errors": errors,
           "file": OUT}
    with open(REP, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2))

if __name__ == "__main__":
    main()
