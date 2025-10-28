#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_fred_oas.py  (ERSATZ)
Lädt IG/HY-OAS für US und EU dynamisch aus config/fred_oas_map.yml (FRED).
Schreibt konsolidiert nach: data/processed/fred_oas.csv
Schema: date,series_id,value,bucket,region
und loggt Fehler nach: data/reports/fred_errors.json
"""

import os, json, csv, time, datetime as dt
from typing import Dict, List
import requests

try:
    import yaml
except Exception:
    yaml = None

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
OUT_CSV = "data/processed/fred_oas.csv"
REP_JSON = "data/reports/fred_errors.json"
CFG_PATH = "config/fred_oas_map.yml"

os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(REP_JSON), exist_ok=True)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

def load_cfg() -> Dict:
    cfg = {"regions": {}}
    if yaml is None:
        return cfg
    if os.path.exists(CFG_PATH):
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or cfg
    return cfg

def fred_pull(series_id: str, obs_start: str) -> List[Dict]:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": obs_start,
    }
    r = requests.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j.get("observations", [])

def as_float(x: str):
    try:
        return float(x)
    except Exception:
        return None

def main():
    report = {"ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
              "errors": [], "rows": 0, "config_used": CFG_PATH}
    rows_out = []

    if not FRED_API_KEY:
        report["errors"].append({"stage": "init", "msg": "FRED_API_KEY missing"})
        json.dump(report, open(REP_JSON, "w"), indent=2)
        raise SystemExit(0)  # soft

    cfg = load_cfg()
    if not cfg.get("regions"):
        report["errors"].append({"stage": "config", "msg": f"no regions in {CFG_PATH}"})
        json.dump(report, open(REP_JSON, "w"), indent=2)
        raise SystemExit(0)

    # hole 2 Jahre zurück (reichlich)
    obs_start = (dt.date.today() - dt.timedelta(days=730)).strftime("%Y-%m-%d")

    for region, buckets in cfg["regions"].items():
        for bucket, sid in (buckets or {}).items():
            if not sid or not isinstance(sid, str):
                report["errors"].append({"stage": "config", "msg": f"{region}.{bucket} series_id missing"})
                continue
            try:
                obs = fred_pull(sid, obs_start)
            except Exception as e:
                report["errors"].append({"stage": "fetch", "region": region, "bucket": bucket, "series_id": sid, "msg": str(e)})
                continue
            for o in obs:
                v = as_float(o.get("value", ""))
                if v is None:
                    continue
                rows_out.append({
                    "date": o.get("date"),
                    "series_id": sid,
                    "value": v,
                    "bucket": bucket.upper(),
                    "region": region.upper(),
                })

    # sortieren & schreiben
    rows_out.sort(key=lambda r: (r["region"], r["bucket"], r["date"]))
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","series_id","value","bucket","region"])
        w.writeheader()
        w.writerows(rows_out)

    report["rows"] = len(rows_out)
    json.dump(report, open(REP_JSON, "w"), indent=2)

if __name__ == "__main__":
    main()
