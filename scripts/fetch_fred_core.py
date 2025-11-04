#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_fred_core.py
Zieht zentrale FRED-Reihen für den RiskIndex (UST, SOFR, RRP, STLFSI, WRESBAL, TGA, OAS)
und schreibt:
  - data/processed/fred_core.csv.gz   (alle "Core"-Reihen als tägliche Timeline)
  - data/processed/fred_oas.csv.gz    (IG/HY OAS separat, daily)
Außerdem je Serie eine Roh-CSV in data/macro/fred/{series}.csv
"""

from __future__ import annotations
import os, sys, time, json
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import requests
import pandas as pd
from dateutil import tz

from util import ensure_dir, write_json, load_env

FRED_API = "https://api.stlouisfed.org/fred/series/observations"
START    = os.getenv("FRED_START", "2003-01-01")  # ausreichend Historie für 200d Z
TIMEOUT  = int(os.getenv("FRED_TIMEOUT", "30"))
RETRIES  = int(os.getenv("FRED_RETRIES", "3"))
SLEEP_MS = int(os.getenv("FRED_SLEEP_MS", "800"))

# Core Macro / Funding (Mapping: friendly -> FRED series_id)
CORE_SERIES: Dict[str, str] = {
    "DGS30"    : "DGS30",     # 30y yield
    "DGS10"    : "DGS10",     # 10y
    "DGS2"     : "DGS2",      # 2y
    "DGS3MO"   : "DGS3MO",    # 3m
    "SOFR"     : "SOFR",      # Secured Overnight Financing Rate
    "RRPONTSYD": "RRPONTSYD", # Overnight Reverse Repurchase Agreements: Award Rate (pct)
    "STLFSI4"  : "STLFSI4",   # St. Louis Fed Financial Stress Index 4.0
    "WRESBAL"  : "WRESBAL",   # Reserve Balances w/ Federal Reserve Banks
    "WTREGEN"  : "WTREGEN",   # Treasury General Account (TGA)
}

# Credit Spreads (OAS)
OAS_SERIES: Dict[str, str] = {
    "IG_OAS": "BAMLC0A0CM",    # ICE BofA US Corp Index OAS
    "HY_OAS": "BAMLH0A0HYM2",  # ICE BofA US High Yield Index OAS
}

def fetch_fred_series(session: requests.Session, series_id: str, api_key: str) -> pd.DataFrame:
    """Fetch a single FRED series as DataFrame(date,value) with float conversion."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": START,
        "frequency": "d"  # daily if possible; weekly series will repeat per FRED behavior
    }
    for k in range(RETRIES + 1):
        try:
            r = session.get(FRED_API, params=params, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json().get("observations", [])
                df = pd.DataFrame(data)
                if df.empty:
                    return pd.DataFrame(columns=["date", "value"])
                df = df[["date", "value"]].copy()
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_convert(tz.UTC).dt.date
                return df
            # retry on 5xx
            if 500 <= r.status_code < 600 and k < RETRIES:
                time.sleep(SLEEP_MS / 1000.0)
                continue
            r.raise_for_status()
        except requests.RequestException:
            if k < RETRIES:
                time.sleep(SLEEP_MS / 1000.0)
                continue
            raise
    return pd.DataFrame(columns=["date", "value"])

def main() -> int:
    env = load_env(["FRED_API_KEY"])
    api_key = env.get("FRED_API_KEY", "")
    if not api_key:
        print("❌ FRED_API_KEY fehlt in ENV.")
        return 1

    # output dirs
    raw_dir = Path("data/macro/fred")
    processed_dir = Path("data/processed")
    reports_dir = Path("data/reports")
    for d in (raw_dir, processed_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    rep = {"ok": [], "err": []}

    # ---- CORE ----
    core_frames: List[pd.DataFrame] = []
    for name, sid in CORE_SERIES.items():
        try:
            df = fetch_fred_series(session, sid, api_key)
            if df.empty:
                rep["err"].append({"series": name, "id": sid, "reason": "empty"})
            else:
                outp = raw_dir / f"{name}.csv"
                df.rename(columns={"value": name}).to_csv(outp, index=False)
                core_frames.append(df.rename(columns={"value": name}).set_index("date"))
                rep["ok"].append(name)
            time.sleep(SLEEP_MS / 1000.0)
        except Exception as e:
            rep["err"].append({"series": name, "id": sid, "reason": str(e)})

    core_merged = pd.DataFrame()
    if core_frames:
        core_merged = pd.concat(core_frames, axis=1).sort_index()
        # forward-fill weekly series; enforce daily index
        full_idx = pd.date_range(core_merged.index.min(), core_merged.index.max(), freq="D").date
        core_merged = core_merged.reindex(full_idx)
        core_merged = core_merged.ffill()
        core_merged.index.name = "date"
        core_out = processed_dir / "fred_core.csv.gz"
        core_merged.to_csv(core_out, index=True, float_format="%.6f", compression="gzip")
        print("✔ wrote", core_out, "rows:", len(core_merged))
    else:
        print("WARN: no core frames merged")

    # ---- OAS ----
    oas_frames: List[pd.DataFrame] = []
    for name, sid in OAS_SERIES.items():
        try:
            df = fetch_fred_series(session, sid, api_key)
            if df.empty:
                rep["err"].append({"series": name, "id": sid, "reason": "empty"})
            else:
                outp = raw_dir / f"{name}.csv"
                df.rename(columns={"value": name}).to_csv(outp, index=False)
                oas_frames.append(df.rename(columns={"value": name}).set_index("date"))
                rep["ok"].append(name)
            time.sleep(SLEEP_MS / 1000.0)
        except Exception as e:
            rep["err"].append({"series": name, "id": sid, "reason": str(e)})

    if oas_frames:
        oas_merged = pd.concat(oas_frames, axis=1).sort_index()
        full_idx = pd.date_range(oas_merged.index.min(), oas_merged.index.max(), freq="D").date
        oas_merged = oas_merged.reindex(full_idx).ffill()
        oas_merged.index.name = "date"
        oas_out = processed_dir / "fred_oas.csv.gz"
        oas_merged.to_csv(oas_out, index=True, float_format="%.6f", compression="gzip")
        print("✔ wrote", oas_out, "rows:", len(oas_merged))
    else:
        print("WARN: no OAS frames merged")

    # report
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "fred_core_report.json"
    rep["ts"] = datetime.utcnow().isoformat() + "Z"
    rep["core_out"] = str(processed_dir / "fred_core.csv.gz")
    rep["oas_out"]  = str(processed_dir / "fred_oas.csv.gz")
    rep["start"]    = START
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print("report →", rep_path)
    return 0

if __name__ == "__main__":
    sys.exit(main())
