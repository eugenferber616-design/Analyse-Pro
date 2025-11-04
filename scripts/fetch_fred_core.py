#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_fred_core.py

Zieht Kern-Makroreihen von FRED:
  - Renditen: DGS30, DGS10, DGS2, DGS3MO
  - Geldmarkt/Policy: SOFR, RRPONTSYD
  - Bilanz & Treasury-Konten: WALCL, WTREGEN, WRESBAL
  - Stress: STLFSI4
  - (separat) OAS: IG_OAS, HY_OAS  (mit alternativen FRED IDs)

Outputs:
  data/processed/fred_core.csv.gz
  data/processed/fred_oas.csv.gz
"""

from __future__ import annotations
import os, time, sys, math, json
from pathlib import Path
from typing import Dict, List
import requests
import pandas as pd

API_BASE = "https://api.stlouisfed.org/fred/series/observations"
API_KEY  = os.getenv("FRED_API_KEY", "")
START    = os.getenv("FRED_START", "2003-01-01")

OUTDIR   = Path("data/processed")
OUTDIR.mkdir(parents=True, exist_ok=True)

# --------- gewünschte Serien (FRED IDs) ----------
CORE_SERIES: Dict[str, str] = {
    # Zinskurve
    "DGS30":  "DGS30",
    "DGS10":  "DGS10",
    "DGS2":   "DGS2",
    "DGS3MO": "DGS3MO",
    # Money/Rates
    "SOFR":       "SOFR",
    "RRPONTSYD":  "RRPONTSYD",  # Overnight RRPs (Treasury)
    # Fed-Bilanz / Treasury / Reserves
    "WALCL":   "WALCL",   # total assets
    "WTREGEN": "WTREGEN", # Treasury General Account (TGA)
    "WRESBAL": "WRESBAL", # Reserve balances
    # Stress
    "STLFSI4": "STLFSI4",
}

# OAS: mehrere mögliche FRED-IDs – nimm die erste, die funktioniert
OAS_CANDIDATES: Dict[str, List[str]] = {
    # IG OAS (ICE BofA US Corporate Index)
    "IG_OAS": [
        "BAMLC0A0CM",      # verbreitet
        "BAMLC0A0CMTRIV",  # alternative
        "BAMLC0A0CM2"      # fallback
    ],
    # HY OAS (ICE BofA US High Yield)
    "HY_OAS": [
        "BAMLH0A0HYM2",    # verbreitet
        "BAMLH0A0HYM"      # fallback
    ],
}

# ------------- HTTP Helper -------------
def get_series_obs(series_id: str, api_key: str, start: str, retries: int = 5, backoff: float = 1.5) -> pd.Series:
    """Pullt eine Serie als pd.Series(date->value)."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": "",    # alles
        "frequency": "d",         # daily (FRED interpoliert, wo nötig)
        "units": "lin",           # roh
        "sort_order": "asc",
    }
    for i in range(retries):
        try:
            r = requests.get(API_BASE, params=params, timeout=30)
            if r.status_code == 429:
                # Rate-Limit: kurz warten und erneut
                wait = min(60, 3 * (i + 1))
                print(f"429 rate-limit for {series_id} → sleep {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            obs = data.get("observations", [])
            if not obs:
                print(f"WARN: keine Beobachtungen für {series_id}")
                return pd.Series(dtype=float, name=series_id)
            # JSON -> Series
            dates = []
            vals  = []
            for o in obs:
                v = o.get("value", ".")
                if v is None or v == ".":
                    continue
                try:
                    fv = float(v)
                except Exception:
                    continue
                dates.append(o.get("date"))
                vals.append(fv)
            s = pd.Series(vals, index=pd.to_datetime(dates), name=series_id)
            s.index = s.index.tz_localize(None)
            return s.sort_index()
        except Exception as e:
            print(f"WARN: fetch {series_id} failed (try {i+1}/{retries}): {e}")
            time.sleep((backoff ** i))
    print(f"WARN: {series_id} endgültig fehlgeschlagen.")
    return pd.Series(dtype=float, name=series_id)

# ------------- Pull-Logik -------------
def pull_core() -> pd.DataFrame:
    cols = []
    for name, sid in CORE_SERIES.items():
        s = get_series_obs(sid, API_KEY, START)
        if not s.empty:
            s.name = name
            cols.append(s)
    if not cols:
        return pd.DataFrame()
    df = pd.concat(cols, axis=1).sort_index()
    # tägliche Frequenz + ffill
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.reindex(full).ffill()

def pull_oas() -> pd.DataFrame:
    out_cols = []
    for target, candidates in OAS_CANDIDATES.items():
        got = pd.Series(dtype=float)
        used = None
        for sid in candidates:
            s = get_series_obs(sid, API_KEY, START)
            if not s.empty:
                got = s
                used = sid
                break
        if used:
            print(f"OK: {target} via {used} (rows={len(got)})")
        else:
            print(f"WARN: {target} nicht verfügbar (candidates: {candidates})")
        got.name = target
        out_cols.append(got)
    if not out_cols:
        return pd.DataFrame()
    df = pd.concat(out_cols, axis=1)
    if df.empty:
        return df
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.sort_index().reindex(full).ffill()

# ------------- Main -------------
def main() -> int:
    if not API_KEY:
        print("ERROR: FRED_API_KEY fehlt (Repo → Settings → Secrets).")
        # weich raus, damit Workflow nicht komplett rot wird:
        return 0

    core = pull_core()
    if core.empty:
        print("WARN: core leer – schreibe keine fred_core.csv.gz")
    else:
        core.index.name = "date"
        (OUTDIR / "fred_core.csv.gz").write_text("", encoding="utf-8")  # ensure file exists before write?
        core.to_csv(OUTDIR / "fred_core.csv.gz", index=True, float_format="%.6f", compression="gzip")
        print(f"✔ wrote {OUTDIR / 'fred_core.csv.gz'}  cols={list(core.columns)}  rows={len(core)}")

    oas = pull_oas()
    if oas.empty:
        print("WARN: oas leer – schreibe keine fred_oas.csv.gz")
    else:
        oas.index.name = "date"
        oas.to_csv(OUTDIR / "fred_oas.csv.gz", index=True, float_format="%.6f", compression="gzip")
        print(f"✔ wrote {OUTDIR / 'fred_oas.csv.gz'}  cols={list(oas.columns)}  rows={len(oas)}")

    # Erfolg, auch wenn eine Datei fehlte – nachgelagerte Skripte sind tolerant
    return 0

if __name__ == "__main__":
    sys.exit(main())
