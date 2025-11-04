#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import time, sys
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("WARN: yfinance fehlt → Market-Core übersprungen:", e)
    sys.exit(0)

RAW_DIR = Path("data/market/core"); RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT = Path("data/processed/market_core.csv.gz"); OUT.parent.mkdir(parents=True, exist_ok=True)
START = "2007-01-01"

SYMS = {
    "VIX":["^VIX"], "VIX3M":["^VIX3M"],
    "DXY":["DX-Y.NYB","DXY","UUP"],
    "USDJPY":["JPY=X","USDJPY=X"],
    "HYG":["HYG"], "LQD":["LQD"], "XLF":["XLF"], "SPY":["SPY"],
}

def dl_one(name, alts, tries=2, pause=0.8):
    for i in range(1, tries+1):
        for t in alts:
            try:
                df = yf.download(t, start=START, progress=False, auto_adjust=False, threads=False)
                if df is None or df.empty:
                    print(f"WARN: leer {name} ({t}) Versuch {i}/{tries}")
                    continue
                col = "Adj Close" if "Adj Close" in df.columns else ("Close" if "Close" in df.columns else None)
                if not col:
                    print(f"WARN: keine Close-Spalte {name} ({t}) {list(df.columns)}"); continue
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if s.empty: print(f"WARN: nur NaN {name} ({t})"); continue
                s.index = pd.to_datetime(s.index).tz_localize(None)
                s.name = name
                (RAW_DIR/f"{name}.csv").write_text(s.to_frame("value").rename_axis("date").to_csv())
                print(f"OK: {name} via {t} rows={len(s)}")
                return s
            except Exception as e:
                print(f"WARN: Download-Fehler {name} ({t}) Versuch {i}/{tries}: {e}")
            finally:
                time.sleep(pause)
    return pd.Series(dtype=float, name=name)

def main()->int:
    arr=[]
    for k,v in SYMS.items():
        s=dl_one(k,v)
        if not s.empty: arr.append(s)
    if not arr:
        print("ERROR: keine Market-Reihen enthalten – kein Merge möglich. (RiskIndex nutzt dann FRED.)")
        return 0
    df = pd.concat(arr, axis=1).sort_index()
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()
    df.to_csv(OUT, compression="gzip", float_format="%.6f")
    print(f"✔ wrote {OUT} cols={list(df.columns)} rows={len(df)}")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
