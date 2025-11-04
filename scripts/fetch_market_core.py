#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_core.py
Zieht Markt-Proxy-Serien (yfinance) EINZELN & robust.
Schreibt:
  - data/market/core/{NAME}.csv    (Roh, date,value)
  - data/processed/market_core.csv.gz  (Merge aller verfügbaren Reihen)

Fixes:
- Kein Multi-Download → keine MultiIndex/tuple-Probleme
- Richtige Ticker (DXY & USDJPY) inkl. Fallbacks
- Hartes Abbrechen vermieden (wir schreiben, was da ist)
"""

from __future__ import annotations
from pathlib import Path
import sys, time
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("WARN: yfinance nicht installiert → überspringe Market-Core. Fehler:", e)
    sys.exit(0)

RAW_DIR = Path("data/market/core")
OUT_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

START = "2007-01-01"
PAUSE = 0.6  # s

SYMS = {
    "VIX":    ["^VIX"],
    "VIX3M":  ["^VIX3M"],
    # DXY: best effort – manche Umgebungen brauchen 'DX-Y.NYB', andere 'DXY'
    "DXY":    ["DX-Y.NYB", "DXY", "UUP"],           # UUP als ETF-Proxy
    "USDJPY": ["JPY=X", "USDJPY=X"],                # korrekt ist JPY=X
    "HYG":    ["HYG"],
    "LQD":    ["LQD"],
    "XLF":    ["XLF"],
    "SPY":    ["SPY"],
}

def pull_one(name: str, alts: list[str]) -> pd.Series:
    for t in alts:
        try:
            df = yf.download(t, start=START, progress=False, auto_adjust=False, threads=False)
            if df is None or df.empty:
                print(f"WARN: leer für {name} ({t})")
                continue
            col = "Adj Close" if "Adj Close" in df.columns else ("Close" if "Close" in df.columns else None)
            if col is None:
                print(f"WARN: keine Close-Spalte für {name} ({t}) → {list(df.columns)}")
                continue
            s = pd.to_numeric(df[col], errors="coerce").dropna()
            if s.empty:
                print(f"WARN: nur NaN für {name} ({t})")
                continue
            s.index = pd.to_datetime(s.index).tz_localize(None)  # naive daily
            s.name = name
            # Roh schreiben
            out_raw = RAW_DIR / f"{name}.csv"
            s.to_frame("value").rename_axis("date").to_csv(out_raw, index=True)
            print(f"OK: {name} via {t} (rows={len(s)}) → {out_raw}")
            return s
        except Exception as e:
            print(f"WARN: Download-Fehler {name} ({t}): {e}")
        finally:
            time.sleep(PAUSE)
    return pd.Series(dtype=float, name=name)

def main() -> int:
    series = []
    for k, v in SYMS.items():
        s = pull_one(k, v)
        if not s.empty:
            series.append(s)

    if not series:
        print("ERROR: keine Market-Reihen erhalten – kein Merge möglich.")
        return 0  # weich raus; RiskIndex hat Fallback auf FRED

    df = pd.concat(series, axis=1).sort_index()
    df.index.name = "date"
    # tägliche Frequenz + FFill (für spätere Joins)
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()
    out = OUT_DIR / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print(f"✔ wrote {out}  cols={list(df.columns)}  rows={len(df)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
