#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_market_core.py
Baut tägliche Markt-Kernreihen für den RiskIndex aus yfinance:
  - ^VIX, ^VIX3M
  - UUP (Proxy für DXY)
  - HYG, LQD  (Credit)
  - XLF, SPY  (Financials vs. Gesamtmarkt)
  - USDJPY=X  (FX)
Ergebnis:
  data/processed/market_core.csv.gz  (Spalten: date, VIX, VIX3M, UUP, HYG, LQD, XLF, SPY, USDJPY)
"""

import sys, os, gzip
from pathlib import Path
import pandas as pd
import yfinance as yf

TICKERS = {
    "VIX":      "^VIX",
    "VIX3M":    "^VIX3M",
    "UUP":      "UUP",        # DXY-Proxy
    "HYG":      "HYG",
    "LQD":      "LQD",
    "XLF":      "XLF",
    "SPY":      "SPY",
    "USDJPY":   "USDJPY=X",
}

START = os.getenv("MC_START", "2003-01-01")

def _download_one(symbol: str) -> pd.Series:
    df = yf.download(symbol, start=START, progress=False, auto_adjust=False)
    if df is None or df.empty:
        return pd.Series(dtype="float64")
    col = "Adj Close" if "Adj Close" in df.columns else "Close"
    s = pd.to_numeric(df[col], errors="coerce")
    s.index = pd.to_datetime(s.index).date
    return s

def main() -> int:
    outdir = Path("data/processed")
    outdir.mkdir(parents=True, exist_ok=True)

    cols = {}
    for name, yft in TICKERS.items():
        try:
            s = _download_one(yft)
            if s.empty:
                print(f"WARN: keine Daten für {name} ({yft})")
            cols[name] = s
        except Exception as e:
            print(f"WARN: Download-Fehler {name} ({yft}): {e}")

    if not cols:
        print("ERROR: Keine Marktserien geladen.")
        return 1

    # zu einem DataFrame mergen, täglicher Kalender + FFill
    df = pd.DataFrame(cols).sort_index()
    full_idx = pd.date_range(df.index.min(), df.index.max(), freq="D").date
    df = df.reindex(full_idx).ffill()
    df.index.name = "date"

    outp = outdir / "market_core.csv.gz"
    df.to_csv(outp, index=True, float_format="%.6f", compression="gzip")
    print("✔ wrote", outp, "rows:", len(df))
    return 0

if __name__ == "__main__":
    sys.exit(main())
