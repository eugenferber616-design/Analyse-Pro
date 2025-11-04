#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_core.py
Holt Markt-Quotes (daily) für RiskIndex: ^VIX, ^VIX3M, DXY (oder UUP), HYG, LQD, XLF, SPY, USDJPY
Schreibt:
  - Einzel-CSV nach data/market/core/{SYMBOL}.csv
  - Merge nach data/processed/market_core.csv.gz (Spalten: date, VIX, VIX3M, DXY, UUP, HYG, LQD, XLF, SPY, USDJPY)
"""

from __future__ import annotations
import os, sys, time
from pathlib import Path
import pandas as pd
import yfinance as yf

START_YEARS = int(os.getenv("MARKET_START_YEARS", "8"))  # ~8y
SLEEP_MS    = int(os.getenv("MARKET_SLEEP_MS", "300"))

# Primär-Tickers (Yahoo)
TICKERS = {
    "VIX": "^VIX",
    "VIX3M": "^VIX3M",
    # DXY kann zickig sein – UUP als Proxy mitziehen
    "DXY": "DX-Y.NYB",    # Alternativen: "DX=F" (continuous future) oder "DXY" über TVC (nicht via Yahoo)
    "UUP": "UUP",
    "HYG": "HYG",
    "LQD": "LQD",
    "XLF": "XLF",
    "SPY": "SPY",
    "USDJPY": "JPY=X",    # Yahoo-Notation: USD/JPY ist "JPY=X" (Preis = Anzahl JPY pro 1 USD)
}

def dl_yf(symbol: str, years: int) -> pd.DataFrame:
    period = f"{365*years}d"
    df = yf.download(symbol, period=period, interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    # Normalize column names
    ren = {c: c.capitalize() for c in df.columns}
    df = df.rename(columns=ren)
    # Ensure Date + Close
    col = "Adj Close" if "Adj Close" in df.columns else "Close"
    df = df[["Date", col]].rename(columns={col: "Close"})
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df

def main() -> int:
    out_dir = Path("data/market/core")
    proc_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    proc_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    frames = []

    for name, yf_ticker in TICKERS.items():
        try:
            df = dl_yf(yf_ticker, START_YEARS)
            if df.empty:
                print(f"WARN {name} ({yf_ticker}): no data")
            else:
                p = out_dir / f"{name}.csv"
                df.to_csv(p, index=False)
                s = df.rename(columns={"Close": name}).set_index("Date")[[name]]
                frames.append(s)
                rows.append({"symbol": name, "src": yf_ticker, "rows": len(s)})
        except Exception as e:
            print(f"ERR {name} ({yf_ticker}): {e}", file=sys.stderr)
        time.sleep(SLEEP_MS / 1000.0)

    if frames:
        big = pd.concat(frames, axis=1).sort_index()
        # fallback: wenn DXY leer, UUP behalten (oder später in Builder UUP als Proxy nutzen)
        big.index.name = "date"
        outp = proc_dir / "market_core.csv.gz"
        big.to_csv(outp, index=True, float_format="%.6f", compression="gzip")
        print("✔ wrote", outp, "rows:", len(big))
    else:
        print("❌ no market frames merged")

    # kleine QA
    (Path("data/reports")).mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_json("data/reports/market_core_report.json", orient="records", indent=2)
    print("report → data/reports/market_core_report.json")
    return 0

if __name__ == "__main__":
    sys.exit(main())
