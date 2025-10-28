#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, time, argparse, csv
from datetime import datetime, timedelta
import pandas as pd

from pandas_datareader.data import DataReader
import yfinance as yf

OUT_DIR = "data/market/stooq"
QUOTES_CSV = "data/processed/fx_quotes.csv"   # aggregierte Letztkurse

def to_stooq_symbol(sym: str) -> str:
    s = sym.strip().lower()
    if "." in s:
        return s  # sap.de, air.pa, cez.pr, asml.as usw.
    # US Ticker ohne Suffix -> stooq erwartet .us
    return f"{s}.us"

def fetch_one(sym: str, start: datetime, end: datetime) -> pd.DataFrame:
    stq = to_stooq_symbol(sym)
    try:
        df = DataReader(stq, "stooq", start=start, end=end)  # OHLCV
        if not df.empty:
            df.sort_index(inplace=True)
            return df
    except Exception:
        pass
    # Fallback: yfinance (gleicher Zeitraum)
    try:
        yf_sym = sym if "." not in sym else sym.replace(".", "-")  # z.B. "AIR.PA" -> "AIR-PA"
        df = yf.download(yf_sym, start=start.date(), end=end.date(), progress=False, auto_adjust=False)
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.rename(columns=str.capitalize, inplace=True)  # Angleichen: Open, High, Low, Close, Adj Close, Volume
            return df
    except Exception:
        pass
    raise RuntimeError(f"no data for {sym}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--days", type=int, default=365)
    args = ap.parse_args()

    os.makedirs(OUT_DIR, exist_ok=True)

    with open(args.watchlist, "r", encoding="utf-8") as f:
        syms = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]

    end = datetime.utcnow()
    start = end - timedelta(days=args.days)

    quotes = []
    for sym in syms:
        try:
            df = fetch_one(sym, start, end)
            outp = os.path.join(OUT_DIR, f"{sym.replace('.', '_')}.csv")
            df.to_csv(outp)
            # Letzter Close
            last = float(df["Close"].dropna().iloc[-1])
            quotes.append({"symbol": sym, "date": df.index[-1].strftime("%Y-%m-%d"), "close": last})
            print(f"✅ {sym}: {len(df)} rows")
        except Exception as e:
            print(f"ERR {sym}: {e}")

    if quotes:
        qdf = pd.DataFrame(quotes).sort_values(["symbol","date"])
        os.makedirs(os.path.dirname(QUOTES_CSV), exist_ok=True)
        qdf.to_csv(QUOTES_CSV, index=False)
        print(f"✔ wrote {QUOTES_CSV} with {len(qdf)} rows")
    else:
        print("⚠️ no quotes aggregated")

if __name__ == "__main__":
    main()
