#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch daily OHLC from Stooq
- One CSV per symbol: data/market/stooq/<symbol>.csv  (date,open,high,low,close,volume)
- Combined file: data/processed/stooq_quotes.csv (symbol,date,close, ...)
Usage:
  python scripts/fetch_stooq.py --symbols "^dax,SAP.DE,AAPL.US"
  python scripts/fetch_stooq.py --watchlist watchlists/mylist.txt
"""

import os, argparse, time
import pandas as pd
import requests

BASE = "https://stooq.com/q/d/l/"
OUT_DIR = "data/market/stooq"
os.makedirs(OUT_DIR, exist_ok=True)
PROC_DIR = "data/processed"
os.makedirs(PROC_DIR, exist_ok=True)

def read_watchlist(path):
    if not path or not os.path.exists(path):
        return []
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): 
                continue
            syms.append(s)
    return syms

def stooq_fetch(symbol, retries=3, timeout=30):
    params = {"s": symbol.lower(), "i": "d"}   # daily
    for attempt in range(1, retries+1):
        try:
            r = requests.get(BASE, params=params, timeout=timeout)
            if r.status_code == 200 and "Date,Open,High,Low,Close,Volume" in r.text:
                df = pd.read_csv(pd.compat.StringIO(r.text))
                # normalize
                df.columns = [c.lower() for c in df.columns]
                df.rename(columns={"date":"date","open":"open","high":"high","low":"low","close":"close","volume":"volume"}, inplace=True)
                return df
        except requests.RequestException:
            pass
        time.sleep(1.2*attempt)
    raise RuntimeError(f"Stooq fetch failed for {symbol}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", help="comma separated list like ^dax,SAP.DE,AAPL.US")
    ap.add_argument("--watchlist", help="optional watchlist file")
    args = ap.parse_args()

    symbols = []
    if args.symbols:
        symbols += [s.strip() for s in args.symbols.split(",") if s.strip()]
    symbols += read_watchlist(args.watchlist)
    symbols = sorted(set(symbols))
    if not symbols:
        print("No symbols given.")
        return

    rows = []
    for sym in symbols:
        try:
            df = stooq_fetch(sym)
            out = os.path.join(OUT_DIR, f"{sym.replace('^','idx_').lower()}.csv")
            df.to_csv(out, index=False)
            # add to combined (last close only to keep small, or full?)
            last = df.tail(1).copy()
            last.insert(0, "symbol", sym.upper())
            rows.append(last)
            print(f"OK {sym}: {len(df)} rows")
        except Exception as e:
            print(f"ERR {sym}: {e}")

    if rows:
        comb = pd.concat(rows, ignore_index=True)
        comb.to_csv(os.path.join(PROC_DIR,"stooq_quotes.csv"), index=False)

if __name__ == "__main__":
    main()
