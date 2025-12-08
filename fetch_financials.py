#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_financials_ts.py (Yahoo Finance Edition)
----------------------------------------------
Lädt historische Quartalszahlen (Income, Balance, Cashflow) via yfinance.
Ersetzt die Finnhub-Variante, da diese im Free-Tier oft leer ist.

Output: data/processed/financials_timeseries.csv
"""

import os
import sys
import pandas as pd
import yfinance as yf
import numpy as np
from datetime import datetime

# ──────────────────────────────────────────────────────────────
# Settings
# ──────────────────────────────────────────────────────────────
WATCHLIST = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OUT_FILE = "data/processed/financials_timeseries.csv"

# Mapping: Yahoo Felder -> Unsere CSV Spalten
MAP_IC = {
    "Total Revenue": "revenue",
    "Gross Profit": "grossProfit",
    "Operating Income": "operatingIncome",
    "Net Income": "netIncome",
    "Basic EPS": "eps",
    "Diluted EPS": "epsDiluted",
    "Research And Development": "researchAndDevelopment"
}

MAP_BS = {
    "Total Assets": "totalAssets",
    "Total Liabilities Net Minority Interest": "totalLiabilities",
    "Total Debt": "totalDebt",
    "Cash And Cash Equivalents": "cashAndCashEquivalents",
    "Stockholders Equity": "shareholdersEquity",
    "Inventory": "inventory"
}

MAP_CF = {
    "Operating Cash Flow": "operatingCashFlow",
    "Capital Expenditure": "capitalExpenditure",
    "Free Cash Flow": "freeCashFlow", # Manchmal direkt da, sonst berechnet
    "Repayment Of Debt": "debtRepayment"
}

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def load_watchlist():
    if not os.path.exists(WATCHLIST): return []
    syms = []
    with open(WATCHLIST, "r") as f:
        for line in f:
            if "," in line: line = line.split(",")[0]
            s = line.strip().upper()
            if s and not s.startswith("#") and s != "SYMBOL":
                syms.append(s)
    return sorted(list(set(syms)))

def process_dataframe(sym, df, mapping, statement_type):
    rows = []
    if df is None or df.empty: return rows
    
    # Yahoo liefert Spalten als Datum (z.B. 2023-09-30)
    # Wir iterieren durch die Spalten (Zeitpunkte)
    for date_col in df.columns:
        date_str = date_col.strftime("%Y-%m-%d")
        
        for yf_key, my_key in mapping.items():
            try:
                if yf_key in df.index:
                    val = df.loc[yf_key, date_col]
                    if pd.notna(val):
                        rows.append({
                            "symbol": sym,
                            "statement": statement_type,
                            "period": date_str,
                            "metric": my_key,
                            "value": float(val)
                        })
            except: pass
    return rows

# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    os.makedirs("data/processed", exist_ok=True)
    symbols = load_watchlist()
    print(f"Fetching Financials (YFinance) for {len(symbols)} symbols...")
    
    all_rows = []
    
    for sym in symbols:
        try:
            tk = yf.Ticker(sym)
            
            # 1. Income Statement (Quarterly)
            ic = tk.quarterly_income_stmt
            all_rows.extend(process_dataframe(sym, ic, MAP_IC, "ic"))
            
            # 2. Balance Sheet (Quarterly)
            bs = tk.quarterly_balance_sheet
            all_rows.extend(process_dataframe(sym, bs, MAP_BS, "bs"))
            
            # 3. Cash Flow (Quarterly)
            cf = tk.quarterly_cashflow
            all_rows.extend(process_dataframe(sym, cf, MAP_CF, "cf"))
            
            sys.stdout.write(".")
            sys.stdout.flush()
            
        except Exception as e:
            # print(f"Err {sym}: {e}")
            continue

    if all_rows:
        df = pd.DataFrame(all_rows)
        # Sortieren
        df = df.sort_values(["symbol", "statement", "period", "metric"])
        df.to_csv(OUT_FILE, index=False)
        print(f"\n✔ Saved {len(df)} rows to {OUT_FILE}")
    else:
        print("\n⚠ No data found.")

if __name__ == "__main__":
    main()
