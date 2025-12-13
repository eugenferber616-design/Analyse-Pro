
import os
import sys
import pandas as pd
import yfinance as yf
from datetime import datetime
import re

def parse_iso_date(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip()
    if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    return None

def make_fiscal_period(year, quarter, period_str: str | None) -> str | None:
    return str(period_str)

def main():
    symbol = "AMZN"
    print(f"Testing earnings fetch for {symbol}...")
    
    tk = yf.Ticker(symbol)
    
    # Simulate fetch_earnings_results.py logic EXACTLY
    limit = 16
    
    try:
        ed = getattr(tk, "earnings_dates", None)
        print(f"ed attribute: {ed}")
        
        if callable(ed):
            print("ed is callable, calling...")
            df = ed(limit=limit)
            print(f"df type: {type(df)}")
            print(f"df shape: {df.shape if hasattr(df, 'shape') else 'None'}")
            print("DF Content Head:")
            print(df.head() if hasattr(df, "head") else "Not a DF")
            
            if df is not None and hasattr(df, "reset_index"):
                print("Resetting index...")
                dfe = df.reset_index().rename(columns={
                    "Earnings Date": "report_date",
                    "Reported EPS": "eps_actual",
                    "EPS Estimate": "eps_estimate",
                    "Surprise(%)": "surprise_pct",
                })
                print(f"DFE Columns: {dfe.columns}")
                print(dfe.head())
            else:
                print("DF is None or has no reset_index")
        else:
            print("ed is not callable")

    except Exception as e:
        print(f"EXCEPTION: {e}")

if __name__ == "__main__":
    main()
