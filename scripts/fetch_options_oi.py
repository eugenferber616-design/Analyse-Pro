#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_options_oi.py
-------------------
Fetches Options Open Interest (OI) from Yahoo Finance.
Builds the base summary CSVs needed for the pipeline.

Outputs:
  - data/processed/options_oi_summary.csv
  - data/processed/options_oi_totals.csv
  - data/processed/options_oi_by_expiry.csv
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

def parse_args():
    parser = argparse.ArgumentParser()
    # Optional arguments if needed, but we mostly rely on ENV
    return parser.parse_args()

def get_watchlist():
    """Load watchlist from ENV or default file"""
    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    if not os.path.exists(wl_path):
        # Fallback
        return ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA", "AMD"]
    
    symbols = []
    with open(wl_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Handle CSV format (take first column)
            parts = line.split(',')
            sym = parts[0].strip().upper()
            if sym and sym != "SYMBOL":
                symbols.append(sym)
    return sorted(list(set(symbols)))

def fetch_options_for_symbol(sym):
    """
    Fetches option chain for a symbol.
    Returns:
      - valid (bool)
      - spot (float)
      - summary_dict (dict)
      - totals_list (list of dicts per expiry)
    """
    try:
        tk = yf.Ticker(sym)
        # Force fetch history to get spot
        hist = tk.history(period="5d")
        if hist.empty:
            return False, 0, {}, []
        
        spot = hist["Close"].iloc[-1]
        
        try:
            expiries = tk.options
        except:
            return False, spot, {}, []
            
        if not expiries:
            return False, spot, {}, []
            
        all_opts = []
        totals_by_exp = []
        
        # Limit expiries if needed (env var)
        # For base OI summary, we usually take ALL expiries to get total market structure
        
        total_call_oi = 0
        total_put_oi = 0
        
        # Iterate expiries
        for e_str in expiries:
            try:
                chain = tk.option_chain(e_str)
                calls = chain.calls
                puts = chain.puts
                
                c_oi = calls["openInterest"].fillna(0).sum() if not calls.empty else 0
                p_oi = puts["openInterest"].fillna(0).sum() if not puts.empty else 0
                
                total_call_oi += c_oi
                total_put_oi += p_oi
                
                totals_by_exp.append({
                    "symbol": sym,
                    "expiry": e_str,
                    "total_call_oi": int(c_oi),
                    "total_put_oi": int(p_oi),
                    "total_oi": int(c_oi + p_oi)
                })
                
                # Append to raw data for top strikes calc
                if not calls.empty:
                    calls = calls.copy()
                    calls["kind"] = "call"
                    calls["expiry"] = e_str
                    all_opts.append(calls)
                if not puts.empty:
                    puts = puts.copy()
                    puts["kind"] = "put"
                    puts["expiry"] = e_str
                    all_opts.append(puts)
                    
            except Exception as e:
                continue
                
        if not all_opts:
            return False, spot, {}, totals_by_exp

        # Concat
        df = pd.concat(all_opts, ignore_index=True)
        df["openInterest"] = pd.to_numeric(df["openInterest"], errors="coerce").fillna(0)
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        
        # Find Top Strikes (Global)
        # Groups by strike + kind
        g = df.groupby(["strike", "kind"])["openInterest"].sum().reset_index()
        
        # 3 Top Calls
        top_calls = g[g["kind"]=="call"].sort_values("openInterest", ascending=False).head(3)
        top_call_strikes = top_calls["strike"].tolist()
        
        # 3 Top Puts
        top_puts = g[g["kind"]=="put"].sort_values("openInterest", ascending=False).head(3)
        top_put_strikes = top_puts["strike"].tolist()
        
        summary = {
            "symbol": sym,
            "spot": round(spot, 2),
            "total_call_oi": int(total_call_oi),
            "total_put_oi": int(total_put_oi),
            "pcr_total": round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0,
            "call_top_strikes": str(top_call_strikes),
            "put_top_strikes": str(top_put_strikes),
            # Simple fallback HV if we don't have it yet
            "hv_current": 0.16 # Default hook, will be enriched later if available
        }
        
        return True, spot, summary, totals_by_exp
        
    except Exception as e:
        print(f"Error fetching {sym}: {e}")
        return False, 0, {}, []

def main():
    os.makedirs("data/processed", exist_ok=True)
    
    tickers = get_watchlist()
    print(f"Fetching Options data for {len(tickers)} symbols...")
    
    summary_list = []
    totals_list_flat = []
    
    for sym in tickers:
        ok, spot, summ, totals = fetch_options_for_symbol(sym)
        if ok:
            summary_list.append(summ)
            totals_list_flat.extend(totals)
            print(f"  {sym}: {summ['total_call_oi']+summ['total_put_oi']} OI")
        else:
            print(f"  {sym}: No data")

    if not summary_list:
        print("No data fetched.")
        # Create empty file to prevent pipeline crash? Or fail?
        # Better to save empty dataframe with columns
        pd.DataFrame(columns=["symbol", "spot", "total_call_oi", "total_put_oi", "pcr_total"]).to_csv("data/processed/options_oi_summary.csv", index=False)
        return

    # 1. Save Summary
    df_sum = pd.DataFrame(summary_list)
    out_sum = "data/processed/options_oi_summary.csv"
    df_sum.to_csv(out_sum, index=False)
    print(f"Saved {out_sum}")
    
    # 2. Save Totals (by expiry)
    if totals_list_flat:
        df_tot = pd.DataFrame(totals_list_flat)
        out_tot = "data/processed/options_oi_by_expiry.csv"
        df_tot.to_csv(out_tot, index=False)
        print(f"Saved {out_tot}")
        
        # 3. Create options_oi_totals.csv (Max Expiry)
        # Finds the expiry with max OI for each symbol
        df_tot_ag = df_tot.sort_values("total_oi", ascending=False).groupby("symbol").first().reset_index()
        df_tot_ag = df_tot_ag.rename(columns={"expiry": "max_oi_expiry", "total_oi": "max_oi_value"})
        df_tot_ag.to_csv("data/processed/options_oi_totals.csv", index=False)
        print("Saved data/processed/options_oi_totals.csv")

if __name__ == "__main__":
    main()

