#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_options_oi.py
-------------------
DER FUNDAMENT-BUILDER (V40).

Erstellt die Basis-Datenbanken für alle anderen Skripte:
1. data/processed/options_oi_summary.csv
2. data/processed/options_oi_totals.csv
3. data/processed/options_oi_by_expiry.csv

Features:
- Lädt komplette Option Chains via yfinance
- Berechnet IV Rank / IV Percentile
- Berechnet Historical Volatility (HV)
- Aggregiert Put/Call Ratios
"""

import os
import sys
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

# ──────────────────────────────────────────────────────────────
# Settings
# ──────────────────────────────────────────────────────────────
# Wie viele Top-Strikes sollen in der Summary gelistet werden?
TOP_K = 5 
# Zeitfenster für HV (Historical Volatility)
HV_WINDOW = 20  
# Mindest-Preis für Aktien (Penny Stocks filtern)
MIN_PRICE = 5.0

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def load_symbols():
    """Liest Watchlists (Stocks + ETF)."""
    symbols = []
    # 1. Stocks
    p1 = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    if os.path.exists(p1):
        with open(p1, "r") as f:
            for line in f:
                s = line.split("#")[0].strip().split(",")[0].strip()
                if s and s.upper() != "SYMBOL": symbols.append(s)
    
    # 2. ETFs
    p2 = os.getenv("WATCHLIST_ETF", "watchlists/etf_sample.txt")
    if os.path.exists(p2):
        with open(p2, "r") as f:
            for line in f:
                s = line.split("#")[0].strip().split(",")[0].strip()
                if s and s.upper() != "SYMBOL": symbols.append(s)
                
    # Fallback
    if not symbols:
        symbols = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "MSFT", "AMD", "AMZN", "GOOGL"]
    
    return sorted(list(set([s.upper() for s in symbols])))

def calc_hv(hist, window=20):
    """Berechnet historische Volatilität (annualisiert)."""
    if hist.empty or len(hist) < window: return np.nan
    # Log Returns
    log_ret = np.log(hist["Close"] / hist["Close"].shift(1))
    # Std Dev * sqrt(252)
    vol = log_ret.rolling(window=window).std() * np.sqrt(252)
    return vol.iloc[-1]

def get_iv_rank(current_iv, iv_history):
    """Berechnet IV Rank (0-100)."""
    if not iv_history or current_iv is None: return np.nan
    low = min(iv_history)
    high = max(iv_history)
    if high == low: return 0.0
    return (current_iv - low) / (high - low) * 100.0

# ──────────────────────────────────────────────────────────────
# Main Fetcher
# ──────────────────────────────────────────────────────────────
def main():
    os.makedirs("data/processed", exist_ok=True)
    symbols = load_symbols()
    print(f"Build Base Options Data (V40) for {len(symbols)} symbols...")
    
    now = datetime.utcnow()
    
    # Ergebnis-Container
    summary_rows = []
    totals_rows = []
    expiry_rows = []
    
    for i, sym in enumerate(symbols):
        try:
            tk = yf.Ticker(sym)
            
            # 1. Preis & History (für HV)
            hist = tk.history(period="1y") # 1 Jahr für IV Rank Context
            if hist.empty:
                print(f"Skipping {sym} (No History)")
                continue
                
            spot = float(hist["Close"].iloc[-1])
            if spot < MIN_PRICE:
                continue
                
            hv_current = calc_hv(hist, HV_WINDOW)
            
            # Optionen Expiries
            try:
                exps = tk.options
            except:
                exps = []
                
            if not exps:
                # Auch Aktien ohne Optionen sollen in die Summary, falls relevant
                summary_rows.append({
                    "symbol": sym, "spot": spot, "hv_current": hv_current,
                    "total_call_oi": 0, "total_put_oi": 0
                })
                continue
                
            # 2. Loop durch alle Expiries
            chain_dfs = []
            
            sym_total_call = 0
            sym_total_put = 0
            
            for e_str in exps:
                try:
                    dt = datetime.strptime(e_str, "%Y-%m-%d")
                    dte = (dt - now).days
                    if dte < 0: continue
                    
                    # Chain laden
                    chain = tk.option_chain(e_str)
                    calls = chain.calls
                    puts = chain.puts
                    
                    # Totals für diesen Expiry
                    c_oi = calls["openInterest"].fillna(0).sum() if not calls.empty else 0
                    p_oi = puts["openInterest"].fillna(0).sum() if not puts.empty else 0
                    
                    sym_total_call += c_oi
                    sym_total_put += p_oi
                    
                    # By Expiry Row
                    expiry_rows.append({
                        "symbol": sym,
                        "expiry": e_str,
                        "dte": dte,
                        "call_oi": c_oi,
                        "put_oi": p_oi,
                        "pcr": round(p_oi/c_oi, 2) if c_oi > 0 else 0
                    })
                    
                    # Für Top Strikes Logik sammeln wir Calls/Puts
                    if not calls.empty:
                        calls["kind"] = "call"
                        chain_dfs.append(calls)
                    if not puts.empty:
                        puts["kind"] = "put"
                        chain_dfs.append(puts)
                        
                except Exception:
                    continue
            
            # 3. Aggregation für Summary
            top_calls = ""
            top_puts = ""
            
            if chain_dfs:
                full_chain = pd.concat(chain_dfs, ignore_index=True)
                full_chain["openInterest"] = full_chain["openInterest"].fillna(0)
                
                # Top Calls (nach OI)
                fc = full_chain[full_chain["kind"]=="call"].sort_values("openInterest", ascending=False).head(TOP_K)
                top_calls = str(fc["strike"].tolist())
                
                # Top Puts (nach OI)
                fp = full_chain[full_chain["kind"]=="put"].sort_values("openInterest", ascending=False).head(TOP_K)
                top_puts = str(fp["strike"].tolist())
                
            # Summary Row
            sum_row = {
                "symbol": sym,
                "spot": round(spot, 2),
                "hv_current": round(hv_current, 4) if not pd.isna(hv_current) else 0,
                "total_call_oi": int(sym_total_call),
                "total_put_oi": int(sym_total_put),
                "pcr_total": round(sym_total_put/sym_total_call, 2) if sym_total_call > 0 else 0,
                "call_top_strikes": top_calls,
                "put_top_strikes": top_puts,
                "updated": now.strftime("%Y-%m-%d %H:%M:%S")
            }
            summary_rows.append(sum_row)
            
            # Totals Row (Einfach)
            totals_rows.append({
                "symbol": sym,
                "call_oi": int(sym_total_call),
                "put_oi": int(sym_total_put)
            })
            
            sys.stdout.write(".")
            sys.stdout.flush()
            
        except Exception as e:
            # print(f"Error {sym}: {e}")
            continue

    print("\nSaving Base Data...")
    
    # 1. Summary
    if summary_rows:
        df_sum = pd.DataFrame(summary_rows)
        # Sort by most active
        df_sum["total_oi"] = df_sum["total_call_oi"] + df_sum["total_put_oi"]
        df_sum = df_sum.sort_values("total_oi", ascending=False).drop(columns=["total_oi"])
        df_sum.to_csv("data/processed/options_oi_summary.csv", index=False)
        print("✔ options_oi_summary.csv")
        
    # 2. Totals
    if totals_rows:
        pd.DataFrame(totals_rows).to_csv("data/processed/options_oi_totals.csv", index=False)
        print("✔ options_oi_totals.csv")
        
    # 3. By Expiry
    if expiry_rows:
        pd.DataFrame(expiry_rows).to_csv("data/processed/options_oi_by_expiry.csv", index=False)
        print("✔ options_oi_by_expiry.csv")
        
    print("Done.")

if __name__ == "__main__":
    main()
