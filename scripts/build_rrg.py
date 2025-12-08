#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_rrg.py
------------
Berechnet RRG (Relative Rotation Graph) Werte für US Sektoren vs SPY.
Output: data/processed/rrg_sectors.csv
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Konfiguration
BENCHMARK = "SPY"
SECTORS = {
    "XLE": "Energy",
    "XLB": "Materials",
    "XLI": "Industrials",
    "XLY": "Discretionary",
    "XLP": "Staples",
    "XLV": "Health Care",
    "XLF": "Financials",
    "XLK": "Technology",
    "XLC": "Comm. Services",
    "XLU": "Utilities",
    "XLRE": "Real Estate",
    "IWM": "Small Caps",
    "QQQ": "Nasdaq 100",
    "SMH": "Semiconductors"
}

def calc_rrg(series, window=14):
    # JdK RS-Ratio (Vereinfacht: Normalized Close / Rolling Mean / StdDev)
    # Hier: Einfache Relative Stärke + Momentum
    
    # 1. RS = Price / Benchmark
    rs = series
    
    # 2. RS-Ratio = (RS / MA(RS)) * 100
    # Wir nehmen 100 als Baseline
    ma = rs.rolling(window).mean()
    std = rs.rolling(window).std()
    
    # Zentrierung um 100
    rs_ratio = 100 + ((rs - ma) / std)
    
    # 3. RS-Momentum = Rate of Change von RS-Ratio
    # Wir glätten es leicht
    rs_mom = 100 + (rs_ratio.diff() * 10) # Skalierungsfaktor 10 für Sichtbarkeit
    
    return rs_ratio, rs_mom

def main():
    print("--- Building RRG Sector Rotation ---")
    os.makedirs("data/processed", exist_ok=True)
    
    tickers = list(SECTORS.keys()) + [BENCHMARK]
    start_date = (datetime.now() - timedelta(days=300)).strftime("%Y-%m-%d")
    
    # Download
    print(f"Lade Daten für {len(tickers)} Symbole...")
    data = yf.download(tickers, start=start_date, progress=False)['Close']
    
    if data.empty or BENCHMARK not in data.columns:
        print("Fehler: Keine Daten oder Benchmark fehlt.")
        return

    results = []
    
    bench = data[BENCHMARK]
    
    for sym, name in SECTORS.items():
        if sym not in data.columns: continue
        
        prices = data[sym]
        
        # Relative Stärke Linie
        rs_line = prices / bench
        
        # RRG Werte
        ratio, mom = calc_rrg(rs_line, window=14)
        
        # Letzte Werte
        curr_ratio = ratio.iloc[-1]
        curr_mom = mom.iloc[-1]
        
        # Quadrant bestimmen
        quadrant = "Unknown"
        if curr_ratio > 100 and curr_mom > 100: quadrant = "LEADING"
        elif curr_ratio > 100 and curr_mom < 100: quadrant = "WEAKENING"
        elif curr_ratio < 100 and curr_mom < 100: quadrant = "LAGGING"
        elif curr_ratio < 100 and curr_mom > 100: quadrant = "IMPROVING"
        
        # Trend Score (einfach: beide > 100 ist top)
        score = 0
        if quadrant == "LEADING": score = 2
        elif quadrant == "IMPROVING": score = 1
        elif quadrant == "WEAKENING": score = -1
        elif quadrant == "LAGGING": score = -2
        
        results.append({
            "Symbol": sym,
            "Name": name,
            "RS_Ratio": round(curr_ratio, 2),
            "RS_Momentum": round(curr_mom, 2),
            "Quadrant": quadrant,
            "Score": score
        })
        
    df = pd.DataFrame(results)
    df = df.sort_values("Score", ascending=False)
    
    out_path = "data/processed/rrg_sectors.csv"
    df.to_csv(out_path, index=False)
    print(f"✔ RRG gespeichert: {out_path}")
    print(df[["Symbol", "Quadrant", "RS_Ratio", "RS_Momentum"]].head(10).to_string(index=False))

if __name__ == "__main__":
    main()
