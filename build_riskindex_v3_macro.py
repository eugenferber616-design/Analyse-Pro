#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex_v3_macro.py
---------------------------
V3: Erweitert den "Sniper" Risk Index um die V10 Deep Macro Indikatoren von Pine Script.

Zusätzliche Indikatoren:
1. Funding Stress (SOFR vs Yields)
2. Net Liquidity (Fed Balance Sheet)
3. Credit Spreads (OAS)

Output:
  - data/processed/macro_status.json (Für Dashboard Dashboard Warning Lights)
"""

import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime

# --- KONFIGURATION ---
OUTDIR = Path("data/processed")
START_DATE = "2020-01-01" # Wir brauchen nur recent history für Snapshot

def fetch_macro_data():
    print("--- Lade V10 Macro Daten ---")
    
    # 1. FRED Data
    tickers_fred = [
        "DGS3MO",      # 3M Yield
        "SOFR",        # Secured Overnight Financing Rate
        "WALCL",       # Fed Total Assets
        "WDTGAL",      # Treasury General Account
        "RRPONTSYD",   # Reverse Repo
        "BAMLC0A0CM",  # US Corp Master Option-Adjusted Spread (IG)
        "BAMLH0A0HYM2" # US High Yield Master II Option-Adjusted Spread
    ]
    
    try:
        df_fred = web.DataReader(tickers_fred, 'fred', START_DATE, datetime.now())
        df_fred = df_fred.resample('D').ffill()
    except Exception as e:
        print(f"[WARN] FRED fetch failed: {e}")
        return None

    return df_fred

def calc_normalize(series, min_v, max_v, inverse=False):
    clipped = series.clip(min_v, max_v)
    scaled = (clipped - min_v) / (max_v - min_v)
    if inverse:
        score = (1.0 - scaled) * 100
    else:
        score = scaled * 100
    return score

def get_traffic_light(score):
    if score < 30: return "RED"
    if score < 60: return "YELLOW"
    return "GREEN"

def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    
    df = fetch_macro_data()
    if df is None: return
    
    # --- 1. Funding Stress ---
    # Logic: SOFR - 3M Yield. 
    # Normal: SOFR < Yield (Spread negative). 
    # Stress: SOFR > Yield (Spread positive -> Liquidity crunch).
    # Range: -0.2 (Good) to +0.2 (Bad)
    fund_spread = df['SOFR'] - df['DGS3MO']
    # Inverse: High Spread is BAD (Score 0), Low Spread is GOOD (Score 100)
    score_funding = calc_normalize(fund_spread, -0.2, 0.2, inverse=True)
    
    # --- 2. Net Liquidity ---
    # Logic: Fed Assets - TGA - RRP
    # Wir schauen auf 4-Wochen Veränderung (Momentum)
    # Unit: Walcl is millions, others billions. Normalize to Billions.
    nl = (df['WALCL']/1000) - df['WDTGAL'] - df['RRPONTSYD']
    nl_chg = nl.diff(20) # 20 days ~ 4 weeks
    # Range: -200B (Bad) to +200B (Good)
    score_liq = calc_normalize(nl_chg, -200, 200, inverse=False)
    
    # --- 3. Credit Spreads ---
    # Logic: High Yield OAS.
    # Range: 3.0 (Good) to 8.0 (Bad/Panic)
    score_credit = calc_normalize(df['BAMLH0A0HYM2'], 3.0, 8.0, inverse=True)
    
    # --- Snapshot ---
    # Fill NaNs in columns to ensure we have values
    score_funding = score_funding.fillna(50) # Neutral fallback
    score_liq = score_liq.fillna(50)
    score_credit = score_credit.fillna(50)
    fund_spread = fund_spread.fillna(0)
    nl_chg = nl_chg.fillna(0)
    df['BAMLH0A0HYM2'] = df['BAMLH0A0HYM2'].fillna(4.0)
    
    # Recalculate last metrics
    last_fund = fund_spread.iloc[-1]
    last_liq_chg = nl_chg.iloc[-1]
    last_credit = df['BAMLH0A0HYM2'].iloc[-1]
    
    snapshot = {
        "asof": df.index[-1].strftime('%Y-%m-%d'),
        "indicators": {
            "funding_stress": {
                "name": "Funding Stress",
                "value": f"{last_fund:.2f}%", 
                "score": int(score_funding.iloc[-1]),
                "status": get_traffic_light(score_funding.iloc[-1]),
                "desc": "SOFR vs 3M Yield"
            },
            "net_liquidity": {
                "name": "Net Liquidity (4w)",
                "value": f"{last_liq_chg:+.0f}B",
                "score": int(score_liq.iloc[-1]),
                "status": get_traffic_light(score_liq.iloc[-1]),
                "desc": "Fed Balance Sheet Momentum"
            },
            "credit_spread": {
                "name": "Credit Spreads",
                "value": f"{last_credit:.2f}%",
                "score": int(score_credit.iloc[-1]),
                "status": get_traffic_light(score_credit.iloc[-1]),
                "desc": "High Yield OAS"
            }
        }
    }
    
    out_json = OUTDIR / "macro_status.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
        
    print(f"[OK] Macro Status Exportiert: {out_json}")
    print(json.dumps(snapshot, indent=2))

if __name__ == "__main__":
    main()
