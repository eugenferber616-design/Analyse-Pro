#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Options Data V41 - The "Three-Stage-Rocket".

1. TACTICAL (0-14 Tage): Gamma, Max Pain -> Timing.
2. MEDIUM   (15-120 Tage): Swing Magneten, Quartals-Levels -> Trend.
3. STRATEGIC (>120 Tage): LEAPS, Stock Replacement -> Big Picture.
"""

import os
import sys
import math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm

# ──────────────────────────────────────────────────────────────
# Settings
# ──────────────────────────────────────────────────────────────
RISK_FREE_RATE = 0.045
DAYS_TACTICAL_MAX = 14
DAYS_MEDIUM_MAX = 120  # Alles darüber ist Strategic

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def calculate_max_pain(calls, puts):
    try:
        strikes = sorted(list(set(calls["strike"].tolist() + puts["strike"].tolist())))
        if not strikes: return 0.0
        loss = []
        for s in strikes:
            c_l = calls.apply(lambda r: max(0, s - r["strike"]) * r["openInterest"], axis=1).sum()
            p_l = puts.apply(lambda r: max(0, r["strike"] - s) * r["openInterest"], axis=1).sum()
            loss.append(c_l + p_l)
        return float(strikes[np.argmin(loss)])
    except: return 0.0

def get_top_oi_strikes(df, n=1):
    if df.empty: return 0
    # Gruppieren nach Strike (falls mehrere Expiries im Bucket sind)
    grp = df.groupby("strike")["openInterest"].sum().sort_values(ascending=False)
    if grp.empty: return 0
    return grp.index[0]

# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    os.makedirs("data/processed", exist_ok=True)
    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    
    symbols = []
    if os.path.exists(wl_path):
        with open(wl_path, "r") as f:
            symbols = [line.strip().split("#")[0].strip() for line in f if line.strip()]
    if not symbols: symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD"]

    print(f"Processing V41 (3-Stages) for {len(symbols)} symbols...")

    res_tactical = []
    res_medium = []
    res_strategic = []

    now = datetime.utcnow()

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)
            try:
                hist = tk.history(period="5d", interval="1d")
                spot = float(hist["Close"].iloc[-1])
            except: continue
            
            exps = tk.options
            if not exps: continue

            # Bucket Container
            bucket_tactical = [] # (calls, puts, expiry_date)
            bucket_medium = []
            bucket_strategic = []

            # 1. Daten laden und sortieren
            for e_str in exps:
                try:
                    dt = datetime.strptime(e_str, "%Y-%m-%d")
                    days = (dt - now).days
                    
                    if days < 0: continue # Vergangen

                    # Fetch Chain
                    chain = tk.option_chain(e_str)
                    c = chain.calls.fillna(0)
                    p = chain.puts.fillna(0)
                    for df in [c, p]:
                        if "openInterest" not in df.columns: df["openInterest"] = 0
                        if "strike" not in df.columns: df["strike"] = 0
                    
                    data_tuple = (c, p, dt)

                    if days <= DAYS_TACTICAL_MAX:
                        bucket_tactical.append(data_tuple)
                    elif days <= DAYS_MEDIUM_MAX:
                        bucket_medium.append(data_tuple)
                    else:
                        bucket_strategic.append(data_tuple)
                except: continue

            # 2. ANALYSE: TACTICAL (Fokus auf Next Expiry Gamma & Pain)
            # Wir nehmen nur den allerersten (nächsten) Verfall für "Tactical Precision"
            if bucket_tactical:
                c, p, dt = bucket_tactical[0] 
                mp = calculate_max_pain(c, p)
                
                # Simple Net GEX Approximation (Call OI - Put OI anstatt komplexes Gamma, 
                # da Gamma bei yfinance oft fehlt. Für Richtung reicht OI oft als Proxy kurzfristig)
                # Besser: Wenn Strike nahe Spot -> Gamma hoch.
                # Hier nehmen wir Walls.
                c_wall = get_top_oi_strikes(c)
                p_wall = get_top_oi_strikes(p)

                res_tactical.append({
                    "Symbol": sym,
                    "Expiry": dt.strftime("%Y-%m-%d"),
                    "Spot": spot,
                    "Max_Pain": mp,
                    "Call_Wall_Tac": c_wall,
                    "Put_Wall_Tac": p_wall,
                    "Days": (dt - now).days
                })

            # 3. ANALYSE: MEDIUM (Der Swing-Trend)
            if bucket_medium:
                # Wir aggregieren ALLE Expiries im Medium Bucket (15-120 Tage)
                all_c = pd.concat([x[0] for x in bucket_medium])
                all_p = pd.concat([x[1] for x in bucket_medium])
                
                # Der "Quarterly Magnet" (Strike mit absolut meistem OI in diesem Zeitraum)
                swing_target_c = get_top_oi_strikes(all_c)
                swing_target_p = get_top_oi_strikes(all_p)
                
                total_c_oi = all_c["openInterest"].sum()
                total_p_oi = all_p["openInterest"].sum()
                
                res_medium.append({
                    "Symbol": sym,
                    "Spot": spot,
                    "Swing_Magnet_Call": swing_target_c, # Das Ziel der Bullen
                    "Swing_Magnet_Put": swing_target_p,  # Das Ziel der Bären
                    "Medium_PCR": round(total_p_oi / max(1, total_c_oi), 2),
                    "Bias_Medium": "Bullish" if total_c_oi > total_p_oi else "Bearish"
                })

            # 4. ANALYSE: STRATEGIC (Big Money / LEAPS)
            if bucket_strategic:
                all_c = pd.concat([x[0] for x in bucket_strategic])
                all_p = pd.concat([x[1] for x in bucket_strategic])
                
                leaps_c = get_top_oi_strikes(all_c)
                leaps_p = get_top_oi_strikes(all_p)
                
                res_strategic.append({
                    "Symbol": sym,
                    "Leaps_Target_Call": leaps_c,
                    "Leaps_Target_Put": leaps_p,
                    "Strategic_Bias": "Bullish" if all_c["openInterest"].sum() > all_p["openInterest"].sum() else "Bearish"
                })

            sys.stdout.write(".")
            sys.stdout.flush()

        except Exception as e:
            # print(e)
            continue

    print("\nSaving 3-Stage Reports...")
    
    if res_tactical: pd.DataFrame(res_tactical).to_csv("data/processed/tactical.csv", index=False)
    if res_medium: pd.DataFrame(res_medium).to_csv("data/processed/medium.csv", index=False)
    if res_strategic: pd.DataFrame(res_strategic).to_csv("data/processed/strategic.csv", index=False)
    
    print("Done.")

if __name__ == "__main__":
    main()
