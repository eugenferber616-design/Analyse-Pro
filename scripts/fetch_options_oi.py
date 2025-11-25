#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Options Data V40 - Tactical vs. Strategic Analysis.

Unterscheidet:
1. TACTICAL (Nächster Verfall): Gamma, Pinning, Max Pain.
2. STRATEGIC (LEAPS > 6 Monate): Großes OI, Institutionelle Levels.
"""

import os
import sys
import math
from datetime import datetime, timedelta
from typing import List, Dict

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm

# ──────────────────────────────────────────────────────────────
# Konfiguration
# ──────────────────────────────────────────────────────────────

RISK_FREE_RATE = 0.045
STRATEGIC_DAYS_THRESHOLD = 180  # Ab wann gilt es als "Langfristig/LEAPS"?

# ──────────────────────────────────────────────────────────────
# Math & Greeks Helpers
# ──────────────────────────────────────────────────────────────

def calculate_greeks_row(row, spot_price, time_to_expiry_years, risk_free_rate, opt_type):
    # (Identisch zu V39 - Black Scholes Fallback)
    current_delta = row.get("delta", 0)
    current_gamma = row.get("gamma", 0)
    
    if (current_delta != 0) and not pd.isna(current_delta):
        return current_delta, current_gamma

    sigma = row.get("impliedVolatility", 0)
    K = row.get("strike", 0)
    
    if sigma <= 0 or K <= 0 or spot_price <= 0:
        return 0.0, 0.0

    T = max(time_to_expiry_years, 0.001) 
    r = risk_free_rate
    S = spot_price

    try:
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        pdf_d1 = norm.pdf(d1)
        gamma_calc = pdf_d1 / (S * sigma * np.sqrt(T))
        
        if opt_type == "C":
            delta_calc = norm.cdf(d1)
        else:
            delta_calc = norm.cdf(d1) - 1.0
            
        return delta_calc, gamma_calc
    except Exception:
        return 0.0, 0.0

def calculate_max_pain(calls: pd.DataFrame, puts: pd.DataFrame) -> float:
    """
    Berechnet den Strike-Preis, bei dem die Option-Writer (Verkäufer)
    den geringsten Verlust erleiden (Max Pain Theory).
    """
    try:
        # Alle Strikes sammeln
        strikes = sorted(list(set(calls["strike"].tolist() + puts["strike"].tolist())))
        if not strikes:
            return 0.0
        
        loss_at_strike = []
        for s_curr in strikes:
            # Verlust für Call Writer bei Preis s_curr:
            # Wenn s_curr > k: Verlust = (s_curr - k) * OI
            c_loss = calls.apply(lambda row: max(0, s_curr - row["strike"]) * row["openInterest"], axis=1).sum()
            
            # Verlust für Put Writer bei Preis s_curr:
            # Wenn s_curr < k: Verlust = (k - s_curr) * OI
            p_loss = puts.apply(lambda row: max(0, row["strike"] - s_curr) * row["openInterest"], axis=1).sum()
            
            loss_at_strike.append(c_loss + p_loss)
            
        # Finde den Index mit dem geringsten Gesamtverlust
        min_loss_idx = np.argmin(loss_at_strike)
        return float(strikes[min_loss_idx])
    except Exception:
        return 0.0

# ──────────────────────────────────────────────────────────────
# Main Logic
# ──────────────────────────────────────────────────────────────

def main() -> int:
    os.makedirs("data/processed", exist_ok=True)
    
    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    
    # Symbole laden
    symbols = []
    if os.path.exists(wl_path):
        try:
            with open(wl_path, "r") as f:
                symbols = [line.strip().split("#")[0].strip() for line in f if line.strip()]
        except: pass
    if not symbols:
        symbols = ["SPY", "QQQ", "IWM", "NVDA", "MSFT", "AAPL"] # Default

    print(f"Analyzing {len(symbols)} symbols. Splitting Tactical vs. Strategic...")
    
    tactical_data = []  # Nächster Verfall
    strategic_data = [] # Langfristige LEAPS
    
    now = datetime.utcnow()

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)
            try:
                hist = tk.history(period="5d", interval="1d")
                spot = float(hist["Close"].iloc[-1])
            except:
                continue
                
            exps = tk.options
            if not exps: continue

            # 1. TACTICAL: Finde den nächsten sinnvollen Verfall (z.B. nächsten Freitag)
            # Wir nehmen den ersten Verfall, der mind. 2 Tage in der Zukunft liegt (um 0DTE noise zu meiden),
            # oder einfach den allernächsten.
            
            # Filtere vergangene
            valid_exps = [e for e in exps if datetime.strptime(e, "%Y-%m-%d") > now]
            if not valid_exps: continue
            
            next_expiry = valid_exps[0] # Der nächste Verfall (Tactical)
            
            # Suche LEAPS (Strategic)
            leaps_exps = [e for e in valid_exps if (datetime.strptime(e, "%Y-%m-%d") - now).days > STRATEGIC_DAYS_THRESHOLD]
            
            # --- PROCESS TACTICAL (Next Expiry) ---
            try:
                dt_exp = datetime.strptime(next_expiry, "%Y-%m-%d")
                days_to_exp = (dt_exp - now).days
                
                chain = tk.option_chain(next_expiry)
                calls = chain.calls.fillna(0)
                puts = chain.puts.fillna(0)
                
                # Cleanup
                for df in [calls, puts]:
                    if "openInterest" not in df.columns: df["openInterest"] = 0
                    if "impliedVolatility" not in df.columns: df["impliedVolatility"] = 0
                
                # Max Pain Calculation
                max_pain = calculate_max_pain(calls, puts)
                
                # GEX Calculation (Nur für Tactical relevant!)
                dt_exp_close = dt_exp + timedelta(hours=16)
                years = max((dt_exp_close - now).total_seconds() / (365*24*3600), 0.001)
                
                net_gex = 0
                for df, otype, sign in [(calls, "C", 1), (puts, "P", -1)]:
                    # Berechne Gamma
                    gammas = df.apply(lambda r: calculate_greeks_row(r, spot, years, RISK_FREE_RATE, otype)[1], axis=1)
                    # GEX Contribution: Gamma * OI * 100 * Spot (vereinfacht Gamma * OI * 100)
                    gex = gammas * df["openInterest"] * 100
                    # Net GEX: Call GEX - Put GEX (typischerweise)
                    # Aber hier einfacher: Net Exposure. 
                    # Konvention: Dealer ist Short Call (Long Gamma nötig -> +) ?? 
                    # Standard "SqueezeMetrics": Call = +GEX, Put = -GEX
                    net_gex += (gex.sum() * sign)

                # Top OI Strike (The Pin)
                top_c_oi = calls.sort_values("openInterest", ascending=False).iloc[0] if not calls.empty else None
                top_p_oi = puts.sort_values("openInterest", ascending=False).iloc[0] if not puts.empty else None
                
                tactical_data.append({
                    "Symbol": sym,
                    "Expiry": next_expiry,
                    "Days": days_to_exp,
                    "Spot": spot,
                    "Max_Pain": max_pain,
                    "Net_GEX": round(net_gex, 2),
                    "Call_Wall": top_c_oi["strike"] if top_c_oi is not None else 0,
                    "Put_Wall": top_p_oi["strike"] if top_p_oi is not None else 0,
                    "Top_Call_OI": int(top_c_oi["openInterest"]) if top_c_oi is not None else 0,
                    "Sentiment": "Bullish/Stable" if net_gex > 0 else "Bearish/Volatile"
                })
                
            except Exception as e:
                # print(f"Tactical Error {sym}: {e}")
                pass

            # --- PROCESS STRATEGIC (All LEAPS Combined) ---
            if leaps_exps:
                long_term_calls = []
                long_term_puts = []
                
                for lexp in leaps_exps:
                    try:
                        lchain = tk.option_chain(lexp)
                        c = lchain.calls.fillna(0)
                        p = lchain.puts.fillna(0)
                        c["expiry"] = lexp
                        p["expiry"] = lexp
                        long_term_calls.append(c)
                        long_term_puts.append(p)
                    except: continue
                
                if long_term_calls and long_term_puts:
                    all_c = pd.concat(long_term_calls)
                    all_p = pd.concat(long_term_puts)
                    
                    # Hier interessiert uns nur BULK OI
                    total_c_oi = all_c["openInterest"].sum()
                    total_p_oi = all_p["openInterest"].sum()
                    
                    # Wo liegen die großen Wetten? (Strikes über alle Laufzeiten summieren)
                    c_strike_grp = all_c.groupby("strike")["openInterest"].sum().sort_values(ascending=False)
                    p_strike_grp = all_p.groupby("strike")["openInterest"].sum().sort_values(ascending=False)
                    
                    top_long_call = c_strike_grp.index[0] if not c_strike_grp.empty else 0
                    top_long_put = p_strike_grp.index[0] if not p_strike_grp.empty else 0
                    
                    strategic_data.append({
                        "Symbol": sym,
                        "Spot": spot,
                        "Long_Term_PCR": round(total_p_oi / max(1, total_c_oi), 2),
                        "Big_Money_Call_Target": top_long_call,
                        "Big_Money_Put_Safety": top_long_put,
                        "Total_Leaps_Call_OI": int(total_c_oi),
                        "Total_Leaps_Put_OI": int(total_p_oi),
                        "Bias": "Long Term Bullish" if total_c_oi > total_p_oi else "Long Term Bearish"
                    })

            sys.stdout.write(".")
            sys.stdout.flush()

        except Exception as e:
            continue

    print("\nDone.")
    
    # Speichern
    if tactical_data:
        pd.DataFrame(tactical_data).to_csv("data/processed/tactical_next_expiry.csv", index=False)
        print("Tactical report saved.")
        
    if strategic_data:
        pd.DataFrame(strategic_data).to_csv("data/processed/strategic_leaps.csv", index=False)
        print("Strategic report saved.")
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
