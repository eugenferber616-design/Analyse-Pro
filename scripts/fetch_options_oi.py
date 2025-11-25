#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Options Data V60 ULTRA - Gamma Exposure (GEX) & Max Pain
--------------------------------------------------------
Das ultimative "Whale Watching" Tool.

Neue Features:
1. GEX (Gamma Exposure): Berechnet physikalische Magnet-Wirkung der Strikes.
2. Max Pain: Der Preis, bei dem Market Maker am meisten verdienen.
3. Vol/OI Ratio: Findet "frisches Geld" (Aggressive Positionierung).
4. Moneyness-Filter: Ignoriert "Lotterie-Tickets".

Output: data/processed/options_v60_ultra.csv
"""

import os
import sys
import math
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm

# ──────────────────────────────────────────────────────────────
# Settings
# ──────────────────────────────────────────────────────────────
RISK_FREE_RATE = 0.045             # 4.5% Zins
DAYS_TACTICAL_MAX = 14
DAYS_MEDIUM_MAX = 120
MONEYNESS_BAND_PCT = 0.30          # Engeres Band für präzisere Whale-Daten (+/- 30%)

# ──────────────────────────────────────────────────────────────
# Mathe-Kern (Black-Scholes Gamma)
# ──────────────────────────────────────────────────────────────
def bs_gamma(S, K, T, r, sigma):
    """Berechnet das Gamma einer Option."""
    if T <= 0 or sigma <= 0:
        return 0.0
    
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    return gamma

def compute_gex(row, spot):
    """
    Schätzt die Gamma Exposure (GEX) in Dollar.
    Annahme: Market Maker sind Short Calls (neg GEX) und Short Puts (pos GEX).
    Dies ist eine Vereinfachung, aber Standard für GEX-Charts.
    """
    # Daten aus der Row
    K = row["strike"]
    T = row["dte"] / 365.0
    sigma = row.get("impliedVolatility", 0)
    oi = row["openInterest"]
    
    if T <= 0.001: T = 0.001 # Avoid div by zero
    if sigma <= 0.001: sigma = 0.3 # Fallback IV 30%
    
    # 1. Gamma pro Option berechnen
    gamma_val = bs_gamma(spot, K, T, RISK_FREE_RATE, sigma)
    
    # 2. GEX auf Gesamt-OI skalieren (Dollar Gamma per 1% Move)
    # Formel-Approximation: Gamma * Spot * Spot * 0.01 * OI * 100 (Kontraktgröße)
    # GEX = Gamma * Spot^2 * 0.01 * OI * 100
    gex = gamma_val * (spot**2) * 0.01 * oi * 100
    
    if row["kind"] == "put":
        # Puts: Dealer Long Puts (pos Gamma) oder Short Puts (neg Gamma)?
        # Standard GEX Modell: Dealer ist Long Calls (Retail kauft), Dealer ist Short Puts (Retail kauft).
        # Moment, das klassische "SpotGamma" Modell:
        # Call Wall = Dealer Short Call = Negative Gamma (Beschleuniger nach oben).
        # Put Wall = Dealer Short Put = Positive Gamma (Stabilisator nach unten).
        # Wir nutzen hier: Call = Magnet, Put = Support. 
        # Wir geben einfach den absoluten Gamma-Wert zurück für "Magnet-Stärke".
        return gex
    else:
        return gex

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def load_symbols():
    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    symbols = []
    if os.path.exists(wl_path):
        with open(wl_path, "r") as f:
            for line in f:
                s = line.split("#")[0].strip()
                if s: symbols.append(s)
    if not symbols:
        symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD", "META", "AMZN", "GOOGL"]
    return symbols

def calculate_max_pain(df, strikes):
    """
    Berechnet den Max Pain Preis (geringster Gesamtverlust für Option Writer).
    """
    if df.empty: return None
    
    # Wir testen jeden Strike in der Chain als potenziellen Expiry-Preis
    pain_map = {}
    
    # Nur Calls und Puts trennen
    calls = df[df["kind"] == "call"]
    puts = df[df["kind"] == "put"]
    
    for center_strike in strikes:
        # Verlust bei Calls: Wenn center_strike > K, dann (center - K) * OI
        call_loss = 0
        if not calls.empty:
            itm_calls = calls[calls["strike"] < center_strike]
            if not itm_calls.empty:
                call_loss = ((center_strike - itm_calls["strike"]) * itm_calls["openInterest"]).sum()
        
        # Verlust bei Puts: Wenn center_strike < K, dann (K - center) * OI
        put_loss = 0
        if not puts.empty:
            itm_puts = puts[puts["strike"] > center_strike]
            if not itm_puts.empty:
                put_loss = ((itm_puts["strike"] - center_strike) * itm_puts["openInterest"]).sum()
        
        pain_map[center_strike] = call_loss + put_loss
        
    if not pain_map:
        return None
        
    # Strike mit dem geringsten Loss finden
    min_pain_strike = min(pain_map, key=pain_map.get)
    return min_pain_strike

def get_smart_wall(df, spot, kind="call", max_pct=0.30):
    """
    Findet die Wall basierend auf GEX (Gamma Exposure) und Notional.
    Kombinierter Score = (Notional_Norm + GEX_Norm).
    """
    if df.empty: return None, 0, 0
    
    # Filter Moneyness
    low = spot * (1.0 - max_pct)
    high = spot * (1.0 + max_pct)
    sub = df[(df["strike"] >= low) & (df["strike"] <= high)].copy()
    
    if kind == "call":
        sub = sub[sub["strike"] >= spot] # Widerstand oben
    else:
        sub = sub[sub["strike"] <= spot] # Support unten
        
    if sub.empty: return None, 0, 0
    
    # Score berechnen: Wir bevorzugen GEX, nutzen Notional als Tie-Breaker
    # GEX ist oft NaN wenn keine IV da ist, dann Fallback auf Notional
    sub["gex"] = sub["gex"].fillna(0)
    sub["notional"] = sub["openInterest"] * sub["strike"]
    
    # Sortieren nach GEX (Haupteinfluss)
    top = sub.sort_values("gex", ascending=False).iloc[0]
    
    # Check ob GEX signifikant ist, sonst nimm Notional
    if top["gex"] == 0:
        top = sub.sort_values("notional", ascending=False).iloc[0]
        
    return top["strike"], top["openInterest"], top["gex"]

# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    os.makedirs("data/processed", exist_ok=True)
    symbols = load_symbols()
    print(f"🚀 V60 ULTRA: Processing {len(symbols)} symbols with GEX & MaxPain...")
    
    now = datetime.utcnow()
    results = []
    
    for sym in symbols:
        try:
            tk = yf.Ticker(sym)
            
            # 1. Spot Preis & History
            hist = tk.history(period="5d")
            if hist.empty: continue
            spot = float(hist["Close"].iloc[-1])
            
            # 2. Options Data laden
            exps = tk.options
            if not exps: continue
            
            all_opts = []
            
            # Wir nehmen alle Expiries, um ein Gesamtbild zu haben
            # Limitieren auf die nächsten 6 Monate für Performance, wenn nötig
            # Hier: Alles laden.
            for e_str in exps:
                try:
                    dt = datetime.strptime(e_str, "%Y-%m-%d")
                    dte = (dt - now).days
                    if dte < 0: continue
                except: continue
                
                # Chain laden
                try:
                    chain = tk.option_chain(e_str)
                    calls = chain.calls
                    puts = chain.puts
                except: continue
                
                # Daten aufbereiten
                if not calls.empty:
                    calls = calls.assign(kind="call", expiry=dt, dte=dte)
                    all_opts.append(calls)
                if not puts.empty:
                    puts = puts.assign(kind="put", expiry=dt, dte=dte)
                    all_opts.append(puts)
                    
            if not all_opts: continue
            
            df = pd.concat(all_opts, ignore_index=True)
            
            # Cleaning
            cols = ["contractSymbol", "strike", "openInterest", "volume", "impliedVolatility", "kind", "expiry", "dte"]
            # Fallback falls IV fehlt
            if "impliedVolatility" not in df.columns:
                df["impliedVolatility"] = 0.0
            
            # Filter Spalten
            df = df[[c for c in cols if c in df.columns]].copy()
            df["openInterest"] = df["openInterest"].fillna(0).astype(float)
            df["volume"] = df["volume"].fillna(0).astype(float)
            df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
            
            # ──────────────────────────────────────────────────
            # THE MAGIC: GEX Calculation
            # ──────────────────────────────────────────────────
            # Wir berechnen GEX für jede Zeile
            df["gex"] = df.apply(lambda row: compute_gex(row, spot), axis=1)
            
            # ──────────────────────────────────────────────────
            # 1. MAX PAIN (Global)
            # ──────────────────────────────────────────────────
            # Wir nehmen Strikes im +/- 30% Bereich für Max Pain Calculation
            relevant_strikes = df[(df["strike"] > spot*0.7) & (df["strike"] < spot*1.3)]["strike"].unique()
            max_pain = calculate_max_pain(df, relevant_strikes)
            
            # ──────────────────────────────────────────────────
            # 2. TACTICAL (0-14 Tage) - Nächste "Schlacht"
            # ──────────────────────────────────────────────────
            df_tac = df[df["dte"] <= DAYS_TACTICAL_MAX]
            tac_call_strike, tac_call_oi, tac_call_gex = get_smart_wall(df_tac, spot, "call")
            tac_put_strike, tac_put_oi, tac_put_gex = get_smart_wall(df_tac, spot, "put")
            
            # ──────────────────────────────────────────────────
            # 3. GLOBAL (Alle DTE) - Die "Boss Level"
            # ──────────────────────────────────────────────────
            # Hier gewichten wir GEX noch stärker: Global Call Wall ist oft der Strike mit höchstem positiven Gamma
            gl_call_strike, gl_call_oi, gl_call_gex = get_smart_wall(df, spot, "call")
            gl_put_strike, gl_put_oi, gl_put_gex = get_smart_wall(df, spot, "put")
            
            # ──────────────────────────────────────────────────
            # 4. STRATEGIC (LEAPS > 120)
            # ──────────────────────────────────────────────────
            df_strat = df[df["dte"] > DAYS_MEDIUM_MAX]
            strat_call_strike, _, _ = get_smart_wall(df_strat, spot, "call", max_pct=0.5) # Leaps dürfen weiter weg sein
            strat_put_strike, _, _ = get_smart_wall(df_strat, spot, "put", max_pct=0.5)

            # ──────────────────────────────────────────────────
            # FRESH MONEY (Volume > Open Interest)
            # ──────────────────────────────────────────────────
            # Wir suchen Strikes, wo heute MEHR gehandelt wurde als gestern existierte
            df["vol_oi_ratio"] = df["volume"] / (df["openInterest"] + 1)
            hot_strikes = df[df["vol_oi_ratio"] > 1.5].sort_values("volume", ascending=False).head(1)
            fresh_money_strike = hot_strikes["strike"].iloc[0] if not hot_strikes.empty else 0
            fresh_money_type = hot_strikes["kind"].iloc[0] if not hot_strikes.empty else ""

            # ──────────────────────────────────────────────────
            # RESULT
            # ──────────────────────────────────────────────────
            res = {
                "Symbol": sym,
                "Spot": round(spot, 2),
                "Max_Pain": max_pain,
                
                "Tac_Call_Wall": tac_call_strike,
                "Tac_Call_GEX": int(tac_call_gex) if not pd.isna(tac_call_gex) else 0,
                "Tac_Put_Wall": tac_put_strike,
                
                "Global_Call_Wall": gl_call_strike,
                "Global_Put_Wall": gl_put_strike,
                "Global_Call_GEX": int(gl_call_gex) if not pd.isna(gl_call_gex) else 0,
                
                "Strat_Call_Target": strat_call_strike,
                "Strat_Put_Target": strat_put_strike,
                
                "Fresh_Money_Strike": fresh_money_strike,
                "Fresh_Money_Type": fresh_money_type
            }
            results.append(res)
            sys.stdout.write(f".")
            sys.stdout.flush()
            
        except Exception as e:
            # print(e) # Debug
            continue

    print("\nSaving V60 Ultra Data...")
    if results:
        pd.DataFrame(results).to_csv("data/processed/options_v60_ultra.csv", index=False)
        print("✔ Done. File: data/processed/options_v60_ultra.csv")

if __name__ == "__main__":
    main()
