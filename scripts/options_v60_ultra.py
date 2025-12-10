#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Options Data V90 QUANT PRO - Professional Quant Metrics
--------------------------------------------------------
Multi-timeframe analysis with:
- Tactical (≤14 days): Day trading / Scalping
- Medium (15-60 days): Swing Trading
- Strategic (>60 days): Position Trading

Metrics:
- GEX (Gamma Exposure) per horizon
- Net GEX (aggregate, positive = stable, negative = volatile)
- Vanna (delta sensitivity to IV changes)
- Charm (delta decay over time)
- Gamma Magnet per horizon
- Call/Put Walls per horizon
- Max Pain
"""

import os
import sys
import math
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.stats import norm
from scipy.optimize import brentq

# ============================================================================
# CONFIGURATION
# ============================================================================
RISK_FREE_RATE = 0.045

# Time horizons (in days)
DAYS_TACTICAL_MAX = 35      # Intraday / Day trading (includes next Monthly)
DAYS_MEDIUM_MIN = 36        # Swing trading start
DAYS_MEDIUM_MAX = 90        # Swing trading end  
DAYS_STRATEGIC_MIN = 91     # Position trading

MONEYNESS_BAND_PCT = 0.30   # ±30% around spot for wall detection

# ============================================================================
# BLACK-SCHOLES GREEKS
# ============================================================================

def bs_d1(S, K, T, r, sigma):
    """Calculate d1 for Black-Scholes formula"""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    return (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))

def bs_d2(S, K, T, r, sigma):
    """Calculate d2 for Black-Scholes formula"""
    if sigma <= 0 or T <= 0:
        return 0.0
    return bs_d1(S, K, T, r, sigma) - sigma * np.sqrt(T)

def bs_gamma(S, K, T, r, sigma):
    """
    Gamma: Rate of change of delta with respect to underlying price
    Higher gamma = more sensitive delta
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    try:
        d1 = bs_d1(S, K, T, r, sigma)
        return norm.pdf(d1) / (S * sigma * np.sqrt(T))
    except:
        return 0.0

def bs_vanna(S, K, T, r, sigma):
    """
    Vanna: d(Delta)/d(IV) - sensitivity of delta to volatility changes
    Important for: Event trading, earnings, volatility regime changes
    
    Vanna = -d1 * d2 * gamma / sigma (simplified form)
    Or: Vanna = (vega / S) * (1 - d1 / (sigma * sqrt(T)))
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    try:
        d1 = bs_d1(S, K, T, r, sigma)
        d2 = bs_d2(S, K, T, r, sigma)
        # Vanna = -e^(-d1^2/2) * d2 / (S * sigma^2 * sqrt(2*pi*T))
        vanna = -norm.pdf(d1) * d2 / (S * sigma)
        return vanna
    except:
        return 0.0

def bs_charm(S, K, T, r, sigma, is_call=True):
    """
    Charm: d(Delta)/d(Time) - delta decay per day
    Shows how delta changes as time passes (even if price doesn't move)
    
    Important for: Understanding dealer rebalancing needs over time
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    try:
        d1 = bs_d1(S, K, T, r, sigma)
        d2 = bs_d2(S, K, T, r, sigma)
        
        # Charm formula
        pdf_d1 = norm.pdf(d1)
        sqrt_T = np.sqrt(T)
        
        term1 = pdf_d1 * (2 * r * T - d2 * sigma * sqrt_T) / (2 * T * sigma * sqrt_T)
        
        if is_call:
            charm = -term1
        else:
            charm = -term1
            
        return charm / 365.0  # Convert to per-day
    except:
        return 0.0

# ============================================================================
# GEX CALCULATION
# ============================================================================

def compute_gex(row, spot):
    """
    Calculate GEX (Gamma Exposure) for a single option
    GEX = Gamma * Spot^2 * 0.01 * OI * 100 (contract multiplier)
    
    Positive for calls, Negative for puts (from dealer perspective)
    """
    try:
        K = float(row["strike"])
        dte_days = max(0.5, float(row["dte"]))
        T = dte_days / 365.0
        
        sigma = float(row.get("impliedVolatility", 0))
        oi = float(row["openInterest"])
        
        # Fallback for missing IV
        if sigma <= 0.001:
            sigma = 0.4
        
        gamma_val = bs_gamma(spot, K, T, RISK_FREE_RATE, sigma)
        
        # GEX in Dollar nominal per 1% move
        gex = gamma_val * (spot**2) * 0.01 * oi * 100
        
        # Flip sign for puts (dealers typically sell puts to buyers)
        if row.get("kind") == "put":
            gex = -gex
            
        return gex
    except:
        return 0.0

def compute_vanna_exposure(row, spot):
    """Calculate Vanna exposure for a single option"""
    try:
        K = float(row["strike"])
        dte_days = max(0.5, float(row["dte"]))
        T = dte_days / 365.0
        
        sigma = float(row.get("impliedVolatility", 0))
        oi = float(row["openInterest"])
        
        if sigma <= 0.001:
            sigma = 0.4
        
        vanna_val = bs_vanna(spot, K, T, RISK_FREE_RATE, sigma)
        
        # Vanna exposure = Vanna * OI * 100 * Spot
        exposure = vanna_val * oi * 100 * spot
        
        if row.get("kind") == "put":
            exposure = -exposure
            
        return exposure
    except:
        return 0.0

def compute_charm_exposure(row, spot):
    """Calculate Charm exposure for a single option"""
    try:
        K = float(row["strike"])
        dte_days = max(0.5, float(row["dte"]))
        T = dte_days / 365.0
        
        sigma = float(row.get("impliedVolatility", 0))
        oi = float(row["openInterest"])
        is_call = row.get("kind") == "call"
        
        if sigma <= 0.001:
            sigma = 0.4
        
        charm_val = bs_charm(spot, K, T, RISK_FREE_RATE, sigma, is_call)
        
        # Charm exposure = Charm * OI * 100
        exposure = charm_val * oi * 100
        
        return exposure
    except:
        return 0.0

def calculate_zero_gamma_level(spot, df_options):
    """
    Finds the Zero Gamma Price Level (Gamma Flip) by solving for Total Market Gamma = 0.
    """
    try:
        # Prepare arrays for vectorized calculation
        strikes = df_options["strike"].values
        dtes = df_options["dte"].values.astype(float)
        sigmas = df_options.get("impliedVolatility", 0)
        
        # If series, convert to array
        if hasattr(sigmas, "values"): sigmas = sigmas.values
        # If scaler, broadcast
        if np.isscalar(sigmas): sigmas = np.full(len(strikes), sigmas)
        
        ois = df_options["openInterest"].values.astype(float)
        kinds = df_options["kind"].values # 'call' or 'put'
        
        # Pre-calc T
        Ts = np.maximum(0.5, dtes) / 365.0
        
        signs = np.where((kinds == 'call') | (kinds == 'CALL') | (kinds == 'C'), 1.0, -1.0)
        
        # Objective function
        def net_gamma_at_price(p):
            if p <= 1: return -1e9
            
            vol_sqrt_t = sigmas * np.sqrt(Ts)
            # Avoid division by zero
            vol_sqrt_t[vol_sqrt_t <= 1e-6] = 1e-6

            d1s = (np.log(p / strikes) + (RISK_FREE_RATE + 0.5 * sigmas**2) * Ts) / vol_sqrt_t
            gammas = norm.pdf(d1s) / (p * vol_sqrt_t)
            
            # GEX contribution
            gex_vals = gammas * (p**2) * ois * signs
            return np.sum(gex_vals)

        # Bracket search
        lower = spot * 0.7
        upper = spot * 1.3
        
        val_low = net_gamma_at_price(lower)
        val_high = net_gamma_at_price(upper)
        
        if np.sign(val_low) == np.sign(val_high):
            lower = spot * 0.5
            upper = spot * 2.0
            val_low = net_gamma_at_price(lower)
            val_high = net_gamma_at_price(upper)
        
        if np.sign(val_low) != np.sign(val_high):
            flip_level = brentq(net_gamma_at_price, lower, upper, xtol=0.1)
            return flip_level
        
        return None
        
    except Exception as e:
        return None

# ============================================================================
# UTILITIES
# ============================================================================

# Mapping for Futures -> Yahoo Tickers (Proxies)
# Indices match reasonably well in price. Commodities need =F suffix.
PROXY_MAP = {
    "ES": "^SPX", "MES": "^SPX",  # S&P 500
    "NQ": "^NDX", "MNQ": "^NDX",  # Nasdaq 100
    "YM": "^DJI", "MYM": "^DJI",  # Dow Jones
    "RTY": "^RUT", "M2K": "^RUT", # Russell 2000
    "FDAXM": "^GDAXI", "FESX": "^STOXX50E", # DAX / EuroStoxx
    "NKD": "^N225",               # Nikkei
    "BTC": "BTC-USD",             # Bitcoin
    "ETH": "ETH-USD"
}

def load_symbols():
    """Load watchlist symbols"""
    wl_path = "watchlists/mylist.txt"
    if os.getenv("WATCHLIST_STOCKS"):
        wl_path = os.getenv("WATCHLIST_STOCKS")
    
    symbols = []
    if os.path.exists(wl_path):
        with open(wl_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(',')
                sym = parts[0].strip().upper()
                if sym != "SYMBOL":
                    symbols.append(sym)
    else:
        symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD"]
    
    return sorted(list(set(symbols)))

def calculate_max_pain(df, strikes):
    """Calculate max pain strike"""
    if df.empty:
        return 0
    
    pain_map = {}
    try:
        calls = df[df["kind"] == "call"]
        puts = df[df["kind"] == "put"]
        
        for center_strike in strikes:
            call_loss = 0
            put_loss = 0
            
            if not calls.empty:
                call_loss = ((center_strike - calls["strike"]) * calls["openInterest"]).clip(lower=0).sum()
            if not puts.empty:
                put_loss = ((puts["strike"] - center_strike) * puts["openInterest"]).clip(lower=0).sum()
            
            pain_map[center_strike] = call_loss + put_loss
        
        if not pain_map:
            return 0
        return min(pain_map, key=pain_map.get)
    except:
        return 0

def get_gamma_magnet(df, spot, max_pct=0.30):
    """
    Find the strike with highest TOTAL OPEN INTEREST (Liquidity Magnet)
    Reverted to OI logic per user request ("first variant").
    """
    if df.empty:
        return 0
    
    try:
        low = spot * (1.0 - max_pct)
        high = spot * (1.0 + max_pct)
        sub = df[(df["strike"] >= low) & (df["strike"] <= high)].copy()
        
        if sub.empty:
            return 0
        
        # Sum TOTAL OI per strike
        strike_oi = sub.groupby("strike")["openInterest"].sum()
        
        if strike_oi.empty:
            return 0
        
        return strike_oi.idxmax()
    except:
        return 0

def get_smart_wall(df, spot, kind="call", max_pct=0.30):
    """
    Find the strongest call/put wall based on OPEN INTEREST (Classic/Liquidity Logic)
    Reverted from GEX logic.
    """
    if df.empty:
        return 0, 0
    
    try:
        low = spot * (1.0 - max_pct)
        high = spot * (1.0 + max_pct)
        sub = df[(df["strike"] >= low) & (df["strike"] <= high)].copy()
        
        if kind == "call":
            sub = sub[(sub["strike"] >= spot) & (sub["kind"] == "call")]
        else:
            sub = sub[(sub["strike"] <= spot) & (sub["kind"] == "put")]
        
        if sub.empty:
            # Fallback: search wider if nothing found in immediate range
            if kind == "call":
                sub = df[(df["strike"] >= spot) & (df["kind"] == "call")]
            else:
                sub = df[(df["strike"] <= spot) & (df["kind"] == "put")]
                
            if sub.empty:
                return 0, 0
        
        # Sort by Open Interest (Descending)
        top = sub.sort_values("openInterest", ascending=False).iloc[0]
        
        return top["strike"], top["gex"] # Return GEX just for info/metrics, but selection is OI
    except:
        return 0, 0

def get_dominant_expiry_for_subset(df_subset):
    """
    Find the expiry date with the most total OPEN INTEREST directly (Liquidity Dominance).
    Reverted from GEX logic.
    """
    if df_subset.empty:
        return ""
    try:
        # Sum TOTAL OI per expiry
        exp_oi = df_subset.groupby("expiry")["openInterest"].sum()
        if exp_oi.empty:
            return ""
        return exp_oi.idxmax().strftime("%Y-%m-%d")
    except:
        return ""

def bs_price(S, K, T, r, sigma, kind="call"):
    d1 = bs_d1(S, K, T, r, sigma)
    d2 = d1 - sigma * np.sqrt(T)
    if kind == "call":
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def calculate_imp_vol(price, S, K, T, r, kind="call"):
    """
    Calculate Implied Volatility from option price using Brent's method.
    """
    if price <= 0 or T <= 0: return 0.0
    
    def obj(sigma):
        return bs_price(S, K, T, r, sigma, kind) - price
    
    try:
        # Check boundaries
        low = 0.01
        high = 5.0
        
        if obj(low) * obj(high) > 0:
            return 0.0 # No solution in range
            
        return brentq(obj, low, high, xtol=1e-4)
    except:
        return 0.0

def get_robust_atm_iv(df, spot):
    """
    Get a robust ATM IV.
    1. Try median of provider IVs.
    2. If provider IV is suspicious (< 10%), calculate from Price.
    """
    if df.empty or spot <= 0:
        return 0.0
        
    try:
        # Filter near the money
        low = spot * 0.95
        high = spot * 1.05
        near = df[(df["strike"] >= low) & (df["strike"] <= high)].copy()
        
        if near.empty:
            near = df[(df["strike"] >= spot*0.9) & (df["strike"] <= spot*1.1)].copy()
            
        if near.empty:
            return 0.0

        # Check provider IV
        valid_ivs = near[near["impliedVolatility"] > 0.01]["impliedVolatility"]
        median_iv = valid_ivs.median() if not valid_ivs.empty else 0.0
        
        # If IV seems too low (e.g. < 5% for equities is rare), recalc
        if median_iv < 0.10: 
            # Re-calculate IV from lastPrice for these options
            calced_ivs = []
            for _, row in near.iterrows():
                p = float(row.get("lastPrice", 0))
                k = float(row["strike"])
                dte = float(row["dte"])
                typ = str(row["kind"]).lower()
                
                if p > 0 and dte >= 1: # Calculate even for short term
                    iv = calculate_imp_vol(p, spot, k, dte/365.0, RISK_FREE_RATE, typ)
                    if iv > 0.01:
                        calced_ivs.append(iv)
            
            if calced_ivs:
                return np.median(calced_ivs)
            
        return median_iv
    except:
        return 0.0

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def main():
    os.makedirs("data/processed", exist_ok=True)
    today_ymd = datetime.now().strftime('%Y-%m-%d')
    print(f"Running Options V90 Analysis for {today_ymd}...")
    
    tickers = load_symbols()

    # -------------------------------------------------------------------------
    # 0. HISTORY / GHOST WALLS (Smart Logic)
    # -------------------------------------------------------------------------
    prev_map = {}
    history_file = "data/processed/options_v60_ultra.csv"
    if os.path.exists(history_file):
        try:
            df_old = pd.read_csv(history_file)
            
            # Helper to safely get value (handle NaN)
            def safe_get(row, col):
                if col in row and pd.notna(row[col]):
                    return row[col]
                return 0

            # Iterate old rows to build prev_map
            for _, row in df_old.iterrows():
                s_sym = str(row["Symbol"]).upper().strip()
                if "Symbol" not in row: continue
                
                # Check Date of the OLD record
                old_date = str(row.get("Date", "1900-01-01"))
                
                entry = {}
                
                if old_date == today_ymd:
                    # RERUN SAME DAY: Keep the EXISTING Prev_ values
                    # Do NOT shift current to prev, otherwise we lose true history
                    entry["Prev_Tac_Call_Wall"] = safe_get(row, "Prev_Tac_Call_Wall")
                    entry["Prev_Tac_Put_Wall"]  = safe_get(row, "Prev_Tac_Put_Wall")
                    entry["Prev_Med_Call_Wall"] = safe_get(row, "Prev_Med_Call_Wall")
                    entry["Prev_Med_Put_Wall"]  = safe_get(row, "Prev_Med_Put_Wall")
                    entry["Prev_Max_Pain"]      = safe_get(row, "Prev_Max_Pain")
                else:
                    # NEW DAY: Shift Current -> Prev
                    entry["Prev_Tac_Call_Wall"] = safe_get(row, "Tac_Call_Wall")
                    entry["Prev_Tac_Put_Wall"]  = safe_get(row, "Tac_Put_Wall")
                    entry["Prev_Med_Call_Wall"] = safe_get(row, "Med_Call_Wall")
                    entry["Prev_Med_Put_Wall"]  = safe_get(row, "Med_Put_Wall")
                    entry["Prev_Max_Pain"]      = safe_get(row, "Max_Pain")
                
                prev_map[s_sym] = entry
                
            print(f"[INFO] Ghost History prepared for {len(prev_map)} symbols (Day-Switch Logic).")
            
        except Exception as e:
            print(f"[WARN] History load failed: {e}")

    print(f"[Quant Pro] Processing {len(tickers)} symbols with multi-horizon analysis...")
    
    now = datetime.utcnow()
    results = []
    
    for sym in tickers:
        try:
            # 1. Determine Yahoo Symbol (Proxy or Suffix)
            y_sym = sym
            if sym in PROXY_MAP:
                y_sym = PROXY_MAP[sym]
            
            tk = yf.Ticker(y_sym)
            hist = tk.history(period="5d")
            
            # 2. Fallback: Try Adding '=F' if empty (common for Commodities like CL, GC)
            if hist.empty and "=" not in y_sym and "^" not in y_sym:
                y_sym = sym + "=F"
                tk = yf.Ticker(y_sym)
                hist = tk.history(period="5d")
            
            if hist.empty:
                # print(f"  [Skip] No data for {sym} (tried {y_sym})")
                continue
            
            spot = float(hist["Close"].iloc[-1])

            
            try:
                exps = tk.options
            except:
                exps = []
            
            if not exps:
                continue
            
            all_opts = []
            for e_str in exps:
                try:
                    dt = datetime.strptime(e_str, "%Y-%m-%d")
                    dte = (dt - now).days
                    if dte < 0:
                        continue
                    
                    chain = tk.option_chain(e_str)
                    
                    if not chain.calls.empty:
                        calls = chain.calls.copy()
                        calls['kind'] = 'call'
                        calls['expiry'] = dt
                        calls['dte'] = dte
                        all_opts.append(calls)
                    
                    if not chain.puts.empty:
                        puts = chain.puts.copy()
                        puts['kind'] = 'put'
                        puts['expiry'] = dt
                        puts['dte'] = dte
                        all_opts.append(puts)
                except:
                    continue
            
            if not all_opts:
                continue
            
            df = pd.concat(all_opts, ignore_index=True)
            
            # Cleanup columns
            needed = ["contractSymbol", "strike", "openInterest", "volume", 
                      "impliedVolatility", "kind", "expiry", "dte", "lastPrice"]
            for c in needed:
                if c not in df.columns:
                    df[c] = 0
            df = df[needed].copy()
            
            df["openInterest"] = pd.to_numeric(df["openInterest"], errors='coerce').fillna(0)
            df["volume"] = pd.to_numeric(df["volume"], errors='coerce').fillna(0)
            df["strike"] = pd.to_numeric(df["strike"], errors='coerce')
            df["impliedVolatility"] = pd.to_numeric(df["impliedVolatility"], errors='coerce').fillna(0)
            
            # ================================================================
            # CALCULATE ALL GREEKS
            # ================================================================
            df["gex"] = df.apply(lambda row: compute_gex(row, spot), axis=1)
            df["vanna"] = df.apply(lambda row: compute_vanna_exposure(row, spot), axis=1)
            df["charm"] = df.apply(lambda row: compute_charm_exposure(row, spot), axis=1)
            
            # ================================================================
            # FILTER BY HORIZON
            # ================================================================
            df_tactical = df[df["dte"] <= DAYS_TACTICAL_MAX]
            df_medium = df[(df["dte"] >= DAYS_MEDIUM_MIN) & (df["dte"] <= DAYS_MEDIUM_MAX)]
            df_strategic = df[df["dte"] >= DAYS_STRATEGIC_MIN]
            
            # ================================================================
            # AGGREGATE METRICS (ALL EXPIRIES)
            # ================================================================
            net_gex = df["gex"].sum()  # Positive = dealers long gamma (stable)
            total_vanna = df["vanna"].sum()
            total_charm = df["charm"].sum()
            
            # Determine regime
            if net_gex > 0:
                gex_regime = "STABLE"
            else:
                gex_regime = "VOLATILE"

            # [NEW] Calculate Gamma Flip (Zero Gamma Level) using Root Finding
            gamma_flip = spot # Default to spot if fails
            try:
                # Use only options with relevant OI to speed up / reduce noise?
                # All options are important for total market gamma.
                flip_calc = calculate_zero_gamma_level(spot, df)
                if flip_calc is not None:
                    gamma_flip = flip_calc
                else:
                    gamma_flip = spot # Fallback
            except:
                gamma_flip = spot
                
            # Fallback legacy logic if root finding failed completely?
            # No, if root finding fails, it means no zero crossing in range -> very strong dominance.
            # Stick to spot or maybe use Max Pain as proxy? Spot is safe.
            
            # ================================================================
            # MAX PAIN (all contracts)
            # ================================================================
            rel_strikes = df[(df["strike"] > spot*0.7) & (df["strike"] < spot*1.3)]["strike"].unique()
            max_pain = calculate_max_pain(df, rel_strikes)
            
            # ================================================================
            # TACTICAL METRICS (≤14 days) - Day Trading
            # ================================================================
            tac_gamma_magnet = get_gamma_magnet(df_tactical, spot)
            tac_call_wall, tac_call_gex = get_smart_wall(df_tactical, spot, "call")
            tac_put_wall, _ = get_smart_wall(df_tactical, spot, "put")
            tac_expiry = get_dominant_expiry_for_subset(df_tactical)
            
            # ================================================================
            # MEDIUM METRICS (15-60 days) - Swing Trading
            # ================================================================
            med_gamma_magnet = get_gamma_magnet(df_medium, spot)
            med_call_wall, med_call_gex = get_smart_wall(df_medium, spot, "call")
            med_put_wall, _ = get_smart_wall(df_medium, spot, "put")
            med_net_gex = df_medium["gex"].sum() if not df_medium.empty else 0
            med_expiry = get_dominant_expiry_for_subset(df_medium)
            
            # ================================================================
            # STRATEGIC METRICS (>60 days) - Position Trading
            # ================================================================
            strat_call_target, _ = get_smart_wall(df_strategic, spot, "call", max_pct=0.5)
            strat_put_target, _ = get_smart_wall(df_strategic, spot, "put", max_pct=0.5)
            strat_expiry = get_dominant_expiry_for_subset(df_strategic)
            
            # ================================================================
            # FRESH MONEY DETECTION (unusual volume/OI ratio)
            # ================================================================
            df["vol_oi_ratio"] = df["volume"] / (df["openInterest"] + 1)
            hot = df.sort_values("vol_oi_ratio", ascending=False).head(1)
            fresh_strike = 0
            fresh_type = ""
            if not hot.empty:
                row0 = hot.iloc[0]
                if row0["vol_oi_ratio"] > 1.5:
                    fresh_strike = row0["strike"]
                    fresh_type = row0["kind"]
            
            # ================================================================
            # DOMINANT EXPIRY SPLIT (Call vs Put)
            # ================================================================
            dominant_expiry = "N/A"
            dominant_oi_expiry = "N/A" # [NEW] Max OI Expiry
            dominant_call_expiry = "N/A"
            dominant_put_expiry = "N/A"
            
            if not df.empty:
                # Total Dominant (Absolute GEX)
                expiry_gex_sum = df.groupby("expiry")["gex"].apply(lambda x: x.abs().sum())
                if not expiry_gex_sum.empty:
                    dominant_expiry = expiry_gex_sum.idxmax().strftime("%Y-%m-%d")

                # [NEW] Total Dominant OI
                expiry_oi_sum = df.groupby("expiry")["openInterest"].sum()
                if not expiry_oi_sum.empty:
                    dominant_oi_expiry = expiry_oi_sum.idxmax().strftime("%Y-%m-%d")
                
                # Call Dominant
                df_calls = df[df['kind'].str.upper().isin(['CALL', 'C'])]
                if not df_calls.empty:
                    call_exp_sum = df_calls.groupby("expiry")["gex"].sum() 
                    if not call_exp_sum.empty:
                        dominant_call_expiry = call_exp_sum.idxmax().strftime("%Y-%m-%d")
                        
                # Put Dominant
                df_puts = df[df['kind'].str.upper().isin(['PUT', 'P'])]
                if not df_puts.empty:
                    put_exp_sum = df_puts.groupby("expiry")["gex"].apply(lambda x: x.abs().sum()) 
                    if not put_exp_sum.empty:
                        dominant_put_expiry = put_exp_sum.idxmax().strftime("%Y-%m-%d")



            # ================================================================
            # EXPORT GAMMA & OI PROFILE (Strike Level Data)
            # ================================================================
            # EXPORT GAMMA & OI PROFILE (Strike Level Data)
            # ================================================================
            # [DEBUG] Removed try-except to expose potential errors in CI
            print(f"[DEBUG] Exporting profile for {sym}...")
            
            # [FILTER] Exclude 0DTE/1DTE (<= 1 Day) - User preference
            df_profile = df
            if 'dte' in df.columns:
                filtered = df[df['dte'] > 1]
                if not filtered.empty:
                    df_profile = filtered.copy()
                
            # Group by Strike and Type - Aggregate GEX AND OI
            grouped = df_profile.groupby(['strike', 'kind'])[['gex', 'openInterest']].sum().unstack(fill_value=0)
            
            # Setup GEX columns: (gex, call) -> CALL_GEX
            gex_part = grouped['gex'].copy()
            gex_part.columns = [str(c).upper() for c in gex_part.columns]
            
            # Handle aliases if needed (C/CALL, P/PUT)
            if 'C' in gex_part.columns: gex_part['CALL'] = gex_part.get('CALL', 0) + gex_part['C']; gex_part.drop(columns=['C'], errors='ignore', inplace=True)
            if 'P' in gex_part.columns: gex_part['PUT']  = gex_part.get('PUT', 0)  + gex_part['P']; gex_part.drop(columns=['P'], errors='ignore', inplace=True)
            
            # Ensure standard names
            if 'CALL' not in gex_part.columns: gex_part['CALL'] = 0
            if 'PUT'  not in gex_part.columns: gex_part['PUT']  = 0
            
            # Rename for export
            gex_part.rename(columns={'CALL': 'CALL_GEX', 'PUT': 'PUT_GEX'}, inplace=True)
            gex_part['TOTAL_GEX'] = gex_part['CALL_GEX'] + gex_part['PUT_GEX']

            # Setup OI columns: (openInterest, call) -> CALL_OI
            oi_part = grouped['openInterest'].copy()
            oi_part.columns = [str(c).upper() for c in oi_part.columns]
            
            if 'C' in oi_part.columns: oi_part['CALL'] = oi_part.get('CALL', 0) + oi_part['C']; oi_part.drop(columns=['C'], errors='ignore', inplace=True)
            if 'P' in oi_part.columns: oi_part['PUT']  = oi_part.get('PUT', 0)  + oi_part['P']; oi_part.drop(columns=['P'], errors='ignore', inplace=True)
            
            if 'CALL' not in oi_part.columns: oi_part['CALL'] = 0
            if 'PUT'  not in oi_part.columns: oi_part['PUT']  = 0
            
            oi_part.rename(columns={'CALL': 'CALL_OI', 'PUT': 'PUT_OI'}, inplace=True)
            oi_part['TOTAL_OI'] = oi_part['CALL_OI'] + oi_part['PUT_OI']
            
            # Merge
            profile_export = pd.concat([gex_part, oi_part], axis=1)
            
            # Save to specific profile file
            profile_filename = f"profile_{sym}.csv"
            profile_path = os.path.join("data", "processed", "profiles", profile_filename)
            os.makedirs(os.path.dirname(profile_path), exist_ok=True)
            profile_export.to_csv(profile_path)
            print(f"[DEBUG] Wrote {profile_path}")

            # ================================================================
            # BUILD RESULT
            # ================================================================
            res = {
                "Symbol": sym,
                "Date": today_ymd, # Store today's date for history stability
                "Spot": round(spot, 2),
                "Max_Pain": max_pain,
                
                # Aggregate metrics
                "Net_GEX": int(net_gex),
                "GEX_Regime": gex_regime,
                "Gamma_Flip": gamma_flip, # [NEW]
                "Total_Vanna": int(total_vanna),
                "Total_Charm": round(total_charm, 2),
                
                # Dominant Expiries
                "Dominant_Expiry": dominant_expiry,
                "Dominant_OI_Expiry": dominant_oi_expiry,
                "Dominant_Call_Expiry": dominant_call_expiry,
                "Dominant_Put_Expiry": dominant_put_expiry,
                
                # Tactical (≤14 days)
                "Tac_Gamma_Magnet": tac_gamma_magnet,
                "Tac_Call_Wall": tac_call_wall,
                "Tac_Put_Wall": tac_put_wall,
                "Tac_Call_GEX": int(tac_call_gex),
                
                # Medium (15-60 days) - SWING
                "Med_Gamma_Magnet": med_gamma_magnet,
                "Med_Call_Wall": med_call_wall,
                "Med_Put_Wall": med_put_wall,
                "Med_Net_GEX": int(med_net_gex),
                
                # Strategic (>60 days)
                "Strat_Call_Target": strat_call_target,
                "Strat_Put_Target": strat_put_target,
                
                # Expiry Dates (STABLE LINES)
                "Tac_Expiry": tac_expiry,
                "Med_Expiry": med_expiry,
                "Strat_Expiry": strat_expiry,
                
                # Fresh money
                "Fresh_Money_Strike": fresh_strike,
                "Fresh_Money_Type": fresh_type,

                # ATM IV (for Scanner)
                "IV_ATM": round(get_robust_atm_iv(df, spot), 4),
                "Dominant_Expiry": dominant_expiry,

                # GHOST / HISTORY
                "Prev_Tac_Call_Wall": 0,
                "Prev_Tac_Put_Wall": 0,
                "Prev_Med_Call_Wall": 0,
                "Prev_Med_Put_Wall": 0,
                "Prev_Max_Pain": 0
            }

            # Merge History if available
            if sym in prev_map:
                pm = prev_map[sym]
                res["Prev_Tac_Call_Wall"] = pm.get("Prev_Tac_Call_Wall", 0)
                res["Prev_Tac_Put_Wall"] = pm.get("Prev_Tac_Put_Wall", 0)
                res["Prev_Med_Call_Wall"] = pm.get("Prev_Med_Call_Wall", 0)
                res["Prev_Med_Put_Wall"] = pm.get("Prev_Med_Put_Wall", 0)
                res["Prev_Max_Pain"] = pm.get("Prev_Max_Pain", 0)
                
            results.append(res)
            sys.stdout.write(".")
            sys.stdout.flush()
            
        except Exception as e:
            continue
    
    if results:
        out_path = "data/processed/options_v60_ultra.csv"
        pd.DataFrame(results).to_csv(out_path, index=False)
        print(f"\n[OK] Saved {len(results)} symbols to {out_path}")
        print(f"     Columns: Net_GEX, GEX_Regime, Total_Vanna, Total_Charm, Med_* metrics, Prev_Walls")
    else:
        print("\n[WARN] No results to save")

if __name__ == "__main__":
    main()
