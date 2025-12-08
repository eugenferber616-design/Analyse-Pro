#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex_v2.py
---------------------
Der "Sniper" Risk Index basierend auf ML-Backtest V5 (+24,000% ROI Strategie).
Logik: "Holy Trinity" (Trend + VIX + Credit).

Output:
  - data/processed/riskindex_snapshot.json (Für Dashboard/Scanner)
  - data/processed/riskindex_v2.csv (Historie)
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

# --- KONFIGURATION ---
OUTDIR = Path("data/processed")
START_DATE = "2007-01-01"

# Ticker Mapping
TICKERS = {
    "SPY": "SPY",       # Trend Anchor
    "VIX": "^VIX",      # Fear Gauge
    "HYG": "HYG",       # High Yield (Risk)
    "LQD": "LQD",       # Investment Grade (Safe)
    "KRE": "KRE",       # Regional Banks (Sector Stress)
    "XLF": "XLF"        # Financials (Sector Base)
}

# --- LOGIK ---
def fetch_data():
    print(f"--- Lade Live Daten ab {START_DATE} ---")
    data = yf.download(list(TICKERS.values()), start=START_DATE, progress=False)['Close']
    
    # Rename Spalten (Mapping umkehren)
    inv_map = {v: k for k, v in TICKERS.items()}
    data.rename(columns=inv_map, inplace=True)
    
    # Cleanup (Forward Fill für Feiertage, Drop Nulls am Anfang)
    data.ffill(inplace=True)
    data.bfill(inplace=True)
    
    print(f"Daten geladen: {len(data)} Tage ({data.index.min().date()} bis {data.index.max().date()})")
    return data

def calc_normalize(series, min_v, max_v, inverse=False):
    """
    Normalisiert Werte auf 0-100 Skala.
    inverse=True: Niedriger Wert ist gut (z.B. VIX).
    inverse=False: Hoher Wert ist gut (z.B. Trend, Credit Ratio).
    """
    # Clip Outliers
    clipped = series.clip(min_v, max_v)
    
    # Scale 0-1
    scaled = (clipped - min_v) / (max_v - min_v)
    
    if inverse:
        # 1.0 (Max Vix) -> 0 Score
        # 0.0 (Min Vix) -> 100 Score
        score = (1.0 - scaled) * 100
    else:
        # 1.0 (Max Trend) -> 100 Score
        score = scaled * 100
        
    return score

def calculate_risk_index(df):
    print("--- Berechne Risk Index V2 (Sniper Logic) ---")
    
    # 1. TREND (40% Gewicht)
    # Metrik: Abstand zum SMA 200 in Prozent
    sma200 = df['SPY'].rolling(200).mean()
    dist_sma200 = (df['SPY'] - sma200) / sma200
    # Range: -10% (Crash) bis +10% (Bull). Alles > +10% ist 100, alles < -10% ist 0.
    score_trend = calc_normalize(dist_sma200, -0.10, 0.10, inverse=False)
    
    # 2. FEAR (30% Gewicht)
    # Metrik: VIX Level
    # Range: 12 (Relaxed) bis 35 (Panik).
    if 'VIX' in df.columns:
        score_vix = calc_normalize(df['VIX'], 12, 35, inverse=True)
    else:
        score_vix = 50 # Fallback
    
    # 3. STRESS (30% Gewicht)
    # Metrik: Credit Ratio (HYG/LQD) Trend (30 Tage Change)
    if 'HYG' in df.columns and 'LQD' in df.columns:
        cr = df['HYG'] / df['LQD']
        # Wir schauen auf Momentum: Fällt die Ratio schnell?
        # Z-Score ähnlicher Ansatz: 30d Chg
        cr_chg = cr.pct_change(30)
        # Range: -3% (Stress) bis +3% (Relaxed)
        score_credit = calc_normalize(cr_chg, -0.03, 0.03, inverse=False)
    else:
        score_credit = 50
        
    # --- TOTAL SCORE ---
    # Gewichte: 40% Trend, 30% VIX, 30% Credit
    total_score = (score_trend * 0.40) + (score_vix * 0.30) + (score_credit * 0.30)
    
    # --- OUTPUT DATAFRAME ---
    out = pd.DataFrame(index=df.index)
    out['date'] = df.index
    out['Trend_Score'] = score_trend
    out['VIX_Score'] = score_vix
    out['Credit_Score'] = score_credit
    out['Risk_Index'] = total_score
    
    # Smoothing für weniger "Gezappel" (3 Tage Durchschnitt)
    out['Risk_Index'] = out['Risk_Index'].rolling(3).mean()
    
    return out.dropna()

def get_regime(score):
    if score < 20: return "RISK-OFF (SHORT)"
    if score < 50: return "CAUTION (CASH)"
    return "RISK-ON (LONG)"

def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Fetch
    df_raw = fetch_data()
    
    # 2. Calc
    df_risk = calculate_risk_index(df_raw)
    
    # 3. Save History (CSV)
    out_csv = OUTDIR / "riskindex_v2.csv"
    df_risk.to_csv(out_csv, index=False)
    print(f"[OK] CSV gespeichert: {str(out_csv)}")
    
    # 4. Save Snapshot (JSON) für C# Dashboard
    last_row = df_risk.iloc[-1]
    
    snapshot = {
        "asof": last_row['date'].isoformat(),
        "composite": float(last_row['Risk_Index']),
        "regime": get_regime(last_row['Risk_Index']),
        "scores": {
            "trend": float(last_row['Trend_Score']),
            "vix": float(last_row['VIX_Score']),
            "credit": float(last_row['Credit_Score'])
        },
        "details": {
            "spy_price": float(df_raw['SPY'].iloc[-1]),
            "vix_price": float(df_raw['VIX'].iloc[-1])
        },
        "one_liner": f"Score: {last_row['Risk_Index']:.1f} | {get_regime(last_row['Risk_Index'])}"
    }
    
    out_json = OUTDIR / "riskindex_snapshot.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
    print(f"[OK] JSON Snapshot gespeichert: {str(out_json)}")
    
    print("\n--- STATUS AKTUELL ---")
    print(f"Datum: {snapshot['asof']}")
    print(f"Score: {snapshot['composite']:.1f} / 100")
    print(f"Regime: {snapshot['regime']}")
    print(f"Komponenten: Trend={snapshot['scores']['trend']:.0f}, VIX={snapshot['scores']['vix']:.0f}, Credit={snapshot['scores']['credit']:.0f}")

if __name__ == "__main__":
    main()
