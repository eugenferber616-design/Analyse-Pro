#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_seasonality.py
--------------------
Berechnet Seasonality für:
- aktuellen Monat (letzte 10 Jahre)
- aktuelle Kalenderwoche (letzte 10 Jahre)

Output: data/processed/seasonality.csv
Spalten:
Symbol, Month, Week, Month_Avg_Return, Month_Win_Rate, Month_Bias,
        Week_Avg_Return, Week_Win_Rate, Week_Bias
"""

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas as pd
import yfinance as yf

# Mapping for Futures -> Yahoo Tickers
PROXY_MAP = {
    "ES": "^SPX", "MES": "^SPX",
    "NQ": "^NDX", "MNQ": "^NDX",
    "YM": "^DJI", "MYM": "^DJI",
    "RTY": "^RUT", "M2K": "^RUT",
    "FDAXM": "^GDAXI", "FESX": "^STOXX50E",
    "NKD": "^N225",
    "BTC": "BTC-USD", "ETH": "ETH-USD"
}


def load_watchlist():
    path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    symbols = []
    if os.path.exists(path):
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(",")
                sym = parts[0].strip()
                if sym and sym.upper() != "SYMBOL":
                    symbols.append(sym)
    return sorted(set(symbols))


def classify_bias(avg_ret, win_rate):
    bias = "NEUTRAL"
    if avg_ret > 1.0 and win_rate > 60:
        bias = "BULLISH"
    elif avg_ret < -1.0 and win_rate < 40:
        bias = "BEARISH"
    return bias


def main():
    print("--- Seasonality Check (10 Years; Month + Week) ---")

    symbols = load_watchlist()
    if not symbols:
        symbols = ["SPY", "QQQ", "NVDA", "AAPL"]

    now = datetime.now()
    current_month = now.month
    current_week = now.isocalendar().week
    month_name = now.strftime("%B")
    start_date = (now - timedelta(days=365 * 10)).strftime("%Y-%m-%d")

    print(f"[INFO] Watchlist geladen ({len(symbols)} Symbole): {', '.join(symbols)}")
    print(f"[INFO] Analysiere Monat: {month_name}, KW: {current_week} ...")

    results = []

    for sym in symbols:
        try:
            print(f"[SYM] {sym} ... ", end="", flush=True)
            
            # Proxy / Suffix Logic for Futures
            y_sym = sym
            if sym in PROXY_MAP:
                y_sym = PROXY_MAP[sym]
            
            df = yf.download(y_sym, start=start_date, progress=False)
            
            # Fallback for Commodities (append =F)
            if df.empty and "=" not in y_sym and "^" not in y_sym:
                 y_sym_fallback = sym + "=F"
                 df = yf.download(y_sym_fallback, start=start_date, progress=False)

            if df.empty:
                print("SKIP (keine Daten)")
                continue

            # MultiIndex-Fix
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            close = df.get("Close")
            if close is None or close.dropna().empty:
                print("SKIP (kein Close)")
                continue

            close = close.dropna()

            # ---------- Monats-Seasonality ----------
            monthly_ret = close.resample("ME").ffill().pct_change() * 100.0
            hist_month = monthly_ret[monthly_ret.index.month == current_month].dropna()

            if hist_month.empty:
                month_avg = np.nan
                month_wr = np.nan
                month_bias = "NEUTRAL"
            else:
                month_avg = float(hist_month.mean())
                month_wr = float((hist_month > 0).sum() / len(hist_month) * 100.0)
                month_bias = classify_bias(month_avg, month_wr)

            # ---------- Wochen-Seasonality ----------
            weekly_ret = close.resample("W-FRI").ffill().pct_change() * 100.0
            week_idx = weekly_ret.index.isocalendar().week
            hist_week = weekly_ret[week_idx == current_week].dropna()

            if hist_week.empty:
                week_avg = np.nan
                week_wr = np.nan
                week_bias = "NEUTRAL"
            else:
                week_avg = float(hist_week.mean())
                week_wr = float((hist_week > 0).sum() / len(hist_week) * 100.0)
                week_bias = classify_bias(week_avg, week_wr)

            results.append(
                {
                    "Symbol": sym,
                    "Month": month_name,
                    "Week": int(current_week),
                    "Month_Avg_Return": round(month_avg, 2) if not np.isnan(month_avg) else np.nan,
                    "Month_Win_Rate": round(month_wr, 0) if not np.isnan(month_wr) else np.nan,
                    "Month_Bias": month_bias,
                    "Week_Avg_Return": round(week_avg, 2) if not np.isnan(week_avg) else np.nan,
                    "Week_Win_Rate": round(week_wr, 0) if not np.isnan(week_wr) else np.nan,
                    "Week_Bias": week_bias,
                }
            )
            print("OK")

        except Exception as e:
            print(f"ERROR: {e}")

    os.makedirs("data/processed", exist_ok=True)
    df_out = pd.DataFrame(results)
    df_out.to_csv("data/processed/seasonality.csv", index=False)
    print(f"✔ Seasonality erstellt: data/processed/seasonality.csv (Zeilen: {len(df_out)})")


if __name__ == "__main__":
    main()
