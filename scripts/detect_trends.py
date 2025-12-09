"""
Auto Trendlines (V4 - LuxAlgo Style)
=====================================
Connects CONSECUTIVE pivot points to form trendlines.
- Upper: Pivot High N -> Pivot High N+1 (only if lower high)
- Lower: Pivot Low N  -> Pivot Low N+1  (only if higher low)
"""

import pandas as pd
import numpy as np
import yfinance as yf
import os

# CONFIG
TICKER = "SPY"
START_DATE = "2023-01-01"  # More recent data for cleaner lines
SWING_LOOKBACK = 20         # Larger lookback = fewer, more significant pivots
LINE_EXTENSION = 50         # Extend line X bars beyond pivot 2
CACHE_DIR = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache")

def fetch_data():
    print(f"Fetching {TICKER}...")
    df = yf.download(TICKER, start=START_DATE, progress=False, auto_adjust=True)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.index = df.index.tz_localize(None)
    return df

def find_pivots(df, length=20):
    """Find pivot highs and lows using rolling window."""
    highs = df["High"].values
    lows = df["Low"].values
    n = len(df)
    
    pivot_high_indices = []
    pivot_low_indices = []
    
    for i in range(length, n - length):
        # Pivot High: Current high is the highest in the window
        window_start = max(0, i - length)
        window_end = min(n, i + length + 1)
        
        if highs[i] == max(highs[window_start:window_end]):
            pivot_high_indices.append(i)
        
        # Pivot Low: Current low is the lowest in the window
        if lows[i] == min(lows[window_start:window_end]):
            pivot_low_indices.append(i)
    
    return pivot_high_indices, pivot_low_indices

def build_consecutive_trendlines(df, pivot_indices, is_upper=True):
    """
    Connect consecutive pivots to form trendlines.
    For upper (resistance): Only connect if PH2 < PH1 (lower highs = downtrend)
    For lower (support): Only connect if PL2 > PL1 (higher lows = uptrend)
    """
    lines = []
    price_col = "High" if is_upper else "Low"
    
    for i in range(len(pivot_indices) - 1):
        idx1 = pivot_indices[i]
        idx2 = pivot_indices[i + 1]
        
        p1 = df[price_col].iloc[idx1]
        p2 = df[price_col].iloc[idx2]
        
        # Filter: Only valid trendlines
        if is_upper:
            # Upper trendline: P2 should be LOWER than P1 (falling resistance)
            if p2 >= p1:
                continue
        else:
            # Lower trendline: P2 should be HIGHER than P1 (rising support)
            if p2 <= p1:
                continue
        
        # Calculate slope
        bars_diff = idx2 - idx1
        slope = (p2 - p1) / bars_diff
        
        # Extend line forward
        end_idx = min(idx2 + LINE_EXTENSION, len(df) - 1)
        extend_bars = end_idx - idx2
        end_price = p2 + slope * extend_bars
        
        lines.append({
            "start_idx": idx1,
            "start_price": p1,
            "end_idx": end_idx,
            "end_price": end_price,
            "pivot1_date": df.index[idx1],
            "pivot2_date": df.index[idx2],
            "slope": slope
        })
    
    return lines

def run_detection():
    df = fetch_data()
    
    # 1. Find Pivots
    pivot_highs, pivot_lows = find_pivots(df, length=SWING_LOOKBACK)
    
    print(f"Found {len(pivot_highs)} Pivot Highs and {len(pivot_lows)} Pivot Lows.")
    
    # 2. Build Consecutive Trendlines
    upper_lines = build_consecutive_trendlines(df, pivot_highs, is_upper=True)
    lower_lines = build_consecutive_trendlines(df, pivot_lows, is_upper=False)
    
    print(f"\n--- Valid Upper Trendlines (falling highs): {len(upper_lines)} ---")
    for line in upper_lines[-5:]:  # Show last 5
        print(f"  {line['pivot1_date'].date()} -> {line['pivot2_date'].date()}: {line['start_price']:.2f} -> {line['end_price']:.2f}")
    
    print(f"\n--- Valid Lower Trendlines (rising lows): {len(lower_lines)} ---")
    for line in lower_lines[-5:]:  # Show last 5
        print(f"  {line['pivot1_date'].date()} -> {line['pivot2_date'].date()}: {line['start_price']:.2f} -> {line['end_price']:.2f}")
    
    # 3. Export for AgenaTrader
    export_data = []
    MAX_LINES = 8  # Max lines per type to keep chart clean
    
    # Export upper lines (Red - Resistance)
    for line in upper_lines[-MAX_LINES:]:
        export_data.append({
            "StartDate": line["pivot1_date"].strftime("%Y-%m-%d"),
            "StartPrice": line["start_price"],
            "EndDate": df.index[line["end_idx"]].strftime("%Y-%m-%d"),
            "EndPrice": line["end_price"],
            "Color": "Red"
        })
    
    # Export lower lines (Green - Support)
    for line in lower_lines[-MAX_LINES:]:
        export_data.append({
            "StartDate": line["pivot1_date"].strftime("%Y-%m-%d"),
            "StartPrice": line["start_price"],
            "EndDate": df.index[line["end_idx"]].strftime("%Y-%m-%d"),
            "EndPrice": line["end_price"],
            "Color": "Green"
        })
    
    out_file = os.path.join(CACHE_DIR, "trend_lines.csv")
    pd.DataFrame(export_data).to_csv(out_file, index=False)
    print(f"\nExported {len(export_data)} lines to: {out_file}")

if __name__ == "__main__":
    run_detection()
