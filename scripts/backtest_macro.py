
import pandas as pd
import numpy as np
import yfinance as yf
from fredapi import Fred
from datetime import datetime, timedelta
import os

# CONFIG
FRED_API_KEY = "a62b1a06c6cdc4bb8c32d733a492326f"
START_DATE = "2018-01-01"
TICKER_SYMBOL = "SPY"

# MA Settings for Liquidity
LIQ_SMA_WINDOW = 20 # Days (fast reaction) or 50? Lets try 20.

def run_backtest():
    print(f"--- MACRO LIQUIDITY BACKTEST (Start: {START_DATE}) ---")
    
    # 1. Fetch Market Data
    print(f"Fetching {TICKER_SYMBOL}...")
    spy = yf.download(TICKER_SYMBOL, start=START_DATE, progress=False)
    if isinstance(spy.columns, pd.MultiIndex):
        spy.columns = spy.columns.droplevel(1)
    
    # Standardize cols
    spy = spy[["Close"]].rename(columns={"Close": "Price"})
    spy.index = spy.index.tz_localize(None)
    
    # 2. Fetch FRED Data
    print("Fetching FRED Data (Net Liquidity)...")
    fred = Fred(api_key=FRED_API_KEY)
    
    try:
        # Net Liquidity = Fed Balance Sheet (WALCL) - TGA (WDTGAL) - Reverse Repo (RRPONTSYD)
        walcl = fred.get_series("WALCL", observation_start=START_DATE)
        tga = fred.get_series("WDTGAL", observation_start=START_DATE)
        rrp = fred.get_series("RRPONTSYD", observation_start=START_DATE)
        
        # Merge FRED series
        macro = pd.DataFrame({
            "WALCL": walcl,
            "TGA": tga,
            "RRP": rrp
        })
        
        # Forward fill because FRED data is weekly/daily mixed
        macro.ffill(inplace=True)
        
        # All in Millions? 
        # WALCL: Millions
        # WDTGAL: Millions
        # RRP: Billions -> Convert to Millions!
        macro["Net_Liq"] = macro["WALCL"] - macro["TGA"] - (macro["RRP"] * 1000)
        
    except Exception as e:
        print(f"Error fetching FRED data: {e}")
        return

    # 3. Align Data
    # Reindex macro to market days
    df = spy.join(macro["Net_Liq"], how="inner")
    df.ffill(inplace=True)
    
    # 4. Strategy Logic
    # Signal: If Net Liquidity > Net Liquidity SMA -> RISK ON (Long SPY)
    #         Else -> RISK OFF (Cash / Zero Return)
    
    df["Liq_SMA"] = df["Net_Liq"].rolling(window=LIQ_SMA_WINDOW).mean()
    
    # Shift signal by 1 day to avoid lookahead bias!
    # If Liq > SMA today, we own SPY tomorrow.
    df["Signal"] = np.where(df["Net_Liq"] > df["Liq_SMA"], 1.0, 0.0)
    df["Position"] = df["Signal"].shift(1)
    
    # 5. Calculate Returns
    df["Spy_Ret"] = df["Price"].pct_change()
    df["Strat_Ret"] = df["Position"] * df["Spy_Ret"]
    
    # 6. Equity Curves
    df["Eq_Spy"] = (1 + df["Spy_Ret"]).cumprod()
    df["Eq_Strat"] = (1 + df["Strat_Ret"]).cumprod()
    
    # 7. Stats
    total_days = (df.index[-1] - df.index[0]).days
    years = total_days / 365.25
    
    cagr_spy = (df["Eq_Spy"].iloc[-1] ** (1/years)) - 1
    cagr_strat = (df["Eq_Strat"].iloc[-1] ** (1/years)) - 1
    
    # Max Drawdown
    def max_dd(series):
        peak = series.cummax()
        dd = (series - peak) / peak
        return dd.min()
        
    dd_spy = max_dd(df["Eq_Spy"])
    dd_strat = max_dd(df["Eq_Strat"])
    
    vol_spy = df["Spy_Ret"].std() * (252**0.5)
    vol_strat = df["Strat_Ret"].std() * (252**0.5)
    
    exposure = df["Position"].mean()  # % Time in Market
    
    print("\n--- RESULTS ---")
    print(f"Period: {years:.1f} years")
    print("-" * 40)
    print(f"METRIC            | SPY (B&H) | LIQUIDITY STRATEGY")
    print("-" * 40)
    print(f"CAGR              | {cagr_spy:8.1%} | {cagr_strat:8.1%}")
    print(f"Max Drawdown      | {dd_spy:8.1%} | {dd_strat:8.1%}")
    print(f"Volatility        | {vol_spy:8.1%} | {vol_strat:8.1%}")
    print(f"Time in Market    | {1.0:8.0%} | {exposure:8.0%}")
    print("-" * 40)
    
    if cagr_strat > cagr_spy:
        print("\n[+] Strategy OUTPERFORMS benchmark.")
    else:
        print("\n[-] Strategy UNDERPERFORMS benchmark (Trend might be too slow/lagging).")
        
    # Validation check
    print(f"\nNet Liq Latest: {df['Net_Liq'].iloc[-1]/1000:.2f} Billion")
    print(f"Latest Signal: {'RISK ON' if df['Signal'].iloc[-1]==1 else 'RISK OFF'}")

    
    # Save CSV for user to plot
    out_file = "data/processed/backtest_macro.csv"
    os.makedirs("data/processed", exist_ok=True)
    df[["Price", "Net_Liq", "Eq_Spy", "Eq_Strat"]].to_csv(out_file)
    print(f"Equity curves saved to: {out_file}")

    # [NEW] Export for AgenaTrader Overlay
    cache_dir = os.path.join(os.path.expanduser("~"), "Documents", "AgenaTrader_QuantCache")
    signal_file = os.path.join(cache_dir, "macro_signals.csv")
    
    # Format: Date, Signal (1/0), NetLiq
    df_sig = df[["Signal", "Net_Liq", "Liq_SMA"]].copy()
    df_sig.to_csv(signal_file)
    print(f"Signals exported to: {signal_file}")

if __name__ == "__main__":
    run_backtest()
