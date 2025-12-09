import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def fetch_prices():
    # Configuration
    days = int(os.getenv("PRICES_DAYS", "750"))
    base_path = "data/prices"
    os.makedirs(base_path, exist_ok=True)
    
    # helper to read symbols from watchlist files
    def read_list(p):
        if not p or not os.path.exists(p): return []
        try:
            df = pd.read_csv(p, header=None) # Assume no header or handle it
            # If first row looks like header "symbol", skip it
            if str(df.iloc[0,0]).lower() == "symbol":
                df = df.iloc[1:]
            
            # Use column 0
            return [str(s).strip().upper() for s in df[0].dropna().tolist()]
        except:
            return []

    # Load Watchlists
    wl_stocks = read_list(os.getenv("WATCHLIST_STOCKS"))
    wl_etf = read_list(os.getenv("WATCHLIST_ETF"))
    wl = sorted(set(wl_stocks + wl_etf))
    
    print(f"Fetching prices for {len(wl)} symbols (Window: {days} days)...")
    
    start = (datetime.utcnow() - timedelta(days=days*1.2)).strftime("%Y-%m-%d")
    
    # Chunking to avoid massive memory usage if list is huge (though yf caches)
    # yfinance download(tickers=list) is faster but requires handling MultiIndex.
    # We will stick to loop for simplicity per file, but handle connection errors.
    
    success_count = 0
    
    for sym in wl:
        if not sym: continue
        try:
            # Create subdir based on first char
            first_char = sym[0].upper() if sym[0].isalpha() else "#"
            sub_dir = os.path.join(base_path, first_char)
            os.makedirs(sub_dir, exist_ok=True)
            
            # Download
            df = yf.download(sym, start=start, progress=False, threads=False, auto_adjust=False)
            
            if df.empty:
                print(f"WARN: No data for {sym}")
                continue
                
            # Handle MultiIndex Columns (Price, Ticker) if they appear (rare for single symbol but possible in new versions)
            if isinstance(df.columns, pd.MultiIndex):
                # Dropping level 1 usually leaves just Price
                df.columns = df.columns.droplevel(1)
            
            # Normalize columns
            df.index = df.index.tz_localize(None)
            df = df.reset_index()
            
            # Map columns to lowercase standard
            col_map = {
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume"
            }
            df.rename(columns=col_map, inplace=True)
            
            # Save
            out_path = os.path.join(sub_dir, f"{sym}.csv")
            df.to_csv(out_path, index=False)
            success_count += 1
            
        except Exception as e:
            print(f"ERR: Failed {sym} - {e}")
            
    print(f"Done. Saved {success_count}/{len(wl)} price files.")

if __name__ == "__main__":
    fetch_prices()
