import yfinance as yf
import pandas_datareader.data as web
import datetime

start = datetime.datetime(2020, 1, 1)
end = datetime.datetime.now()

print("--- TESTING DATA SOURCES FROM PINE SCRIPT ---")

# 1. YAHOO (Standard)
yahoo_tickers = {
    "VIX": "^VIX",
    "VVIX": "^VVIX",
    "SKEW": "^SKEW",
    "VIX3M": "^VIX3M",  # CBOE:VIX3M
    "MOVE": "^MOVE",    # Might not work
    "DXY": "DX-Y.NYB",  # TVC:DXY equivalent
    "TNX": "^TNX",      # 10Y Yield
    "IRX": "^IRX",      # 13 Week Yield
}

print("\n[YAHOO TEST]")
for name, ticker in yahoo_tickers.items():
    try:
        data = yf.download(ticker, start=start, progress=False, ignore_tz=True)
        if not data.empty:
            print(f"[OK] {name}: {len(data)} rows")
        else:
            print(f"[FAIL] {name}: Empty")
    except Exception as e:
        print(f"[FAIL] {name}: {e}")

# 2. FRED (Macro/Liquidity)
# Pine references: FRED:DGS30, DGS10, DGS2, DGS3MO, SOFR, RRPONTSYD, WALCL, WDTGAL, WRESBAL, BAMLC0A0CM (IG OAS), BAMLH0A0HYM2 (HY OAS)
fred_tickers = [
    "DGS30", "DGS10", "DGS2", "DGS3MO", 
    "SOFR", "RRPONTSYD", "WALCL", "WDTGAL", "WRESBAL",
    "BAMLC0A0CM", "BAMLH0A0HYM2", "STLFSI4"
]

print("\n[FRED TEST]")
try:
    fred_data = web.DataReader(fred_tickers, 'fred', start, end)
    print(f"[OK] FRED Data fetched: {len(fred_data)} rows")
    # Check individual columns
    for col in fred_tickers:
        if col in fred_data.columns:
            cnt = fred_data[col].count()
            print(f"  - {col}: {cnt} valid points")
        else:
            print(f"  - {col}: MISSING")
except Exception as e:
    print(f"[FAIL] FRED fetch failed: {e}")
