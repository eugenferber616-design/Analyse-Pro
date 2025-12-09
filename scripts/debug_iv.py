
import yfinance as yf
import pandas as pd
from datetime import datetime

sym = "AAPL"
tk = yf.Ticker(sym)
now = datetime.now()

print(f"Fetching options for {sym}...")
exps = tk.options
if not exps:
    print("No expirations found.")
    exit()

print(f"First expiry: {exps[0]}")
chain = tk.option_chain(exps[0])

calls = chain.calls
print("Calls columns:", calls.columns.tolist())
print("First call row:")
print(calls.iloc[0])

spot = tk.history(period="1d")["Close"].iloc[-1]
print(f"Spot: {spot}")

# Check ATM IV
calls["dist"] = (calls["strike"] - spot).abs()
atm_call = calls.sort_values("dist").iloc[0]
print("ATM Call:")
print(atm_call)
print(f"IV: {atm_call.get('impliedVolatility')}")
