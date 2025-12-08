import pandas as pd
from fredapi import Fred
import sys

FRED_API_KEY = "a62b1a06c6cdc4bb8c32d733a492326f"

print(f"Testing FRED API with key: {FRED_API_KEY[:4]}...")
fred = Fred(api_key=FRED_API_KEY)

# Test 1: T5YIE (Inflation)
try:
    print("\nAttempting to fetch T5YIE (Inflation Exp)...")
    s = fred.get_series('T5YIE')
    print(f"T5YIE Success! Last value: {s.iloc[-1]}")
except Exception as e:
    print(f"T5YIE FAILED: {e}")

# Test 2: BAMLH0A0HYM2 (Alternative HY Spread)
try:
    print("\nAttempting to fetch BAMLH0A0HYM2 (HY Master II)...")
    s = fred.get_series('BAMLH0A0HYM2')
    print(f"BAMLH0A0HYM2 Success! Last value: {s.iloc[-1]}")
except Exception as e:
    print(f"BAMLH0A0HYM2 FAILED: {e}")

# Test 3: USSLIND (Leading Index - just to check)
try:
    print("\nAttempting to fetch USSLIND...")
    s = fred.get_series('USSLIND')
    print(f"USSLIND Success! Last value: {s.iloc[-1]}")
except Exception as e:
    print(f"USSLIND FAILED: {e}")
