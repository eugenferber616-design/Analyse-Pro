
import pandas as pd
import os
import re

# Check both TFF and Disaggregated files
TFF_FILE = "c:/Users/eugen/Documents/AgenaTrader_QuantCache/cot_20y_tff.csv"
DISAGG_FILE = "c:/Users/eugen/Documents/AgenaTrader_QuantCache/cot_20y_disagg_merged.csv"

# Keywords to search for
KEYWORDS = [
    "E-MINI S&P",
    "NASDAQ",
    "TREASURY",
    "CRUDE OIL",
    "GOLD",
    "SILVER",
    "NATURAL GAS",
    "10-YEAR",
    "EURODOLLAR",
    "EURO FX",
    "YEN",
]

def search_markets(filepath, label):
    if not os.path.exists(filepath):
        print(f"\n{label}: FILE NOT FOUND")
        return
    
    print(f"\n{'='*80}")
    print(f"{label}")
    print(f"{'='*80}")
    
    df = pd.read_csv(filepath, usecols=["market_and_exchange_names", "report_date_as_yyyy_mm_dd"])
    df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors='coerce')
    
    # Get unique markets and their max dates
    market_dates = df.groupby("market_and_exchange_names")["report_date_as_yyyy_mm_dd"].max().reset_index()
    
    for kw in KEYWORDS:
        matches = market_dates[market_dates["market_and_exchange_names"].str.upper().str.contains(kw.upper(), na=False)]
        if not matches.empty:
            print(f"\n--- {kw} ---")
            matches_sorted = matches.sort_values("report_date_as_yyyy_mm_dd", ascending=False)
            for _, row in matches_sorted.head(5).iterrows():
                date_str = row['report_date_as_yyyy_mm_dd'].strftime('%Y-%m-%d') if pd.notna(row['report_date_as_yyyy_mm_dd']) else "N/A"
                print(f"  {date_str} | {row['market_and_exchange_names']}")

if __name__ == "__main__":
    search_markets(TFF_FILE, "TFF Dataset (Finanzen)")
    search_markets(DISAGG_FILE, "Disaggregated Dataset (Rohstoffe)")
