
import pandas as pd
import os

# Check both TFF and Disaggregated files
TFF_FILE = "c:/Users/eugen/Documents/AgenaTrader_QuantCache/cot_20y_tff.csv"
DISAGG_FILE = "c:/Users/eugen/Documents/AgenaTrader_QuantCache/cot_20y_disagg_merged.csv"

# Symbols to check
SYMBOLS_TO_CHECK = ["ES", "NQ", "MNQ", "ZN", "CL", "GC", "SI"]

# Market names the C# indicator is looking for (from previous analysis)
EXPECTED_MAPPINGS = {
    "ES": "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "NQ": "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
    "MNQ": "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
    "ZN": "10-YEAR U.S. TREASURY NOTES - CHICAGO BOARD OF TRADE",
    "ZB": "U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE",
    "CL": "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "GC": "GOLD - COMMODITY EXCHANGE INC.",
}

def check_file(filepath, label):
    if not os.path.exists(filepath):
        print(f"\n{label}: FILE NOT FOUND")
        return
    
    print(f"\n{'='*60}")
    print(f"{label}: {filepath}")
    print(f"{'='*60}")
    
    df = pd.read_csv(filepath, usecols=["market_and_exchange_names", "report_date_as_yyyy_mm_dd"])
    df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(df["report_date_as_yyyy_mm_dd"], errors='coerce')
    
    # Get unique markets and their max dates
    market_dates = df.groupby("market_and_exchange_names")["report_date_as_yyyy_mm_dd"].max().reset_index()
    market_dates = market_dates.sort_values("report_date_as_yyyy_mm_dd", ascending=False)
    
    # Show top 10 most recent markets
    print("\nTop 10 markets by latest date:")
    for idx, row in market_dates.head(10).iterrows():
        print(f"  {row['report_date_as_yyyy_mm_dd'].strftime('%Y-%m-%d')} | {row['market_and_exchange_names'][:60]}")
    
    # Check expected mappings
    print("\nExpected mappings check:")
    for sym, expected_market in EXPECTED_MAPPINGS.items():
        matches = df[df["market_and_exchange_names"].str.upper() == expected_market.upper()]
        if matches.empty:
            # Try partial match
            partial = df[df["market_and_exchange_names"].str.upper().str.contains(expected_market.split(" - ")[0].upper(), na=False)]
            if partial.empty:
                print(f"  {sym}: NOT FOUND ({expected_market[:40]}...)")
            else:
                max_date = partial["report_date_as_yyyy_mm_dd"].max()
                actual_name = partial.iloc[0]["market_and_exchange_names"]
                print(f"  {sym}: PARTIAL MATCH -> {max_date.strftime('%Y-%m-%d')} | {actual_name[:50]}")
        else:
            max_date = matches["report_date_as_yyyy_mm_dd"].max()
            print(f"  {sym}: FOUND -> {max_date.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    check_file(TFF_FILE, "TFF Dataset")
    check_file(DISAGG_FILE, "Disaggregated Dataset")
