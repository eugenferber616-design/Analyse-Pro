
import pandas as pd
import os

FILE_MERGED = "c:/Users/eugen/Documents/AgenaTrader_QuantCache/cot_20y_disagg_merged.csv"
FILE_MERGED_GZ = "data/processed/cot_20y_disagg_merged.csv.gz"
FILE_MERGED_CSV = "data/processed/cot_20y_disagg_merged.csv"

def check():
    targets = [FILE_MERGED_GZ, FILE_MERGED_CSV]
    
    for target in targets:
        if not os.path.exists(target):
            print(f"Skipping {target}: Not found")
            continue
            
        print(f"\nChecking {target} ...")
        try:
            # Load only relevant columns to be fast
            # Pandas handles compression="infer" automatically for .gz
            df = pd.read_csv(target, usecols=["market_and_exchange_names", "report_date_as_yyyy_mm_dd"])
        except Exception as e:
            print(f"Error reading {target}: {e}")
            continue
    
    
    # Filter for WTI
    # Use str.contains because exact match might fail
    wti_mask = df["market_and_exchange_names"].fillna("").astype(str).str.contains("WTI FINANCIAL CRUDE OIL", case=False, na=False)
    wti_df = df[wti_mask].copy()
    
    if wti_df.empty:
        print("No WTI FINANCIAL CRUDE OIL found!")
        return

    # Convert to datetime to avoid str/float comparison issues
    wti_df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(wti_df["report_date_as_yyyy_mm_dd"], errors='coerce')
    
    # Drop NaT
    wti_df = wti_df.dropna(subset=["report_date_as_yyyy_mm_dd"])

    # Sort by date
    wti_df = wti_df.sort_values("report_date_as_yyyy_mm_dd")
    
    print("Latest 5 entries for WTI FINANCIAL CRUDE OIL:")
    print(wti_df.tail(5)[["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]])

    max_date = wti_df["report_date_as_yyyy_mm_dd"].max()
    print(f"\nMAX DATE found for WTI: {max_date.strftime('%Y-%m-%d')}")

if __name__ == "__main__":
    check()
