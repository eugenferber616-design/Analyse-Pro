
import requests

API_BASE = "https://publicreporting.cftc.gov/resource"

# Check all CFTC COT datasets
DATASETS = {
    "TFF (Traders in Financial Futures)": "yw9f-hn96",
    "Disaggregated Futures": "kh3c-gbw2",
    "Legacy Futures Only": "6dca-aqww",  # Legacy COT dataset
    "Legacy Combined": "jun7-fc8e"       # Legacy combined
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json"
}

def check_all_datasets():
    for name, dataset_id in DATASETS.items():
        print(f"\n{'='*60}")
        print(f"Dataset: {name} ({dataset_id})")
        print(f"{'='*60}")
        
        # Query for Treasury
        params = {
            "$where": "market_and_exchange_names like '%10-YEAR%' OR market_and_exchange_names like '%TREASURY%'",
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": 3
        }
        
        url = f"{API_BASE}/{dataset_id}.json"
        
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if not data:
                    print("No Treasury data found.")
                else:
                    print(f"Found {len(data)} records (showing latest):")
                    for row in data:
                        date = row.get('report_date_as_yyyy_mm_dd', 'N/A')
                        market = row.get('market_and_exchange_names', 'N/A')[:50]
                        print(f"  {date} | {market}")
            else:
                print(f"Error {r.status_code}: {r.text[:100]}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    check_all_datasets()
