
import requests
import json

API_BASE = "https://publicreporting.cftc.gov/resource"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

def check_latest():
    dataset_id = "kh3c-gbw2"
    # Query for any date AFTER 2025-10-07
    params = {
        "$where": "report_date_as_yyyy_mm_dd > '2025-10-07'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 5
    }
    
    url = f"{API_BASE}/{dataset_id}.json"
    print(f"Checking {url} for data > 2025-10-07...")
    
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=15)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if not data:
                print("RESULT: No newer data found. Oct 7, 2025 seems to be the latest on server.")
            else:
                print(f"RESULT: Found {len(data)} newer records!")
                for row in data:
                    print(f" - Found Date: {row.get('report_date_as_yyyy_mm_dd')} for {row.get('market_and_exchange_names')}")
        else:
            print(f"Error: {r.text}")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    check_latest()
