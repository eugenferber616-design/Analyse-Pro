
import requests

API_BASE = "https://publicreporting.cftc.gov/resource"
TFF_DATASET = "yw9f-hn96"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json"
}

def check_treasury_api():
    # Query for 10-YEAR TREASURY in TFF dataset
    params = {
        "$where": "market_and_exchange_names like '%10-YEAR%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 5
    }
    
    url = f"{API_BASE}/{TFF_DATASET}.json"
    print(f"Checking CFTC TFF API for 10-YEAR TREASURY...")
    
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            if not data:
                print("RESULT: No 10-YEAR TREASURY data found in TFF API.")
            else:
                print(f"RESULT: Found {len(data)} records!")
                for row in data:
                    print(f"  Date: {row.get('report_date_as_yyyy_mm_dd')} | {row.get('market_and_exchange_names', 'N/A')[:50]}")
        else:
            print(f"Error: {r.text[:200]}")
            
    except Exception as e:
        print(f"Exception: {e}")
    
    # Also check for any Treasury related
    print("\n\nChecking for ANY Treasury-related markets...")
    params2 = {
        "$where": "market_and_exchange_names like '%TREASURY%'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": 10
    }
    
    try:
        r2 = requests.get(url, headers=HEADERS, params=params2, timeout=30)
        if r2.status_code == 200:
            data2 = r2.json()
            if not data2:
                print("No TREASURY markets found in TFF API.")
            else:
                print(f"Found {len(data2)} records:")
                for row in data2:
                    print(f"  {row.get('report_date_as_yyyy_mm_dd')} | {row.get('market_and_exchange_names', 'N/A')[:60]}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    check_treasury_api()
