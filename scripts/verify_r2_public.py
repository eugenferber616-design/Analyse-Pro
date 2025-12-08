#!/usr/bin/env python3
"""
verify_r2_public.py
-------------------
Checks if critical files are accessible via the Cloudflare R2 Public URL.
"""

import requests
import sys

# Deine Public URL aus dem Screenshot
R2_BASE_URL = "https://pub-c5e8c78162df45f4bed6224f0ebacab6.r2.dev"

# Liste der Dateien, die wir erwarten (basierend auf nightly.yml)
# Beachte: Der Workflow gzip't sie oft (.gz)
EXPECTED_FILES = [
    "data/processed/options_oi_summary.csv",
    "data/processed/options_v60_ultra.csv",
    "data/processed/options_v60_ultra.csv.gz",
    "data/processed/short_interest.csv",
    "data/processed/short_interest.csv.gz",
    "data/processed/equity_master.csv.gz",
    "data/processed/market_core.csv.gz",
    "data/processed/hv_summary.csv.gz",
    "data/processed/riskindex_timeseries.csv.gz"
]

def check_file(path):
    url = f"{R2_BASE_URL}/{path}"
    try:
        # HEAD request only checks headers (faster)
        r = requests.head(url, timeout=5)
        if r.status_code == 200:
            size_kb = int(r.headers.get("Content-Length", 0)) / 1024
            last_mod = r.headers.get("Last-Modified", "Unknown")
            print(f"[OK]  {path:<40} | {size_kb:>6.1f} KB | {last_mod}")
            return True
        elif r.status_code == 404:
            print(f"[MISSING] {path:<40} (404 Not Found)")
            return False
        else:
            print(f"[ERR] {path:<40} (Status: {r.status_code})")
            return False
    except Exception as e:
        print(f"[ERR] {path:<40} | Error: {e}")
        return False

def main():
    print(f"Checking R2 Storage at: {R2_BASE_URL}\n")
    print(f"{'File':<46} | {'Size':>9} | Last Modified")
    print("-" * 80)
    
    found = 0
    for f in EXPECTED_FILES:
        if check_file(f):
            found += 1
            
    print("-" * 80)
    print(f"Summary: Found {found}/{len(EXPECTED_FILES)} checked files.")

    if found == 0:
        print("\n[HINT] Sind die Dateien vielleicht noch nicht hochgeladen?")
        print("       Prüfe den letzten 'Sync to R2' Schritt in GitHub Actions.")

if __name__ == "__main__":
    main()
