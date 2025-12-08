#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_earnings_calendar.py
Fetches global earnings calendar from Finnhub (next 365 days) and saves as CSV.
This allows getting 2026 dates which are often missing in single-symbol endpoints.
"""

import os
import sys
import csv
import time
import json
import datetime
import requests
import pathlib
from typing import List, Dict, Set

# ───────────────────────────── Config ─────────────────────────────
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or ""
WATCHLIST_PATH = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OUT_DIR = pathlib.Path("data/processed")
OUT_FILE = OUT_DIR / "earnings_results.csv"

# Exchanges to fetch (US + Major EU)
# US, DE (Xetra), PA (Paris), LSE (London), AS (Amsterdam), MI (Milan), MC (Madrid), SW (Swiss)
EXCHANGES = ["US", "DE", "PA", "LSE", "AS", "MI", "MC", "SW"]
DAYS_AHEAD = 365
API_BASE = "https://finnhub.io/api/v1"

# ───────────────────────────── Helpers ─────────────────────────────
def load_watchlist(path: str) -> Set[str]:
    """Loads symbols from watchlist to filter the massive calendar."""
    if not os.path.exists(path):
        return set()
    
    syms = set()
    with open(path, "r", encoding="utf-8") as f:
        # Check if CSV header exists
        head = f.read(1024)
        f.seek(0)
        has_header = "symbol" in head.lower() or "#symbol" in head.lower()
        
        if has_header:
            rdr = csv.DictReader(f)
            for row in rdr:
                # Handle various header names
                s = (row.get("symbol") or row.get("#symbol") or row.get("ticker") or "").strip().upper()
                if s and not s.startswith("#"):
                    syms.add(s)
        else:
            # Simple list
            for line in f:
                s = line.strip().upper()
                if s and not s.startswith("#") and "symbol" not in s.lower():
                    syms.add(s)
    return syms

def get_calendar_range(start_date: datetime.date, end_date: datetime.date) -> List[dict]:
    """Fetches earnings calendar for a date range."""
    url = f"{API_BASE}/calendar/earnings"
    params = {
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
        "token": FINNHUB_TOKEN
    }
    
    # Retry logic
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            return data.get("earningsCalendar", [])
        except Exception as e:
            print(f"  [Warn] Fetch failed (attempt {attempt+1}): {e}")
            time.sleep(1)
    return []

def normalize_time(t: str) -> str:
    if not t: return "tbd"
    t = t.lower().strip()
    if t in ["bmo", "amc"]: return t
    return "tbd"

# ───────────────────────────── Main ─────────────────────────────
def main():
    if not FINNHUB_TOKEN:
        print("[Error] FINNHUB_TOKEN not set.")
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading watchlist from {WATCHLIST_PATH}...")
    watchlist = load_watchlist(WATCHLIST_PATH)
    if not watchlist:
        print("[Warn] Watchlist empty or not found. Fetching ALL (might be huge).")
    else:
        print(f"Watchlist contains {len(watchlist)} symbols.")

    # 1. Fetch Calendar (month by month to avoid timeouts/limits if needed, 
    # but Finnhub usually handles 3-6 months ok. We'll do 3-month chunks.)
    
    today = datetime.date.today()
    end_date = today + datetime.timedelta(days=DAYS_AHEAD)
    
    all_rows = []
    
    current = today
    while current < end_date:
        chunk_end = min(current + datetime.timedelta(days=90), end_date)
        print(f"Fetching calendar: {current} to {chunk_end}...")
        
        rows = get_calendar_range(current, chunk_end)
        all_rows.extend(rows)
        
        current = chunk_end + datetime.timedelta(days=1)
        time.sleep(0.5) # Rate limit kindness

    print(f"Total raw events fetched: {len(all_rows)}")

    # 2. Process &Filter
    # Map: Symbol -> Earliest Future Date
    # We want the *next* earnings date for each symbol.
    
    next_earnings = {} # Symbol -> {date, time, quarter...}
    
    for row in all_rows:
        sym = (row.get("symbol") or "").strip().upper()
        if not sym: continue
        
        # Filter by watchlist if active
        # Note: Finnhub symbols might differ (e.g. dots). 
        # Simple check: if watchlist has "MSFT", map "MSFT". 
        # If watchlist has "SIE.DE", map "SIE.DE".
        if watchlist and sym not in watchlist:
            continue

        dte_str = row.get("date")
        if not dte_str: continue
        
        try:
            dte = datetime.datetime.strptime(dte_str, "%Y-%m-%d").date()
        except:
            continue
            
        if dte < today:
            continue # Skip past
            
        # If we already have a date for this symbol, check if this one is sooner?
        # The list might be unsorted. We want the MINIMUM future date.
        if sym not in next_earnings:
            next_earnings[sym] = {
                "date": dte,
                "row": row
            }
        else:
            if dte < next_earnings[sym]["date"]:
                next_earnings[sym] = {
                    "date": dte,
                    "row": row
                }

    print(f"Found next earnings for {len(next_earnings)} symbols.")

    # 3. Write CSV
    # Format: symbol,report_date,report_time,year,quarter
    # Matches requirements of GlobalUtilities_Utility._EnsureLocalData (expects CSV)
    
    with open(OUT_FILE, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "report_date", "report_time", "year", "quarter", "eps_estimate", "revenue_estimate"])
        
        for sym in sorted(next_earnings.keys()):
            entry = next_earnings[sym]
            row = entry["row"]
            d_str = entry["date"].strftime("%Y-%m-%d")
            t_str = normalize_time(row.get("hour"))
            y = row.get("year") or ""
            q = row.get("quarter") or ""
            eps = row.get("epsEstimate") or ""
            rev = row.get("revenueEstimate") or ""
            
            w.writerow([sym, d_str, t_str, y, q, eps, rev])

    print(f"[OK] Saved to {OUT_FILE}")

if __name__ == "__main__":
    main()
