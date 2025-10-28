#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build very-lightweight 'CDS proxy' per symbol by mapping listing region to
US/EU IG (or HY) option-adjusted spreads pulled into data/processed/fred_oas.csv.

Output: data/processed/cds_proxy.csv with columns:
  symbol, bucket, as_of, value
Where:
  bucket ∈ {US_IG, US_HY, EU_IG, EU_HY}
  value  = latest OAS (percent), e.g. 0.77 for IG
"""

import csv, os, sys, datetime as dt
from collections import defaultdict

FRED_OAS_PATH = "data/processed/fred_oas.csv"
WATCHLIST_PATH = os.environ.get("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OUT_PATH = "data/processed/cds_proxy.csv"

# --- Region mapping by ticker suffix (very permissive EU map)
EU_SUFFIXES = {
    ".DE",".F",".FR",".PA",".AS",".BR",".MC",".MI",".SW",".CO",".VX",".L",".LN",
    ".ES",".MC",".ST",".HE",".OL",".WA",".PR",".VI",".BR",".BE",".BRU",".LU",".LS",
    ".IR",".IE",".AT",".DK",".FI",".SE",".NO",".CH",".PL",".CZ",".HU",".PT",".NL"
}
US_SUFFIXES = {".US",".N",".O",".A",".K",".P",".Q",".NY",".AM"}  # not exhaustive

def sym_region(symbol: str) -> str:
    s = symbol.upper()
    for suf in EU_SUFFIXES:
        if s.endswith(suf): return "EU"
    for suf in US_SUFFIXES:
        if s.endswith(suf): return "US"
    # Heuristik: wenn keine EU-Suffixe -> als US behandeln (US ADRs / default)
    return "US"

def choose_bucket(symbol: str, region: str) -> str:
    # Default alles IG. Du kannst hier Regeln für HY ergänzen (Sektoren, Ratings…).
    # Beispiel: sehr zyklische Ticker in HY packen:
    # if region=="US" and symbol.endswith((".X","-J")): return f"{region}_HY"
    return f"{region}_IG"

def load_fred_oas(path: str):
    """
    Erwartetes CSV-Schema (wie unser fetch_fred_oas.py schreibt):
      date_series_id,value,bucket,region
    Wir picken je (region,bucket) den jüngsten Wert.
    """
    latest = {}  # (region,bucket) -> (date, value)
    if not os.path.exists(path):
        raise SystemExit(f"❌ missing {path} – run FRED step first")

    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                val = float(row["value"])
            except Exception:
                continue
            region = row.get("region","").strip().upper() or "US"
            bucket = row.get("bucket","").strip().upper() or "IG"
            key = (region, bucket)
            ds = row.get("date_series_id","")
            # robust: nimm letzte Zeile als "neueste" (Datei ist chronologisch)
            latest[key] = (ds, val)
    return latest

def load_symbols(path: str):
    """
    Akzeptiert watchlists in Form:
      - CSV/TSV mit Spalte 'symbol'   oder
      - einfache 1-Spalten-Liste (ein Symbol pro Zeile)
    """
    if not os.path.exists(path):
        return []

    syms = []
    with open(path, encoding="utf-8") as f:
        head = f.readline()
        f.seek(0)
        if "symbol" in head.lower():
            r = csv.DictReader(f)
            for row in r:
                s = (row.get("symbol") or "").strip()
                if s: syms.append(s)
        else:
            for line in f:
                s = line.strip()
                if s and s.lower() != "symbol":
                    syms.append(s)
    # eindeutige Reihenfolge beibehalten
    uniq, seen = [], set()
    for s in syms:
        if s not in seen:
            uniq.append(s); seen.add(s)
    return uniq

def main():
    latest = load_fred_oas(FRED_OAS_PATH)
    syms   = load_symbols(WATCHLIST_PATH)

    rows = []
    today = dt.date.today().isoformat()

    for s in syms:
        region = sym_region(s)
        bucket = choose_bucket(s, region).split("_")[1]  # IG/HY
        key = (region, bucket)
        ds, val = latest.get(key, (today, 0.0))
        rows.append({
            "symbol": s,
            "bucket": f"{region}_{bucket}",
            "as_of": ds.split(",")[0] if "," in ds else today,
            "value": f"{val:.2f}",
        })

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol","bucket","as_of","value"])
        w.writeheader(); w.writerows(rows)

    print(f"wrote {OUT_PATH} with {len(rows)} rows")

if __name__ == "__main__":
    main()
