#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch COT (Futures+Options Combined) for the last ~10 years from CFTC Socrata.

Writes:
- data/processed/cot_10y.csv           (volle Tabelle)
- data/processed/cot_10y.csv.gz        (gzip-komprimiert)
- data/processed/cot_10y.zip           (ZIP, enthält cot_10y.csv)
- data/reports/cot_10y_report.json     (kleiner Laufbericht)

Env Vars (alle optional mit Defaults):
  COT_DATASET_ID   (default: gpe5-46if)      CFTC Futures+Options COMBINED
  CFTC_APP_TOKEN   (dein App Token; dringend empfohlen)
  COT_YEARS        (default: 10)             Anzahl Jahre rückwärts
  COT_MARKETS_MODE (ALL | FILE | LIST)       Filtermodus (default: ALL)
  COT_MARKETS_FILE (Pfad zu Watchlist; default: watchlists/cot_markets.txt)
  CFTC_API_BASE    (default: https://publicreporting.cftc.gov/resource)
"""

import csv
import gzip
import io
import json
import os
import sys
import time
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Konfiguration aus ENV
# ──────────────────────────────────────────────────────────────────────────────
DATASET_ID   = os.getenv("COT_DATASET_ID", "gpe5-46if")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
YEARS        = int(os.getenv("COT_YEARS", "10"))

MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()   # ALL | FILE | LIST
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")
MARKETS_LIST = [s.strip() for s in os.getenv("COT_MARKETS_LIST", "").split("|") if s.strip()]

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

LIMIT = 50000  # Page size für Socrata

# Spalten, die wir sicher in der CSV haben wollen (alles Weitere lassen wir pass-through)
CORE_COLS = [
    "report_date_as_yyyy_mm_dd",
    "cftc_contract_market_code",
    "market_and_exchange_names",
    "open_interest_all",
    "noncomm_positions_long_all",
    "noncomm_positions_short_all",
    "asset_mgr_positions_long_all",
    "asset_mgr_positions_short_all",
    "lev_money_positions_long_all",
    "lev_money_positions_short_all",
]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def read_markets_file(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out

def sget(path: str, params: Dict) -> List[Dict]:
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    url = f"{API_BASE}/{path}"
    # Kleine Retry-Logik gegen kurzzeitige 5xx/429
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=60)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code} for {r.url} ; body={r.text[:500]}")
            return r.json()
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(1.0 + attempt * 1.5)
    return []

def fetch_range(date_from_iso: str, filtered_markets: List[str]) -> List[Dict]:
    """Zieht alle Zeilen ab 'date_from_iso'. Optional wird auf Märkte gefiltert."""
    rows_all: List[Dict] = []
    offset = 0

    where_clauses = [f"report_date_as_yyyy_mm_dd >= '{date_from_iso}'"]
    if filtered_markets:
        # Socrata IN (…) Stringliste
        quoted = ", ".join([f"'{m.replace(\"'\", \"''\")}'" for m in filtered_markets])
        where_clauses.append(f"market_and_exchange_names IN ({quoted})")
    where = " AND ".join(where_clauses)

    select = ",".join(CORE_COLS)  # wir können gerne die Kerndaten priorisieren
    while True:
        params = {
            "$select": select,              # Kernspalten
            "$where": where,
            "$order": "report_date_as_yyyy_mm_dd ASC",
            "$limit": LIMIT,
            "$offset": offset,
        }
        chunk = sget(f"{DATASET_ID}.json", params)
        if not chunk:
            break
        rows_all.extend(chunk)
        if len(chunk) < LIMIT:
            break
        offset += LIMIT
        time.sleep(0.15)  # nett sein
    return rows_all

def normalize_row(r: Dict) -> Dict:
    # Stelle sicher, dass alle CORE_COLS existieren (sonst None)
    return {k: r.get(k, None) for k in CORE_COLS}

def write_csv(path: str, rows: List[Dict]) -> None:
    # Alle Keys zusammenschalten (falls API zusätzliche Felder liefert)
    keys = list(CORE_COLS)  # Start mit Kernspalten
    # plus alle weiteren Felder (stabil sortiert)
    extra = sorted({k for r in rows for k in r.keys()} - set(keys))
    if extra:
        keys += extra

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in keys})

def gzip_file(src: str, dst: str) -> None:
    with open(src, "rb") as f_in, gzip.open(dst, "wb", compresslevel=6) as f_out:
        while True:
            chunk = f_in.read(1024 * 1024)
            if not chunk:
                break
            f_out.write(chunk)

def zip_file(src: str, dst: str, arcname: str = None) -> None:
    if arcname is None:
        arcname = os.path.basename(src)
    with zipfile.ZipFile(dst, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        z.write(src, arcname)

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    report = {
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset": DATASET_ID,
        "years": YEARS,
        "mode": MODE,
        "rows": 0,
        "date_from": None,
        "date_to": None,
        "errors": [],
        "files": {}
    }

    # Zeitraum bestimmen
    date_to = datetime.utcnow().date()
    date_from = (date_to - timedelta(days=YEARS * 365))
    report["date_from"] = date_from.isoformat()
    report["date_to"] = date_to.isoformat()

    # Marktfilter vorbereiten
    markets: List[str] = []
    if MODE == "FILE":
        markets = read_markets_file(MARKETS_FILE)
    elif MODE == "LIST":
        markets = MARKETS_LIST
    elif MODE == "ALL":
        markets = []
    else:
        report["errors"].append({"stage":"config", "msg": f"unknown COT_MARKETS_MODE={MODE}"})
        json.dump(report, open(os.path.join(REP_DIR, "cot_10y_report.json"), "w"), indent=2)
        sys.exit(1)

    try:
        rows = fetch_range(date_from.isoformat(), markets)
    except Exception as e:
        report["errors"].append({"stage":"fetch", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_10y_report.json"), "w"), indent=2)
        sys.exit(1)

    # Normalisieren & sortieren
    rows = [normalize_row(r) for r in rows]
    rows.sort(key=lambda r: (r.get("report_date_as_yyyy_mm_dd") or "", r.get("cftc_contract_market_code") or ""))

    # Schreiben
    out_csv = os.path.join(OUT_DIR, "cot_10y.csv")
    write_csv(out_csv, rows)

    out_gz  = os.path.join(OUT_DIR, "cot_10y.csv.gz")
    gzip_file(out_csv, out_gz)

    out_zip = os.path.join(OUT_DIR, "cot_10y.zip")
    zip_file(out_csv, out_zip, arcname="cot_10y.csv")

    # Bericht
    report["rows"] = len(rows)
    def fsize(p): 
        return os.path.getsize(p) if os.path.exists(p) else 0
    report["files"] = {
        "csv": {"path": out_csv, "bytes": fsize(out_csv)},
        "gz":  {"path": out_gz,  "bytes": fsize(out_gz)},
        "zip": {"path": out_zip, "bytes": fsize(out_zip)},
    }
    json.dump(report, open(os.path.join(REP_DIR, "cot_10y_report.json"), "w"), indent=2)

    # kleine stdout-Zusammenfassung
    print("\n=== COT 10y Summary ===")
    print(f"Rows: {report['rows']}")
    for k,v in report["files"].items():
        print(f"{k.upper():>3}: {v['path']}  ({v['bytes']/1024/1024:.2f} MB)")
    if MODE != "ALL":
        print(f"Filtered markets: {len(markets)}")

if __name__ == "__main__":
    main()
