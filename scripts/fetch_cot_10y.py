#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pull 10y COT (Futures+Options Combined) from CFTC Socrata (gpe5-46if)

- Keine $select-Liste (wir holen alle Spalten) → vermeidet Spalten-Mismatches
- Datumspanne via $where (Literalwerte, KEINE @f/@t Platzhalter) + Pagination
- Optionaler Marktfilter (market_and_exchange_names IN (...))
- Schreibt:
    data/processed/cot_10y.csv
    data/processed/cot_10y.csv.gz
    data/reports/cot_10y_report.json
"""

import os
import json
import time
import datetime as dt
import pandas as pd
import requests

# ── Konfiguration aus ENV ─────────────────────────────────────────────────────

API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
DATASET_ID   = os.getenv("COT_DATASET_ID", "gpe5-46if")          # Futures+Options combined
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
YEARS        = int(os.getenv("COT_YEARS", "10"))
MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()      # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

LIMIT = 50000   # Socrata page size

# ── Helpers ───────────────────────────────────────────────────────────────────

def soql_quote(s: str) -> str:
    """SOQL-sicheres Stringliteral: einfache Quotes verdoppeln und in '…' einbetten."""
    return "'" + s.replace("'", "''") + "'"

def sget(path: str, params: dict) -> list[dict]:
    """GET JSON von Socrata mit optionalem App-Token, robustem Fehlertext."""
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    url = f"{API_BASE}/{path}"
    try:
        r = requests.get(url, params=params, headers=headers, timeout=90)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        body = ""
        try:
            body = r.text[:800]
        except Exception:
            pass
        raise RuntimeError(f"HTTP {getattr(r,'status_code', '?')} for {url} ; body={body}") from e
    except Exception as e:
        raise RuntimeError(f"REQ_FAIL {url}: {e}") from e

def read_markets(path: str) -> list[str]:
    """Zeilenweise Märkte lesen (leerzeilen ignorieren)."""
    if not os.path.exists(path):
        return []
    out: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out

def fetch_range(date_from: str, date_to: str, markets: list[str] | None = None) -> pd.DataFrame:
    """
    Rows für einen Datumsbereich holen (und optional Märkte).
    WICHTIG: Datumswerte als LITERAL in $where (Socrata akzeptiert hier keine @f/@t).
    """
    rows_all: list[dict] = []
    offset = 0

    base_where = f"report_date_as_yyyy_mm_dd between '{date_from}' and '{date_to}'"
    where = base_where

    if markets:
        quoted = ",".join(soql_quote(m) for m in markets)
        where = f"{base_where} AND market_and_exchange_names in ({quoted})"

    params_base = {
        "$where": where,
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": LIMIT,
    }

    while True:
        params = dict(params_base)
        params["$offset"] = offset
        chunk = sget(f"{DATASET_ID}.json", params)
        if not chunk:
            break
        rows_all.extend(chunk)
        if len(chunk) < LIMIT:
            break
        offset += LIMIT
        time.sleep(0.15)  # höflich bleiben

    return pd.DataFrame(rows_all)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    today = dt.date.today()
    date_to = today.strftime("%Y-%m-%d")
    # +10 Tage Puffer, damit die Spanne garantiert inkl. Randdaten ist
    date_from = (today - dt.timedelta(days=365 * YEARS + 10)).strftime("%Y-%m-%d")

    report: dict = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset": DATASET_ID,
        "years": YEARS,
        "mode": MODE,
        "rows": 0,
        "date_from": date_from,
        "date_to": date_to,
        "errors": [],
        "files": {}
    }

    markets: list[str] | None = None
    if MODE != "ALL":
        markets = read_markets(MARKETS_FILE)

    try:
        df = fetch_range(date_from, date_to, markets=markets)
    except Exception as e:
        report["errors"].append({"stage": "fetch", "msg": str(e)})
        with open(os.path.join(REP_DIR, "cot_10y_report.json"), "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        raise SystemExit(1)

    out_csv = os.path.join(OUT_DIR, "cot_10y.csv")
    df.to_csv(out_csv, index=False)

    # Zusätzlich komprimiert
    out_gz = out_csv + ".gz"
    try:
        df.to_csv(out_gz, index=False, compression="gzip")
        report["files"]["cot_10y_csv_gz"] = out_gz
    except Exception as e:
        report["errors"].append({"stage": "compress", "msg": str(e)})

    report["rows"] = int(len(df))
    report["files"]["cot_10y_csv"] = out_csv

    with open(os.path.join(REP_DIR, "cot_10y_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

if __name__ == "__main__":
    main()
