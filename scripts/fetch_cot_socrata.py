#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_cot_socrata.py
Zieht den *letzten verfügbaren* COT-Report (Futures+Options Combined, Dataset gpe5-46if)
in zwei Schritten:
  1) latest_date via ORDER BY DESC + LIMIT 1 (ohne WHERE/Parameterbindung)
  2) alle Zeilen für latest_date mit literal gequotetem WHERE
Schreibt:
  - data/processed/cot.csv
  - data/processed/cot_summary.csv  (kleine Quick-Summary)
  - data/reports/cot_errors.json    (nur bei Fehlern)
"""

import os, json, time, datetime as dt
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- robuste Netz-Settings
SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 90))
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 50000))

API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
DATASET_ID   = os.getenv("COT_DATASET_ID", "gpe5-46if")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()   # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

def make_session():
    s = requests.Session()
    retry = Retry(
        total=SOC_RETRIES, connect=SOC_RETRIES, read=SOC_RETRIES, status=SOC_RETRIES,
        backoff_factor=SOC_BACKOFF, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    s.headers.update(headers)
    return s

SESSION = make_session()

def sget(path, params):
    url = f"{API_BASE}/{path}"
    r = SESSION.get(url, params=params, timeout=SOC_TIMEOUT)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP {r.status_code} for {url} ; body={r.text[:500]}") from e
    return r.json()

def soql_quote(s: str) -> str:
    """SOQL-sicheres String-Literal (einzelne Quotes verdoppeln)."""
    return "'" + s.replace("'", "''") + "'"

def read_markets(path):
    if not os.path.exists(path):
        return []
    res = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                res.append(s)
    return res

def get_latest_report_date() -> str:
    """
    Holt den letzten Report-Tag rein über ORDER DESC + LIMIT 1.
    Rückgabeformat wie im Dataset (YYYY-MM-DDT00:00:00.000 oder YYYY-MM-DD...).
    """
    params = {
        "$select": "report_date_as_yyyy_mm_dd",
        "$order":  "report_date_as_yyyy_mm_dd DESC",
        "$limit":  1
    }
    data = sget(f"{DATASET_ID}.json", params)
    if not data:
        raise RuntimeError("No rows returned for latest date probe.")
    raw = str(data[0]["report_date_as_yyyy_mm_dd"])
    # auf 'YYYY-MM-DD' kürzen (erste 10 Zeichen decken den ISO-Tag ab)
    return raw[:10]

def fetch_all_for_date(date_str: str, markets=None) -> pd.DataFrame:
    """
    Zieht alle Zeilen für ein Datum (literal gequotetes WHERE).
    Optionales Market-Filter via IN (...).
    """
    base_where = f"report_date_as_yyyy_mm_dd = {soql_quote(date_str)}"
    if markets:
        quoted = ",".join(soql_quote(m) for m in markets)
        where = f"{base_where} AND market_and_exchange_names in ({quoted})"
    else:
        where = base_where

    rows, offset = [], 0
    while True:
        params = {
            "$where": where,
            "$order": "market_and_exchange_names ASC",
            "$limit": SOC_LIMIT,
            "$offset": offset
        }
        chunk = sget(f"{DATASET_ID}.json", params)
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < SOC_LIMIT:
            break
        offset += SOC_LIMIT
        time.sleep(0.15)
    return pd.DataFrame(rows)

def write_summary(df: pd.DataFrame, out_csv: str):
    """
    Sehr einfache Kurz-Zusammenfassung pro Markt (nur Count).
    (Kannst du später beliebig ausbauen.)
    """
    if df.empty:
        pd.DataFrame({"market_and_exchange_names": [], "rows": []}).to_csv(out_csv, index=False)
        return
    grp = (
        df.groupby("market_and_exchange_names", dropna=False)
          .size().reset_index(name="rows")
          .sort_values(["rows","market_and_exchange_names"], ascending=[False, True])
    )
    grp.to_csv(out_csv, index=False)

def main():
    report = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset": DATASET_ID,
        "latest": None,
        "filtered_mode": MODE,
        "errors": []
    }

    try:
        latest = get_latest_report_date()
        report["latest"] = latest
    except Exception as e:
        report["errors"].append({"stage": "probe_latest", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    markets = None
    if MODE != "ALL":
        markets = read_markets(MARKETS_FILE)

    try:
        df = fetch_all_for_date(latest, markets=markets)
    except Exception as e:
        report["errors"].append({"stage": "fetch", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    out_full = os.path.join(OUT_DIR, "cot.csv")
    df.to_csv(out_full, index=False)

    out_sum = os.path.join(OUT_DIR, "cot_summary.csv")
    write_summary(df, out_sum)

    # Erfolg: optional kleinen Report schreiben
    json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)

if __name__ == "__main__":
    main()
