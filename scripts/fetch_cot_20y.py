#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_cot_20y.py
Pull 20y COT von CFTC Socrata (Dataset per ENV, z.B. Disaggregated-Combined: kh3c-gbw2)
- ohne $select-Liste (vermeidet Spalten-Mismatch)
- WHERE: Datumsbereich + optionales Market-IN-Filter (literal gequotet)
- robust (Retry/Timeout/Paging)
- schreibt NUR:
    data/processed/cot_20y.csv.gz
    data/reports/cot_20y_report.json
"""

import os, json, time, datetime as dt
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Netz / Socrata Settings (ENV overridebar) ─────────────────────
SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 25000))

API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
DATASET_ID   = os.getenv("COT_DATASET_ID", "kh3c-gbw2")  # ⚠️ Default: Disaggregated – Combined (Fut+Opt)
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
YEARS        = int(os.getenv("COT_YEARS", "20"))         # ⚠️ Default: 20 Jahre
MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()   # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

# ── HTTP Session mit Retry ─────────────────────────────────────────
def make_session():
    s = requests.Session()
    retry = Retry(
        total=SOC_RETRIES,
        connect=SOC_RETRIES,
        read=SOC_RETRIES,
        status=SOC_RETRIES,
        backoff_factor=SOC_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
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

# ── Helfer ─────────────────────────────────────────────────────────
def read_markets(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith(("#","//")):
                out.append(s)
    return out

def soql_quote(s: str) -> str:
    """SOQL-sicheres String-Literal (einzelne Quotes verdoppeln)."""
    return "'" + s.replace("'", "''") + "'"

def fetch_range(date_from, date_to, markets=None, dataset_id=None):
    rows_all, offset = [], 0
    dataset_id = dataset_id or DATASET_ID

    base_where = f"report_date_as_yyyy_mm_dd between '{date_from}' and '{date_to}'"
    where = base_where
    if markets:
        quoted = ",".join(soql_quote(m) for m in markets)
        where = f"{base_where} AND market_and_exchange_names in ({quoted})"

    params_base = {
        "$where": where,
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": SOC_LIMIT,
    }

    while True:
        params = dict(params_base)
        params["$offset"] = offset
        chunk = sget(f"{dataset_id}.json", params)
        if not chunk:
            break
        rows_all.extend(chunk)
        if len(chunk) < SOC_LIMIT:
            break
        offset += SOC_LIMIT
        time.sleep(0.2)
    return pd.DataFrame(rows_all)

# ── Main ───────────────────────────────────────────────────────────
def main():
    today = dt.date.today()
    date_to = today.strftime("%Y-%m-%d")
    # +10 Tage Puffer gegen Zeitzonen/Kalender-Kanten
    date_from = (today - dt.timedelta(days=365 * YEARS + 10)).strftime("%Y-%m-%d")

    report = {
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

    markets = None
    if MODE != "ALL":
        markets = read_markets(MARKETS_FILE)

    try:
        df = fetch_range(date_from, date_to, markets=markets)
    except Exception as e:
        report["errors"].append({"stage": "fetch", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_20y_report.json"), "w"), indent=2)
        raise SystemExit(1)

    # Nur .gz schreiben (Repo schlank halten)
    out_gz = os.path.join(OUT_DIR, "cot_20y.csv.gz")
    try:
        df.to_csv(out_gz, index=False, compression="gzip")
        report["files"]["cot_20y_csv_gz"] = out_gz
        report["rows"] = int(len(df))
    except Exception as e:
        report["errors"].append({"stage": "compress", "msg": str(e)})

    # Kleiner Abschlussreport
    json.dump(report, open(os.path.join(REP_DIR, "cot_20y_report.json"), "w"), indent=2)
    print(f"wrote {out_gz} rows={report['rows']}")

if __name__ == "__main__":
    main()

