#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_cot_20y.py  — zieht 20y COT von CFTC Socrata
Aufruf-Beispiele:
  python scripts/fetch_cot_20y.py --dataset kh3c-gbw2 --out data/processed/cot_20y_disagg.csv.gz
  python scripts/fetch_cot_20y.py --dataset yw9f-hn96   --out data/processed/cot_20y_tff.csv.gz
"""

import os, json, time, argparse, datetime as dt
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Defaults via ENV
API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
YEARS        = int(os.getenv("COT_YEARS", "20"))
MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()   # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 25000))

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True, help="Socrata Dataset ID (z.B. kh3c-gbw2 oder yw9f-hn96)")
    p.add_argument("--out", required=True, help="Zieldatei (.csv.gz)")
    p.add_argument("--mode", default=MODE, choices=["ALL","FILE","LIST"])
    p.add_argument("--years", type=int, default=YEARS)
    p.add_argument("--markets-file", default=MARKETS_FILE)
    return p.parse_args()

def make_session():
    s = requests.Session()
    retry = Retry(total=SOC_RETRIES, connect=SOC_RETRIES, read=SOC_RETRIES, status=SOC_RETRIES,
                  backoff_factor=SOC_BACKOFF, status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    headers = {"Accept":"application/json"}
    if APP_TOKEN: headers["X-App-Token"] = APP_TOKEN
    s.headers.update(headers)
    return s

SESSION = make_session()

def sget(url, params):
    r = SESSION.get(url, params=params, timeout=SOC_TIMEOUT); r.raise_for_status()
    return r.json()

def read_markets(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith(("#","//"))]

def soql_quote(s): return "'" + s.replace("'", "''") + "'"

def fetch_range(dataset_id, date_from, date_to, markets):
    rows_all, offset = [], 0
    base_where = f"report_date_as_yyyy_mm_dd between '{date_from}' and '{date_to}'"
    where = base_where
    if markets:
        where = f"{base_where} AND market_and_exchange_names in ({','.join(soql_quote(m) for m in markets)})"
    params_base = {"$where": where, "$order": "report_date_as_yyyy_mm_dd ASC", "$limit": SOC_LIMIT}
    while True:
        params = dict(params_base, **{"$offset": offset})
        chunk = sget(f"{API_BASE}/{dataset_id}.json", params)
        if not chunk: break
        rows_all += chunk
        if len(chunk) < SOC_LIMIT: break
        offset += SOC_LIMIT; time.sleep(0.2)
    return pd.DataFrame(rows_all)

def main():
    args = parse_args()
    today = dt.date.today()
    date_to   = today.strftime("%Y-%m-%d")
    date_from = (today - dt.timedelta(days=365*args.years + 10)).strftime("%Y-%m-%d")
    markets = None if args.mode=="ALL" else read_markets(args.markets_file)

    df = fetch_range(args.dataset, date_from, date_to, markets)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False, compression="gzip")

    # kleiner Report (gleicher Name wie out, nur _report.json)
    rep = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "datasets": [args.dataset],
        "years": args.years, "mode": args.mode,
        "rows": int(len(df)), "date_from": date_from, "date_to": date_to,
        "files": { "out": args.out }
    }
    rep_name = os.path.splitext(os.path.basename(args.out))[0].replace(".csv","") + "_report.json"
    os.makedirs("data/reports", exist_ok=True)
    with open(os.path.join("data/reports", rep_name), "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    print(f"wrote {args.out} rows={rep['rows']} dataset={args.dataset}")

if __name__ == "__main__":
    main()
