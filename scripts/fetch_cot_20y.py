#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_cot_20y.py — robust (SMART) CFTC Socrata Pull mit Alias/Token-Matching
Beispiele:
  python scripts/fetch_cot_20y.py --dataset kh3c-gbw2 --out data/processed/cot_20y_disagg.csv.gz --mode SMART
  python scripts/fetch_cot_20y.py --dataset yw9f-hn96   --out data/processed/cot_20y_tff.csv.gz    --mode SMART
Env/Args:
  MODE: ALL | FILE | LIST | SMART   (SMART = FILE + Alias/LIKE-Expansion)
"""

import os, io, json, time, argparse, datetime as dt, gzip
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Defaults via ENV
API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
YEARS        = int(os.getenv("COT_YEARS", "20"))
MODE_DEFAULT = os.getenv("COT_MARKETS_MODE", "ALL").upper()     # + SMART
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")
SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 25000))

ALIASES = {
    # Börsen / Schreibweisen
    "COMEX": ["COMEX", "COMMODITY EXCHANGE INC"],
    "NYMEX": ["NYMEX", "NEW YORK MERCANTILE EXCHANGE"],
    "CBOT":  ["CBOT", "CHICAGO BOARD OF TRADE"],
    "KCBT":  ["KANSAS CITY BOARD OF TRADE", "KANSAS CITY B O T"],
    "MGEX":  ["MINNEAPOLIS GRAIN EXCHANGE", "MGE", "MGEX"],
    "CFE":   ["CBOE FUTURES EXCHANGE", "CFE", "CBOE FUTURES EXCH"],
    "CME":   ["CHICAGO MERCANTILE EXCHANGE", "CME"],

    # Rohstoff-Familien (zusätzliche Varianten)
    "WHEAT_SRW": ["WHEAT-SRW", "WHEAT - CHICAGO BOARD OF TRADE", "WHEAT - CBOT", "SOFT RED WHEAT"],
    "WHEAT_HRW": ["HARD RED WINTER WHEAT", "HRW"],
    "WHEAT_HRS": ["HARD RED SPRING WHEAT", "HRS"],
    "COPPER":    ["COPPER", "COPPER, HIGH GRADE"],
}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True, help="Socrata Dataset ID (kh3c-gbw2 / yw9f-hn96)")
    p.add_argument("--out", required=True, help="Zieldatei (.csv.gz)")
    p.add_argument("--mode", default=MODE_DEFAULT, choices=["ALL","FILE","LIST","SMART"])
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

def read_lines(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith(("#","//"))]

def soql_quote(s): return "'" + s.replace("'", "''") + "'"

def like_expr(token):
    # UPPER(name) LIKE %TOKEN%
    t = token.upper()
    return f"upper(market_and_exchange_names) like '%{t.replace('%','%%')}%'"

def expand_aliases(raw_line):
    """
    Nimmt z.B. 'COPPER - COMMODITY EXCHANGE INC.' und baut eine Liste
    von (required_tokens_any_of_exchange_variants, required_tokens_material).
    """
    line = raw_line.upper()
    # Material (erste Wortgruppe bis ' - '), Börsen-Anteil danach
    parts = [p.strip() for p in line.split(" - ", 1)]
    material = parts[0]
    exch = parts[1] if len(parts) > 1 else ""

    # Material-Varianten
    mats = []
    if "WHEAT" in material:
        if "HARD RED WINTER" in material: mats = ALIASES["WHEAT_HRW"]
        elif "HARD RED SPRING" in material: mats = ALIASES["WHEAT_HRS"]
        else: mats = ALIASES["WHEAT_SRW"]
    elif "COPPER" in material:
        mats = ALIASES["COPPER"]
    else:
        mats = [material]

    # Exchange-Varianten
    exchs = []
    for key in ("COMEX","NYMEX","CBOT","KCBT","MGEX","CFE","CME"):
        if key in exch or key in material or any(x in exch for x in ALIASES[key]):
            exchs = ALIASES[key]; break
    if not exchs and exch:
        exchs = [exch]
    if not exchs:
        # Fallback: keine Exchange-Bindung
        exchs = [""]

    # Kombi aller Varianten (Material × Exchange)
    combos = []
    for m in mats:
        for e in exchs:
            tokens = [m] + ([e] if e else [])
            combos.append([t for t in tokens if t])
    return combos

def build_where_any_of(combos):
    # OR über alle Varianten; jede Variante wird zu AND von LIKEs
    variants = []
    for tokens in combos:
        conds = [like_expr(t) for t in tokens]
        variants.append("(" + " AND ".join(conds) + ")")
    return " OR ".join(variants)

def fetch_range(dataset_id, date_from, date_to, mode, markets):
    rows_all = []
    base = f"report_date_as_yyyy_mm_dd between '{date_from}' and '{date_to}'"

    if mode == "ALL":
        where = base
        rows_all += _paged_pull(dataset_id, where)
    elif mode in ("FILE","LIST"):
        # exakte IN-Menge (wie bisher)
        in_list = ",".join(soql_quote(m) for m in markets)
        where = f"{base} AND market_and_exchange_names in ({in_list})"
        rows_all += _paged_pull(dataset_id, where)
    else:  # SMART
        for line in markets:
            combos = expand_aliases(line)
            where = base + " AND (" + build_where_any_of(combos) + ")"
            rows_all += _paged_pull(dataset_id, where)

    return pd.DataFrame(rows_all)

def _paged_pull(dataset_id, where):
    acc, offset = [], 0
    params_base = {"$where": where, "$order": "report_date_as_yyyy_mm_dd ASC", "$limit": SOC_LIMIT}
    while True:
        chunk = sget(f"{API_BASE}/{dataset_id}.json", dict(params_base, **{"$offset": offset}))
        if not chunk: break
        acc += chunk
        if len(chunk) < SOC_LIMIT: break
        offset += SOC_LIMIT
        time.sleep(0.15)
    return acc

def main():
    args = parse_args()
    today = dt.date.today()
    date_to   = today.strftime("%Y-%m-%d")
    date_from = (today - dt.timedelta(days=365*args.years + 10)).strftime("%Y-%m-%d")
    markets = None
    if args.mode in ("FILE","LIST","SMART"):
        markets = read_lines(args.markets_file)

    df = fetch_range(args.dataset, date_from, date_to, args.mode, markets)

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    if args.out.endswith(".gz"):
        with gzip.open(args.out, "wt", encoding="utf-8", newline="") as gz:
            df.to_csv(gz, index=False)
    else:
        df.to_csv(args.out, index=False)

    rep = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset": args.dataset, "years": args.years, "mode": args.mode,
        "rows": int(len(df)), "date_from": date_from, "date_to": date_to,
        "watchlist": (len(markets or []))
    }
    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/cot_20y_report.json","w",encoding="utf-8") as f: json.dump(rep,f,indent=2)
    print("wrote", args.out, "rows=", rep["rows"], "mode=", args.mode)

if __name__ == "__main__":
    main()
