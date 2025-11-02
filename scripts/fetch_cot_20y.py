#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_cot_20y.py — ROBUST (fuzzy) fetch & filter für CFTC Socrata
Beispiele:
  python scripts/fetch_cot_20y.py --dataset kh3c-gbw2 --out data/processed/cot_20y_disagg.csv.gz --mode FILE
  python scripts/fetch_cot_20y.py --dataset yw9f-hn96   --out data/processed/cot_20y_tff.csv.gz     --mode FILE
"""

import os, json, time, argparse, datetime as dt, re, gzip, io
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Defaults via ENV
API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
YEARS        = int(os.getenv("COT_YEARS", "20"))
MODE         = os.getenv("COT_MARKETS_MODE", "FILE").upper()   # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 50000))  # größer: wir filtern lokal

# ───────────────────────── util ─────────────────────────
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True, help="Socrata Dataset ID (kh3c-gbw2=Disagg, yw9f-hn96=TFF)")
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

def read_markets_from_file(path):
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith(("#","//"))]

# Normalisierung für robuste Vergleichbarkeit
DROP = re.compile(r"\b(THE|OF|AND|INC|LTD|PLC|EXCH|EXCHANGE|BOARD|TRADE|CME|CBOT|NYMEX|COMEX|ICE|EUROPE|FUTURES|MARKET|INDEX)\b")
NONALNUM = re.compile(r"[^A-Z0-9\s]+")
MULTISPC = re.compile(r"\s+")
def norm_key(s: str) -> str:
    if not s: return ""
    s = s.upper()
    s = NONALNUM.sub(" ", s)
    s = DROP.sub(" ", s)
    s = MULTISPC.sub(" ", s).strip()
    return s

def fuzzy_hit(src_norm: str, want_norm: str) -> bool:
    # exact or contains either direction
    return (src_norm == want_norm) or (want_norm in src_norm) or (src_norm in want_norm)

# ───────────────────────── fetch all within date window ─────────────────────────
def fetch_range(dataset_id, date_from, date_to):
    rows_all, offset = [], 0
    where = f"report_date_as_yyyy_mm_dd between '{date_from}' and '{date_to}'"
    params_base = {"$where": where, "$order": "report_date_as_yyyy_mm_dd ASC", "$limit": SOC_LIMIT}
    while True:
        params = dict(params_base, **{"$offset": offset})
        chunk = sget(f"{API_BASE}/{dataset_id}.json", params)
        if not chunk: break
        rows_all += chunk
        if len(chunk) < SOC_LIMIT: break
        offset += SOC_LIMIT; time.sleep(0.2)
    return pd.DataFrame(rows_all)

# ───────────────────────── main ─────────────────────────
def main():
    args = parse_args()

    today = dt.date.today()
    date_to   = today.strftime("%Y-%m-%d")
    date_from = (today - dt.timedelta(days=365*args.years + 10)).strftime("%Y-%m-%d")

    # 1) Ziehen (ohne Markt-Filter → robust gegen Namens-Drift)
    df = fetch_range(args.dataset, date_from, date_to)

    # Spalten-Aliase erkennen
    def col(*cands):
        for c in cands:
            if c in df.columns: return c
        return None

    c_market = col("market_and_exchange_names","market_and_exchange_name",
                   "contract_market_names","contract_market_name","commodity_name","commodity")
    c_date   = col("report_date_as_yyyy_mm_dd","report_date_as_yyyy-mm-dd",
                   "as_of_date_in_form_yyyy_mm_dd","report_date")
    if c_market is None or c_date is None:
        raise SystemExit("COT header missing required columns")

    # 2) Optional: lokal filtern gegen Watchlist (fuzzy)
    used_watch = []
    missing_watch = []
    diag = {}

    if args.mode in ("FILE","LIST"):
        watch = read_markets_from_file(args.markets_file) if args.mode=="FILE" else []
        watch_norm = [(w, norm_key(w)) for w in watch]
        # Norm-Key pro Zeile
        df["_mk_norm"] = df[c_market].map(norm_key)

        keep_mask = pd.Series(False, index=df.index)
        for w_raw, w_norm in watch_norm:
            hit = df["_mk_norm"].apply(lambda s: fuzzy_hit(s, w_norm))
            if hit.any():
                used_watch.append(w_raw)
                keep_mask |= hit
            else:
                missing_watch.append(w_raw)

        df = df.loc[keep_mask].copy()

    # 3) Sort / Diagnose (rows, first, last) pro Markt
    # vereinheitlichte Datumsspalte
    df["_date"] = (df[c_date].astype(str)
                      .str.replace("_","-", regex=False)
                      .str.slice(0,10))
    df.sort_values(by=["_date", c_market], inplace=True)
    for mk, grp in df.groupby(c_market):
        d_first = grp["_date"].iloc[0] if len(grp) else "n/a"
        d_last  = grp["_date"].iloc[-1] if len(grp) else "n/a"
        diag[mk] = {"rows": int(len(grp)), "first": d_first, "last": d_last}

    # 4) Schreiben (CSV.GZ)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with gzip.open(args.out, "wt", encoding="utf-8", newline="") as gz:
        df.drop(columns=[c for c in ["_mk_norm","_date"] if c in df.columns]).to_csv(gz, index=False)

    # 5) Report
    rep = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dataset": args.dataset,
        "years": args.years,
        "mode": args.mode,
        "rows_total": int(len(df)),
        "date_from": date_from,
        "date_to": date_to,
        "markets_used": used_watch,
        "markets_missing": missing_watch,
        "per_market": diag,
        "out": args.out
    }
    rep_name = os.path.splitext(os.path.basename(args.out))[0].replace(".csv","") + "_report.json"
    os.makedirs("data/reports", exist_ok=True)
    with open(os.path.join("data/reports", rep_name), "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

    print(f"[OK] wrote {args.out} rows={rep['rows_total']} dataset={args.dataset}")
    if missing_watch:
        print("WARN: missing from watchlist:")
        for m in missing_watch: print("  -", m)

if __name__ == "__main__":
    main()
