#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build simple CDS proxies from FRED OAS indices.

Inputs
- data/processed/fred_oas.csv    (columns: date,series_id,value)
- config/mappings/proxy_map.csv  (columns: symbol,proxy)  [optional]
- watchlists/mylist.txt or .csv  (env WATCHLIST_STOCKS)

Output
- data/processed/cds_proxy.csv   (columns: symbol,proxy,asof,proxy_spread)
- data/reports/cds_proxy_report.json
"""

import os, json
import pandas as pd
from datetime import datetime

FRED_BUCKETS = {
    "US_IG": ["BAMLC0A0CM"],       # ICE BofA US Corp Master OAS
    "US_HY": ["BAMLH0A0HYM2"],     # ICE BofA US High Yield OAS
    "EU_IG": ["BEMLEIG"],          # ICE BofA Euro IG Corporate OAS
    "EU_HY": ["BEMLEHY"],          # ICE BofA Euro High Yield OAS
}

def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

def read_watchlist(path:str) -> list:
    if not os.path.exists(path): return []
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)
        col = "symbol" if "symbol" in df.columns else df.columns[0]
        return [str(x).strip() for x in df[col].dropna().tolist()]
    return [ln.strip() for ln in open(path, encoding="utf-8") if ln.strip() and ln.strip().lower()!="symbol"]

def load_proxy_map(path="config/mappings/proxy_map.csv") -> dict:
    if not os.path.exists(path):
        return {}
    df = pd.read_csv(path)
    df = df.dropna(subset=["symbol","proxy"])
    return {str(r.symbol).strip(): str(r.proxy).strip().upper() for _,r in df.iterrows()}

def latest_oas_by_series(df_fred: pd.DataFrame) -> pd.Series:
    # df_fred: date, series_id, value
    df_fred["date"] = pd.to_datetime(df_fred["date"])
    idx = df_fred.sort_values(["series_id","date"]).groupby("series_id").tail(1)
    return idx.set_index("series_id")["value"]

def bucket_value(latest_series: pd.Series, bucket: str) -> float|None:
    ids = FRED_BUCKETS.get(bucket, [])
    vals = []
    for sid in ids:
        if sid in latest_series.index:
            v = latest_series.loc[sid]
            try:
                v = float(v)
                if pd.notna(v): vals.append(v)
            except Exception:
                pass
    if not vals:
        return None
    return float(sum(vals)/len(vals))

def main():
    ensure_dirs()

    wl_path = os.getenv("WATCHLIST_STOCKS","watchlists/mylist.txt")
    symbols = read_watchlist(wl_path)
    if not symbols:
        symbols = ["AAPL"]

    fred_p = "data/processed/fred_oas.csv"
    if not os.path.exists(fred_p):
        print("missing", fred_p)
        rep = {"ts": datetime.utcnow().isoformat()+"Z", "rows":0, "errors":["missing fred_oas.csv"]}
        json.dump(rep, open("data/reports/cds_proxy_report.json","w"), indent=2)
        return 0

    fred = pd.read_csv(fred_p)
    if not {"date","series_id","value"}.issubset(fred.columns):
        print("fred_oas.csv has wrong columns")
        rep = {"ts": datetime.utcnow().isoformat()+"Z", "rows":0, "errors":["bad fred_oas columns"]}
        json.dump(rep, open("data/reports/cds_proxy_report.json","w"), indent=2)
        return 0

    latest = latest_oas_by_series(fred)
    asof = fred["date"].max()

    pmap = load_proxy_map()
    rows = []
    errs = []

    for sym in symbols:
        bucket = pmap.get(sym, "US_IG")
        val = bucket_value(latest, bucket)
        if val is None:
            errs.append({"symbol": sym, "proxy": bucket, "msg": "no OAS for proxy bucket"})
            continue
        rows.append({"symbol": sym, "proxy": bucket, "asof": asof, "proxy_spread": round(val, 2)})

    out = pd.DataFrame(rows)
    out_p = "data/processed/cds_proxy.csv"
    out.to_csv(out_p, index=False)
    print("wrote", out_p, "rows=", len(out))

    report = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "asof": asof,
        "rows": len(rows),
        "missing": len(errs),
        "sample": rows[:5],
        "errors": errs
    }
    json.dump(report, open("data/reports/cds_proxy_report.json","w"), indent=2)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
