#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build simple CDS proxy per symbol using FRED OAS indices.

Inputs
- data/processed/fred_oas.csv
- watchlist (env WATCHLIST_STOCKS)
- optional: config/oas_proxy_map.csv   (symbol,proxy) with proxy in {US_IG,US_HY,EU_IG,EU_HY}

Output
- data/processed/cds_proxy.csv  (symbol, proxy, asof, proxy_spread)
- data/reports/cds_proxy_report.json
"""

import os, json, sys, pandas as pd
from datetime import datetime

PROXY_SERIES = {
    "US_IG": "BAMLC0A0CM",
    "US_HY": "BAMLH0A0HYM2",
    "EU_IG": "BEMLEIG",
    "EU_HY": "BEMLEHY",
}

def read_watchlist(path: str):
    if not os.path.exists(path): return []
    if path.lower().endswith(".csv"):
        try:
            df = pd.read_csv(path)
            if "symbol" in df.columns:
                return [str(s).strip() for s in df["symbol"].dropna().tolist()]
        except Exception:
            pass
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if s and s.lower() != "symbol":
                syms.append(s)
    return syms

def auto_proxy(sym: str) -> str:
    s = sym.upper()
    if s in ("HYG","JNK"): return "US_HY"
    if s.endswith(".DE") or s.endswith(".EU") or s.endswith(".MI") or s.endswith(".PA"):
        return "EU_IG"
    return "US_IG"

def load_proxy_map():
    p = "config/oas_proxy_map.csv"
    if not os.path.exists(p):
        return {}
    try:
        df = pd.read_csv(p)
        if set(df.columns) >= {"symbol","proxy"}:
            m = {str(r.symbol).strip().upper(): str(r.proxy).strip().upper() for r in df.itertuples()}
            return m
    except Exception:
        pass
    return {}

def latest_oas(df_oas: pd.DataFrame, series_id: str) -> float:
    d = df_oas[df_oas["series_id"]==series_id]
    if d.empty: return None
    return float(d.sort_values("date").iloc[-1]["value"])

def main():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    oas_path = "data/processed/fred_oas.csv"
    if not os.path.exists(oas_path):
        print("missing", oas_path)
        pd.DataFrame(columns=["symbol","proxy","asof","proxy_spread"]).to_csv(
            "data/processed/cds_proxy.csv", index=False
        )
        json.dump({"error":"missing_fred_oas"}, open("data/reports/cds_proxy_report.json","w"), indent=2)
        return 0

    oas = pd.read_csv(oas_path, parse_dates=["date"])
    wl  = read_watchlist(os.getenv("WATCHLIST_STOCKS","watchlists/mylist.txt"))
    pmap = load_proxy_map()

    # letzte Werte je Serie
    latest = {sid: latest_oas(oas, sid) for sid in set(oas["series_id"].unique())}

    rows, miss = [], []
    asof = str(oas["date"].max().date()) if not oas.empty else ""

    for sym in wl:
        chosen = pmap.get(sym.upper(), auto_proxy(sym))
        sid = PROXY_SERIES.get(chosen)
        val = latest.get(sid)
        if val is None:
            miss.append({"symbol": sym, "proxy": chosen, "series": sid})
        rows.append({
            "symbol": sym,
            "proxy": chosen,
            "asof": asof,
            "proxy_spread": val
        })

    out = "data/processed/cds_proxy.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print("wrote", out, "rows=", len(rows))

    rep = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "watchlist": len(wl),
        "missing": miss[:50]
    }
    with open("data/reports/cds_proxy_report.json","w") as f:
        json.dump(rep, f, indent=2)

    return 0

if __name__ == "__main__":
    sys.exit(main())
