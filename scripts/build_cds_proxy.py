#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Baut cds_proxy.csv auf Basis der letzten verfügbaren OAS.
Regeln:
- US-Ticker: IG vs HY anhand Beta-Threshold (Default 1.15). Fehlt Beta -> IG.
- EU-Ticker: bis EU-OAS vorhanden sind, Fallback auf US-Kurven (klar markiert).
Outputs:
  data/processed/cds_proxy.csv  (symbol, region, proxy, asof, proxy_spread)
  data/reports/cds_proxy_report.json
"""

import os, json, re
import pandas as pd
from datetime import datetime

BETA_TH = float(os.getenv("CDS_BETA_TH", "1.15"))

def guess_region(sym: str) -> str:
    sym = sym.upper()
    if sym.endswith(".DE") or sym.endswith(".PA") or sym.endswith(".AS") or sym.endswith(".MC") or sym.endswith(".MI") or sym.endswith(".BR"):
        return "EU"
    return "US"

def load_watchlist(path: str) -> list:
    if not os.path.exists(path):
        return []
    syms = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"): 
                continue
            syms.append(t)
    return syms

def latest_oas() -> pd.DataFrame:
    p = "data/processed/fred_oas.csv"
    if not os.path.exists(p):
        return pd.DataFrame(columns=["bucket","region","value","date"])
    df = pd.read_csv(p)
    if df.empty:
        return pd.DataFrame(columns=["bucket","region","value","date"])
    df["date"] = pd.to_datetime(df["date"])
    idx = df.groupby(["bucket","region"])["date"].idxmax()
    last = df.loc[idx, ["bucket","region","value","date","series_id"]].reset_index(drop=True)
    last.rename(columns={"date":"asof","value":"proxy_spread","series_id":"source"}, inplace=True)
    return last

def fundamentals_beta() -> dict:
    p = "data/processed/fundamentals_core.csv"
    if not os.path.exists(p):
        return {}
    try:
        df = pd.read_csv(p)
        df = df[["symbol","beta"]].dropna()
        return dict(zip(df["symbol"].str.upper(), df["beta"]))
    except Exception:
        return {}

def choose_bucket(beta: float | None) -> str:
    if beta is None:
        return "IG"
    return "HY" if beta >= BETA_TH else "IG"

def main():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    wl = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    syms = load_watchlist(wl)

    last = latest_oas()
    beta_map = fundamentals_beta()

    have_us_ig = last[(last["region"]=="US") & (last["bucket"]=="IG")]
    have_us_hy = last[(last["region"]=="US") & (last["bucket"]=="HY")]

    if have_us_ig.empty or have_us_hy.empty:
        # Minimaler Fallback: leere Datei + Report
        pd.DataFrame(columns=["symbol","region","proxy","asof","proxy_spread"]).to_csv(
            "data/processed/cds_proxy.csv", index=False
        )
        report = {
            "rows": 0,
            "fred_oas_used": {"US_IG": None, "US_HY": None, "EU_IG": None, "EU_HY": None},
            "errors": [{"reason": "missing_us_oas", "msg": "US IG/HY OAS not available"}],
        }
        json.dump(report, open("data/reports/cds_proxy_report.json","w"), indent=2)
        print("⚠ US OAS not available – empty cds_proxy.csv written.")
        return

    us_ig = have_us_ig.iloc[0].to_dict()
    us_hy = have_us_hy.iloc[0].to_dict()

    rows = []
    for s in syms:
        reg = guess_region(s)
        b = beta_map.get(s.upper())
        bucket = choose_bucket(b)

        # Proxy-Wahl:
        if bucket == "IG":
            src = us_ig
            proxy_name = "US_IG" if reg=="US" else "US_IG (EU-fallback)"
        else:
            src = us_hy
            proxy_name = "US_HY" if reg=="US" else "US_HY (EU-fallback)"

        rows.append({
            "symbol": s,
            "region": reg,
            "proxy": proxy_name,
            "asof": str(src["asof"].date()) if hasattr(src["asof"], "date") else str(src["asof"]),
            "proxy_spread": float(src["proxy_spread"]),
        })

    out = pd.DataFrame(rows)
    out.to_csv("data/processed/cds_proxy.csv", index=False)

    report = {
        "rows": len(out),
        "fred_oas_used": {
            "US_IG": float(us_ig["proxy_spread"]),
            "US_HY": float(us_hy["proxy_spread"]),
            "EU_IG": None,
            "EU_HY": None,
        },
        "errors": [
            {"reason": "eu_curves_missing", "msg": "EU OAS not available on FRED; EU mapped to US curves (fallback)."}
        ],
    }
    json.dump(report, open("data/reports/cds_proxy_report.json","w"), indent=2)
    print(f"cds_proxy.csv rows: {len(out)}")

if __name__ == "__main__":
    main()
