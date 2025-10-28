#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDS Proxy v2
- Region routing: EU vs US
- Base spread from curves (US: FRED OAS; EU: ECB series you provide)
- Adjustments: leverage (debt_to_equity), recent vol (stooq)
"""

import os, json, math
from datetime import datetime
import pandas as pd

PROC = "data/processed"
REP  = "data/reports"
os.makedirs(PROC, exist_ok=True); os.makedirs(REP, exist_ok=True)

EU_SUFFIX = (".DE",".PA",".AS",".MC",".MI",".BR",".WA",".PR",".VI",".LS",".BE",".SW",".HE",".CO",".OL",".ST",".FR",".IR",".NL",".PT",".PL",".CZ",".HU",".AT",".FI",".DK",".IE",".NO",".SE",".CH",".GB")

def load_fred_oas():
    p = os.path.join(PROC, "fred_oas.csv")
    if not os.path.exists(p): 
        return None
    df = pd.read_csv(p)
    # Expect columns: date, series_id, value, bucket, region
    # Choose latest US IG & HY
    latest = df.sort_values("date").groupby(["series_id"], as_index=False).tail(1)
    cur = {}
    for _,r in latest.iterrows():
        sid = str(r["series_id"])
        cur[sid] = float(r["value"])
    # Common IDs (adjust if your fetcher differs):
    # IG (US): BAMLC0A0CM, HY (US): BAMLH0A0HYM2
    us_ig = cur.get("BAMLC0A0CM", None)
    us_hy = cur.get("BAMLH0A0HYM2", None)
    return {"US_IG": us_ig, "US_HY": us_hy}

def load_ecb_eur_curves():
    # Plug in your Euro credit series here (from fetch_ecb.py)
    # For now, we accept any provided aliases and pick the last value.
    base_dir = "data/macro/ecb"
    if not os.path.isdir(base_dir): 
        return {}
    curves = {}
    # Try aliases you created, e.g., "ciss_ea" (as stress proxy),
    # or better: add proper EUR IG/HY OAS series if you have them.
    for alias in os.listdir(base_dir):
        if not alias.endswith(".csv"): 
            continue
        df = pd.read_csv(os.path.join(base_dir, alias))
        if df.empty: 
            continue
        val = float(df.tail(1)["value"].values[0])
        curves[alias.replace(".csv","")] = val
    # Choose a mapping: prefer eur_ig/eur_hy if present, else fallback
    eur_ig = curves.get("eur_ig") or curves.get("ciss_ea")  # placeholder
    eur_hy = curves.get("eur_hy") or (curves.get("ciss_ea") * 1.8 if curves.get("ciss_ea") else None)
    return {"EU_IG": eur_ig, "EU_HY": eur_hy, "raw": curves}

def infer_region(symbol:str) -> str:
    s = symbol.upper()
    return "EU" if s.endswith(EU_SUFFIX) else "US"

def load_fundamentals():
    p = os.path.join(PROC, "fundamentals_core.csv")
    if not os.path.exists(p):
        return pd.DataFrame(columns=["symbol"])
    df = pd.read_csv(p)
    return df

def load_watchlist(path="watchlists/mylist.txt"):
    out=[]
    if os.path.exists(path):
        for line in open(path,encoding="utf-8"):
            s=line.strip()
            if s and not s.startswith("#"): out.append(s)
    return out

def load_stooq_last(symbol):
    p = os.path.join("data/market/stooq", f"{symbol.lower().replace('^','idx_')}.csv")
    if not os.path.exists(p):
        return None
    df = pd.read_csv(p)
    if len(df)<21:
        return None
    # crude vol: 20d realized (close-to-close)
    ret = df["close"].pct_change()
    hv20 = ret.rolling(20).std().iloc[-1]
    return max(0.0, float(hv20)) if pd.notna(hv20) else None

def leverage_adj(debt_to_equity):
    # Map D/E into 0..1 "HY_weight"
    if pd.isna(debt_to_equity): 
        return 0.3
    x = float(debt_to_equity)
    # soft cap
    x = min(max(x, 0.0), 3.0)
    return x/3.0  # 0..1

def vol_adj(hv):
    # hv ~ daily stdev; map to bps add-on (simple)
    if hv is None: 
        return 0.0
    # 20–60 bps when HV is 2–6% (daily)
    return 10000.0 * min(max(hv, 0.0), 0.06) * 0.1  # crude, tune later

def main():
    fred = load_fred_oas()
    ecb  = load_ecb_eur_curves()
    fund = load_fundamentals()

    wl = load_watchlist()
    if not wl and not fund.empty:
        wl = fund["symbol"].tolist()
    wl = sorted(set(wl))

    rows=[]
    errs=[]
    for sym in wl:
        region = infer_region(sym)
        # base curves
        if region=="US":
            ig, hy = fred.get("US_IG"), fred.get("US_HY")
        else:
            ig, hy = ecb.get("EU_IG"), ecb.get("EU_HY")
        if ig is None:
            errs.append({"symbol": sym, "reason":"no_base_curve", "region":region})
            base = None
        else:
            # choose weight by leverage
            de = None
            if not fund.empty:
                f = fund[fund["symbol"].str.upper()==sym.upper()]
                if not f.empty and "debt_to_equity" in f.columns:
                    de = f.iloc[0]["debt_to_equity"]
            w_hy = leverage_adj(de)
            if hy is None: 
                # if no HY, just use IG + small bump
                base = ig + 50.0*w_hy
            else:
                base = ig*(1.0-w_hy) + hy*w_hy

            # add volatility addon
            hv = load_stooq_last(sym)
            base += vol_adj(hv)

        rows.append({
            "symbol": sym,
            "region": region,
            "proxy_spread": round(base,2) if base is not None else None,
        })

    out = pd.DataFrame(rows)
    out.to_csv(os.path.join(PROC,"cds_proxy.csv"), index=False)

    report = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "fred_oas_used": fred,
        "ecb_eur_curves": {k: v for k,v in ecb.items() if k!="raw"},
        "errors": errs,
        "rows": len(out),
        "preview": os.path.join(PROC,"cds_proxy.csv")
    }
    with open(os.path.join(REP,"cds_proxy_report.json"),"w",encoding="utf-8") as f:
        json.dump(report,f,indent=2)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
