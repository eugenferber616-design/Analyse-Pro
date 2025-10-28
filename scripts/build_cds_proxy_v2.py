#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, math, pandas as pd
from datetime import datetime

PROC="data/processed"; REP="data/reports"
os.makedirs(PROC, exist_ok=True); os.makedirs(REP, exist_ok=True)

EU_SUFFIX = (".DE",".PA",".AS",".MC",".MI",".BR",".WA",".PR",".VI",".LS",".BE",".SW",".HE",".CO",".OL",".ST",".FR",".IR",".NL",".PT",".PL",".CZ",".HU",".AT",".FI",".DK",".IE",".NO",".SE",".CH",".GB")

def infer_region(sym): return "EU" if sym.upper().endswith(EU_SUFFIX) else "US"

def load_watchlist(path="watchlists/mylist.txt"):
    if not os.path.exists(path): return []
    return [l.strip() for l in open(path,encoding="utf-8") if l.strip() and not l.startswith("#")]

def load_fred_oas_latest():
    p=os.path.join(PROC,"fred_oas.csv")
    if not os.path.exists(p): return {}
    df=pd.read_csv(p)  # date,series_id,value,bucket,region
    last=df.sort_values("date").groupby(["region","bucket"],as_index=False).tail(1)
    cur={(r.region, r.bucket): float(r.value) for _,r in last.iterrows()}
    return {
        "US_IG": cur.get(("US","IG")), "US_HY": cur.get(("US","HY")),
        "EU_IG": cur.get(("EU","IG")), "EU_HY": cur.get(("EU","HY"))
    }

def load_fundamentals():
    p=os.path.join(PROC,"fundamentals_core.csv")
    return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame(columns=["symbol"])

def load_stooq_hv20(symbol):
    p=os.path.join("data/market/stooq", f"{symbol.lower().replace('^','idx_')}.csv")
    if not os.path.exists(p): return None
    df=pd.read_csv(p)
    if len(df)<22: return None
    ret=df["close"].pct_change()
    hv=ret.rolling(20).std().iloc[-1]
    return float(hv) if pd.notna(hv) else None

def w_hy_from_de(debt_to_equity):
    if pd.isna(debt_to_equity): return 0.3
    x=float(debt_to_equity); x=min(max(x,0.0),3.0)
    return x/3.0

def vol_addon(hv):
    if hv is None: return 0.0
    return 10000.0 * min(max(hv,0.0),0.06) * 0.1  # 0..60bps -> 0..6bps

def main():
    wl = load_watchlist()
    fund = load_fundamentals()
    curves = load_fred_oas_latest()

    rows, errs = [], []
    for sym in sorted(set(wl)):
        region=infer_region(sym)
        ig = curves.get(f"{region}_IG")
        hy = curves.get(f"{region}_HY")
        if ig is None and hy is None:
            errs.append({"symbol":sym,"reason":"no_curves","region":region}); spread=None
        else:
            de=None
            if not fund.empty:
                m=fund[fund["symbol"].str.upper()==sym.upper()]
                if not m.empty and "debt_to_equity" in m.columns: de=m.iloc[0]["debt_to_equity"]
            w=w_hy_from_de(de)
            base = (ig if ig is not None else 0.0)*(1-w) + (hy if hy is not None else ig)*w
            base += vol_addon(load_stooq_hv20(sym))
            spread=round(base,2)
        rows.append({"symbol":sym,"region":region,"proxy_spread":spread})

    out=pd.DataFrame(rows); out.to_csv(os.path.join(PROC,"cds_proxy.csv"), index=False)
    rep={"ts":datetime.utcnow().isoformat()+"Z","rows":len(out),"fred_oas_used":curves,"errors":errs,
         "preview":"data/processed/cds_proxy.csv"}
    json.dump(rep, open(os.path.join(REP,"cds_proxy_report.json"),"w"), indent=2)
    print(json.dumps(rep, indent=2))

if __name__=="__main__": main()
