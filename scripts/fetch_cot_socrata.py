#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch COT (CFTC) via Socrata with schema sniffing.

Outputs:
  - data/processed/cot_summary.csv     (1 Zeile je gesuchtem Markt, letzte Woche)
  - data/processed/cot_latest_raw.csv  (alle Rohzeilen der letzten Woche für diese Märkte)
  - data/reports/cot_errors.json       (Diagnose)

ENV:
  COT_DATASET_ID  (default 'gpe5-46if')
  CFTC_APP_TOKEN  (Socrata App Token)
  COT_SINCE_DATE  (YYYY-MM-DD)  ODER
  COT_WEEKS       (Default 156)
  COT_MARKETS     (Komma-sep. Suchbegriffe, default: 'E-MINI S&P 500, EURO FX, WTI, 10-YEAR NOTE')
"""

import os, json, time, datetime as dt
from typing import Dict, List, Optional

import requests
import pandas as pd

BASE = "https://publicreporting.cftc.gov/resource"

DATASET_ID = os.getenv("COT_DATASET_ID", "gpe5-46if").strip()
APP_TOKEN   = (os.getenv("CFTC_APP_TOKEN") or "").strip()

SINCE_DATE  = (os.getenv("COT_SINCE_DATE") or "").strip()
WEEKS_BACK  = int(os.getenv("COT_WEEKS", "156"))

MARKETS_ENV = os.getenv("COT_MARKETS", "E-MINI S&P 500, EURO FX, WTI, 10-YEAR NOTE")
MARKET_PATTERNS: List[str] = [m.strip() for m in MARKETS_ENV.split(",") if m.strip()]

OUT_DIR     = "data/processed"
REP_DIR     = "data/reports"
OUT_SUMMARY = f"{OUT_DIR}/cot_summary.csv"
OUT_LATEST  = f"{OUT_DIR}/cot_latest_raw.csv"
OUT_REPORT  = f"{REP_DIR}/cot_errors.json"

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(REP_DIR, exist_ok=True)

def since_date() -> str:
    if SINCE_DATE:
        return SINCE_DATE
    return (dt.datetime.utcnow().date() - dt.timedelta(days=7*max(1, WEEKS_BACK))).isoformat()

def req(url: str, params: Dict) -> requests.Response:
    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
        params.setdefault("$$app_token", APP_TOKEN)
    r = requests.get(url, params=params, headers=headers, timeout=60)
    if r.status_code in (401,403) and "$$app_token" in params:
        # Fallback ohne Token – niedrigere Rate, aber manchmal akzeptiert
        params = {k:v for k,v in params.items() if k != "$$app_token"}
        r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r

def socrata_probe_fields(dataset: str) -> List[str]:
    """Hole 1 Zeile ohne $select, um Feldnamen zuverlässig zu bekommen."""
    url = f"{BASE}/{dataset}.json"
    r = req(url, {"$limit": 1})
    js = r.json()
    if not js:
        return []
    return list(js[0].keys())

def socrata_fetch(dataset: str, where: str, select: Optional[str], order: Optional[str], page=50000) -> pd.DataFrame:
    url = f"{BASE}/{dataset}.json"
    rows = []
    off = 0
    while True:
        params = {"$where": where, "$limit": page, "$offset": off}
        if select: params["$select"] = select
        if order:  params["$order"]  = order
        r = req(url, params)
        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < page:
            break
        off += page
        time.sleep(0.15)
    return pd.DataFrame(rows)

def pick_first(existing: List[str], candidates: List[str]) -> Optional[str]:
    ex = set(x.lower() for x in existing)
    for c in candidates:
        if c.lower() in ex:
            return c
    return None

def main() -> int:
    ensure_dirs()
    errs: List[Dict] = []
    sdate = since_date()

    # 1) Schema sniffen
    try:
        fields = socrata_probe_fields(DATASET_ID)
    except Exception as e:
        report = {"ts": dt.datetime.utcnow().isoformat()+"Z",
                  "dataset": DATASET_ID, "since": sdate, "rows": 0,
                  "filtered_markets": MARKET_PATTERNS,
                  "errors":[{"stage":"probe","msg":str(e)}],
                  "token_len": len(APP_TOKEN)}
        pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"]).to_csv(OUT_SUMMARY, index=False)
        pd.DataFrame().to_csv(OUT_LATEST, index=False)
        with open(OUT_REPORT,"w",encoding="utf-8") as f: json.dump(report,f,indent=2)
        print("COT probe failed:", e)
        return 0

    # Kandidaten für übliche Feldnamen (verschiedene Datasets/Versionen)
    cand_market = [
        "market_and_exchange_names","market_and_exchange_name","contract_market_name",
        "mkt_and_exch","market_name","cftc_contract_market_name"
    ]
    cand_date = [
        "report_date_as_yyyy_mm_dd","report_date","as_of_date","report_date_as_yyyy_mm_dd_"
    ]
    cand_oi = [
        "open_interest_all","open_interest_all_","futopt_tot_open_interest","open_interest"
    ]
    cand_noncomm_long = [
        "noncomm_positions_long_all","noncomm_long_all","noncomm_long","noncomm_long_all_",
        "tff_noncomm_long_all","tff_money_mgr_long_all"
    ]
    cand_noncomm_short = [
        "noncomm_positions_short_all","noncomm_short_all","noncomm_short","noncomm_short_all_",
        "tff_noncomm_short_all","tff_money_mgr_short_all"
    ]
    cand_noncomm_net = [
        "noncomm_positions_net_all","noncomm_net_all","noncomm_net","tff_noncomm_net_all",
        "tff_money_mgr_net_all"
    ]

    m_col = pick_first(fields, cand_market)
    d_col = pick_first(fields, cand_date)
    oi_col = pick_first(fields, cand_oi)
    nl_col = pick_first(fields, cand_noncomm_long)
    ns_col = pick_first(fields, cand_noncomm_short)
    nn_col = pick_first(fields, cand_noncomm_net)

    # Mindestset muss existieren
    if not (m_col and d_col):
        errs.append({"stage":"map","msg":f"mandatory columns missing. have={fields[:10]}..."})
        df = pd.DataFrame()
    else:
        # 2) Daten holen – ohne $select (sicherer), wir filtern über $where + sortieren
        where = f"{d_col} >= '{sdate}'"
        try:
            df = socrata_fetch(DATASET_ID, where=where, select=None, order=f"{d_col} ASC")
        except Exception as e:
            errs.append({"stage":"fetch","msg":str(e)})
            df = pd.DataFrame()

    if df.empty:
        pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"]).to_csv(OUT_SUMMARY, index=False)
        pd.DataFrame().to_csv(OUT_LATEST, index=False)
        with open(OUT_REPORT,"w",encoding="utf-8") as f:
            json.dump({"ts": dt.datetime.utcnow().isoformat()+"Z",
                       "dataset": DATASET_ID, "since": sdate, "rows": 0,
                       "filtered_markets": MARKET_PATTERNS, "errors": errs,
                       "token_len": len(APP_TOKEN)}, f, indent=2)
        print("COT: no rows")
        return 0

    # Normieren
    df = df.rename(columns={m_col:"market_name", d_col:"report_date"})
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date

    # Optional-Kennzahlen zu numerisch casten (falls vorhanden)
    for c, newc in [(oi_col,"open_interest_all"),
                    (nl_col,"noncomm_long_all"),
                    (ns_col,"noncomm_short_all"),
                    (nn_col,"noncomm_net_all")]:
        if c and c in df.columns:
            df[newc] = pd.to_numeric(df[c], errors="coerce")

    # 3) Märkte filtern und je Markt die letzte Woche zusammenfassen
    summary = []
    latest_raw_parts = []

    for key in MARKET_PATTERNS:
        sub = df[df["market_name"].str.contains(key, case=False, na=False)].copy()
        if sub.empty:
            errs.append({"market":key,"stage":"filter","msg":"no_match"})
            continue

        latest_date = sub["report_date"].max()
        latest = sub[sub["report_date"] == latest_date].copy()

        # Summen/Netto nur, wenn vorhanden – sonst None
        def safe_sum(col):
            return float(pd.to_numeric(latest[col], errors="coerce").fillna(0).sum()) if col in latest.columns else None

        oi  = safe_sum("open_interest_all")
        ncl = safe_sum("noncomm_long_all")
        ncs = safe_sum("noncomm_short_all")
        nnt = safe_sum("noncomm_net_all")

        summary.append({
            "market": key,
            "report_date": latest_date.isoformat(),
            "oi": int(oi) if oi is not None else None,
            "ncl": int(ncl) if ncl is not None else None,
            "ncs": int(ncs) if ncs is not None else None,
            "noncomm_net": int(nnt) if nnt is not None else None
        })

        latest["filter_key"] = key
        latest_raw_parts.append(latest)

    pd.DataFrame(summary, columns=["market","report_date","oi","ncl","ncs","noncomm_net"]).to_csv(OUT_SUMMARY, index=False)
    if latest_raw_parts:
        pd.concat(latest_raw_parts, ignore_index=True).to_csv(OUT_LATEST, index=False)
    else:
        pd.DataFrame().to_csv(OUT_LATEST, index=False)

    with open(OUT_REPORT,"w",encoding="utf-8") as f:
        json.dump({"ts": dt.datetime.utcnow().isoformat()+"Z",
                   "dataset": DATASET_ID, "since": sdate,
                   "rows": int(len(df)),
                   "latest": str(pd.to_datetime(df["report_date"]).max().date()),
                   "filtered_markets": MARKET_PATTERNS,
                   "mapped_cols": {"market":m_col,"date":d_col,"oi":oi_col,"ncl":nl_col,"ncs":ns_col,"nnet":nn_col},
                   "errors": errs,
                   "token_len": len(APP_TOKEN)}, f, indent=2)

    print(f"wrote {OUT_SUMMARY} rows={len(summary)} ; latest_raw={OUT_LATEST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
