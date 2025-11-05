#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fundamentals (kompakt, US+EU)
Priorität/Fallbacks pro Symbol:
  1) Finnhub: profile2 + metric?metric=all
  2) Alpha Vantage: OVERVIEW
  3) SimFin: statements/ratios (falls verfügbar, free key)
  4) yfinance (rudimentär)

Output:
  - data/processed/fundamentals_core.csv
  - data/reports/fundamentals_report.json
"""

import os, csv, json, time, argparse
from typing import Dict, Any, List
import requests, pandas as pd

FH_KEY  = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN") or ""
AV_KEY  = os.getenv("ALPHAVANTAGE_API_KEY") or ""
SF_KEY  = os.getenv("SIMFIN_API_KEY") or ""

OUT_DIR = "data/processed"; REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True); os.makedirs(REP_DIR, exist_ok=True)

def load_watchlist(path: str) -> List[str]:
    syms=[]
    with open(path, encoding="utf-8") as f:
        head=f.read(2048); f.seek(0)
        if "symbol" in head.lower() or "," in head:
            for r in csv.DictReader(f):
                s=(r.get("symbol") or r.get("ticker") or "").strip().upper()
                if s: syms.append(s)
        else:
            for line in f:
                s=line.strip().upper()
                if s and not s.startswith("#"): syms.append(s)
    out=[]; seen=set()
    for s in syms:
        if s and s not in seen: seen.add(s); out.append(s)
    return out

def num(v):
    try:
        if v in (None,"","NaN","nan"): return None
        return float(v)
    except Exception:
        return None

# ---------- Finnhub ----------
def fh_profile(sym: str) -> Dict[str,Any]:
    if not FH_KEY: return {}
    r = requests.get("https://finnhub.io/api/v1/stock/profile2",
                     params={"symbol": sym, "token": FH_KEY}, timeout=25)
    return r.json() if r.ok else {}

def fh_metric(sym: str) -> Dict[str,Any]:
    if not FH_KEY: return {}
    r = requests.get("https://finnhub.io/api/v1/stock/metric",
                     params={"symbol": sym, "metric":"all", "token": FH_KEY}, timeout=30)
    j = r.json() if r.ok else {}
    return j.get("metric", {}) if isinstance(j, dict) else {}

def normalize_finnhub(sym, prof, m):
    def g(*keys):
        for k in keys:
            if k in m and m[k] not in (None,"","NaN"): return m[k]
        return None
    return {
        "symbol": sym,
        "name": prof.get("name") or prof.get("ticker") or "",
        "exchange": prof.get("exchange") or prof.get("exchangeShortName") or "",
        "country": prof.get("country") or "",
        "industry": prof.get("finnhubIndustry") or prof.get("industry") or "",
        "currency": prof.get("currency") or "",
        "pe": num(g("peInclExtraTTM","peBasicExclExtraTTM","peNormalizedAnnual")),
        "ps": num(g("psTTM","priceToSalesTTM")),
        "pb": num(g("pbAnnual","pbQuarterly")),
        "ev_ebitda": num(g("evToEbitdaAnnual","evToEbitdaTTM")),
        "gross_margin": num(g("grossMarginTTM","grossMarginAnnual")),
        "oper_margin": num(g("operatingMarginTTM","operatingMarginAnnual")),
        "net_margin": num(g("netProfitMarginTTM","netProfitMarginAnnual")),
        "roic": num(g("roicTTM","roicAnnual")),
        "roe": num(g("roeTTM","roeAnnual")),
        "debt_to_equity": num(g("totalDebt/totalEquityAnnual")),
        "net_debt": num(g("netDebtAnnual")),
        "fcf_margin": num(g("fcfMarginTTM")),
        "div_yield": num(g("currentDividendYieldTTM","dividendYieldIndicatedAnnual")),
        "eps_ttm": num(g("epsInclExtraItemsTTM","epsExclExtraItemsTTM")),
        "rev_ttm": num(g("revenueTTM")),
        "rev_growth_yoy": num(g("revenueGrowthTTMYoy","revenueGrowthAnnualYoy")),
        "eps_growth_yoy": num(g("epsGrowthTTMYoy","epsGrowthAnnualYoy")),
        "shares_out": num(g("shareIssued")),
        "beta": num(g("beta")),
        "provider": "finnhub"
    }

# ---------- Alpha Vantage ----------
def av_overview(sym: str) -> Dict[str,Any]:
    if not AV_KEY: return {}
    r = requests.get("https://www.alphavantage.co/query",
        params={"function":"OVERVIEW","symbol":sym,"apikey":AV_KEY}, timeout=30)
    if not r.ok: return {}
    j = r.json() or {}
    if not isinstance(j, dict) or not j: return {}
    return {
        "symbol": sym,
        "name": j.get("Name") or "",
        "exchange": j.get("Exchange") or "",
        "country": j.get("Country") or "",
        "industry": j.get("Industry") or "",
        "currency": j.get("Currency") or "",
        "pe": num(j.get("PERatio")),
        "ps": num(j.get("PriceToSalesRatioTTM")),
        "pb": num(j.get("PriceToBookRatio")),
        "ev_ebitda": num(j.get("EVToEBITDA")),
        "gross_margin": None,
        "oper_margin": None,
        "net_margin": num(j.get("ProfitMargin")),
        "roic": None,
        "roe": num(j.get("ReturnOnEquityTTM")),
        "debt_to_equity": num(j.get("DebtToEquityRatio")),
        "net_debt": None,
        "fcf_margin": None,
        "div_yield": num(j.get("DividendYield")),
        "eps_ttm": num(j.get("EPS")),
        "rev_ttm": num(j.get("RevenueTTM")),
        "rev_growth_yoy": None,
        "eps_growth_yoy": None,
        "shares_out": num(j.get("SharesOutstanding")),
        "beta": num(j.get("Beta")),
        "provider": "alphavantage"
    }

# ---------- SimFin (einige Felder, gratis API) ----------
def simfin_core(sym: str) -> Dict[str,Any]:
    if not SF_KEY: return {}
    base = "https://simfin.com/api/v3"
    try:
        # Mapping Ticker -> SimFin ID
        r = requests.get(f"{base}/companies/list", params={"ticker": sym, "api-key": SF_KEY}, timeout=30)
        if not r.ok: return {}
        lst = r.json() or []
        if not lst: return {}
        sid = lst[0].get("simId")
        if not sid: return {}
        # Ratios (TTM) – kann je nach Firma fehlen
        rr = requests.get(f"{base}/ratios/companies", params={"simIds": sid, "period":"ttm", "api-key": SF_KEY}, timeout=30)
        ratios = rr.json()[0] if rr.ok and isinstance(rr.json(), list) and rr.json() else {}
        return {
            "symbol": sym,
            "name": lst[0].get("name") or "",
            "exchange": lst[0].get("exchangeShortName") or "",
            "country": lst[0].get("country") or "",
            "industry": lst[0].get("industry") or "",
            "currency": None,
            "pe": num(ratios.get("peTTM")),
            "ps": num(ratios.get("psTTM")),
            "pb": num(ratios.get("pbTTM")),
            "ev_ebitda": None,
            "gross_margin": num(ratios.get("grossMarginTTM")),
            "oper_margin": num(ratios.get("operatingMarginTTM")),
            "net_margin": num(ratios.get("netProfitMarginTTM")),
            "roic": num(ratios.get("roicTTM")),
            "roe": num(ratios.get("roeTTM")),
            "debt_to_equity": None,
            "net_debt": None,
            "fcf_margin": None,
            "div_yield": None,
            "eps_ttm": None,
            "rev_ttm": None,
            "rev_growth_yoy": None,
            "eps_growth_yoy": None,
            "shares_out": None,
            "beta": None,
            "provider": "simfin"
        }
    except Exception:
        return {}

# ---------- yfinance rudimentär ----------
def yf_core(sym: str) -> Dict[str,Any]:
    try:
        import yfinance as yf
        ti = yf.Ticker(sym)
        info = ti.get_info() if hasattr(ti,"get_info") else getattr(ti, "info", {})
    except Exception:
        info = {}
    def gi(k): 
        v = info.get(k)
        return None if v in (None,"","NaN") else v
    if not info: return {}
    return {
        "symbol": sym,
        "name": gi("longName") or gi("shortName") or "",
        "exchange": gi("exchange") or "",
        "country": gi("country") or "",
        "industry": gi("industry") or "",
        "currency": gi("currency") or "",
        "pe": gi("trailingPE"),
        "ps": gi("priceToSalesTrailing12Months"),
        "pb": gi("priceToBook"),
        "ev_ebitda": gi("enterpriseToEbitda"),
        "gross_margin": gi("grossMargins"),
        "oper_margin": gi("operatingMargins"),
        "net_margin": gi("profitMargins"),
        "roic": None,
        "roe": gi("returnOnEquity"),
        "debt_to_equity": gi("debtToEquity"),
        "net_debt": gi("netDebt"),
        "fcf_margin": None,
        "div_yield": gi("dividendYield"),
        "eps_ttm": gi("trailingEps"),
        "rev_ttm": gi("totalRevenue"),
        "rev_growth_yoy": None,
        "eps_growth_yoy": None,
        "shares_out": gi("sharesOutstanding"),
        "beta": gi("beta"),
        "provider": "yfinance"
    }

# ---------- Merge: Priorität FH > AV > SimFin > yfinance ----------
PRIO = {"finnhub": 4, "alphavantage": 3, "simfin": 2, "yfinance": 1}

def best_row(rows: List[Dict[str,Any]]) -> Dict[str,Any]:
    if not rows: return {}
    # Sortiere nach Provider-Priorität, dann nach Anzahl nicht-leerer Felder
    def score(r):
        nz = sum(1 for k,v in r.items() if k not in ("symbol","name","exchange","country","industry","currency","provider") and v not in (None,""))
        return (PRIO.get(r.get("provider",""),0), nz)
    rows_sorted = sorted(rows, key=score, reverse=True)
    base = rows_sorted[0].copy()
    # Felder leeren? versuche aus anderen zu füllen
    for r in rows_sorted[1:]:
        for k,v in r.items():
            if k in ("symbol","provider"): continue
            if base.get(k) in (None,"") and v not in (None,""): base[k]=v
    return base

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--sleep_ms", type=int, default=200)
    args = ap.parse_args()

    syms = load_watchlist(args.watchlist)
    out_rows=[]
    report={"ok":0,"miss":0,"providers":{"finnhub":bool(FH_KEY),"alphavantage":bool(AV_KEY),"simfin":bool(SF_KEY)},"errors":[]}

    for i, sym in enumerate(syms, 1):
        cand=[]
        # Finnhub
        if FH_KEY:
            try:
                prof = fh_profile(sym); time.sleep(args.sleep_ms/1000.0)
                met  = fh_metric(sym);  time.sleep(args.sleep_ms/1000.0)
                if prof or met: cand.append(normalize_finnhub(sym, prof, met))
            except Exception as e:
                report["errors"].append({"symbol":sym,"src":"finnhub","err":str(e)[:240]})
        # Alpha Vantage
        if AV_KEY:
            try:
                r = av_overview(sym)
                if r: cand.append(r)
                time.sleep(12.5)  # AV rate limit
            except Exception as e:
                report["errors"].append({"symbol":sym,"src":"alphavantage","err":str(e)[:240]})
        # SimFin
        if SF_KEY:
            try:
                r = simfin_core(sym)
                if r: cand.append(r)
                time.sleep(1.0)
            except Exception as e:
                report["errors"].append({"symbol":sym,"src":"simfin","err":str(e)[:240]})
        # yfinance
        try:
            r = yf_core(sym)
            if r: cand.append(r)
        except Exception as e:
            report["errors"].append({"symbol":sym,"src":"yfinance","err":str(e)[:240]})

        if cand:
            out_rows.append(best_row(cand)); report["ok"] += 1
        else:
            out_rows.append({"symbol":sym}); report["miss"] += 1

    # DataFrame & Persist
    cols = ["symbol","name","exchange","country","industry","currency",
            "pe","ps","pb","ev_ebitda","gross_margin","oper_margin","net_margin",
            "roic","roe","debt_to_equity","net_debt","fcf_margin","div_yield",
            "eps_ttm","rev_ttm","rev_growth_yoy","eps_growth_yoy","shares_out","beta"]
    df = pd.DataFrame(out_rows)[cols]
    df.to_csv(os.path.join(OUT_DIR,"fundamentals_core.csv"), index=False)

    with open(os.path.join(REP_DIR,"fundamentals_report.json"),"w",encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"[summary] symbols={len(syms)} ok={report['ok']} miss={report['miss']} -> data/processed/fundamentals_core.csv")

if __name__ == "__main__":
    main()
