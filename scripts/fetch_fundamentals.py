#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Globale Fundamentals (US + EU)
- Primär Finnhub (profile2 + metrics?metric=all)
- Fallback: yfinance (kleines Paket; keine Kurs-Historien)
- Keine Preis-Historie laden; nur Metadaten/Kennzahlen.
- Output: data/processed/fundamentals_core.csv  (kompakt)
          data/reports/fundamentals_report.json (Log)
"""

import os, sys, time, json, csv
import argparse
import requests
import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None  # Fallback nur, wenn installiert

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

def read_watchlist(path):
    syms = []
    with open(path, encoding="utf-8") as f:
        header = f.readline()
        has_header = header.lower().strip().startswith("symbol")
        if not has_header:
            syms.append(header.strip())
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                syms.append(s)
    # Du nutzt AgenaTrader/TAIPAN für Preise → hier nur Tickers beibehalten
    return sorted(set(syms))

def finnhub_get(url, key, params=None, timeout=25):
    params = dict(params or {})
    params["token"] = key
    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def pull_finnhub(symbol, key, sleep_ms=150):
    base = "https://finnhub.io/api/v1"
    prof = finnhub_get(f"{base}/stock/profile2", key, {"symbol": symbol})
    # metrics: metric=all → viel Inhalt, aber robust
    met  = finnhub_get(f"{base}/stock/metric",   key, {"symbol": symbol, "metric": "all"})
    time.sleep(max(0, int(sleep_ms)/1000.0))
    return prof, met

def normalize_from_finnhub(symbol, prof, met):
    m = met.get("metric", {}) if isinstance(met, dict) else {}
    # robuste Extraktion
    def g(*keys, default=None):
        for k in keys:
            if k in m and m[k] not in (None, "", "NaN"):
                return m[k]
        return default

    # Ein paar Kernfelder (du kannst beliebig erweitern):
    row = {
        "symbol": symbol,
        "name": prof.get("name") or prof.get("ticker") or "",
        "exchange": prof.get("exchange") or prof.get("exchangeShortName") or "",
        "country": prof.get("country") or "",
        "industry": prof.get("finnhubIndustry") or prof.get("industry") or "",
        "currency": prof.get("currency") or "",
        # Bewertungs-/Profitabilitäts-/Wachstums-Kern:
        "pe": g("peInclExtraTTM", "peBasicExclExtraTTM", "peNormalizedAnnual", default=None),
        "ps": g("psTTM", "priceToSalesTTM", default=None),
        "pb": g("pbAnnual", "pbQuarterly", default=None),
        "ev_ebitda": g("evToEbitdaAnnual", "evToEbitdaTTM", default=None),
        "gross_margin": g("grossMarginTTM", "grossMarginAnnual", default=None),
        "op_margin": g("operatingMarginTTM", "operatingMarginAnnual", default=None),
        "net_margin": g("netProfitMarginTTM", "netProfitMarginAnnual", default=None),
        "roic": g("roicTTM", "roicAnnual", default=None),
        "roe": g("roeTTM", "roeAnnual", default=None),
        "debt_to_equity": g("totalDebt/totalEquityAnnual", default=None),
        "net_debt": g("netDebtAnnual", default=None),
        "fcf_margin": g("fcfMarginTTM", default=None),
        "div_yield": g("currentDividendYieldTTM", "dividendYieldIndicatedAnnual", default=None),
        "eps_ttm": g("epsInclExtraItemsTTM", "epsExclExtraItemsTTM", default=None),
        "rev_ttm": g("revenueTTM", default=None),
        "rev_growth_yoy": g("revenueGrowthTTMYoy", "revenueGrowthAnnualYoy", default=None),
        "eps_growth_yoy": g("epsGrowthTTMYoy", "epsGrowthAnnualYoy", default=None),
        "shares_out": g("shareIssued", default=None),
        "beta": g("beta", default=None),
    }
    return row

def pull_yf(symbol):
    if yf is None:
        return {}
    try:
        ti = yf.Ticker(symbol)
        info = ti.get_info() if hasattr(ti, "get_info") else ti.info
    except Exception:
        info = {}
    def gi(key, default=None):
        v = info.get(key, default)
        return None if v in ("NaN", "nan", "", None) else v
    row = {
        "symbol": symbol,
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
        "op_margin": gi("operatingMargins"),
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
    }
    return row

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--finnhub_key", required=False, default=os.getenv("FINNHUB_API_KEY",""))
    ap.add_argument("--sleep_ms", type=int, default=150)
    args = ap.parse_args()

    symbols = read_watchlist(args.watchlist)
    key = args.finnhub_key.strip()
    report = {"ok":[], "miss":[], "fallback":[], "errors":[]}

    rows = []
    for sym in symbols:
        got = None
        # 1) Finnhub
        if key:
            try:
                prof, met = pull_finnhub(sym, key, args.sleep_ms)
                row = normalize_from_finnhub(sym, prof, met)
                rows.append(row); report["ok"].append(sym)
                got = "finnhub"
            except Exception as e:
                report["errors"].append({"symbol":sym, "src":"finnhub", "err": str(e)[:240]})

        # 2) yfinance Fallback (nur, wenn noch nichts da)
        if got is None:
            try:
                yrow = pull_yf(sym)
                if yrow and any(v not in (None,"") for k,v in yrow.items() if k not in ("symbol","name","exchange","country","industry","currency")):
                    rows.append(yrow); report["fallback"].append(sym)
                    got = "yahoo"
                else:
                    report["miss"].append(sym)
            except Exception as e:
                report["errors"].append({"symbol":sym, "src":"yahoo", "err": str(e)[:240]})
                report["miss"].append(sym)

    # DataFrame & Write
    if rows:
        df = pd.DataFrame(rows)
        # Typkonvertierung
        num_cols = [c for c in df.columns if c not in ("symbol","name","exchange","country","industry","currency")]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        out = os.path.join(OUT_DIR, "fundamentals_core.csv")
        df.to_csv(out, index=False, encoding="utf-8")
    else:
        # leere Datei dennoch anlegen (für QA-Anzeige)
        pd.DataFrame(columns=["symbol","name","exchange","country","industry","currency",
                              "pe","ps","pb","ev_ebitda","gross_margin","op_margin","net_margin",
                              "roic","roe","debt_to_equity","net_debt","fcf_margin","div_yield",
                              "eps_ttm","rev_ttm","rev_growth_yoy","eps_growth_yoy","shares_out","beta"]
                    ).to_csv(os.path.join(OUT_DIR,"fundamentals_core.csv"), index=False)

    with open(os.path.join(REP_DIR,"fundamentals_report.json"),"w",encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()
