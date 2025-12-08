#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Earnings (Kalender + historische EPS/Revenue-Überraschungen)
Priorität/Fallbacks:
  1) Finnhub (calendar + stock/earnings) – FINNHUB_API_KEY|FINNHUB_TOKEN
  2) Alpha Vantage (quarterly EARNINGS) – ALPHAVANTAGE_API_KEY
  3) SEC (Filings als Termin-Proxy, keine Surprise%) – SEC_USER_AGENT
  4) yfinance (rudimentär) – optional

Outputs:
  - data/processed/earnings_next.json             (frühester kommender Termin je Symbol)
  - data/processed/earnings_results.csv           (historische EPS/Revenue Überraschungen; zusammengeführt über Provider)
  - data/reports/earnings_report.json             (Kurzbericht)
"""

import os, sys, csv, json, time, argparse, datetime as dt
from typing import List, Dict, Any
import requests
import pandas as pd

# ---- ENV / Keys ----
FH_KEY  = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN") or ""
AV_KEY  = os.getenv("ALPHAVANTAGE_API_KEY") or ""
SEC_UA  = os.getenv("SEC_USER_AGENT") or ""
SLEEP_MS = int(os.getenv("FINNHUB_SLEEP_MS", "1200"))

OUT_DIR = "data/processed"; REP_DIR = "data/reports"; os.makedirs(OUT_DIR, exist_ok=True); os.makedirs(REP_DIR, exist_ok=True)

# ---------------- Helpers ----------------
def load_watchlist(path: str) -> List[str]:
    syms: List[str] = []
    if not path or not os.path.exists(path): return syms
    with open(path, encoding="utf-8") as f:
        head = f.read(2048); f.seek(0)
        if "symbol" in head.lower() or "," in head:
            for r in csv.DictReader(f):
                s = (r.get("symbol") or r.get("ticker") or "").strip().upper()
                if s: syms.append(s)
        else:
            for line in f:
                s = line.strip().upper()
                if s and not s.startswith("#"): syms.append(s)
    out=[]; seen=set()
    for s in syms:
        if s and s not in seen: seen.add(s); out.append(s)
    return out

def to_float(x):
    try:
        if x in (None, "", "NaN"): return None
        return float(x)
    except Exception:
        return None

def sleep():
    time.sleep(max(0.05, SLEEP_MS/1000.0))

# ---------------- Finnhub ----------------
def fh_calendar_window(a: dt.date, b: dt.date) -> List[Dict[str, Any]]:
    if not FH_KEY: return []
    url = "https://finnhub.io/api/v1/calendar/earnings"
    params = {"from": a.strftime("%Y-%m-%d"), "to": b.strftime("%Y-%m-%d"), "token": FH_KEY}
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    j = r.json() or {}
    return j.get("earningsCalendar") or j.get("earnings") or []

def fh_stock_earnings(sym: str, limit: int = 12) -> List[Dict[str, Any]]:
    if not FH_KEY: return []
    url = "https://finnhub.io/api/v1/stock/earnings"
    r = requests.get(url, params={"symbol": sym, "limit": limit, "token": FH_KEY}, timeout=25)
    r.raise_for_status()
    j = r.json() or []
    return j if isinstance(j, list) else []

# ---------------- Alpha Vantage ----------------
def av_quarterly_earnings(sym: str) -> List[Dict[str, Any]]:
    """Alpha Vantage: function=EARNINGS; liefert EPS actual/estimate und surprise%."""
    if not AV_KEY: return []
    url = "https://www.alphavantage.co/query"
    r = requests.get(url, params={"function":"EARNINGS","symbol":sym,"apikey":AV_KEY}, timeout=25)
    r.raise_for_status()
    j = r.json() or {}
    rows = j.get("quarterlyEarnings") or []
    out=[]
    for q in rows:
        out.append({
            "period": q.get("fiscalDateEnding"),
            "epsActual": to_float(q.get("reportedEPS")),
            "epsEstimate": to_float(q.get("estimatedEPS")),
            "surprisePercent": to_float(q.get("surprisePercentage")),
            "symbol_src":"AV"
        })
    return out

# ---------------- SEC Filings (Proxy für Termine) ----------------
def sec_company_submissions(sym: str) -> List[Dict[str, Any]]:
    """SEC submissions (8-K, 10-Q, 10-K). Nutzt sym->CIK Mapping über SEC-Search."""
    if not SEC_UA: return []
    # 1) CIK lookup
    hdr = {"User-Agent": SEC_UA, "Accept-Encoding":"gzip, deflate"}
    lk = requests.get(f"https://www.sec.gov/files/company_tickers.json", headers=hdr, timeout=30)
    if lk.ok:
        try:
            m = {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in lk.json().values()}
            cik = m.get(sym.upper())
        except Exception:
            cik = None
    else:
        cik = None
    if not cik: return []
    r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=hdr, timeout=30)
    if not r.ok: return []
    j = r.json()
    forms = (j.get("filings") or {}).get("recent") or {}
    out=[]
    for form, date in zip(forms.get("form",[]), forms.get("filingDate",[])):
        if form in ("8-K","10-Q","10-K"):
            out.append({"symbol": sym, "form": form, "filingDate": date})
    return out

# ---------------- yfinance rudimentär ----------------
def yf_quarter_dates(sym: str) -> List[str]:
    try:
        import yfinance as yf
        t = yf.Ticker(sym)
        cal = t.get_calendar() if hasattr(t,"get_calendar") else None
        # yfinance liefert hier meist nur nächstes Datum – begrenzter Nutzen
        return []
    except Exception:
        return []

# ---------------- Pipeline ----------------
def build_calendar(watch: List[str], window_days: int, lookahead_days: int) -> List[Dict[str,str]]:
    start = dt.datetime.utcnow().date()
    end   = start + dt.timedelta(days=lookahead_days)
    step  = max(1, int(window_days))

    all_rows=[]
    if FH_KEY:
        cur = start
        while cur <= end:
            nxt = min(end, cur + dt.timedelta(days=step-1))
            try:
                rows = fh_calendar_window(cur, nxt)
                all_rows += rows
            except Exception:
                pass
            sleep()
            cur = nxt + dt.timedelta(days=1)

    # Konsolidieren: frühester kommender Termin je Symbol aus watchlist
    by={}
    for r in all_rows:
        sym = (r.get("symbol") or r.get("ticker") or "").upper()
        d   = (r.get("date") or r.get("time") or r.get("epsReportDate") or "")[:10]
        if not sym or not d: continue
        if watch and sym not in watch: continue
        if sym not in by or d < by[sym]["next_date"]:
            by[sym] = {"symbol": sym, "next_date": d, "src": "finnhub"}

    # SEC-Proxy (wenn nichts gefunden wurde)
    if SEC_UA:
        for sym in watch:
            if sym in by: continue
            try:
                filings = sec_company_submissions(sym)
                dates = [f["filingDate"] for f in filings if f["form"] in ("8-K","10-Q")]
                if dates:
                    dmin = min(dates)
                    by[sym] = {"symbol": sym, "next_date": dmin, "src": "sec_proxy"}
            except Exception:
                pass

    return list(by.values())

def build_results(watch: List[str], limit: int) -> pd.DataFrame:
    rows=[]
    # 1) Finnhub first
    if FH_KEY:
        for sym in watch:
            try:
                for r in fh_stock_earnings(sym, limit):
                    rows.append({
                        "symbol": sym,
                        "period": r.get("period"),
                        "eps_actual": to_float(r.get("epsActual")),
                        "eps_estimate": to_float(r.get("epsEstimate")),
                        "surprise_pct": to_float(r.get("surprisePercent")),
                        "revenue_actual": to_float(r.get("revenueActual")),
                        "revenue_estimate": to_float(r.get("revenueEstimate")),
                        "provider": "finnhub"
                    })
            except Exception:
                pass
            sleep()

    # 2) Alpha Vantage – nur EPS surprise (keine Revenue)
    if AV_KEY:
        for sym in watch:
            try:
                for r in av_quarterly_earnings(sym):
                    rows.append({
                        "symbol": sym,
                        "period": r.get("period"),
                        "eps_actual": to_float(r.get("epsActual")),
                        "eps_estimate": to_float(r.get("epsEstimate")),
                        "surprise_pct": to_float(r.get("surprisePercent")),
                        "revenue_actual": None,
                        "revenue_estimate": None,
                        "provider": "alphavantage"
                    })
            except Exception:
                pass
            time.sleep(12.5)  # AV free rate limit

    # 3) (optional) SEC/yf liefern keine Surprise%, daher nur als Termin-Proxy genutzt

    df = pd.DataFrame(rows, columns=[
        "symbol","period","eps_actual","eps_estimate","surprise_pct",
        "revenue_actual","revenue_estimate","provider"
    ])
    # Duplikate (gleicher symbol+period) per Priorität auflösen: Finnhub > AV
    if not df.empty:
        prio = {"finnhub":2, "alphavantage":1}
        df["prio"] = df["provider"].map(prio).fillna(0)
        df.sort_values(["symbol","period","prio"], ascending=[True, True, False], inplace=True)
        df = df.drop_duplicates(subset=["symbol","period"], keep="first").drop(columns=["prio"])
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="watchlists/mylist.csv|.txt")
    ap.add_argument("--window-days", type=int, default=int(os.getenv("WINDOW_DAYS","30")))
    ap.add_argument("--lookahead-days", type=int, default=int(os.getenv("LOOKAHEAD_DAYS","365")))
    ap.add_argument("--limit", type=int, default=12, help="historische Perioden je Symbol")
    args = ap.parse_args()

    watch = load_watchlist(args.watchlist)
    rep = {"ts": dt.datetime.utcnow().isoformat()+"Z","symbols":len(watch),"calendar_rows":0,"result_rows":0,"providers":{
        "finnhub": bool(FH_KEY), "alphavantage": bool(AV_KEY), "sec": bool(SEC_UA)
    }}

    # Kalender
    cal = build_calendar(watch, args.window_days, args.lookahead_days)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR,"earnings_next.json"),"w",encoding="utf-8") as f:
        json.dump(cal, f, indent=2, ensure_ascii=False)
    rep["calendar_rows"] = len(cal)

    # Historische Überraschungen
    df = build_results(watch, args.limit)
    out_csv = os.path.join(OUT_DIR,"earnings_results.csv")
    df.to_csv(out_csv, index=False)
    rep["result_rows"] = int(len(df))

    with open(os.path.join(REP_DIR,"earnings_report.json"),"w",encoding="utf-8") as f:
        json.dump(rep, f, indent=2, ensure_ascii=False)
    print("[summary]", rep)

if __name__ == "__main__":
    sys.exit(main())
