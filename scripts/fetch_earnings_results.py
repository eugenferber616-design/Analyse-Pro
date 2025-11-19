#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_earnings_results.py – robuste Historie von Earnings (EPS/Revenue + Surprise)

Quellen / Fallback-Reihenfolge (Priorität hoch → niedrig):
  1) Finnhub /stock/earnings (FINNHUB_TOKEN)              -> source='finnhub'      (prio 4)
  2) Yahoo Finance earnings_dates (gratis)                 -> source='yahoo.ed'     (prio 3)
  3) SEC Companyfacts (gratis; braucht SEC_USER_AGENT)     -> source='sec'          (prio 2)
  4) Yahoo quarterly_earnings (EPS+Revenue, teils lücken)  -> source='yahoo.qe'     (prio 1)
  5) Yahoo quarterly_financials (Revenue, grob)            -> source='yahoo.qf'     (prio 0)

Outputs:
  data/processed/earnings_results.csv(.gz)
  data/reports/eu_checks/earnings_results_preview.txt
  data/reports/eu_checks/earnings_results_missing.txt
  data/reports/earn_errors.json
"""

from __future__ import annotations
import os, csv, time, json, gzip, io, math, re
from typing import List, Dict, Tuple
from pathlib import Path

import requests
import pandas as pd

# ───────────────────────────── Config / ENV ─────────────────────────────
FINNHUB_TOKEN   = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or ""
WATCHLIST_PATH  = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OVR_FILE        = os.getenv("EARNINGS_OVERRIDES", "watchlists/earnings_overrides.csv")
SLEEP_MS        = int(os.getenv("FINNHUB_SLEEP_MS", "1200"))
SEC_UA          = os.getenv("SEC_USER_AGENT", "").strip()
OUT_DIR         = Path("data/processed")
REP_DIR         = Path("data/reports")
EU_DIR          = REP_DIR / "eu_checks"
DEFAULT_LIMIT   = int(os.getenv("EARNINGS_LIMIT", "16"))
FINNHUB_BASE    = "https://finnhub.io/api/v1/stock/earnings"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL   = "https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"

for p in (OUT_DIR, REP_DIR, EU_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ───────────────────────────── Utilities ─────────────────────────────
def sleep_ms(ms: int): time.sleep(max(0.0, ms) / 1000.0)
def _nan(): return float("nan")

def to_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return _nan()
        s = str(x).strip()
        # 1.234,56 → 1234.56
        if re.match(r"^-?\d{1,3}(\.\d{3})+,\d+$", s):
            s = s.replace(".", "").replace(",", ".")
        # 1,234.56 → 1234.56
        elif re.match(r"^-?\d{1,3}(,\d{3})+\.\d+$", s):
            s = s.replace(",", "")
        return float(s)
    except Exception:
        return _nan()

def parse_iso_date(s: str | None) -> str | None:
    if not s: return None
    s = str(s).strip()
    if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s): return s[:10]
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"^(\d{4})[/-]?\s*[Qq]([1-4])$", s)
    if m:
        y, q = int(m.group(1)), int(m.group(2))
        mm = {1:"03",2:"06",3:"09",4:"12"}[q]
        return f"{y}-{mm}-01"
    return None

def make_fiscal_period(year, quarter, period_str: str | None) -> str | None:
    if pd.notna(year) and pd.notna(quarter):
        try: return f"{int(year)}Q{int(quarter)}"
        except Exception: pass
    if period_str:
        m = re.search(r"(\d{4})[/-]?\s*[Qq]([1-4])", period_str)
        if m: return f"{m.group(1)}Q{m.group(2)}"
        d = parse_iso_date(period_str)
        if d:
            try:
                y, mth = int(d[:4]), int(d[5:7])
                q = (mth-1)//3 + 1
                return f"{y}Q{q}"
            except Exception: return None
    return None

def load_watchlist(path: str | Path) -> List[str]:
    path = str(path)
    if not path or not os.path.exists(path): return []
    syms: List[str] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        head = f.read(2048); f.seek(0)
        if "," in head or "symbol" in head.lower():
            rdr = csv.DictReader(f)
            for row in rdr:
                s = (row.get("symbol") or row.get("ticker") or "").strip().upper()
                if s and not s.startswith("#"): syms.append(s)
        else:
            for line in f:
                s = line.strip().upper()
                if s and not s.startswith("#") and s.lower() != "symbol": syms.append(s)
    seen=set(); out=[]
    for s in syms:
        if s and s not in seen: seen.add(s); out.append(s)
    return out

def load_overrides(path: str | Path) -> Dict[str, str]:
    path = str(path)
    if not os.path.exists(path): return {}
    out: Dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sym = (row.get("symbol") or "").strip().upper()
            api = (row.get("api_symbol") or "").strip().upper()
            if sym and api: out[sym] = api
    return out

def api_symbol_for(sym: str, overrides: Dict[str, str]) -> str:
    if sym in overrides: return overrides[sym]
    if "." in sym: return sym.split(".", 1)[0]   # SAP.DE -> SAP
    return sym

# ───────────────────────────── Provider: Finnhub ─────────────────────────────
def finnhub_get(symbol: str, limit: int, retries: int = 3, base_sleep_ms: int | None = None) -> List[dict]:
    base_sleep_ms = base_sleep_ms if base_sleep_ms is not None else SLEEP_MS
    params = {"symbol": symbol, "limit": int(limit), "token": FINNHUB_TOKEN}
    for attempt in range(retries):
        try:
            r = requests.get(FINNHUB_BASE, params=params, timeout=30)
            if r.status_code == 429 and attempt + 1 < retries:
                sleep_ms(base_sleep_ms * (2 ** attempt)); continue
            r.raise_for_status()
            data = r.json() or []
            if isinstance(data, dict): return []
            return data
        except requests.RequestException:
            if attempt + 1 == retries: return []
            sleep_ms(base_sleep_ms * (2 ** attempt))
    return []

def normalize_finnhub_rows(sym: str, api_sym: str, rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        year, quarter, period = r.get("year"), r.get("quarter"), r.get("period")
        report_date = r.get("reportDate") or r.get("date")
        fp = make_fiscal_period(year, quarter, period)
        out.append({
            "symbol": sym, "api_symbol": api_sym,
            "period": fp or period or parse_iso_date(report_date) or None,
            "report_date": parse_iso_date(report_date),
            "year": year, "quarter": quarter, "report_time": r.get("hour"),
            "eps_actual": r.get("epsActual"), "eps_estimate": r.get("epsEstimate"),
            "surprise_pct": r.get("surprisePercent"),
            "surprise_eps_abs": None,
            "revenue_actual": r.get("revenueActual"), "revenue_estimate": r.get("revenueEstimate"),
            "surprise_rev_pct": None,
            "currency": r.get("currency") or "", "source": "finnhub",
        })
    return out

# ───────────────────────────── Provider: Yahoo Finance ───────────────────────
_YF_AVAILABLE = None
def yf_available() -> bool:
    global _YF_AVAILABLE
    if _YF_AVAILABLE is None:
        try:
            import yfinance as yf  # noqa: F401
            _YF_AVAILABLE = True
        except Exception:
            _YF_AVAILABLE = False
    return _YF_AVAILABLE

def fetch_yf(symbol: str, limit: int = 16) -> Tuple[List[dict], str]:
    if not yf_available(): return [], symbol
    import yfinance as yf
    rows: List[dict] = []; api_sym = symbol
    try:
        tk = yf.Ticker(symbol)

        # 1) earnings_dates (hat oft EPS actual/estimate + Surprise %)
        try:
            ed = getattr(tk, "earnings_dates", None)
            df = ed(limit=limit) if callable(ed) else None
            if df is not None and hasattr(df, "reset_index"):
                dfe = df.reset_index().rename(columns={
                    "Earnings Date":"report_date","Reported EPS":"eps_actual",
                    "EPS Estimate":"eps_estimate","Surprise(%)":"surprise_pct"
                })
                for _, rr in dfe.iterrows():
                    rd = parse_iso_date(rr.get("report_date"))
                    rows.append({
                        "symbol": symbol, "api_symbol": api_sym,
                        "period": make_fiscal_period(None, None, str(rd)),
                        "report_date": rd, "year": None, "quarter": None, "report_time": None,
                        "eps_actual": rr.get("eps_actual"), "eps_estimate": rr.get("eps_estimate"),
                        "surprise_pct": rr.get("surprise_pct"), "surprise_eps_abs": None,
                        "revenue_actual": None, "revenue_estimate": None, "surprise_rev_pct": None,
                        "currency": "", "source": "yahoo.ed"
                    })
        except Exception:
            pass

        # 2) quarterly_earnings (EPS + Revenue)
        try:
            qe = getattr(tk, "quarterly_earnings", None)
            if qe is not None and hasattr(qe, "reset_index"):
                dfq = qe.reset_index().rename(columns={
                    "Quarter":"period", "Revenue":"revenue_actual", "Earnings":"eps_actual"
                })
                for _, rr in dfq.iterrows():
                    p = make_fiscal_period(None, None, str(rr.get("period")))
                    rd = parse_iso_date(str(rr.get("period")))
                    rows.append({
                        "symbol": symbol, "api_symbol": api_sym,
                        "period": p, "report_date": rd, "year": None, "quarter": None, "report_time": None,
                        "eps_actual": rr.get("eps_actual"), "eps_estimate": None,
                        "surprise_pct": None, "surprise_eps_abs": None,
                        "revenue_actual": rr.get("revenue_actual"), "revenue_estimate": None,
                        "surprise_rev_pct": None, "currency": "", "source": "yahoo.qe"
                    })
        except Exception:
            pass

        # 3) quarterly_financials (nur Revenue)
        try:
            qf = getattr(tk, "quarterly_financials", None)
            if qf is not None and hasattr(qf, "T"):
                qf_t = qf.T
                for idx, row in qf_t.iterrows():
                    rev = row.get("Total Revenue") or row.get("TotalRevenue") or row.get("Revenue")
                    if pd.notna(rev):
                        p = make_fiscal_period(None, None, str(idx))
                        rd = parse_iso_date(str(idx))
                        rows.append({
                            "symbol": symbol, "api_symbol": api_sym,
                            "period": p, "report_date": rd, "year": None, "quarter": None, "report_time": None,
                            "eps_actual": None, "eps_estimate": None, "surprise_pct": None, "surprise_eps_abs": None,
                            "revenue_actual": rev, "revenue_estimate": None, "surprise_rev_pct": None,
                            "currency": "", "source": "yahoo.qf"
                        })
        except Exception:
            pass

    except Exception:
        return rows, api_sym
    return rows, api_sym

# ───────────────────────────── Provider: SEC Companyfacts (optional) ─────────
_SEC_CACHE: Dict[str, str] = {}

def sec_headers():
    h = {"User-Agent": SEC_UA or "youremail@example.com"}
    return h

def sec_cik_for_symbol(sym: str) -> str | None:
    # cache
    if sym in _SEC_CACHE: return _SEC_CACHE[sym]
    try:
        r = requests.get(SEC_TICKERS_URL, headers=sec_headers(), timeout=30)
        r.raise_for_status()
        js = r.json()
        # file ist index-basiert: { "0":{"cik_str":..., "ticker":"A","title":"Agilent"} ... }
        for _, rec in js.items():
            if str(rec.get("ticker","")).upper() == sym.upper():
                cik = str(rec.get("cik_str","")).zfill(10)
                _SEC_CACHE[sym] = cik
                return cik
    except Exception:
        return None
    return None

def sec_fetch_companyfacts(sym: str, limit: int = 16) -> List[dict]:
    """Liefert grobe EPS/Revenue pro Quartal aus SEC-Facts (us-gaap).
       Achtung: Perioden-Mapping ist nicht perfekt, dient als Lückenfüller.
    """
    if not SEC_UA: return []
    cik = sec_cik_for_symbol(sym)
    if not cik: return []
    try:
        r = requests.get(SEC_FACTS_URL.replace("{CIK}", cik), headers=sec_headers(), timeout=30)
        r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return []

    def extract_series(tag: str) -> Dict[str, float]:
        out = {}
        try:
            facts = data.get("facts", {}).get("us-gaap", {}).get(tag, {}).get("units", {})
            # bevorzugte Units
            for unit in ("USD", "USD/shares", "pure"):
                arr = facts.get(unit) or []
                for itm in arr:
                    p = parse_iso_date(itm.get("end")) or parse_iso_date(itm.get("fy"))
                    if not p: continue
                    k = make_fiscal_period(None, None, p) or p
                    out[k] = to_float(itm.get("val"))
        except Exception:
            pass
        return out

    eps = extract_series("EarningsPerShareDiluted") or extract_series("EarningsPerShareBasic")
    rev = extract_series("RevenueFromContractWithCustomerExcludingAssessedTax") or \
          extract_series("SalesRevenueNet") or extract_series("Revenues")

    # bau Zeilen aus gemergten Keys (Perioden)
    keys = list(set(list(eps.keys()) + list(rev.keys())))
    keys.sort(reverse=True)
    rows: List[dict] = []
    for k in keys[:limit]:
        rows.append({
            "symbol": sym, "api_symbol": sym,
            "period": k, "report_date": parse_iso_date(k),
            "year": None, "quarter": None, "report_time": None,
            "eps_actual": eps.get(k, None), "eps_estimate": None,
            "surprise_pct": None, "surprise_eps_abs": None,
            "revenue_actual": rev.get(k, None), "revenue_estimate": None,
            "surprise_rev_pct": None, "currency": "USD", "source": "sec"
        })
    return rows

# ───────────────────────────── Merge / IO ─────────────────────────────
def read_existing(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists(): return pd.DataFrame()
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f: raw = f.read()
        bio = io.BytesIO(raw)
        try:
            df = pd.read_csv(bio, sep=";", low_memory=False)
            if df.shape[1] == 1: bio.seek(0); df = pd.read_csv(bio, sep=",")
        except Exception:
            bio.seek(0); df = pd.read_csv(bio)
        return df
    else:
        try:
            df = pd.read_csv(path, sep=";", low_memory=False)
            if df.shape[1] == 1: df = pd.read_csv(path, sep=",")
            return df
        except Exception:
            return pd.read_csv(path)

def df_safe_head_csv(df: pd.DataFrame, path: Path, n: int = 80):
    with open(path, "w", encoding="utf-8") as f:
        f.write("empty\n" if df.empty else df.head(n).to_csv(index=False))

def write_missing(rows: List[Dict[str, str]], path: Path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["symbol","tried","status"])
        for r in rows: w.writerow([r.get("symbol",""), r.get("tried",""), r.get("status","")])

def write_report(report: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f: json.dump(report, f, indent=2)

# ───────────────────────────── Year/Quarter aus period ableiten ─────────────
def infer_year_quarter_from_period(df: pd.DataFrame) -> pd.DataFrame:
    """Füllt year/quarter, wenn sie leer sind, aus period (z.B. '2023Q4' oder Datum)."""
    if "period" not in df.columns or "year" not in df.columns or "quarter" not in df.columns:
        return df

    def _yq(row):
        y, q = row.get("year"), row.get("quarter")
        if pd.notna(y) and pd.notna(q):
            return y, q
        p = row.get("period")
        if pd.isna(p):
            return y, q
        s = str(p)
        m = re.match(r"^(\d{4})Q([1-4])$", s)
        if m:
            try:
                return float(m.group(1)), float(m.group(2))
            except Exception:
                return y, q
        d = parse_iso_date(s)
        if d:
            try:
                yy = int(d[:4]); mth = int(d[5:7]); qq = (mth-1)//3 + 1
                return float(yy), float(qq)
            except Exception:
                return y, q
        return y, q

    yq = df.apply(_yq, axis=1, result_type="expand")
    yq.columns = ["__year_fix", "__quarter_fix"]
    df.loc[df["year"].isna(), "year"] = yq.loc[df["year"].isna(), "__year_fix"]
    df.loc[df["quarter"].isna(), "quarter"] = yq.loc[df["quarter"].isna(), "__quarter_fix"]
    df = df.drop(columns=["__year_fix","__quarter_fix"])
    return df

# ───────────────────────────── Main ─────────────────────────────
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--watchlist', default=WATCHLIST_PATH)
    ap.add_argument('--limit', type=int, default=DEFAULT_LIMIT)
    ap.add_argument('--use-yf', action='store_true')
    ap.add_argument('--merge-existing', default='data/processed/earnings_results.csv.gz')
    args = ap.parse_args()

    report = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "watchlist": args.watchlist, "overrides": str(OVR_FILE),
              "rows": 0, "symbols": 0, "errors": [], "missing": 0,
              "files": {}, "use_yf": bool(args.use_yf), "limit": int(args.limit),
              "sec_enabled": bool(SEC_UA)}

    watch = load_watchlist(args.watchlist)
    overrides = load_overrides(OVR_FILE)
    report["symbols"] = len(watch)

    out_rows: List[dict] = []
    missing: List[Dict[str, str]] = []

    for sym in watch:
        api_sym = api_symbol_for(sym, overrides)

        # Finnhub
        fin_rows: List[dict] = []
        if FINNHUB_TOKEN:
            fin_raw = finnhub_get(api_sym, limit=args.limit)
            fin_rows = normalize_finnhub_rows(sym, api_sym, fin_raw)

        # Yahoo Fallbacks
        yf_rows: List[dict] = []
        if args.use_yf and (not fin_rows or len(fin_rows) < max(4, args.limit//4)):
            yf_rows, _ = fetch_yf(api_sym, limit=args.limit)

        # SEC Fallback (nur wenn noch Lücken), ergänzt Revenue/EPS
        sec_rows: List[dict] = []
        if SEC_UA and (not fin_rows or not any(pd.notna(r.get("revenue_actual")) for r in fin_rows)) \
                  and (not yf_rows or not any(pd.notna(r.get("revenue_actual")) for r in yf_rows)):
            try:
                sec_rows = sec_fetch_companyfacts(api_sym, limit=args.limit)
            except Exception:
                pass

        if not fin_rows and not yf_rows and not sec_rows:
            missing.append({"symbol": sym, "tried": api_sym, "status": "no-data"})

        out_rows.extend(fin_rows)
        out_rows.extend(yf_rows)
        out_rows.extend(sec_rows)

        sleep_ms(SLEEP_MS)

    # DataFrame
    cols = [
        "symbol","api_symbol","period","report_date",
        "year","quarter","report_time",
        "eps_actual","eps_estimate","surprise_pct","surprise_eps_abs",
        "revenue_actual","revenue_estimate","surprise_rev_pct",
        "currency","source"
    ]
    df = pd.DataFrame(out_rows, columns=cols).dropna(how='all')

    # Typisieren
    for c in ["eps_actual","eps_estimate","surprise_pct","revenue_actual",
              "revenue_estimate","surprise_rev_pct","surprise_eps_abs","year","quarter"]:
        if c in df.columns: df[c] = df[c].apply(to_float)

    # Normalize dates/periods
    if "period" in df.columns:
        df["period"] = df["period"].apply(lambda x: make_fiscal_period(None, None, str(x)) if pd.notna(x) else None)
    if "report_date" in df.columns:
        df["report_date"] = df["report_date"].apply(lambda x: parse_iso_date(str(x)) if pd.notna(x) else None)

    # Surprise-Berechnungen
    m_eps = df["surprise_pct"].isna() & df["eps_actual"].notna() & df["eps_estimate"].notna() & (df["eps_estimate"] != 0)
    df.loc[m_eps, "surprise_pct"] = (df.loc[m_eps, "eps_actual"] - df.loc[m_eps, "eps_estimate"]) / df.loc[m_eps, "eps_estimate"] * 100.0
    m_eps_abs = df["eps_actual"].notna() & df["eps_estimate"].notna()
    df.loc[m_eps_abs, "surprise_eps_abs"] = df.loc[m_eps_abs, "eps_actual"] - df.loc[m_eps_abs, "eps_estimate"]
    m_rev = df["revenue_actual"].notna() & df["revenue_estimate"].notna() & (df["revenue_estimate"] != 0)
    df.loc[m_rev, "surprise_rev_pct"] = (df.loc[m_rev, "revenue_actual"] - df.loc[m_rev, "revenue_estimate"]) / df.loc[m_rev, "revenue_estimate"] * 100.0

    # Perioden-Fallback aus year/quarter
    if "period" in df.columns and "year" in df.columns and "quarter" in df.columns:
        fp_missing = df["period"].isna() & df["year"].notna() & df["quarter"].notna()
        df.loc[fp_missing, "period"] = df.loc[fp_missing].apply(
            lambda r: make_fiscal_period(r["year"], r["quarter"], None), axis=1
        )

    # Year/Quarter aus period ableiten (zusätzlich)
    df = infer_year_quarter_from_period(df)

    # Priorisierung & Dedupe
    priority = {"finnhub": 4, "yahoo.ed": 3, "sec": 2, "yahoo.qe": 1, "yahoo.qf": 0}
    df["_prio"] = df["source"].map(priority).fillna(-1)
    df = df.sort_values(["symbol","period","_prio"], ascending=[True, True, False])
    df = df.drop_duplicates(subset=["symbol","period"], keep="first").drop(columns=["_prio"], errors="ignore")
    df = df.sort_values(["symbol","period"]).reset_index(drop=True)

    # Merge mit bestehender Datei (optional)
    if args.merge_existing:
        old = read_existing(args.merge_existing)
        if not old.empty:
            for c in cols:
                if c not in old.columns: old[c] = pd.NA
            old = old[cols]
            merged = pd.concat([old, df], ignore_index=True)
            merged["_prio"] = merged["source"].map(priority).fillna(-1)
            merged = (merged.sort_values(["symbol","period","_prio"], ascending=[True, True, False])
                              .drop_duplicates(subset=["symbol","period"], keep="first")
                              .drop(columns=["_prio"], errors="ignore")
                              .sort_values(["symbol","period"]).reset_index(drop=True))
            df = merged

    # Schreiben
    out_csv = OUT_DIR / "earnings_results.csv"
    out_gz  = OUT_DIR / "earnings_results.csv.gz"
    df.to_csv(out_csv, index=False)
    df.to_csv(out_gz, index=False, compression="gzip")

    # Reports
    report["rows"] = int(len(df))
    report["files"]["earnings_results_csv"] = str(out_csv)
    report["files"]["earnings_results_gz"]  = str(out_gz)

    preview_path = EU_DIR / "earnings_results_preview.txt"
    df_safe_head_csv(df, preview_path, n=80)
    missing_path = EU_DIR / "earnings_results_missing.txt"
    write_missing(missing, missing_path)
    report["missing"] = len(missing)
    report["files"]["preview"] = str(preview_path)
    report["files"]["missing"] = str(missing_path)

    write_report(report, REP_DIR / "earn_errors.json")
    print(f"[summary] rows={len(df)} symbols~={df['symbol'].nunique()} out={out_csv}")

if __name__ == "__main__":
    main()
