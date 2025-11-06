#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_earnings_v2.py – robuste Historie von Earnings (EPS/Revenue + Surprise)

Ziele:
- Mehrquellen-Ansatz mit Fallbacks:
  1) Finnhub (primär) – /stock/earnings (FINNHUB_TOKEN)
  2) Yahoo Finance – earnings_dates + quarterly_earnings/financials (gratis)
  3) Merge mit vorhandener Datei – data/processed/earnings_results.csv[.gz]
- EU/ADR-Overrides per CSV (symbol,api_symbol)
- Rate-Limit + Retry/Backoff
- Typisierung, Dedupe, Surprise%-/Surprise-Abs-Berechnung
- Outputs:
    data/processed/earnings_results.csv
    data/processed/earnings_results.csv.gz
    data/reports/eu_checks/earnings_results_preview.txt
    data/reports/eu_checks/earnings_results_missing.txt
    data/reports/earn_errors.json
"""

from __future__ import annotations
import os, csv, time, json, gzip, io, math, re
from typing import List, Dict, Iterable, Tuple
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd

# ───────────────────────────── Config / ENV ─────────────────────────────
FINNHUB_TOKEN  = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or ""
WATCHLIST_PATH = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OVR_FILE       = os.getenv("EARNINGS_OVERRIDES", "watchlists/earnings_overrides.csv")
SLEEP_MS       = int(os.getenv("FINNHUB_SLEEP_MS", "1200"))
OUT_DIR        = Path("data/processed")
REP_DIR        = Path("data/reports")
EU_DIR         = REP_DIR / "eu_checks"
API_BASE       = "https://finnhub.io/api/v1/stock/earnings"
DEFAULT_LIMIT  = int(os.getenv("EARNINGS_LIMIT", "16"))  # Perioden pro Symbol (Finnhub)

for p in (OUT_DIR, REP_DIR, EU_DIR):
    p.mkdir(parents=True, exist_ok=True)

# ───────────────────────────── Utilities ─────────────────────────────

def load_watchlist(path: str | Path) -> List[str]:
    path = str(path)
    if not path or not os.path.exists(path):
        return []
    syms: List[str] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        head = f.read(2048)
        f.seek(0)
        if "," in head or "symbol" in head.lower():
            rdr = csv.DictReader(f)
            for row in rdr:
                s = (row.get("symbol") or row.get("ticker") or "").strip().upper()
                if s and not s.startswith("#"):
                    syms.append(s)
        else:
            for line in f:
                s = line.strip().upper()
                if s and not s.startswith("#") and s.lower() != "symbol":
                    syms.append(s)
    # order-preserving dedup
    seen = set(); out = []
    for s in syms:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out


def load_overrides(path: str | Path) -> Dict[str, str]:
    path = str(path)
    if not os.path.exists(path):
        return {}
    out: Dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sym = (row.get("symbol") or "").strip().upper()
            api = (row.get("api_symbol") or "").strip().upper()
            if sym and api:
                out[sym] = api
    return out


def api_symbol_for(sym: str, overrides: Dict[str, str]) -> str:
    if sym in overrides:
        return overrides[sym]
    # heuristik: Suffix entfernen (z. B. SAP.DE -> SAP). Für echte ADR besser Overrides pflegen.
    if "." in sym:
        return sym.split(".", 1)[0]
    return sym


def sleep_ms(ms: int):
    time.sleep(max(0.0, ms) / 1000.0)


def to_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return float("nan")
        # handle strings like "1,234.56" / "1.234,56"
        s = str(x).strip()
        if re.match(r"^-?\d{1,3}(\.\d{3})+,\d+$", s):
            s = s.replace(".", "").replace(",", ".")
        elif re.match(r"^-?\d{1,3}(,\d{3})+\.\d+$", s):
            s = s.replace(",", "")
        return float(s)
    except Exception:
        return float("nan")


def pct(x):
    try:
        return float(x) * 100.0
    except Exception:
        return float("nan")


def parse_iso_date(s: str | None) -> str | None:
    if not s: return None
    s = s.strip()
    # prefer YYYY-MM-DD first 10 chars if present
    if len(s) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    # try YYYYMMDD
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # try YYYY/Qx or YYYYQx → approximate to last month of quarter
    m = re.match(r"^(\d{4})[/-]?Q([1-4])$", s, re.I)
    if m:
        y, q = int(m.group(1)), int(m.group(2))
        mm = {1:"03",2:"06",3:"09",4:"12"}[q]
        return f"{y}-{mm}-01"
    return None


def make_fiscal_period(year, quarter, period_str: str | None) -> str | None:
    if pd.notna(year) and pd.notna(quarter):
        try:
            return f"{int(year)}Q{int(quarter)}"
        except Exception:
            pass
    if period_str:
        # Normalize common forms
        m = re.search(r"(\d{4})[/-]?\s*[Qq]([1-4])", period_str)
        if m:
            return f"{m.group(1)}Q{m.group(2)}"
        # YYYY-MM-DD -> infer quarter
        d = parse_iso_date(period_str)
        if d:
            try:
                y, mth = int(d[:4]), int(d[5:7])
                q = (mth-1)//3 + 1
                return f"{y}Q{q}"
            except Exception:
                return None
    return None

# ───────────────────────────── Provider: Finnhub ─────────────────────────────

def finnhub_get(symbol: str, limit: int, retries: int = 3, base_sleep_ms: int = None) -> List[dict]:
    base_sleep_ms = base_sleep_ms if base_sleep_ms is not None else SLEEP_MS
    params = {"symbol": symbol, "limit": int(limit), "token": FINNHUB_TOKEN}
    for attempt in range(retries):
        try:
            r = requests.get(API_BASE, params=params, timeout=30)
            if r.status_code == 429 and attempt + 1 < retries:
                sleep_ms(base_sleep_ms * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json() or []
            if isinstance(data, dict):
                return []
            return data
        except requests.RequestException:
            if attempt + 1 == retries:
                return []
            sleep_ms(base_sleep_ms * (2 ** attempt))
    return []


def normalize_finnhub_rows(sym: str, api_sym: str, rows: List[dict]) -> List[dict]:
    out = []
    for r in rows:
        year = r.get("year")
        quarter = r.get("quarter")
        period = r.get("period")
        report_date = r.get("reportDate") or r.get("date")
        hour = r.get("hour")  # BMO/AMC
        eps_act = r.get("epsActual")
        eps_est = r.get("epsEstimate")
        rev_act = r.get("revenueActual")
        rev_est = r.get("revenueEstimate")
        cur = r.get("currency") or ""

        fp = make_fiscal_period(year, quarter, period)
        out.append({
            "symbol": sym,
            "api_symbol": api_sym,
            "period": fp or period or parse_iso_date(report_date) or None,
            "report_date": parse_iso_date(report_date),
            "year": year,
            "quarter": quarter,
            "report_time": hour,  # BMO / AMC / unkn
            "eps_actual": eps_act,
            "eps_estimate": eps_est,
            "surprise_pct": r.get("surprisePercent"),
            "revenue_actual": rev_act,
            "revenue_estimate": rev_est,
            "surprise_rev_pct": None,   # wird später berechnet
            "surprise_eps_abs": None,   # wird später berechnet
            "currency": cur,
            "source": "finnhub",
        })
    return out

# ───────────────────────────── Provider: Yahoo Finance ─────────────────────────────

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
    """Holt Daten aus yfinance:
       - earnings_dates(limit): Reported EPS, EPS Estimate, Surprise(%), Earnings Date
       - quarterly_earnings: EPS und Revenue (fallback)
       - quarterly_financials: Total Revenue (zusätzlicher Fallback)
       Gibt (rows, api_symbol_used) zurück.
    """
    if not yf_available():
        return [], symbol
    import yfinance as yf
    api_sym = symbol
    rows: List[dict] = []
    try:
        tk = yf.Ticker(symbol)

        # 1) earnings_dates
        try:
            # neuere yfinance-Versionen: .earnings_dates(limit=..)
            ed = getattr(tk, "earnings_dates", None)
            if callable(ed):
                df = ed(limit=limit)
            else:
                df = getattr(tk, "calendar", None)  # alt, nicht hilfreich historisch
                df = None
            if df is not None and hasattr(df, "reset_index"):
                dfe = df.reset_index().rename(columns={
                    "Earnings Date": "report_date",
                    "Reported EPS": "eps_actual",
                    "EPS Estimate": "eps_estimate",
                    "Surprise(%)": "surprise_pct"
                })
                for _, rr in dfe.iterrows():
                    rows.append({
                        "symbol": symbol,
                        "api_symbol": api_sym,
                        "period": make_fiscal_period(None, None, str(rr.get("report_date"))),
                        "report_date": parse_iso_date(str(rr.get("report_date"))),
                        "year": None,
                        "quarter": None,
                        "report_time": None,
                        "eps_actual": rr.get("eps_actual"),
                        "eps_estimate": rr.get("eps_estimate"),
                        "surprise_pct": rr.get("surprise_pct"),
                        "revenue_actual": None,
                        "revenue_estimate": None,
                        "surprise_rev_pct": None,
                        "surprise_eps_abs": None,
                        "currency": "",
                        "source": "yahoo.ed"
                    })
        except Exception:
            pass

        # 2) quarterly_earnings (EPS + Revenue)
        try:
            qe = getattr(tk, "quarterly_earnings", None)
            if qe is not None and hasattr(qe, "reset_index"):
                dfq = qe.reset_index().rename(columns={
                    "Quarter": "period",
                    "Revenue": "revenue_actual",
                    "Earnings": "eps_actual"
                })
                for _, rr in dfq.iterrows():
                    rows.append({
                        "symbol": symbol,
                        "api_symbol": api_sym,
                        "period": make_fiscal_period(None, None, str(rr.get("period"))),
                        "report_date": parse_iso_date(str(rr.get("period"))),
                        "year": None,
                        "quarter": None,
                        "report_time": None,
                        "eps_actual": rr.get("eps_actual"),
                        "eps_estimate": None,
                        "surprise_pct": None,
                        "revenue_actual": rr.get("revenue_actual"),
                        "revenue_estimate": None,
                        "surprise_rev_pct": None,
                        "surprise_eps_abs": None,
                        "currency": "",
                        "source": "yahoo.qe"
                    })
        except Exception:
            pass

        # 3) quarterly_financials (Total Revenue)
        try:
            qf = getattr(tk, "quarterly_financials", None)
            if qf is not None and hasattr(qf, "T"):
                qf_t = qf.T  # Perioden als Index (Datetime)
                for idx, row in qf_t.iterrows():
                    rev = row.get("Total Revenue") or row.get("TotalRevenue") or row.get("Revenue")
                    if pd.notna(rev):
                        rows.append({
                            "symbol": symbol,
                            "api_symbol": api_sym,
                            "period": make_fiscal_period(None, None, str(idx)),
                            "report_date": parse_iso_date(str(idx)),
                            "year": None,
                            "quarter": None,
                            "report_time": None,
                            "eps_actual": None,
                            "eps_estimate": None,
                            "surprise_pct": None,
                            "revenue_actual": rev,
                            "revenue_estimate": None,
                            "surprise_rev_pct": None,
                            "surprise_eps_abs": None,
                            "currency": "",
                            "source": "yahoo.qf"
                        })
        except Exception:
            pass

    except Exception:
        return rows, api_sym

    return rows, api_sym

# ───────────────────────────── Merge / IO ─────────────────────────────

def read_existing(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            raw = f.read()
        bio = io.BytesIO(raw)
        try:
            df = pd.read_csv(bio, sep=";", low_memory=False)
            if df.shape[1] == 1:
                bio.seek(0)
                df = pd.read_csv(bio, sep=",")
        except Exception:
            bio.seek(0)
            df = pd.read_csv(bio)
        return df
    else:
        try:
            df = pd.read_csv(path, sep=";", low_memory=False)
            if df.shape[1] == 1:
                df = pd.read_csv(path, sep=",")
            return df
        except Exception:
            return pd.read_csv(path)


def df_safe_head_csv(df: pd.DataFrame, path: Path, n: int = 50):
    with open(path, "w", encoding="utf-8") as f:
        if df.empty:
            f.write("empty\n")
        else:
            f.write(df.head(n).to_csv(index=False))


def write_missing(rows: List[Dict[str, str]], path: Path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "tried", "status"])
        for r in rows:
            w.writerow([r.get("symbol",""), r.get("tried",""), r.get("status","")])


def write_report(report: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

# ───────────────────────────── Main ─────────────────────────────

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--watchlist', default=WATCHLIST_PATH)
    ap.add_argument('--limit', type=int, default=DEFAULT_LIMIT, help='Max Perioden pro Symbol (Finnhub & Yahoo)')
    ap.add_argument('--use-yf', action='store_true', help='yfinance als Fallback verwenden')
    ap.add_argument('--merge-existing', default='data/processed/earnings_results.csv.gz', help='Bestehende Datei .csv/.csv.gz zum Mergen')
    args = ap.parse_args()

    report = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "watchlist": args.watchlist,
        "overrides": str(OVR_FILE),
        "rows": 0,
        "symbols": 0,
        "errors": [],
        "missing": 0,
        "files": {},
        "use_yf": bool(args.use_yf),
        "limit": int(args.limit),
    }

    watch = load_watchlist(args.watchlist)
    overrides = load_overrides(OVR_FILE)
    report["symbols"] = len(watch)

    out_rows: List[dict] = []
    missing: List[Dict[str, str]] = []

    for i, sym in enumerate(watch, 1):
        api_sym = api_symbol_for(sym, overrides)

        # 1) Finnhub (wenn Token vorhanden)
        fin_rows: List[dict] = []
        if FINNHUB_TOKEN:
            fin_raw = finnhub_get(api_sym, limit=args.limit)
            fin_rows = normalize_finnhub_rows(sym, api_sym, fin_raw)

        # 2) yfinance Fallback (optional), falls keine oder zu wenige Zeilen
        yf_rows: List[dict] = []
        if args.use_yf and (not fin_rows or len(fin_rows) < max(4, args.limit//4)):
            yf_rows, _ = fetch_yf(api_sym, limit=args.limit)

        if not fin_rows and not yf_rows:
            missing.append({"symbol": sym, "tried": api_sym, "status": "no-data"})

        out_rows.extend(fin_rows or [])
        out_rows.extend(yf_rows or [])

        sleep_ms(SLEEP_MS)

    # DataFrame bauen
    cols = [
        "symbol","api_symbol","period","report_date",
        "year","quarter","report_time",
        "eps_actual","eps_estimate","surprise_pct","surprise_eps_abs",
        "revenue_actual","revenue_estimate","surprise_rev_pct",
        "currency","source"
    ]
    df = pd.DataFrame(out_rows, columns=cols).dropna(how='all')

    # Typisieren
    for c in ["eps_actual","eps_estimate","surprise_pct","revenue_actual","revenue_estimate","surprise_rev_pct","surprise_eps_abs","year","quarter"]:
        if c in df.columns:
            df[c] = df[c].apply(to_float)

    # Normalize dates/periods
    if "period" in df.columns:
        df["period"] = df["period"].apply(lambda x: make_fiscal_period(None, None, str(x)) if pd.notna(x) else None)
    if "report_date" in df.columns:
        df["report_date"] = df["report_date"].apply(lambda x: parse_iso_date(str(x)) if pd.notna(x) else None)

    # Surprise neu berechnen, falls möglich (und fehlend)
    m_eps = df["surprise_pct"].isna() & df["eps_actual"].notna() & df["eps_estimate"].notna() & (df["eps_estimate"] != 0)
    df.loc[m_eps, "surprise_pct"] = (df.loc[m_eps, "eps_actual"] - df.loc[m_eps, "eps_estimate"]) / df.loc[m_eps, "eps_estimate"] * 100.0

    # Surprise EPS abs
    m_eps_abs = df["eps_actual"].notna() & df["eps_estimate"].notna()
    df.loc[m_eps_abs, "surprise_eps_abs"] = df.loc[m_eps_abs, "eps_actual"] - df.loc[m_eps_abs, "eps_estimate"]

    # Surprise Revenue %
    m_rev = df["revenue_actual"].notna() & df["revenue_estimate"].notna() & (df["revenue_estimate"] != 0)
    df.loc[m_rev, "surprise_rev_pct"] = (df.loc[m_rev, "revenue_actual"] - df.loc[m_rev, "revenue_estimate"]) / df.loc[m_rev, "revenue_estimate"] * 100.0

    # Fiscal period fallback aus year/quarter
    if "period" in df.columns:
        fp_missing = df["period"].isna() & df["year"].notna() & df["quarter"].notna()
        df.loc[fp_missing, "period"] = df.loc[fp_missing].apply(lambda r: make_fiscal_period(r["year"], r["quarter"], None), axis=1)

    # Dedupe: Primärschlüssel (symbol, period) – Quelle priorisieren (finnhub > yahoo.ed > yahoo.qe > yahoo.qf)
    priority = {"finnhub": 3, "yahoo.ed": 2, "yahoo.qe": 1, "yahoo.qf": 0}
    df["_prio"] = df["source"].map(priority).fillna(-1)
    df = df.sort_values(["symbol","period","_prio"], ascending=[True, True, False])
    df = df.drop_duplicates(subset=["symbol","period"], keep="first").drop(columns=["_prio"], errors="ignore")
    df = df.sort_values(["symbol","period"]).reset_index(drop=True)

    # Optional: Merge mit existierender Datei
    if args.merge_existing:
        old = read_existing(args.merge_existing)
        if not old.empty:
            # gleiche Spalten sicherstellen
            for c in cols:
                if c not in old.columns:
                    old[c] = pd.NA
            old = old[cols]
            # existierende Surprise-Felder ggf. übernehmen, aber neue priorisieren
            merged = pd.concat([old, df], ignore_index=True)
            # Re-Priorisierung
            merged["_prio"] = merged["source"].map(priority).fillna(-1)
            merged = merged.sort_values(["symbol","period","_prio"], ascending=[True, True, False]) \
                           .drop_duplicates(subset=["symbol","period"], keep="first") \
                           .drop(columns=["_prio"], errors="ignore") \
                           .sort_values(["symbol","period"]).reset_index(drop=True)
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
