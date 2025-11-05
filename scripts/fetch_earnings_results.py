#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_earnings_v2.py – robuste Historie von Earnings (EPS/Revenue + Surprise)

Ziele:
- Mehrquellen-Ansatz mit Fallbacks:
  1) Finnhub (primär) – /stock/earnings (benötigt FINNHUB_TOKEN)
  2) Yahoo Finance (optional) – via yfinance (kostenlos, keine Surprise%, aber EPS/Revenue)
  3) Merge mit vorhandener Datei (optional) – data/processed/earnings_results.csv[.gz]
- EU/ADR-Overrides per CSV (symbol,api_symbol)
- Rate-Limit + Retry/Backoff
- Typisierung, Dedupe, Surprise%-Berechnung falls möglich
- Outputs:
    data/processed/earnings_results.csv
    data/processed/earnings_results.csv.gz
    data/reports/eu_checks/earnings_results_preview.txt
    data/reports/eu_checks/earnings_results_missing.txt
    data/reports/earn_errors.json

CLI:
  python scripts/fetch_earnings_v2.py \
    --watchlist watchlists/mylist.txt \
    --use-yf \
    --limit 40 \
    --merge-existing data/processed/earnings_results.csv.gz

Benötigte Pakete:
  pip install pandas yfinance requests
"""

from __future__ import annotations
import os, csv, time, json, gzip, io, math
from typing import List, Dict, Iterable, Tuple
from pathlib import Path

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
DEFAULT_LIMIT  = int(os.getenv("EARNINGS_LIMIT", "12"))  # periods pro Symbol

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


# ───────────────────────────── Provider: Finnhub ─────────────────────────────

def finnhub_get(symbol: str, limit: int, retries: int = 3, base_sleep_ms: int = None) -> List[dict]:
    base_sleep_ms = base_sleep_ms if base_sleep_ms is not None else SLEEP_MS
    params = {"symbol": symbol, "limit": int(limit), "token": FINNHUB_TOKEN}
    for attempt in range(retries):
        try:
            r = requests.get(API_BASE, params=params, timeout=30)
            if r.status_code == 429 and attempt + 1 < retries:
                # Rate limited → Backoff
                sleep_ms(base_sleep_ms * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json() or []
            if isinstance(data, dict):
                # z. B. {"error": "..."}
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
        out.append({
            "symbol": sym,
            "api_symbol": api_sym,
            "period": r.get("period"),
            "report_date": r.get("reportDate") or r.get("date"),
            "eps_actual": r.get("epsActual"),
            "eps_estimate": r.get("epsEstimate"),
            "surprise_pct": r.get("surprisePercent"),
            "revenue_actual": r.get("revenueActual"),
            "revenue_estimate": r.get("revenueEstimate"),
            "currency": r.get("currency") or "",
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


def fetch_yf(symbol: str) -> Tuple[List[dict], str]:
    """Hol EPS/Revenue aus yfinance. Surprise% nicht garantiert.
    Versucht verschiedene DataFrames (quarterly_earnings, quarterly_financials, income_stmt).
    Gibt (rows, api_symbol_used) zurück.
    """
    if not yf_available():
        return [], symbol
    import yfinance as yf
    api_sym = symbol
    try:
        tk = yf.Ticker(symbol)
        rows: List[dict] = []
        # 1) quarterly_earnings (hat meist 'Earnings' (EPS) und 'Revenue')
        try:
            qe = tk.quarterly_earnings
            if qe is not None and hasattr(qe, "reset_index"):
                df = qe.reset_index().rename(columns={"Quarter": "period", "Revenue": "revenue_actual", "Earnings": "eps_actual"})
                for _, rr in df.iterrows():
                    rows.append({
                        "symbol": symbol,
                        "api_symbol": api_sym,
                        "period": str(rr.get("period")),
                        "report_date": None,
                        "eps_actual": rr.get("eps_actual"),
                        "eps_estimate": None,
                        "surprise_pct": None,
                        "revenue_actual": rr.get("revenue_actual"),
                        "revenue_estimate": None,
                        "currency": "",
                        "source": "yfinance.qe",
                    })
        except Exception:
            pass
        # 2) quarterly_financials (falls vorhanden – kann Umsätze liefern)
        try:
            qf = tk.quarterly_financials
            if qf is not None and hasattr(qf, "T"):
                qf_t = qf.T  # Perioden als Index
                # versuche Kennzahlen zu finden
                for idx, row in qf_t.iterrows():
                    rev = row.get("Total Revenue") or row.get("TotalRevenue") or row.get("Revenue")
                    eps = None  # EPS ist hier oft nicht direkt
                    if pd.notna(rev) or pd.notna(eps):
                        rows.append({
                            "symbol": symbol,
                            "api_symbol": api_sym,
                            "period": str(idx),
                            "report_date": None,
                            "eps_actual": eps,
                            "eps_estimate": None,
                            "surprise_pct": None,
                            "revenue_actual": rev,
                            "revenue_estimate": None,
                            "currency": "",
                            "source": "yfinance.qf",
                        })
        except Exception:
            pass
        return rows, api_sym
    except Exception:
        return [], api_sym


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


def df_safe_head_csv(df: pd.DataFrame, path: Path, n: int = 30):
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


def to_float(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")


# ───────────────────────────── Main ─────────────────────────────

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('--watchlist', default=WATCHLIST_PATH)
    ap.add_argument('--limit', type=int, default=DEFAULT_LIMIT, help='Max Perioden pro Symbol (Finnhub)')
    ap.add_argument('--use-yf', action='store_true', help='yfinance als Fallback verwenden')
    ap.add_argument('--merge-existing', default='', help='Bestehende Datei .csv/.csv.gz zum Mergen')
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
        if args.use_yf and (not fin_rows or len(fin_rows) < 4):
            yf_rows, api_used = fetch_yf(api_sym)
            # Anmerkung: yfinance liefert u. U. Period-Indizes wie '2024-09-30' oder '2024Q3'
        if not fin_rows and not yf_rows:
            missing.append({"symbol": sym, "tried": api_sym, "status": "no-data"})
        out_rows.extend(fin_rows or [])
        out_rows.extend(yf_rows or [])
        sleep_ms(SLEEP_MS)

    # DataFrame bauen
    cols = [
        "symbol","api_symbol","period","report_date",
        "eps_actual","eps_estimate","surprise_pct",
        "revenue_actual","revenue_estimate","currency","source"
    ]
    df = pd.DataFrame(out_rows, columns=cols).dropna(how='all')

    # Typisieren und Surprise% ggf. berechnen
    for c in ["eps_actual","eps_estimate","surprise_pct","revenue_actual","revenue_estimate"]:
        df[c] = df[c].apply(to_float)

    # Surprise neu berechnen, falls möglich (und fehlend)
    mask = df["surprise_pct"].isna() & df["eps_actual"].notna() & df["eps_estimate"].notna() & (df["eps_estimate"] != 0)
    df.loc[mask, "surprise_pct"] = (df.loc[mask, "eps_actual"] - df.loc[mask, "eps_estimate"]) / df.loc[mask, "eps_estimate"] * 100.0

    # Period/Report-Date säubern → Period bevorzugen, YYYY-MM-DD extrahieren
    def norm_date(x: str | float | None) -> str | None:
        if not isinstance(x, str):
            return None
        s = x.strip()
        if not s:
            return None
        # grob: nehme die ersten 10 Zeichen wenn im ISO Format
        return s[:10]

    df["period"] = df["period"].apply(norm_date)
    df["report_date"] = df["report_date"].apply(norm_date)

    # Dedupe nach (symbol, period, source)
    df = df.drop_duplicates(subset=["symbol","period","source"]) \
           .sort_values(["symbol","period"]) \
           .reset_index(drop=True)

    # Optional: Merge mit existierender Datei
    if args.merge_existing:
        old = read_existing(args.merge_existing)
        if not old.empty:
            # gleiche Spalten sicherstellen
            for c in cols:
                if c not in old.columns:
                    old[c] = pd.NA
            old = old[cols]
            merged = pd.concat([old, df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["symbol","period"], keep="last") \
                             .sort_values(["symbol","period"]).reset_index(drop=True)
            df = merged

    # Schreiben
    out_csv = OUT_DIR / "earnings_results.csv"
    out_gz  = OUT_DIR / "earnings_results.csv.gz"
    df.to_csv(out_csv, index=False)
    df.to_csv(out_gz, index=False, compression="gzip")

    report["rows"] = int(len(df))
    report["files"]["earnings_results_csv"] = str(out_csv)
    report["files"]["earnings_results_gz"]  = str(out_gz)

    # Reports
    preview_path = EU_DIR / "earnings_results_preview.txt"
    df_safe_head_csv(df, preview_path, n=50)

    missing_path = EU_DIR / "earnings_results_missing.txt"
    write_missing(missing, missing_path)
    report["missing"] = len(missing)
    report["files"]["preview"] = str(preview_path)
    report["files"]["missing"] = str(missing_path)

    write_report(report, REP_DIR / "earn_errors.json")
    print(f"[summary] rows={len(df)} symbols~={df['symbol'].nunique()} out={out_csv}")


if __name__ == "__main__":
    main()
