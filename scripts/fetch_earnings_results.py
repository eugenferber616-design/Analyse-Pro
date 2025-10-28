#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch historical earnings (EPS/Revenue surprises) from Finnhub.

- Robust for EU tickers: optional ADR/US overrides via watchlists/earnings_overrides.csv
- Tolerant to 403/404 (logs as "missing", job continues)
- Throttling via FINNHUB_SLEEP_MS
- Outputs:
    data/processed/earnings_results.csv
    data/reports/eu_checks/earnings_results_preview.txt
    data/reports/eu_checks/earnings_results_missing.txt
    data/reports/earn_errors.json
"""

import os, csv, time, json
from typing import List, Dict, Iterable
import requests
import pandas as pd

# ── ENV / Paths
FINNHUB_TOKEN  = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY") or ""
WATCHLIST_PATH = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
OVR_FILE       = os.getenv("EARNINGS_OVERRIDES", "watchlists/earnings_overrides.csv")
SLEEP_MS       = int(os.getenv("FINNHUB_SLEEP_MS", "1200"))
OUT_DIR        = "data/processed"
REP_DIR        = "data/reports"
EU_DIR         = os.path.join(REP_DIR, "eu_checks")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)
os.makedirs(EU_DIR, exist_ok=True)

API_BASE = "https://finnhub.io/api/v1/stock/earnings"
LIMIT    = int(os.getenv("EARNINGS_LIMIT", "12"))  # periods per symbol

# ── Helpers
def load_watchlist(path: str) -> List[str]:
    """Accepts simple txt (one symbol per line) or CSV with a 'symbol' column."""
    if not os.path.exists(path):
        return []
    syms: List[str] = []
    if path.lower().endswith(".csv"):
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                s = (row.get("symbol") or "").strip()
                if s:
                    syms.append(s)
    else:
        with open(path, encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.lower().startswith("symbol"):
                    continue
                syms.append(s)
    # normalize & dedup, keeping order
    seen = set()
    out = []
    for s in syms:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def load_overrides(path: str) -> Dict[str, str]:
    """CSV with columns: symbol,api_symbol (ADR/US mapping)."""
    if not os.path.exists(path):
        return {}
    out: Dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sym = (row.get("symbol") or "").strip()
            api = (row.get("api_symbol") or "").strip()
            if sym and api:
                out[sym] = api
    return out

def api_symbol_for(sym: str, overrides: Dict[str, str]) -> str:
    """Pick API symbol: override first; else heuristic strips exchange suffix."""
    if sym in overrides:
        return overrides[sym]
    if "." in sym:  # e.g. SAP.DE -> SAP (only heuristic; ADR may differ!)
        base = sym.split(".", 1)[0]
        return base
    return sym

def finnhub_get(symbol: str) -> Iterable[dict]:
    """Yield earnings rows from Finnhub for a given symbol."""
    params = {"symbol": symbol, "limit": LIMIT, "token": FINNHUB_TOKEN}
    r = requests.get(API_BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json() or []
    if isinstance(data, dict):
        # Finnhub sometimes returns {"error":"..."} with 200; treat as empty
        return []
    return data

def df_safe_head_csv(df: pd.DataFrame, path: str, n: int = 30):
    with open(path, "w", encoding="utf-8") as f:
        if df.empty:
            f.write("empty\n")
        else:
            f.write(df.head(n).to_csv(index=False))

def write_missing(rows: List[Dict[str, str]], path: str):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "tried", "status"])
        for r in rows:
            w.writerow([r.get("symbol",""), r.get("tried",""), r.get("status","")])

def write_report(report: dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

# ── Main
def main():
    report = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "watchlist": WATCHLIST_PATH,
        "overrides": OVR_FILE,
        "rows": 0,
        "symbols": 0,
        "errors": [],
        "missing": 0,
        "files": {},
    }

    if not FINNHUB_TOKEN:
        report["errors"].append({"stage": "init", "msg": "FINNHUB_TOKEN/FINNHUB_API_KEY missing"})
        write_report(report, os.path.join(REP_DIR, "earn_errors.json"))
        raise SystemExit(1)

    watch = load_watchlist(WATCHLIST_PATH)
    overrides = load_overrides(OVR_FILE)
    report["symbols"] = len(watch)

    out_rows = []
    missing: List[Dict[str, str]] = []

    for i, sym in enumerate(watch, 1):
        api_sym = api_symbol_for(sym, overrides)
        try:
            rows = list(finnhub_get(api_sym))
            # normalize into our schema
            for r in rows:
                out_rows.append({
                    "symbol": sym,
                    "api_symbol": api_sym,
                    "period": r.get("period"),
                    "eps_actual": r.get("epsActual"),
                    "eps_estimate": r.get("epsEstimate"),
                    "surprise_pct": r.get("surprisePercent"),
                    "revenue_actual": r.get("revenueActual"),
                    "revenue_estimate": r.get("revenueEstimate"),
                })
        except requests.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code in (403, 404):
                missing.append({"symbol": sym, "tried": api_sym, "status": str(code)})
            else:
                report["errors"].append({"stage": "fetch", "symbol": sym, "tried": api_sym, "msg": f"HTTP {code}"})
        except Exception as ex:
            report["errors"].append({"stage": "fetch", "symbol": sym, "tried": api_sym, "msg": str(ex)})
        finally:
            time.sleep(SLEEP_MS / 1000.0)

    # Build DataFrame & write outputs
    df = pd.DataFrame(out_rows, columns=[
        "symbol","api_symbol","period","eps_actual","eps_estimate","surprise_pct",
        "revenue_actual","revenue_estimate"
    ])

    out_csv = os.path.join(OUT_DIR, "earnings_results.csv")
    df.to_csv(out_csv, index=False)
    report["rows"] = int(len(df))
    report["files"]["earnings_results_csv"] = out_csv

    # Previews & missing
    preview_path = os.path.join(EU_DIR, "earnings_results_preview.txt")
    df_safe_head_csv(df, preview_path, n=30)

    missing_path = os.path.join(EU_DIR, "earnings_results_missing.txt")
    write_missing(missing, missing_path)
    report["missing"] = len(missing)
    report["files"]["preview"] = preview_path
    report["files"]["missing"] = missing_path

    write_report(report, os.path.join(REP_DIR, "earn_errors.json"))

if __name__ == "__main__":
    main()
