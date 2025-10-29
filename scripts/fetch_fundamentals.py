# scripts/fetch_fundamentals.py
import os, sys, csv, time, json
import argparse
from typing import Dict, Any, Tuple, List

import requests
import yfinance as yf
import pandas as pd

OUT_CSV = "data/processed/fundamentals_core.csv"
os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────
def read_watchlist(p: str) -> List[str]:
    syms = []
    with open(p, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            # nur Roh-Ticker verwenden (keine angehängten Kommas/Suffixe)
            if "," in s:
                s = s.split(",")[0].strip()
            # absolut KEINE Proxy-/Region-Suffixe wie ",US_IG"
            s = s.replace(" US_IG", "").replace(" EU_IG", "")
            syms.append(s)
    # Deduplizieren, Reihenfolge beibehalten
    seen = set(); uniq = []
    for s in syms:
        if s not in seen:
            seen.add(s); uniq.append(s)
    return uniq

def is_xetra(sym: str) -> bool:
    return sym.endswith(".DE")

def finnhub_get_metrics(sym: str, api_key: str) -> Dict[str, Any]:
    """Liest Profile + Metrics (TTM) von Finnhub; wirft bei 4xx/5xx eine Exception,
    damit der Aufrufer sauber auf yfinance fallen kann."""
    ses = requests.Session()
    base = "https://finnhub.io/api/v1"
    params = {"symbol": sym, "token": api_key}

    # Profile2 (für shares_out, beta als Fallback)
    r1 = ses.get(f"{base}/stock/profile2", params=params, timeout=20)
    if r1.status_code != 200:
        raise RuntimeError(f"profile2 {r1.status_code}")
    prof = r1.json() or {}

    # Metrics (TTM/Annual)
    params_m = {"symbol": sym, "metric": "all", "token": api_key}
    r2 = ses.get(f"{base}/stock/metric", params=params_m, timeout=25)
    if r2.status_code != 200:
        raise RuntimeError(f"metric {r2.status_code}")
    met = (r2.json() or {}).get("metric", {})

    # Map auf unser Zielschema
    out = {
        "market_cap": met.get("marketCapitalization") or prof.get("marketCapitalization"),
        "beta":        met.get("beta") or prof.get("beta"),
        "shares_out":  prof.get("shareOutstanding"),
        "pe_ttm":      met.get("peTTM") or met.get("peNormalizedAnnual"),
        "ps_ttm":      met.get("psTTM"),
        "pb_ttm":      met.get("pbAnnual") or met.get("pbTTM"),
        "roe_ttm":     met.get("roeTTM"),
        "gross_margin":met.get("grossMarginTTM"),
        "oper_margin": met.get("operatingMarginTTM"),
        "net_margin":  met.get("netProfitMarginTTM"),
        "debt_to_equity": met.get("totalDebtToEquityAnnual") or met.get("totalDebt/EquityAnnual"),
    }
    return out

def yfin_get_metrics(sym: str) -> Dict[str, Any]:
    t = yf.Ticker(sym)
    info = {}
    # yfinance kann manchmal .fast_info/.info getrennt liefern
    try:
        info = t.info or {}
    except Exception:
        info = {}
    try:
        finfo = getattr(t, "fast_info", {}) or {}
    except Exception:
        finfo = {}

    def pick(*keys, src=None):
        src = src or info
        for k in keys:
            v = src.get(k)
            if v is not None:
                return v
        return None

    out = {
        "market_cap":    pick("marketCap", src=info) or finfo.get("market_cap"),
        "beta":          pick("beta", src=info),
        "shares_out":    pick("sharesOutstanding", src=info),
        "pe_ttm":        pick("trailingPE", "peTrailing", src=info),
        "ps_ttm":        pick("priceToSalesTrailing12Months", src=info),
        "pb_ttm":        pick("priceToBook", src=info),
        "roe_ttm":       pick("returnOnEquity", src=info),
        "gross_margin":  pick("grossMargins", src=info),
        "oper_margin":   pick("operatingMargins", src=info),
        "net_margin":    pick("profitMargins", src=info),
        "debt_to_equity":pick("debtToEquity", src=info),
    }
    return out

def norm_num(x):
    try:
        if x in (None, "", "NaN"):
            return ""
        # yfinance liefert oft Brüche (0.32) für Margins → auf % umrechnen?
        return float(x)
    except Exception:
        return ""

# ──────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--finnhub_key", default=os.getenv("FINNHUB_API_KEY", ""))
    ap.add_argument("--sleep_ms", type=int, default=int(os.getenv("FINNHUB_SLEEP_MS", "1300")))
    args = ap.parse_args()

    symbols = read_watchlist(args.watchlist)
    rows = []

    for s in symbols:
        data: Dict[str, Any] = {}
        try:
            if not is_xetra(s) and args.finnhub_key:
                # US (oder non-.DE) → Finnhub (Rate-Limit beachten)
                data = finnhub_get_metrics(s, args.finnhub_key)
                time.sleep(args.sleep_ms / 1000.0)
            else:
                # .DE oder kein Key → direkt yfinance
                data = yfin_get_metrics(s)

        except Exception as e:
            # Fallback für US: yfinance
            try:
                data = yfin_get_metrics(s)
            except Exception:
                data = {}
            # Logging ins CI-Log
            print(f"[fundamentals] Fallback yfinance for {s}: {e}", file=sys.stderr)

        rows.append([
            s,
            norm_num(data.get("market_cap")),
            norm_num(data.get("beta")),
            norm_num(data.get("shares_out")),
            norm_num(data.get("pe_ttm")),
            norm_num(data.get("ps_ttm")),
            norm_num(data.get("pb_ttm")),
            norm_num(data.get("roe_ttm")),
            norm_num(data.get("gross_margin")),
            norm_num(data.get("oper_margin")),
            norm_num(data.get("net_margin")),
            norm_num(data.get("debt_to_equity")),
        ])

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "symbol","market_cap","beta","shares_out","pe_ttm","ps_ttm","pb_ttm",
            "roe_ttm","gross_margin","oper_margin","net_margin","debt_to_equity"
        ])
        w.writerows(rows)

    print(f"fundamentals_core.csv rows: {len(rows)}")
    # kleine Vorschau
    for r in rows[:10]:
        print(",".join(str(x) for x in r))

if __name__ == "__main__":
    main()
