# scripts/fetch_fundamentals.py
import os, sys, csv, time, argparse
from typing import Dict, Any, List
import requests, yfinance as yf, pandas as pd  # pandas nur für yfinance-Interna

def read_watchlist(p: str) -> List[str]:
    out = []
    with open(p, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"): continue
            if "," in s: s = s.split(",")[0].strip()
            s = s.replace(" US_IG","").replace(" EU_IG","")
            out.append(s)
    seen=set(); uniq=[]
    for s in out:
        if s not in seen:
            seen.add(s); uniq.append(s)
    return uniq

def is_xetra(sym: str) -> bool: return sym.endswith(".DE")

def finnhub_get_metrics(sym: str, api_key: str) -> Dict[str, Any]:
    ses = requests.Session()
    base = "https://finnhub.io/api/v1"
    p = {"symbol": sym, "token": api_key}
    r1 = ses.get(f"{base}/stock/profile2", params=p, timeout=20)
    if r1.status_code != 200: raise RuntimeError(f"profile2 {r1.status_code}")
    prof = r1.json() or {}
    r2 = ses.get(f"{base}/stock/metric", params={"symbol":sym, "metric":"all", "token":api_key}, timeout=25)
    if r2.status_code != 200: raise RuntimeError(f"metric {r2.status_code}")
    met = (r2.json() or {}).get("metric", {})
    return {
        "market_cap": met.get("marketCapitalization") or prof.get("marketCapitalization"),
        "beta": met.get("beta") or prof.get("beta"),
        "shares_out": prof.get("shareOutstanding"),
        "pe_ttm": met.get("peTTM") or met.get("peNormalizedAnnual"),
        "ps_ttm": met.get("psTTM"),
        "pb_ttm": met.get("pbAnnual") or met.get("pbTTM"),
        "roe_ttm": met.get("roeTTM"),
        "gross_margin": met.get("grossMarginTTM"),
        "oper_margin": met.get("operatingMarginTTM"),
        "net_margin": met.get("netProfitMarginTTM"),
        "debt_to_equity": met.get("totalDebtToEquityAnnual") or met.get("totalDebt/EquityAnnual"),
    }

def yfin_get_metrics(sym: str) -> Dict[str, Any]:
    t = yf.Ticker(sym)
    try: info = t.info or {}
    except Exception: info = {}
    finfo = getattr(t, "fast_info", {}) or {}
    def pick(*keys, src=None):
        src = src or info
        for k in keys:
            v = src.get(k)
            if v is not None: return v
        return None
    return {
        "market_cap": pick("marketCap", src=info) or finfo.get("market_cap"),
        "beta": pick("beta", src=info),
        "shares_out": pick("sharesOutstanding", src=info),
        "pe_ttm": pick("trailingPE","peTrailing", src=info),
        "ps_ttm": pick("priceToSalesTrailing12Months", src=info),
        "pb_ttm": pick("priceToBook", src=info),
        "roe_ttm": pick("returnOnEquity", src=info),
        "gross_margin": pick("grossMargins", src=info),
        "oper_margin": pick("operatingMargins", src=info),
        "net_margin": pick("profitMargins", src=info),
        "debt_to_equity": pick("debtToEquity", src=info),
    }

def norm_num(x):
    try:
        if x in (None,"","NaN"): return ""
        return float(x)
    except Exception:
        return ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="data/processed")
    ap.add_argument("--finnhub_key", default=os.getenv("FINNHUB_API_KEY",""))
    ap.add_argument("--sleep_ms", type=int, default=int(os.getenv("FINNHUB_SLEEP_MS","1300")))
    args = ap.parse_args()

    outfile = os.path.join(args.outdir, "fundamentals_core.csv")
    os.makedirs(args.outdir, exist_ok=True)

    symbols = read_watchlist(args.watchlist)
    rows = []
    for s in symbols:
        data: Dict[str, Any] = {}
        try:
            if not is_xetra(s) and args.finnhub_key:
                data = finnhub_get_metrics(s, args.finnhub_key)
                time.sleep(args.sleep_ms/1000.0)
            else:
                data = yfin_get_metrics(s)
        except Exception as e:
            try: data = yfin_get_metrics(s)
            except Exception: data = {}
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

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","market_cap","beta","shares_out","pe_ttm","ps_ttm","pb_ttm",
                    "roe_ttm","gross_margin","oper_margin","net_margin","debt_to_equity"])
        w.writerows(rows)

    print(f"{os.path.basename(outfile)} rows: {len(rows)}")

if __name__ == "__main__":
    main()
