# scripts/fetch_fundamentals.py
import os, csv, time, argparse, math
import requests
import yfinance as yf

OUTCSV = "data/processed/fundamentals_core.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--watchlist", required=True, help="Path to watchlists/*.txt")
parser.add_argument("--finnhub_key", default=os.getenv("FINNHUB_API_KEY", ""))
parser.add_argument("--sleep_ms", type=int, default=150)
args = parser.parse_args()

os.makedirs(os.path.dirname(OUTCSV), exist_ok=True)

def read_watchlist(p):
    syms = []
    with open(p, encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s or s.startswith("#"): 
                continue
            syms.append(s)
    return syms

def safe(v):
    try:
        if v is None: return None
        if isinstance(v, (int, float)) and (math.isnan(v) or math.isinf(v)): return None
        return v
    except Exception:
        return None

def finnhub_profile2(symbol, key):
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={symbol}&token={key}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json() or {}

def finnhub_metrics(symbol, key):
    url = f"https://finnhub.io/api/v1/stock/metric?symbol={symbol}&metric=all&token={key}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    j = r.json() or {}
    return j.get("metric", {}) or {}

def yf_fields(symbol):
    t = yf.Ticker(symbol)
    info = t.info or {}
    def g(*keys):
        for k in keys:
            if k in info and info[k] is not None:
                return info[k]
        return None
    return {
        "market_cap": g("marketCap"),
        "beta": g("beta", "beta3Year"),
        "shares_out": g("sharesOutstanding"),
        "pe_ttm": g("trailingPE"),
        "ps_ttm": g("priceToSalesTrailing12Months"),
        "pb_ttm": g("priceToBook"),
        "roe_ttm": None,  # selten zuverlässig in YF
        "gross_margin": g("grossMargins"),
        "oper_margin": g("operatingMargins"),
        "net_margin": g("profitMargins"),
        "debt_to_equity": g("debtToEquity"),
    }

def row_for_symbol(sym, finnhub_key):
    # 1) Finnhub für Nicht-DE (wenn Key vorhanden)
    if finnhub_key and not sym.endswith(".DE"):
        try:
            prof = finnhub_profile2(sym, finnhub_key)
            met  = finnhub_metrics(sym, finnhub_key)
            return [
                sym,
                safe(prof.get("marketCapitalization")),
                safe(met.get("beta")),
                safe(met.get("shareOutstanding")),
                safe(met.get("peInclExtraTTM") or met.get("peTTM")),
                safe(met.get("psTTM")),
                safe(met.get("pbAnnual") or met.get("pbQuarterly") or met.get("priceToBook")),
                safe(met.get("roeTTM")),
                safe(met.get("grossMarginTTM")),
                safe(met.get("operatingMarginTTM")),
                safe(met.get("netProfitMarginTTM")),
                safe(met.get("totalDebtTotalEquityAnnual") or met.get("totalDebtToTotalEquity")),
            ]
        except Exception:
            pass  # Fallback auf YF

    # 2) Fallback: yfinance (für .DE und Fehlerfälle)
    try:
        f = yf_fields(sym)
        return [
            sym,
            safe(f["market_cap"]), safe(f["beta"]), safe(f["shares_out"]), safe(f["pe_ttm"]),
            safe(f["ps_ttm"]), safe(f["pb_ttm"]), safe(f["roe_ttm"]), safe(f["gross_margin"]),
            safe(f["oper_margin"]), safe(f["net_margin"]), safe(f["debt_to_equity"]),
        ]
    except Exception:
        return [sym] + [None]*11

symbols = read_watchlist(args.watchlist)

with open(OUTCSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow([
        "symbol","market_cap","beta","shares_out","pe_ttm","ps_ttm","pb_ttm",
        "roe_ttm","gross_margin","oper_margin","net_margin","debt_to_equity"
    ])
    for s in symbols:
        row = row_for_symbol(s, args.finnhub_key)
        w.writerow(row)
        time.sleep(args.sleep_ms/1000.0)

print(f"fundamentals_core.csv rows: {len(symbols)}")
