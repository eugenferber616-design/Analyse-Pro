# scripts/fetch_fundamentals.py
import argparse, os, time, csv, json
from typing import Dict, Any, Optional
import requests
import yfinance as yf

OUT_PROCESSED = "data/processed/fundamentals_core.csv"
PREVIEW_TXT   = "data/reports/eu_checks/fundamentals_preview.txt"
MISSING_TXT   = "data/reports/eu_checks/fundamentals_missing.txt"

HEADERS = [
    "symbol","market_cap","beta","shares_out",
    "pe_ttm","ps_ttm","pb_ttm","roe_ttm",
    "gross_margin","oper_margin","net_margin","debt_to_equity"
]

FINNHUB_BASE = "https://finnhub.io/api/v1"

def read_watchlist(path:str):
    syms=[]
    with open(path, encoding="utf-8") as f:
        for line in f:
            s=line.strip()
            if not s or s.startswith("#"): continue
            # watchlist darf „SAP.DE“ etc enthalten
            syms.append(s.split(",")[0].strip())
    return list(dict.fromkeys(syms))

def fget(session:requests.Session, url:str, params:dict, key:str, sleep_ms:int)->Optional[dict]:
    if not key: return None
    params = dict(params or {})
    params["token"] = key
    r = session.get(url, params=params, timeout=30)
    if sleep_ms: time.sleep(sleep_ms/1000)
    if r.status_code!=200: return None
    try: return r.json()
    except: return None

def from_finnhub(sym:str, key:str, sleep_ms:int)->Dict[str,Any]:
    """Try Finnhub profile2 + metric=all"""
    out = {h:"" for h in HEADERS}
    out["symbol"] = sym
    if not key: return out

    with requests.Session() as s:
        prof = fget(s, f"{FINNHUB_BASE}/stock/profile2", {"symbol": sym}, key, sleep_ms)
        metr = fget(s, f"{FINNHUB_BASE}/stock/metric",   {"symbol": sym, "metric":"all"}, key, sleep_ms)

    if not prof and not metr:
        return out

    # Profile2
    if prof:
        out["market_cap"]  = prof.get("marketCapitalization") or ""
        out["beta"]        = prof.get("beta") or ""
        out["shares_out"]  = prof.get("shareOutStanding") or prof.get("shareOutstanding") or ""

    # Metrics (ttm)
    m = (metr or {}).get("metric", {})
    out["pe_ttm"]       = m.get("peNormalizedAnnual") or m.get("peTTM") or ""
    out["ps_ttm"]       = m.get("psTTM") or ""
    out["pb_ttm"]       = m.get("pbAnnual") or m.get("pbTTM") or ""
    out["roe_ttm"]      = m.get("roeTTM") or m.get("roeAnnual") or ""
    out["gross_margin"] = m.get("grossMarginTTM") or m.get("grossMarginAnnual") or ""
    out["oper_margin"]  = m.get("operatingMarginTTM") or m.get("operatingMarginAnnual") or ""
    out["net_margin"]   = m.get("netProfitMarginTTM") or m.get("netProfitMarginAnnual") or ""
    out["debt_to_equity"]= m.get("totalDebtTotalEquityAnnual") or m.get("totalDebtTotalEquityTTM") or ""
    return out

def safe(d, *keys):
    cur=d
    for k in keys:
        if not isinstance(cur, dict): return None
        cur=cur.get(k)
    return cur

def from_yahoo(sym:str)->Dict[str,Any]:
    """Fallback via yfinance (funktioniert auch für .DE)"""
    out = {h:"" for h in HEADERS}
    out["symbol"] = sym
    try:
        t = yf.Ticker(sym)
        info = t.info or {}
        # Kernzahlen
        out["market_cap"]  = info.get("marketCap") or ""
        out["beta"]        = info.get("beta") or info.get("beta3Year") or ""
        out["shares_out"]  = info.get("sharesOutstanding") or ""
        out["pe_ttm"]      = info.get("trailingPE") or ""
        out["ps_ttm"]      = info.get("priceToSalesTrailing12Months") or ""
        out["pb_ttm"]      = info.get("priceToBook") or ""
        # Margins/ROE (falls vorhanden)
        out["roe_ttm"]     = info.get("returnOnEquity") or ""
        out["gross_margin"]= info.get("grossMargins") or ""
        out["oper_margin"] = info.get("operatingMargins") or ""
        out["net_margin"]  = info.get("profitMargins") or ""
        out["debt_to_equity"]= info.get("debtToEquity") or ""
    except Exception:
        pass
    return out

def row_has_data(row:Dict[str,Any])->bool:
    return any(str(row.get(k,"")).strip() not in ("","nan","None") for k in HEADERS if k!="symbol")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--finnhub_key", default=os.getenv("FINNHUB_API_KEY",""))
    ap.add_argument("--sleep_ms", type=int, default=int(os.getenv("FINNHUB_SLEEP_MS","0")))
    args = ap.parse_args()

    os.makedirs(os.path.dirname(OUT_PROCESSED), exist_ok=True)
    os.makedirs(os.path.dirname(PREVIEW_TXT), exist_ok=True)

    symbols = read_watchlist(args.watchlist)
    rows, missing, preview = [], [], []

    for s in symbols:
        # 1) Finnhub
        r = from_finnhub(s, args.finnhub_key, args.sleep_ms)
        # 2) Yahoo Fallback falls leer
        if not row_has_data(r):
            r = from_yahoo(s)
        if row_has_data(r):
            rows.append(r)
            # kleine Vorschau nur für US MegaCaps, sonst zu lang
            if s in ("AAPL","MSFT","AMZN","META","NVDA","GOOGL"):
                preview.append(r)
        else:
            missing.append(s)

    # schreiben
    with open(OUT_PROCESSED, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        for r in rows: w.writerow(r)

    # Reports
    if preview:
        with open(PREVIEW_TXT, "w", encoding="utf-8") as f:
            f.write(",".join(HEADERS) + "\n")
            for r in preview:
                f.write(",".join(str(r.get(h,"")) for h in HEADERS) + "\n")
    with open(MISSING_TXT, "w", encoding="utf-8") as f:
        for s in missing: f.write(s+"\n")

    print(f"fundamentals_core.csv rows: {len(rows)}")
    if missing:
        print("Missing:", len(missing))

if __name__ == "__main__":
    main()
