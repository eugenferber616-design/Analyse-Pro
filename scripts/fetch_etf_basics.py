import os, sys, csv, time, json, requests
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"
ENV = load_env()
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

FIELDS = ["symbol","name","category","asset_class","expense_ratio","aum","nav","beta","currency"]

def read_list(path: str):
    with open(path, "r", encoding="utf-8") as f:
        rows = [l.strip() for l in f if l.strip() and not l.lower().startswith("symbol")]
    # dedupe & sort, nur reine Tickers (SPY, QQQ, GLD, …)
    return sorted(set(rows))

def clean_num(x):
    try:
        if x in (None, "", "None"): return None
        return float(x)
    except Exception:
        return None

def get_finnhub_etf_profile(session, symbol: str):
    """Primärquelle: Finnhub ETF profile2."""
    if not TOKEN:
        return {}
    url = f"{API}/etf/profile2"
    r = session.get(url, params={"symbol": symbol, "token": TOKEN}, timeout=20)
    if r.status_code == 429:
        time.sleep(2.5)
        r = session.get(url, params={"symbol": symbol, "token": TOKEN}, timeout=20)
    r.raise_for_status()
    j = r.json() or {}
    # typische Keys: name, category, assetClass, expenseRatio, nav, aum, beta, currency
    out = {}
    out["name"]         = j.get("name")
    out["category"]     = j.get("category")
    out["asset_class"]  = j.get("assetClass")
    out["expense_ratio"]= clean_num(j.get("expenseRatio"))
    out["aum"]          = clean_num(j.get("aum"))
    out["nav"]          = clean_num(j.get("nav"))
    out["beta"]         = clean_num(j.get("beta"))
    out["currency"]     = j.get("currency")
    # wenn absolut nichts Sinnvolles drin ist, leere dict zurück
    if all(v in (None, "", 0) for v in out.values()):
        return {}
    return out

def get_yf_basics(symbol: str):
    """Fallback über yfinance (Name, TER, AUM, NAV, Beta, Währung)."""
    try:
        import yfinance as yf
    except Exception:
        return {}  # yfinance nicht installiert
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, "info", {}) or {}
        # modern: .get_info() fällt bei manchen Versionen weg – info bleibt kompatibel genug
        out = {
            "name"         : info.get("longName") or info.get("shortName"),
            "category"     : info.get("category"),
            "asset_class"  : "ETF",
            "expense_ratio": clean_num(info.get("annualReportExpenseRatio")),
            "aum"          : clean_num(info.get("totalAssets")),
            "nav"          : clean_num(info.get("navPrice")),
            "beta"         : clean_num(info.get("beta")),
            "currency"     : info.get("currency"),
        }
        # wenn auch info leer: kleines Try über fast_info
        fi = getattr(t, "fast_info", None)
        if fi:
            out["currency"] = out.get("currency") or getattr(fi, "currency", None)
        # am Ende nur Felder behalten, die es wirklich gibt
        return {k:v for k,v in out.items() if v not in (None, "", 0)}
    except Exception:
        return {}

def merge_basics(sym: str, a: dict, b: dict):
    """a = Finnhub, b = yfinance → a hat Priorität; ergänze fehlende Felder aus b."""
    out = {"symbol": sym}
    for k in FIELDS:
        if k == "symbol": 
            continue
        va = a.get(k) if a else None
        vb = b.get(k) if b else None
        out[k] = va if va not in (None, "", 0) else vb
    return out

def main(watchlist: str, outcsv: str, errors_path: str = "data/reports/etf_errors.json"):
    os.makedirs(os.path.dirname(outcsv), exist_ok=True)
    os.makedirs(os.path.dirname(errors_path), exist_ok=True)

    symbols = read_list(watchlist)
    rl = RateLimiter(50, 1300)
    session = requests.Session()

    rows = []
    errs = {"total": len(symbols), "ok": 0, "failed": 0, "errors": []}

    for sym in symbols:
        rl.wait()
        try:
            fin = {}
            if TOKEN:
                fin = get_finnhub_etf_profile(session, sym)
            yf  = get_yf_basics(sym)
            rec = merge_basics(sym, fin, yf)

            # mindestens Name oder NAV/AUM/TER erwartet; sonst als Fehler markieren
            has_any = any(rec.get(k) not in (None, "", 0) for k in ["name","expense_ratio","aum","nav","beta"])
            if has_any:
                rows.append(rec)
                errs["ok"] += 1
            else:
                errs["failed"] += 1
                errs["errors"].append({"symbol": sym, "reason": "no_profile"})
        except Exception as e:
            errs["failed"] += 1
            errs["errors"].append({"symbol": sym, "reason": "exception", "msg": str(e)})

    # schreiben
    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            # nur gewünschte Felder (und Reihenfolge)
            w.writerow({k: r.get(k) for k in FIELDS})

    with open(errors_path, "w", encoding="utf-8") as f:
        json.dump(errs, f, ensure_ascii=False, indent=2)

    print(f"wrote {outcsv} rows={len(rows)} / total={len(symbols)}  (errors: {errs['failed']})")
    return 0

if __name__ == "__main__":
    wl  = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/etf_sample.txt"
    out = sys.argv[sys.argv.index("--out")+1]       if "--out" in sys.argv       else "data/processed/etf_basics.csv"
    err = sys.argv[sys.argv.index("--errors")+1]    if "--errors" in sys.argv    else "data/reports/etf_errors.json"
    sys.exit(main(wl, out, err))
