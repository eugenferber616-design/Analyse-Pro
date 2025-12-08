# scripts/fetch_etf_basics.py
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
    return sorted(set(rows))

def clean_num(x):
    try:
        if x in (None, "", "None"): return None
        return float(x)
    except Exception:
        return None

def safe_json(resp: requests.Response):
    """Versucht JSON zu parsen; gibt (dict|list|None, info_dict) zurück."""
    info = {
        "status": resp.status_code,
        "ctype": resp.headers.get("Content-Type",""),
        "len": len(resp.content or b""),
    }
    if not resp.content:
        return None, info
    # manche Gate-Seiten liefern text/html
    if "application/json" not in info["ctype"].lower():
        return None, info
    try:
        return resp.json(), info
    except Exception:
        return None, info

def get_finnhub_etf_profile(session, symbol: str):
    """Primär: Finnhub /etf/profile2. Gibt (dict|None, meta) zurück."""
    if not TOKEN:
        return None, {"status": None, "reason": "no_token"}
    url = f"{API}/etf/profile2"
    params = {"symbol": symbol, "token": TOKEN}
    r = session.get(url, params=params, timeout=20)
    if r.status_code == 429:
        time.sleep(2.5)
        r = session.get(url, params=params, timeout=20)

    # JSON sicher parsen, ohne Exception
    payload, meta = safe_json(r)
    meta["url"] = "/etf/profile2"
    meta["symbol"] = symbol

    if r.status_code >= 400:
        meta["reason"] = f"http_{r.status_code}"
        return None, meta
    if not payload or not isinstance(payload, dict):
        meta["reason"] = "empty_or_non_json"
        return None, meta

    out = {
        "name"         : payload.get("name"),
        "category"     : payload.get("category"),
        "asset_class"  : payload.get("assetClass"),
        "expense_ratio": clean_num(payload.get("expenseRatio")),
        "aum"          : clean_num(payload.get("aum")),
        "nav"          : clean_num(payload.get("nav")),
        "beta"         : clean_num(payload.get("beta")),
        "currency"     : payload.get("currency"),
    }
    if all(v in (None, "", 0) for v in out.values()):
        meta["reason"] = "no_fields"
        return None, meta
    meta["reason"] = "ok"
    return out, meta

def get_yf_basics(symbol: str):
    """Fallback via yfinance. Gibt dict (evtl. leer) zurück."""
    try:
        import yfinance as yf
    except Exception:
        return {}
    try:
        t = yf.Ticker(symbol)
        info = getattr(t, "info", {}) or {}
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
        # minimaler Zusatz über fast_info (nur Currency, falls leer)
        fi = getattr(t, "fast_info", None)
        if fi and not out.get("currency"):
            out["currency"] = getattr(fi, "currency", None)
        return {k:v for k,v in out.items() if v not in (None, "", 0)}
    except Exception:
        return {}

def merge_basics(sym: str, fin: dict | None, yfi: dict | None):
    """Finnhub hat Priorität; fehlende Felder aus yfinance ergänzen."""
    out = {"symbol": sym}
    fin = fin or {}; yfi = yfi or {}
    for k in FIELDS:
        if k == "symbol": 
            continue
        va = fin.get(k)
        vb = yfi.get(k)
        out[k] = va if va not in (None, "", 0) else vb
    return out

def has_any_core(rec: dict):
    """Mindestens ein sinnvolles Feld vorhanden?"""
    return any(rec.get(k) not in (None, "", 0) for k in ["name","expense_ratio","aum","nav","beta","currency"])

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
        fin, meta = None, {"symbol": sym, "reason": "skipped"}
        try:
            fin, meta = get_finnhub_etf_profile(session, sym)
        except Exception as e:
            meta = {"symbol": sym, "reason": "exception_finnhub", "msg": str(e)}

        yfi = {}
        try:
            # yfinance immer versuchen – ergänzt auch bei finnhub "ok" fehlende Felder
            yfi = get_yf_basics(sym)
        except Exception as e:
            errs["errors"].append({"symbol": sym, "reason": "exception_yfinance", "msg": str(e)})

        rec = merge_basics(sym, fin, yfi)

        if has_any_core(rec):
            rows.append({k: rec.get(k) for k in FIELDS})
            errs["ok"] += 1
        else:
            errs["failed"] += 1
            # kompaktes Log: Finnhub-Meta + Hinweise, ob yfinance etwas hatte
            errs["errors"].append({
                "symbol": sym,
                "finnhub": meta,
                "yfinance_has": bool(yfi),
                "reason": meta.get("reason","no_data")
            })

    # schreiben
    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    with open(errors_path, "w", encoding="utf-8") as f:
        json.dump(errs, f, ensure_ascii=False, indent=2)

    print(f"wrote {outcsv} rows={len(rows)} / total={len(symbols)}  (errors: {errs['failed']})")
    return 0

if __name__ == "__main__":
    wl  = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/etf_sample.txt"
    out = sys.argv[sys.argv.index("--out")+1]       if "--out" in sys.argv       else "data/processed/etf_basics.csv"
    err = sys.argv[sys.argv.index("--errors")+1]    if "--errors" in sys.argv    else "data/reports/etf_errors.json"
    sys.exit(main(wl, out, err))
