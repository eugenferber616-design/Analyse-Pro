import os, csv, requests, sys, time
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"
ENV = load_env()
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

def read_list(path):
    with open(path, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.lower().startswith("symbol")]

def get_json(url, params, rl):
    rl.wait()
    r = requests.get(url, params=params, timeout=25)
    if r.status_code == 429:
        time.sleep(2.0)
        r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json() or {}

def main(wl, outcsv):
    if not TOKEN:
        print("No FINNHUB token"); return 0

    syms = read_list(wl)
    os.makedirs(os.path.dirname(outcsv), exist_ok=True)
    fieldnames = ["symbol","name","category","asset_class","expense_ratio","aum","nav","beta","currency"]

    rl = RateLimiter(50, 1300)
    rows = []
    for s in syms:
        try:
            # 1) Primär: /etf/profile
            prof = get_json(f"{API}/etf/profile", {"symbol": s, "token": TOKEN}, rl)
            # 2) Fallback Name/Currency: /stock/profile2
            prof2 = {}
            if not prof.get("name") or not prof.get("currency"):
                prof2 = get_json(f"{API}/stock/profile2", {"symbol": s, "token": TOKEN}, rl)
            # 3) Fallback Kennzahlen: /stock/metric?metric=all
            metr = get_json(f"{API}/stock/metric", {"symbol": s, "metric": "all", "token": TOKEN}, rl)
            m = metr.get("metric") or {}

            rows.append({
                "symbol": s,
                "name": prof.get("name") or prof2.get("name"),
                "category": prof.get("category"),
                "asset_class": prof.get("assetClass"),
                "expense_ratio": prof.get("expenseRatio"),
                "aum": prof.get("totalAssets"),
                "nav": prof.get("nav"),
                "beta": m.get("beta") or prof.get("beta"),
                "currency": prof.get("currency") or prof2.get("currency"),
            })
        except Exception as e:
            print("etf fail", s, e)

    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print("wrote", outcsv, len(rows))
    return 0

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/etf_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/etf_basics.csv"
    sys.exit(main(wl, outcsv))
