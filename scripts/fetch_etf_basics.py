import os, csv, requests, sys, time
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"
ENV = load_env()
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

def read_list(path):
    with open(path, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.lower().startswith("symbol")]

def main(wl, outcsv):
    if not TOKEN:
        print("No FINNHUB token"); return 0
    syms = read_list(wl)
    os.makedirs(os.path.dirname(outcsv), exist_ok=True)

    fieldnames = ["symbol","name","category","asset_class","expense_ratio","aum","nav","beta","currency"]
    rl = RateLimiter(50, 1300)
    rows = []
    for s in syms:
        rl.wait()
        try:
            r = requests.get(f"{API}/etf/profile", params={"symbol": s, "token": TOKEN}, timeout=30)
            if r.status_code == 429:
                time.sleep(2); r = requests.get(f"{API}/etf/profile", params={"symbol": s, "token": TOKEN}, timeout=30)
            r.raise_for_status()
            j = r.json() or {}
            rows.append({
                "symbol": s,
                "name": j.get("name"),
                "category": j.get("category"),
                "asset_class": j.get("assetClass"),
                "expense_ratio": j.get("expenseRatio"),
                "aum": j.get("totalAssets"),
                "nav": j.get("nav"),
                "beta": j.get("beta"),
                "currency": j.get("currency")
            })
        except Exception as e:
            print("etf fail", s, e)
    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader(); w.writerows(rows)
    print("wrote", outcsv, len(rows)); return 0

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/etf_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/etf_basics.csv"
    sys.exit(main(wl, outcsv))
