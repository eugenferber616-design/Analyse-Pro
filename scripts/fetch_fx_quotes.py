import os, csv, requests, sys, time
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"
ENV = load_env()
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

def read_list(path):
    with open(path, "r", encoding="utf-8") as f:
        rows = [l.strip() for l in f if l.strip() and not l.lower().startswith("symbol")]
    return rows

# akzeptiere Formate wie EURUSD, EUR/USD, eurusd, OANDA:EUR_USD -> normiert auf OANDA:EUR_USD
def normalize_fx(sym: str, provider: str = "OANDA") -> str:
    s = sym.strip().upper().replace(":", "").replace("-", "").replace(" ", "")
    if "/" in s:
        base, quote = s.split("/", 1)
        return f"{provider}:{base}_{quote}"
    if "_" in s:
        base, quote = s.split("_", 1)
        return f"{provider}:{base}_{quote}"
    # 6-stellig ohne Trenner
    if len(s) == 6:
        return f"{provider}:{s[:3]}_{s[3:]}"
    # bereits mit Provider?
    if sym.upper().startswith(f"{provider}:"):
        return sym.upper()
    # Fallback: unverändert
    return sym

def main(wl, outcsv):
    if not TOKEN:
        print("No FINNHUB token"); return 0
    pairs_raw = read_list(wl)
    pairs = [normalize_fx(p) for p in pairs_raw]

    os.makedirs(os.path.dirname(outcsv), exist_ok=True)
    fieldnames = ["pair","bid","ask","last","timestamp"]
    rl = RateLimiter(50, 1300)
    rows = []

    for p in pairs:
        rl.wait()
        try:
            r = requests.get(f"{API}/forex/quote", params={"symbol": p, "token": TOKEN}, timeout=20)
            if r.status_code == 429:
                time.sleep(2.5)
                r = requests.get(f"{API}/forex/quote", params={"symbol": p, "token": TOKEN}, timeout=20)
            r.raise_for_status()
            j = r.json() or {}
            # Finnhub liefert u.a. a(ask), b(bid), c(last), t(timestamp)
            rows.append({
                "pair": p,
                "bid": j.get("b"),
                "ask": j.get("a"),
                "last": j.get("c"),
                "timestamp": j.get("t"),
            })
        except Exception as e:
            print("fx fail", p, e)

    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print("wrote", outcsv, len(rows))
    return 0

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/fx_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/fx_quotes.csv"
    sys.exit(main(wl, outcsv))
