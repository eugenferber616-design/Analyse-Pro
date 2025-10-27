# scripts/fetch_earnings_results.py
import os, csv, argparse, requests
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"

def read_watchlist(path):
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip().split(",")[0]
            if t and t.lower() != "symbol": syms.append(t.upper())
    return list(dict.fromkeys(syms))

def safe(v):
    return "" if v is None else v

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--watchlist", required=True)
    p.add_argument("--outdir",     default="data/earnings/results")
    p.add_argument("--limit", type=int, default=8)         # << Historie
    args = p.parse_args()

    env  = load_env()
    token = env.get("FINNHUB_TOKEN") or env.get("FINNHUB_API_KEY")
    if not token:
        print("No FINNHUB token"); return 0

    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    out_rows = []
    syms = read_watchlist(args.watchlist)

    rl = RateLimiter(50, 1200)  # defensiv

    for sym in syms:
        rl.wait()
        r = requests.get(f"{API}/stock/earnings",
                         params={"symbol": sym, "limit": args.limit, "token": token},
                         timeout=30)
        if r.status_code == 429:
            rl.wait(); r = requests.get(f"{API}/stock/earnings",
                                        params={"symbol": sym, "limit": args.limit, "token": token},
                                        timeout=30)
        r.raise_for_status()
        data = r.json() or []
        for d in data:
            out_rows.append({
                "symbol": sym,
                "period":          safe(d.get("period")),
                "eps_actual":      safe(d.get("actual")),
                "eps_estimate":    safe(d.get("estimate")),
                "surprise":        safe(d.get("surprise")),
                "surprise_pct":    safe(d.get("surprisePercent")),
                "revenue":         safe(d.get("revenue")),
                "revenue_estimate":safe(d.get("revenueEstimate")),
            })

    # Schreiben
    out_csv = "data/processed/earnings_results.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "symbol","period","eps_actual","eps_estimate","surprise","surprise_pct",
            "revenue","revenue_estimate"
        ])
        w.writeheader()
        w.writerows(out_rows)

    print(f"Wrote {len(out_rows)} rows -> {out_csv}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
