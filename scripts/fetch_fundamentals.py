# scripts/fetch_fundamentals.py
import os, sys, csv, time, requests, argparse
from util import read_yaml, write_json, load_env
from cache import RateLimiter, get_json, set_json

CONF = read_yaml('config/config.yaml') or {}
ENV  = load_env() or {}
FINN = "https://finnhub.io/api/v1"

def read_watchlist(path):
    if path.lower().endswith(".csv"):
        import csv
        syms=[]
        with open(path, newline="", encoding="utf-8") as f:
            r=csv.reader(f); head=next(r,[])
            col = 0
            if head and any(h.lower()=="symbol" for h in head):
                col = [h.lower() for h in head].index("symbol")
            else:
                syms.append(head[0]) if head else None
            for row in r:
                if row and row[col]: syms.append(row[col].strip().upper())
        return sorted(set(syms))
    return [l.strip().upper() for l in open(path,encoding="utf-8") if l.strip() and not l.startswith("#")]

def cached_get(url, params, key, rl, retries=2):
    c=get_json(key)
    if c: return c
    for a in range(retries):
        rl.wait()
        r=requests.get(url, params=params, timeout=20)
        if r.status_code==429 and a+1<retries: time.sleep(2.0); continue
        r.raise_for_status()
        js=r.json() or {}
        set_json(key, js, ttl_days=int(CONF.get("finnhub",{}).get("cache_ttl_days",7)))
        return js
    return {}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="data/fundamentals")
    args=ap.parse_args()

    token = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")
    if not token:
        print("No FINNHUB token"); return 0

    rl_cfg = CONF.get("rate_limits",{}) or {}
    rl = RateLimiter(int(rl_cfg.get("finnhub_per_minute",50)),
                     int(rl_cfg.get("finnhub_sleep_ms",1200)))

    syms = read_watchlist(args.watchlist)
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    out_rows=[]
    for sym in syms:
        try:
            prof = cached_get(f"{FINN}/stock/profile2", {"symbol":sym,"token":token}, f"profile2:{sym}", rl)
            metr = cached_get(f"{FINN}/stock/metric",  {"symbol":sym,"metric":"all","token":token}, f"metric:{sym}", rl)
            data = {"symbol":sym, "profile":prof, "metrics":metr.get("metric") or {}}
            write_json(os.path.join(args.outdir, f"{sym}.json"), data)

            # kompaktes Set für Scanner
            m = data["metrics"]
            row = {
                "symbol": sym,
                "market_cap": prof.get("marketCapitalization"),
                "beta": prof.get("beta"),
                "shares_out": prof.get("shareOutstanding"),
                "pe_ttm": m.get("peNormalizedAnnual") or m.get("peTTM"),
                "ps_ttm": m.get("psTTM"),
                "pb_ttm": m.get("pbAnnual") or m.get("pbTTM"),
                "roe_ttm": m.get("roeTTM"),
                "gross_margin": m.get("grossMarginTTM"),
                "oper_margin": m.get("operatingMarginTTM"),
                "net_margin":   m.get("netMarginTTM"),
                "debt_to_equity": m.get("totalDebt/totalEquityAnnual") or m.get("debtToEquityAnnual"),
            }
            out_rows.append(row)
        except Exception as e:
            print("fundamentals fail", sym, e)

    # Sammel-CSV
    out_csv="data/processed/fundamentals_core.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=list(out_rows[0].keys()) if out_rows else
                         ["symbol","market_cap","beta","shares_out","pe_ttm","ps_ttm","pb_ttm","roe_ttm",
                          "gross_margin","oper_margin","net_margin","debt_to_equity"])
        w.writeheader()
        for r in out_rows: w.writerow(r)
    print("fundamentals rows:", len(out_rows))
    return 0

if __name__=="__main__":
    sys.exit(main())
