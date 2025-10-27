# scripts/fetch_earnings_results.py
import os, sys, csv, time, requests, argparse
from util import read_yaml, write_json, load_env
from cache import RateLimiter, get_json, set_json

CONF = read_yaml('config/config.yaml') or {}
ENV  = load_env() or {}
FINN = "https://finnhub.io/api/v1"

def read_watchlist(path):
    # akzeptiert .txt (ein Symbol pro Zeile) oder .csv (Spalte 'symbol' oder erste Spalte)
    syms = []
    if path.lower().endswith(".csv"):
        import csv
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            header = next(r, [])
            col = 0
            if header and any(h.lower()=="symbol" for h in header):
                col = [h.lower() for h in header].index("symbol")
            else:
                # header ist vermutlich erstes Symbol -> wiederverwenden
                syms.append(header[0]) if header else None
            for row in r:
                if row and row[col]:
                    syms.append(row[col].strip().upper())
    else:
        with open(path, encoding="utf-8") as f:
            for line in f:
                s=line.strip().upper()
                if s and not s.startswith("#"): syms.append(s)
    return sorted(set(syms))

def fetch_earnings(symbol, token, rl:RateLimiter, retries=2):
    key = f"earnres:{symbol}"
    c = get_json(key)
    if c: return c
    params = {"symbol": symbol, "token": token, "limit": 40}
    for a in range(retries):
        rl.wait()
        r = requests.get(f"{FINN}/stock/earnings", params=params, timeout=20)
        if r.status_code==429 and a+1<retries:
            time.sleep(2.0); continue
        r.raise_for_status()
        js = r.json() or []
        set_json(key, js, ttl_days=int(CONF.get("earnings",{}).get("cache_ttl_days",7)))
        return js
    return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="data/earnings/results")
    args = ap.parse_args()

    token = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")
    if not token:
        print("No FINNHUB_TOKEN / FINNHUB_API_KEY"); return 0

    rl_cfg = CONF.get("rate_limits",{}) or {}
    per_min = int(rl_cfg.get("finnhub_per_minute",50))
    sleep_ms= int(rl_cfg.get("finnhub_sleep_ms",1200))
    rl = RateLimiter(per_min, sleep_ms)

    syms = read_watchlist(args.watchlist)
    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)

    rows_csv = []
    for i,sym in enumerate(syms,1):
        try:
            er = fetch_earnings(sym, token, rl)
            write_json(os.path.join(args.outdir, f"{sym}.json"), er)
            # flache CSV-Zeilen (letzte 4 Quartale z.B.)
            for rec in er[:8]:   # begrenzen, damit CSV nicht explodiert
                rows_csv.append({
                    "symbol": sym,
                    "period": rec.get("period") or rec.get("quarter") or "",
                    "date":   rec.get("date") or rec.get("epsReportDate") or "",
                    "eps_act": rec.get("epsActual"),
                    "eps_est": rec.get("epsEstimate"),
                    "surprise_pct": rec.get("surprisePercent"),
                    "rev_act": rec.get("revenueActual"),
                    "rev_est": rec.get("revenueEstimate"),
                })
        except Exception as e:
            print("earn-res fail", sym, e)

    # Sammel-CSV
    out_csv = "data/processed/earnings_results.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_csv[0].keys()) if rows_csv else
                           ["symbol","period","date","eps_act","eps_est","surprise_pct","rev_act","rev_est"])
        w.writeheader()
        for r in rows_csv: w.writerow(r)
    print("earnings results rows:", len(rows_csv))
    return 0

if __name__=="__main__":
    sys.exit(main())
