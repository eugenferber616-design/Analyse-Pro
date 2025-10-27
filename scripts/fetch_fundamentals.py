# scripts/fetch_fundamentals.py
import os, sys, csv, time, json, argparse, requests
from pathlib import Path
from util import read_yaml, load_env
from cache import RateLimiter

CONF = read_yaml('config/config.yaml') or {}
ENV  = load_env() or {}
API  = "https://finnhub.io/api/v1"

# Rate-Limit Defaults
rl = CONF.get("rate_limits", {}) or {}
PER_MIN  = int(rl.get("finnhub_per_minute", 50))
SLEEP_MS = int(rl.get("finnhub_sleep_ms", max(1200, 60000 // max(1, PER_MIN))))
lim = RateLimiter(PER_MIN, SLEEP_MS)

# --- helpers
def read_watchlist(path: Path):
    syms = []
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as f:
            sniffer = csv.Sniffer()
            sample = f.read(1024); f.seek(0)
            dialect = sniffer.sniff(sample) if sample else csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            # akzeptiere Varianten von Kopfzeilen
            cols = {c.lower(): c for c in reader.fieldnames or []}
            key = cols.get("symbol") or cols.get("ticker") or list(cols.values())[0]
            for row in reader:
                s = (row.get(key) or "").strip()
                if s:
                    syms.append(s)
    else:
        # .txt: eine Zeile pro Symbol; Kopfzeile optional
        with path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                s = line.strip()
                if not s: 
                    continue
                if i == 0 and s.lower() in ("symbol", "ticker"):
                    continue
                syms.append(s)
    # dedup & sort
    out = []
    seen = set()
    for s in syms:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def get_json(url, params, token, retries=3):
    p = dict(params or {})
    p["token"] = token
    for a in range(retries):
        lim.wait()
        r = requests.get(url, params=p, timeout=30)
        if r.status_code == 429 and a+1 < retries:
            time.sleep(2.5); continue
        r.raise_for_status()
        return r.json() or {}
    return {}

def fetch_one(sym, token):
    prof = get_json(f"{API}/stock/profile2", {"symbol": sym}, token)
    # Finnhub liefert für ETFs/FX oft leere profile2 => dann abbrechen
    if not prof or not prof.get("ticker"):
        return None, "no_profile"
    met  = get_json(f"{API}/stock/metric", {"symbol": sym, "metric": "all"}, token)
    metrics = (met.get("metric") or {}) if isinstance(met, dict) else {}
    row = {
        "symbol": prof.get("ticker") or sym,
        "market_cap": prof.get("marketCapitalization"),
        "beta": metrics.get("beta"),
        "shares_out": metrics.get("shareOutstanding") or prof.get("shareOutstanding"),
        "pe_ttm": metrics.get("peBasicExclExtraTTM") or metrics.get("peTTM"),
        "ps_ttm": metrics.get("psTTM"),
        "pb_ttm": metrics.get("pbAnnual") or metrics.get("pbQuarterly"),
        "roe_ttm": metrics.get("roeTTM"),
        "gross_margin": metrics.get("grossMarginTTM"),
        "oper_margin": metrics.get("operatingMarginTTM"),
        "net_margin": metrics.get("netProfitMarginTTM"),
        "debt_to_equity": metrics.get("totalDebt/totalEquityAnnual")
                           or metrics.get("totalDebt/totalEquityQuarterly")
    }
    # mindestens symbol + ein Zahlenfeld
    if any(v not in (None, "", "NA") for k, v in row.items() if k != "symbol"):
        return row, None
    return None, "no_metrics"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="data/fundamentals")
    args = ap.parse_args()

    token = (ENV.get("FINNHUB_TOKEN") or
             ENV.get("FINNHUB_API_KEY") or
             os.getenv("FINNHUB_TOKEN") or
             os.getenv("FINNHUB_API_KEY"))
    if not token:
        print("No FINNHUB token")
        return 0

    wl = Path(args.watchlist)
    syms = read_watchlist(wl)
    print(f"fundamentals: {len(syms)} symbols from {wl}")

    os.makedirs(args.outdir, exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    rows, errs = [], []
    for i, s in enumerate(syms, 1):
        try:
            row, err = fetch_one(s, token)
            if row:
                rows.append(row)
            else:
                errs.append({"symbol": s, "reason": err or "unknown"})
        except Exception as e:
            errs.append({"symbol": s, "reason": str(e)})
        if i % 25 == 0:
            print(f"... {i}/{len(syms)}")

    # schreiben
    header = ["symbol","market_cap","beta","shares_out","pe_ttm","ps_ttm","pb_ttm",
              "roe_ttm","gross_margin","oper_margin","net_margin","debt_to_equity"]
    out_csv = Path("data/processed/fundamentals_core.csv")
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in header})
    with Path("data/reports/fund_errors.json").open("w", encoding="utf-8") as f:
        json.dump({"total": len(syms), "ok": len(rows), "failed": len(errs), "errors": errs}, f, ensure_ascii=False, indent=2)

    print(f"wrote {len(rows)} rows to {out_csv}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
