# scripts/fetch_earnings_results.py
import os, sys, csv, json, time, argparse, requests
from pathlib import Path
from typing import List, Dict, Tuple

from util import read_yaml, load_env
from cache import RateLimiter

CONF = read_yaml('config/config.yaml') or {}
ENV  = load_env() or {}

API  = "https://finnhub.io/api/v1"

# ---- Rate-Limit Default
rl = CONF.get("rate_limits", {}) or {}
PER_MIN  = int(rl.get("finnhub_per_minute", 50))
SLEEP_MS = int(rl.get("finnhub_sleep_ms", max(1200, 60000 // max(1, PER_MIN))))
lim = RateLimiter(PER_MIN, SLEEP_MS)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def read_watchlist(path: Path) -> List[str]:
    """CSV (mit Kopf 'symbol'|'ticker' o. erste Spalte) oder TXT (Zeile pro Symbol).
       Entdoppelt + getrimmt."""
    syms: List[str] = []
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as f:
            sample = f.read(1024); f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample) if sample else csv.excel
            except Exception:
                dialect = csv.excel
            reader = csv.DictReader(f, dialect=dialect)
            cols = {c.lower(): c for c in (reader.fieldnames or [])}
            key = cols.get("symbol") or cols.get("ticker") or (list(cols.values())[0] if cols else None)
            if not key:
                return syms
            for row in reader:
                s = (row.get(key) or "").strip()
                if s:
                    syms.append(s)
    else:
        with path.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                s = line.strip()
                if not s:
                    continue
                # mögliche Kopfzeile ignorieren
                if i == 0 and s.lower() in ("symbol", "ticker"):
                    continue
                syms.append(s)
    out, seen = [], set()
    for s in syms:
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def get_json(url: str, params: Dict, token: str, retries: int = 3) -> Dict:
    p = dict(params or {})
    p["token"] = token
    for att in range(retries):
        lim.wait()
        r = requests.get(url, params=p, timeout=30)
        if r.status_code == 429 and att + 1 < retries:
            time.sleep(2.5); continue
        r.raise_for_status()
        try:
            return r.json() or {}
        except Exception:
            return {}
    return {}

# ---------------------------------------------------------------------
# Finnhub earnings results
#   Endpoint: /stock/earnings?symbol=SYM [&limit=N]
#   Typische Felder:
#     period, epsActual, epsEstimate, surprise, surprisePercent,
#     revenue, revenueEstimate (manchmal revenueEstimated)
# ---------------------------------------------------------------------
def fetch_one_symbol(sym: str, token: str, limit: int = 16) -> Tuple[List[Dict], str]:
    j = get_json(f"{API}/stock/earnings", {"symbol": sym, "limit": limit}, token)
    arr = j if isinstance(j, list) else j.get("earnings", [])
    if not isinstance(arr, list):
        return [], "no_list"

    rows = []
    for it in arr:
        period  = (it.get("period") or it.get("date") or "")[:10]
        eps_a   = it.get("epsActual")
        eps_e   = it.get("epsEstimate")
        surpr   = it.get("surprise")
        surprp  = it.get("surprisePercent") or it.get("surprisePercentage")
        rev_a   = it.get("revenue")
        rev_e   = it.get("revenueEstimate") or it.get("revenueEstimated")

        # Nur Zeilen, die wenigstens ein Nutzfeld enthalten:
        if not (eps_a is not None or eps_e is not None or rev_a is not None or rev_e is not None):
            continue

        rows.append({
            "symbol": sym,
            "period": period,
            "eps_actual": eps_a,
            "eps_estimate": eps_e,
            "surprise": surpr,
            "surprise_pct": surprp,
            "revenue": rev_a,
            "revenue_estimate": rev_e
        })

    if not rows:
        return [], "empty"
    return rows, ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--outdir", default="data/earnings/results")
    ap.add_argument("--limit", type=int, default=16, help="max. Quartale je Symbol (default 16)")
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
    print(f"earnings-results: {len(syms)} symbols from {wl}")

    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("data/reports").mkdir(parents=True, exist_ok=True)

    all_rows: List[Dict] = []
    errors: List[Dict]   = []

    for i, s in enumerate(syms, 1):
        try:
            rows, err = fetch_one_symbol(s, token, args.limit)
            if rows:
                # pro Symbol JSON (optional)
                with Path(args.outdir, f"{s}.json").open("w", encoding="utf-8") as f:
                    json.dump(rows, f, ensure_ascii=False, indent=2)
                all_rows.extend(rows)
            else:
                errors.append({"symbol": s, "reason": err or "no_data"})
        except Exception as e:
            errors.append({"symbol": s, "reason": str(e)})

        if i % 25 == 0:
            print(f"... {i}/{len(syms)} processed")

    # CSV schreiben (sortiert: symbol, period absteigend)
    all_rows.sort(key=lambda r: (r["symbol"], r.get("period", "")), reverse=False)
    out_csv = Path("data/processed/earnings_results.csv")
    header = ["symbol","period","eps_actual","eps_estimate","surprise","surprise_pct",
              "revenue","revenue_estimate"]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in all_rows:
            w.writerow({k: r.get(k) for k in header})

    rep = {
        "total_symbols": len(syms),
        "symbols_ok": len({r["symbol"] for r in all_rows}),
        "rows_written": len(all_rows),
        "errors": errors,
    }
    with Path("data/reports/earn_errors.json").open("w", encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)

    print(f"wrote {len(all_rows)} rows to {out_csv}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
