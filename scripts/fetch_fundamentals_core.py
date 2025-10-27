# scripts/fetch_fundamentals_core.py
import os, sys, csv, time, json, argparse, requests
from pathlib import Path

def read_symbols(path):
    p = Path(path)
    if p.suffix.lower() == ".csv":
        import csv as _csv
        with p.open("r", encoding="utf-8", newline="") as f:
            r = _csv.DictReader(f)
            col = None
            # erste sinnvolle Spalte finden
            for c in r.fieldnames or []:
                if c.lower() in ("symbol","ticker","ric"):
                    col = c; break
            if col is None:
                col = (r.fieldnames or ["symbol"])[0]
            return [row[col].strip() for row in r if row.get(col, "").strip()]
    else:
        return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip() and not ln.startswith("#")]

def get_token():
    return (os.getenv("FINNHUB_TOKEN") or
            os.getenv("FINNHUB_API_KEY") or
            os.environ.get("FINNHUB_TOKEN") or
            os.environ.get("FINNHUB_API_KEY"))

def fetch_metric(symbol, token):
    # Finnhub "all" Metriken
    url = "https://finnhub.io/api/v1/stock/metric"
    r = requests.get(url, params={"symbol": symbol, "metric": "all", "token": token}, timeout=30)
    if r.status_code == 429:
        # harte Rate-Limit Kante – kurz warten und nochmal
        time.sleep(2.5)
        r = requests.get(url, params={"symbol": symbol, "metric": "all", "token": token}, timeout=30)
    r.raise_for_status()
    j = r.json() or {}
    return (j.get("metric") or {}, j.get("metricType") or "")

def pick(m, key, alt=None):
    for k in (key, alt) if alt else (key,):
        if k and k in m and m[k] is not None:
            return m[k]
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="Pfad zu .txt oder .csv (erste Spalte = Symbol)")
    ap.add_argument("--out", default="data/processed/fundamentals_core.csv")
    ap.add_argument("--sleep-ms", type=int, default=int(os.getenv("FINNHUB_SLEEP_MS", "1300")))
    args = ap.parse_args()

    token = get_token()
    if not token:
        print("ERROR: Kein FINNHUB_TOKEN/FINNHUB_API_KEY gesetzt.", file=sys.stderr)
        return 1

    symbols = read_symbols(args.watchlist)
    symbols = [s for s in symbols if s and s.upper() == s.upper()]  # trivialer Clean
    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "symbol","market_cap","beta","shares_out",
        "pe_ttm","ps_ttm","pb_ttm","roe_ttm",
        "gross_margin","oper_margin","net_margin","debt_to_equity"
    ]

    written = 0
    with outp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for i, sym in enumerate(symbols, 1):
            try:
                m, _ = fetch_metric(sym, token)
            except Exception as e:
                # still & continue
                print(f"skip {sym}: {e}")
                continue

            row = {
                "symbol":       sym,
                "market_cap":   pick(m, "marketCapitalization"),
                "beta":         pick(m, "beta"),
                "shares_out":   pick(m, "shareOutstanding"),
                "pe_ttm":       pick(m, "peNormalizedAnnual", "peTTM"),
                "ps_ttm":       pick(m, "psRatioTTM", "priceToSalesTTM"),
                "pb_ttm":       pick(m, "pbAnnual", "priceToBookAnnual"),
                "roe_ttm":      pick(m, "roeTTM"),
                "gross_margin": pick(m, "grossMarginTTM"),
                "oper_margin":  pick(m, "operatingMarginTTM"),
                "net_margin":   pick(m, "netProfitMarginTTM"),
                "debt_to_equity": pick(m, "totalDebt/totalEquityAnnual", "debtToEquity")
            }
            # wenn fast alles None -> ETF/FX/kein Treffer; auslassen
            if sum(v is not None for v in row.values()) <= 2:
                continue

            w.writerow(row); written += 1
            time.sleep(max(0.0, args.sleep_ms/1000.0))

    print(f"fundamentals_core rows: {written}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
