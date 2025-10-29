# scripts/fetch_prices_simple.py
# Lädt Daily-Prices (close) über Finnhub für eine Watchlist und speichert CSVs unter data/prices/{SYMBOL}.csv
# Eingaben: --watchlist DATEI (eine Zeile pro Symbol), --days N
import os, sys, time, json, argparse
from datetime import datetime, timedelta
import urllib.request, urllib.parse
import csv

def finnhub_get_candles(symbol, fr, to, token, sleep_ms=1200):
    base = "https://finnhub.io/api/v1/stock/candle"
    qs = urllib.parse.urlencode({"symbol": symbol, "resolution": "D", "from": fr, "to": to, "token": token})
    url = f"{base}?{qs}"
    with urllib.request.urlopen(url, timeout=30) as r:
        data = json.loads(r.read().decode("utf-8"))
    # rate-limit höflich
    time.sleep(sleep_ms/1000.0)
    return data

def write_csv(out_path, ts, c):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","close"])
        for t, close in zip(ts, c):
            dt = datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
            w.writerow([dt, close])

def load_watchlist(path):
    if not os.path.exists(path):
        return []
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.lower().startswith(("symbol", "#", "//")): continue
            # erlaubt CSV mit Spalte 'symbol' oder reine Textliste
            if "," in s:
                parts = [p.strip() for p in s.split(",")]
                # heuristik: erste Spalte = symbol
                if parts: syms.append(parts[0])
            else:
                syms.append(s)
    # de-dupe, Reihenfolge stabil
    out, seen = [], set()
    for s in syms:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--sleep-ms", type=int, default=int(os.getenv("FINNHUB_SLEEP_MS","1200")))
    ap.add_argument("--outdir", default="data/prices")
    ap.add_argument("--token", default=os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY"))
    args = ap.parse_args()

    if not args.token:
        print("ERR: FINNHUB_TOKEN/FINNHUB_API_KEY fehlt.", file=sys.stderr); sys.exit(1)

    syms = load_watchlist(args.watchlist)
    if not syms:
        print(f"WARN: Watchlist {args.watchlist} leer?", file=sys.stderr)

    to_ts   = int(time.time())
    from_ts = int((datetime.utcnow()-timedelta(days=args.days+5)).timestamp())

    os.makedirs(args.outdir, exist_ok=True)
    rep = {"ok":[], "err":[]}

    for sym in syms:
        try:
            data = finnhub_get_candles(sym, from_ts, to_ts, args.token, args.sleep_ms)
            if data.get("s") != "ok" or not data.get("t") or not data.get("c"):
                raise RuntimeError(f"bad status for {sym}: {data.get('s')}")
            outp = os.path.join(args.outdir, f"{sym}.csv")
            write_csv(outp, data["t"], data["c"])
            rep["ok"].append(sym)
            print(f"✔ {sym} → {outp} ({len(data['c'])} rows)")
        except Exception as e:
            rep["err"].append({"symbol": sym, "error": str(e)})
            print(f"✖ {sym}: {e}", file=sys.stderr)

    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/fetch_prices_report.json","w",encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    if rep["ok"]:
        print(f"Done. OK={len(rep['ok'])}, ERR={len(rep['err'])}")

if __name__ == "__main__":
    main()
