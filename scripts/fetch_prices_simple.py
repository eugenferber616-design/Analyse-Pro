# scripts/fetch_prices_simple.py  (nur die relevanten Ergänzungen/Änderungen)

def _canon_symbol(s: str) -> str:
    # schneidet Kommentare ab und nimmt nur das erste "Wort" als Ticker
    s = s.split("#", 1)[0].split("//", 1)[0].strip()
    if "," in s:                    # CSV-Zeile -> nimm 1. Spalte
        s = s.split(",", 1)[0].strip()
    # nimm nur das erste Token bis zum ersten Whitespace
    s = s.split()[0] if s else ""
    return s

def load_watchlist(path):
    if not os.path.exists(path):
        return []
    syms, seen = [], set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = _canon_symbol(line)
            if not t or t.lower() in ("symbol","ticker"): 
                continue
            if t not in seen:
                seen.add(t); syms.append(t)
    return syms

def _alpha_path(root: str, sym: str, use_alpha_buckets: bool) -> str:
    if not use_alpha_buckets:
        return os.path.join(root, f"{sym}.csv")
    first = sym[0].upper() if sym else "_"
    if not first.isalpha():
        first = "_"
    pdir = os.path.join(root, first)
    os.makedirs(pdir, exist_ok=True)
    return os.path.join(pdir, f"{sym}.csv")

def write_csv(out_path, ts, c):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","close"])
        for t, close in zip(ts, c):
            dt = datetime.utcfromtimestamp(t).strftime("%Y-%m-%d")
            w.writerow([dt, close])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--sleep-ms", type=int, default=int(os.getenv("FINNHUB_SLEEP_MS","1200")))
    ap.add_argument("--outdir", default="data/prices")
    ap.add_argument("--token", default=os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY"))
    ap.add_argument("--provider", default="auto", choices=["finnhub","yfinance","stooq","auto"])
    ap.add_argument("--alpha-buckets", action="store_true", help="A/B/… Unterordner")
    ap.add_argument("--parquet", default="data/processed/prices.parquet")
    args = ap.parse_args()
    ...
    rep = {"ok":[], "err":[]}

    for sym in syms:
        try:
            # (Provider-Logik wie gehabt …)
            outp = _alpha_path(args.outdir, sym, args.alpha_buckets)
            write_csv(outp, data["t"], data["c"])
            rep["ok"].append(sym)
            print(f"✔ {sym} → {outp} ({len(data['c'])} rows)")
        except Exception as e:
            rep["err"].append({"symbol": sym, "error": str(e)})
            print(f"✖ {sym}: {e}", file=sys.stderr)

    # Report
    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/fetch_prices_report.json","w",encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

    # Konsolidierung nach Parquet (alphabetisch sortiert)
    try:
        import pandas as pd, glob
        frames = []
        pattern = os.path.join(args.outdir, "**", "*.csv") if args.alpha_buckets else os.path.join(args.outdir, "*.csv")
        for p in glob.glob(pattern, recursive=True):
            df = pd.read_csv(p)
            if "close" in df.columns and "date" in df.columns and len(df) > 0:
                sym = os.path.splitext(os.path.basename(p))[0]
                df["symbol"] = sym
                frames.append(df[["symbol","date","close"]])
        if frames:
            big = pd.concat(frames, ignore_index=True)
            big.sort_values(["symbol","date"], inplace=True)
            os.makedirs(os.path.dirname(args.parquet), exist_ok=True)
            big.to_parquet(args.parquet, index=False)
            print(f"📦 consolidated → {args.parquet} ({len(big):,} rows)")
    except Exception as e:
        print("WARN parquet:", e, file=sys.stderr)

    if rep["ok"]:
        print(f"Done. OK={len(rep['ok'])}, ERR={len(rep['err'])}")
