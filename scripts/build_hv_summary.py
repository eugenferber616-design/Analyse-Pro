# scripts/build_hv_summary.py
import os, sys, csv, gzip, time, math, json, argparse, concurrent.futures
from io import StringIO
from datetime import datetime, timedelta
import requests
import pandas as pd

STOOQ_URL = "https://stooq.com/q/d/l/?s={sym}&i=d"

def guess_stooq_symbol(symbol: str) -> str:
    # Erwartet Symbole wie AAPL, MSFT oder Xetra mit .DE (SAP.DE)
    # Stooq nutzt Kleinbuchstaben
    return symbol.lower()

def fetch_stooq_df(sym: str, days: int, timeout=20) -> pd.DataFrame | None:
    url = STOOQ_URL.format(sym=sym)
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200 or not r.text or r.text.lstrip().startswith("<"):
        return None
    df = pd.read_csv(StringIO(r.text))
    if "Date" not in df or "Close" not in df:  # leeres Papier
        return None
    # auf die letzten N Handelstage begrenzen
    if days and len(df) > days:
        df = df.tail(days)
    df = df.rename(columns={"Date":"date","Close":"close"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["close"]).sort_values("date")
    return df

def hv_from_df(df: pd.DataFrame, win: int) -> float | None:
    if len(df) < max(5, win):
        return None
    rets = df["close"].pct_change().dropna()
    if len(rets) < win:
        return None
    vol = rets.tail(win).std(ddof=0)
    # Annualisieren mit ~252 Handelstagen
    return float(vol * math.sqrt(252))

def process_symbol(raw_symbol: str, days: int) -> dict:
    stooq_sym = guess_stooq_symbol(raw_symbol)
    df = fetch_stooq_df(stooq_sym, days)
    if df is None or df.empty:
        return {"symbol": raw_symbol, "hv20": None, "hv60": None, "asof": None, "ok": False, "src": stooq_sym}
    hv20 = hv_from_df(df, 20)
    hv60 = hv_from_df(df, 60)
    asof = df["date"].max().strftime("%Y-%m-%d")
    return {"symbol": raw_symbol, "hv20": hv20, "hv60": hv60, "asof": asof, "ok": True, "src": stooq_sym}

def read_watchlist(path: str) -> list[str]:
    syms = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.lower().startswith("symbol"):  # CSV-Header tolerieren
                continue
            # CSV/TSV tolerant: Symbol steht in der ersten Spalte
            t = t.split(",")[0].split("\t")[0].strip()
            if t:
                syms.append(t)
    return syms

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="Pfad zu watchlists/*.txt|csv")
    ap.add_argument("--days", type=int, default=252, help="Handelstage, die für HV berücksichtigt werden")
    ap.add_argument("--out", default="data/processed/hv_summary.csv.gz")
    ap.add_argument("--max_workers", type=int, default=8)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    syms = read_watchlist(args.watchlist)

    rows = []
    errs = []
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(process_symbol, s, args.days): s for s in syms}
        for fut in concurrent.futures.as_completed(futs):
            res = fut.result()
            if res["ok"]:
                rows.append(res)
            else:
                errs.append(res["symbol"])

    # Schreiben (komprimiert)
    out_tmp = args.out + ".tmp"
    with gzip.open(out_tmp, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(["symbol","hv20","hv60","asof"])
        for r in rows:
            w.writerow([r["symbol"], r["hv20"] if r["hv20"] is not None else "", r["hv60"] if r["hv60"] is not None else "", r["asof"] or ""])

    os.replace(out_tmp, args.out)

    # Report
    report = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "watchlist": args.watchlist,
        "symbols": len(syms),
        "ok": len(rows),
        "failed": len(errs),
        "out": args.out,
        "fail": errs[:50],
        "t_sec": round(time.time()-t0,2)
    }
    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/hv_report.json","w",encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
