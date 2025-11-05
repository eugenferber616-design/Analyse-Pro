# scripts/fetch_stooq.py
# Lädt Tagesdaten via yfinance und schreibt EIN CSV pro Anfangsbuchstaben:
# data/market/stooq/A.csv, B.csv, ..., Z.csv, _.csv
# Optional: nur .DE (Xetra) → Default. Mit --include-all werden alle Symbole genutzt.
# Optional: HV-Summary (hv20/hv60) aus den konsolidierten Dateien.

from __future__ import annotations
import argparse, csv, json, os, sys, time, gzip, glob, string
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import yfinance as yf

LETTERS = list(string.ascii_uppercase) + ["_"]

def _canon_symbol(s: str) -> str:
    s = s.split("#", 1)[0].split("//", 1)[0].strip()
    if "," in s:
        s = s.split(",", 1)[0].strip()
    s = s.split()[0] if s else ""
    return s

def read_watchlist(path: str, include_all: bool) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Watchlist nicht gefunden: {path}")
    syms: List[str] = []
    if p.suffix.lower() == ".csv":
        df = pd.read_csv(p)
        col = "symbol" if "symbol" in df.columns else df.columns[0]
        for v in df[col].astype(str).tolist():
            t = _canon_symbol(v)
            if t and t.lower() not in ("symbol","ticker"):
                syms.append(t)
    else:
        with p.open(encoding="utf-8") as f:
            for ln in f:
                t = _canon_symbol(ln)
                if t and t.lower() not in ("symbol","ticker"):
                    syms.append(t)
    if not include_all:
        syms = [s for s in syms if s.upper().endswith(".DE")]
    return sorted(set(syms))

def ensure_dirs():
    Path("data/market/stooq").mkdir(parents=True, exist_ok=True)
    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("data/reports/eu_checks").mkdir(parents=True, exist_ok=True)

def bucket_for_symbol(sym: str) -> str:
    if not sym:
        return "_"
    first = sym[0].upper()
    return first if first in string.ascii_uppercase else "_"

def dl_yahoo_daily(ticker: str, days: int, retries: int = 3, sleep_ms: int = 200) -> pd.DataFrame:
    for i in range(retries):
        try:
            df = yf.download(
                ticker,
                period=f"{days}d",
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            if df is None or df.empty:
                raise RuntimeError("empty data from yfinance")
            df = df.reset_index()
            ren = {
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Adj Close": "adj_close",
                "Volume": "volume",
            }
            df = df.rename(columns=ren)
            keep = [c for c in ["date","open","high","low","close","adj_close","volume"] if c in df.columns]
            out = df[keep].copy()
            out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
            return out
        except Exception:
            if i == retries - 1:
                return pd.DataFrame()
            time.sleep(max(0.05, sleep_ms/1000.0) * (i + 1))
    return pd.DataFrame()

def write_bucket_csvs(buckets: Dict[str, List[pd.DataFrame]], outdir: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for b in LETTERS:
        frames = buckets.get(b, [])
        if not frames:
            continue
        big = pd.concat(frames, ignore_index=True)
        # einheitliche Sortierung: symbol, date
        if "date" in big.columns:
            big.sort_values(["symbol","date"], inplace=True)
        out_p = os.path.join(outdir, f"{b}.csv")
        Path(outdir).mkdir(parents=True, exist_ok=True)
        big.to_csv(out_p, index=False)
        counts[b] = big.shape[0]
    return counts

def build_hv_from_buckets(outdir: str, hv_out: str, w20: int, w60: int) -> int:
    # Lädt A.csv, B.csv, … und berechnet pro Symbol hv20/hv60
    rows: List[Dict[str, Any]] = []
    for b in LETTERS:
        p = os.path.join(outdir, f"{b}.csv")
        if not os.path.exists(p):
            continue
        df = pd.read_csv(p)
        if df.empty or "symbol" not in df.columns:
            continue
        for sym, g in df.groupby("symbol"):
            col = "adj_close" if "adj_close" in g.columns else ("close" if "close" in g.columns else None)
            if not col:
                continue
            s = pd.to_numeric(g[col], errors="coerce").dropna()
            if s.size < max(w20, w60) + 2:
                continue
            r = s.pct_change().dropna()
            hv20 = float(r.tail(w20).std() * (252 ** 0.5))
            hv60 = float(r.tail(w60).std() * (252 ** 0.5))
            rows.append({"symbol": sym, "hv20": hv20, "hv60": hv60})
    Path(hv_out).parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if hv_out.endswith(".gz") else open
    with opener(hv_out, "wt", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol","hv20","hv60"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return len(rows)

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="TXT oder CSV (Spalte 'symbol')")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--sleep-ms", type=int, default=200)
    ap.add_argument("--include-all", action="store_true", help="nicht nur .DE/Xetra")
    ap.add_argument("--outdir", default="data/market/stooq", help="Zielordner für A.csv, B.csv, …")
    ap.add_argument("--preview", default="data/processed/stooq_latest.csv", help="QA: letzte Kurse pro Symbol")
    ap.add_argument("--build-hv", action="store_true")
    ap.add_argument("--hv-out", default="data/processed/hv_summary.csv.gz")
    ap.add_argument("--hv20", type=int, default=20)
    ap.add_argument("--hv60", type=int, default=60)
    args = ap.parse_args()

    ensure_dirs()
    symbols = read_watchlist(args.watchlist, include_all=args.include_all)
    print(f"🔎 Watchlist: {args.watchlist}")
    print(f"   Symbole: {len(symbols)} | include_all={args.include_all}")

    # Buckets: Buchstabe → Liste von DataFrames (jedes DF hat alle Zeilen eines Symbols)
    buckets: Dict[str, List[pd.DataFrame]] = {b: [] for b in LETTERS}
    ok = 0
    fail: List[Dict[str, Any]] = []
    preview_rows: List[List[Any]] = []

    for s in symbols:
        try:
            df = dl_yahoo_daily(s, args.days, retries=3, sleep_ms=args.sleep_ms)
            if df.empty:
                fail.append({"symbol": s, "reason": "no_data"})
            else:
                df.insert(0, "symbol", s)
                b = bucket_for_symbol(s)
                buckets[b].append(df)
                ok += 1
                # QA-Preview
                last_date = str(df["date"].iloc[-1]) if "date" in df.columns else ""
                last_close = float(df["close"].iloc[-1]) if "close" in df.columns else float("nan")
                preview_rows.append([s, last_date, last_close, int(df.shape[0])])
        except Exception as e:
            fail.append({"symbol": s, "reason": str(e)})
        time.sleep(args.sleep_ms / 1000.0)

    # Schreibe A.csv, B.csv, …
    counts = write_bucket_csvs(buckets, args.outdir)

    # Preview/Reports
    Path(args.preview).parent.mkdir(parents=True, exist_ok=True)
    with open(args.preview, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol","last_date","last_close","rows"])
        w.writerows(preview_rows)

    preview_txt = "data/reports/eu_checks/stooq_preview.txt"
    with open(preview_txt, "w", encoding="utf-8") as f:
        f.write("symbol,last_date,last_close,rows\n")
        for r in preview_rows[:80]:
            f.write(",".join(map(str, r)) + "\n")

    hv_rows = 0
    if args.build_hv:
        hv_rows = build_hv_from_buckets(args.outdir, args.hv_out, args.hv20, args.hv60)
        print(f"🧮 HV-Summary gebaut: {hv_rows} Zeilen → {args.hv_out}")

    report = {
        "ts": pd.Timestamp.utcnow().isoformat() + "Z",
        "watchlist": args.watchlist,
        "include_all": bool(args.include_all),
        "days": int(args.days),
        "symbols": len(symbols),
        "ok": ok,
        "failed": len(fail),
        "bucket_row_counts": counts,   # z.B. {"A": 12345, "B": 6789, ...}
        "files_dir": str(args.outdir),
        "preview_csv": args.preview,
        "preview_txt": preview_txt,
        "hv_summary_rows": hv_rows,
        "hv_summary_file": args.hv_out if args.build_hv else None,
        "note": "Ein CSV pro Anfangsbuchstaben. Spalten: symbol,date,open,high,low,close,adj_close,volume.",
        "fail": fail,
    }
    with open("data/reports/stooq_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"✅ Erfolgreich: {ok} / {len(symbols)}")
    print(f"📂 Buckets geschrieben: {', '.join([k for k in counts.keys()])}")
    print(f"↳ Preview: {args.preview}")
    if fail:
        print(f"⚠️ Fehlgeschlagen: {len(fail)} (siehe data/reports/stooq_report.json)")
    return 0

if __name__ == "__main__":
    sys.exit(main())
