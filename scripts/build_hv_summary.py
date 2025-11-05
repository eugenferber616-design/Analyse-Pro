# scripts/build_hv_summary.py
# Robustes HV (hv20/hv60) aus Stooq (CSV-Endpoint); optionaler yfinance-Fallback.
# Output: data/processed/hv_summary.csv.gz  (symbol,hv20,hv60,asof)

import os, sys, csv, gzip, time, math, json, argparse, concurrent.futures, random
from io import StringIO
from datetime import datetime
from typing import List, Optional, Tuple

import requests
import pandas as pd

try:
    import yfinance as yf  # optionaler Fallback
except Exception:
    yf = None

STOOQ_URL = "https://stooq.com/q/d/l/?s={sym}&i=d"
UA = "hv-summary-bot/1.0 (+https://example.local) Python-requests"

# ──────────────────────────────────────────────────────────────────────────────
# Symbol-Mapping & Varianten
# ──────────────────────────────────────────────────────────────────────────────

EU_SUFFIXES = {".de",".pa",".as",".mi",".br",".mc",".be",".sw",".ol",".st",".he",".co",".ir",".ls",".wa",".vi",".pr",".vx",".l",".pl",".nl",".es",".fi",".dk",".no",".se"}

def _clean_symbol(s: str) -> str:
    s = s.split("#",1)[0].split("//",1)[0].strip()
    if "," in s: s = s.split(",",1)[0].strip()
    if "\t" in s: s = s.split("\t",1)[0].strip()
    return s

def read_watchlist(path: str) -> List[str]:
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            t = _clean_symbol(line)
            if not t or t.lower().startswith("symbol"): 
                continue
            out.append(t)
    # eindeutige Reihenfolge beibehalten
    seen = set(); uniq = []
    for s in out:
        if s not in seen:
            seen.add(s); uniq.append(s)
    return uniq

def variants_for(symbol: str) -> List[str]:
    """
    Liefert eine Liste möglicher Stooq-Ticker in sinnvoller Reihenfolge.
    Regeln:
      - Mit Börsensuffix → nur lowercase (z.B. SAP.DE → sap.de, BARC.L → barc.l)
      - Ohne Suffix → zuerst US (.us), dann plain lowercase (manche ETFs/FX/Index)
    """
    s = symbol.strip()
    lower = s.lower()
    if "." in lower:
        return [lower]  # Suffix schon vorhanden
    # Sonderfälle: häufige US/ETFs ohne Suffix
    return [f"{lower}.us", lower]

# ──────────────────────────────────────────────────────────────────────────────
# Datenfetch & Berechnung
# ──────────────────────────────────────────────────────────────────────────────

def fetch_stooq_csv(sym: str, timeout=20, retries=2, backoff=0.6) -> Optional[str]:
    headers = {"User-Agent": UA}
    url = STOOQ_URL.format(sym=sym)
    for i in range(retries+1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)
            if r.status_code == 200 and r.text and not r.text.lstrip().startswith("<"):
                return r.text
        except Exception:
            pass
        time.sleep(backoff * (1.5**i) + random.random()*0.15)
    return None

def parse_stooq_df(csv_text: str, days: int) -> Optional[pd.DataFrame]:
    df = pd.read_csv(StringIO(csv_text))
    if "Date" not in df or "Close" not in df or df.empty:
        return None
    if days and len(df) > days:
        df = df.tail(days)
    df = df.rename(columns={"Date":"date","Close":"close"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["close","date"]).sort_values("date")
    return df if not df.empty else None

def fetch_stooq_df_multi(symbol: str, days: int) -> Optional[pd.DataFrame]:
    """
    Probiert alle Varianten für ein Symbol durch, gibt das erste valide DF zurück.
    """
    for v in variants_for(symbol):
        txt = fetch_stooq_csv(v)
        if not txt:
            continue
        df = parse_stooq_df(txt, days)
        if df is not None and not df.empty:
            return df
    return None

def fetch_yf_df(symbol: str, days: int) -> Optional[pd.DataFrame]:
    if yf is None:
        return None
    try:
        per = f"{max(days,1)}d"
        df = yf.download(symbol, period=per, interval="1d", auto_adjust=False, progress=False, threads=False)
        if df is None or df.empty:
            # try mapping: AAPL -> AAPL (ok), SAP.DE -> SAP.DE (ok)
            return None
        df = df.reset_index()
        if "Date" not in df or "Close" not in df:
            return None
        if days and len(df) > days:
            df = df.tail(days)
        out = df.rename(columns={"Date":"date","Close":"close"}).dropna(subset=["close"])
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"]).sort_values("date")
        return out if not out.empty else None
    except Exception:
        return None

def hv_from_df(df: pd.DataFrame, win: int) -> Optional[float]:
    if len(df) < max(5, win): return None
    rets = df["close"].pct_change().dropna()
    if len(rets) < win: return None
    vol = rets.tail(win).std(ddof=0)
    return float(vol * math.sqrt(252))

def process_symbol(raw_symbol: str, days: int, yf_fallback: bool) -> dict:
    # 1) Stooq
    df = fetch_stooq_df_multi(raw_symbol, days)
    src = "stooq"
    # 2) optional yfinance fallback
    if (df is None or df.empty) and yf_fallback:
        df = fetch_yf_df(raw_symbol, days)
        src = "yfinance" if df is not None else "stooq"
    if df is None or df.empty:
        return {"symbol": raw_symbol, "hv20": None, "hv60": None, "asof": None, "ok": False, "src": src}
    hv20 = hv_from_df(df, 20)
    hv60 = hv_from_df(df, 60)
    asof = df["date"].max().strftime("%Y-%m-%d")
    return {"symbol": raw_symbol, "hv20": hv20, "hv60": hv60, "asof": asof, "ok": True, "src": src}

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="Pfad zu watchlists/*.txt|csv")
    ap.add_argument("--days", type=int, default=252, help="Handelstage für HV")
    ap.add_argument("--out", default="data/processed/hv_summary.csv.gz")
    ap.add_argument("--max_workers", type=int, default=8)
    ap.add_argument("--yf-fallback", action="store_true", help="Wenn Stooq leer → yfinance versuchen")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    syms = read_watchlist(args.watchlist)

    rows, errs = [], []
    t0 = time.time()
    # leichte Zufallsreihenfolge verhindert Server-Bursts bei gleichen Symbolen über viele Runs
    work = list(syms)

    def _worker(s: str):
        try:
            return process_symbol(s, args.days, args.yf_fallback)
        except Exception as e:
            return {"symbol": s, "hv20": None, "hv60": None, "asof": None, "ok": False, "src": "error", "err": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(_worker, s): s for s in work}
        for fut in concurrent.futures.as_completed(futs):
            res = fut.result()
            if res.get("ok"):
                rows.append(res)
            else:
                errs.append(res.get("symbol"))

    # Schreiben (komprimiert)
    tmp = args.out + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(["symbol","hv20","hv60","asof"])
        for r in rows:
            w.writerow([
                r["symbol"],
                "" if r["hv20"] is None else f"{r['hv20']:.6f}",
                "" if r["hv60"] is None else f"{r['hv60']:.6f}",
                r["asof"] or ""
            ])
    os.replace(tmp, args.out)

    # Preview TXT (erste 40)
    prev_path = "data/reports/hv_preview.txt"
    with open(prev_path, "w", encoding="utf-8") as f:
        f.write("symbol,asof,hv20,hv60\n")
        for r in rows[:40]:
            f.write(f"{r['symbol']},{r['asof']},{r['hv20']},{r['hv60']}\n")

    # Report JSON
    report = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "watchlist": args.watchlist,
        "symbols": len(syms),
        "ok": len(rows),
        "failed": len(errs),
        "out": args.out,
        "preview": prev_path,
        "yf_fallback": bool(args.yf_fallback),
        "t_sec": round(time.time()-t0, 2),
        "fail": errs[:100]
    }
    with open("data/reports/hv_report.json","w",encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    sys.exit(main())
