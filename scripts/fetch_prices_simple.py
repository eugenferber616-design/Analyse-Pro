# scripts/fetch_prices_simple.py
# Lädt Daily-Prices (close) für eine Watchlist und speichert CSVs unter data/prices/{SYMBOL}.csv
# Provider auswählbar: Finnhub, yfinance, Stooq; Default: auto (Finnhub -> yfinance -> Stooq)
import os, sys, time, json, argparse
from datetime import datetime, timedelta
import urllib.request, urllib.parse
import csv
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def load_watchlist(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.lower().startswith(("symbol", "#", "//")):
                continue
            if "," in s:  # CSV möglich: erste Spalte = symbol
                parts = [p.strip() for p in s.split(",")]
                if parts:
                    syms.append(parts[0])
            else:
                syms.append(s)
    out, seen = [], set()
    for s in syms:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def write_csv(out_path: str, df: pd.DataFrame) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df2 = df.copy()
    # vereinheitlichen
    if "Close" in df2.columns:
        df2 = df2.rename(columns={"Close": "close"})
    elif "close" in df2.columns:
        pass
    else:
        raise RuntimeError("Kein Close-Feld im DataFrame")
    # Index zu Datumsspalte
    if isinstance(df2.index, pd.DatetimeIndex):
        df2.index.name = "date"
        df2.reset_index(inplace=True)
    if "date" in df2.columns and not pd.api.types.is_string_dtype(df2["date"]):
        df2["date"] = pd.to_datetime(df2["date"]).dt.strftime("%Y-%m-%d")
    df2 = df2[["date", "close"]].dropna()
    df2.to_csv(out_path, index=False)

# ──────────────────────────────────────────────────────────────────────────────
# Provider: Finnhub (JSON -> DataFrame)
# ──────────────────────────────────────────────────────────────────────────────

def finnhub_get_candles(symbol: str, fr_ts: int, to_ts: int, token: str, sleep_ms: int = 1200) -> pd.DataFrame:
    if not token:
        raise RuntimeError("FINNHUB_TOKEN/FINNHUB_API_KEY fehlt")
    base = "https://finnhub.io/api/v1/stock/candle"
    qs = urllib.parse.urlencode({
        "symbol": symbol, "resolution": "D", "from": fr_ts, "to": to_ts, "token": token
    })
    url = f"{base}?{qs}"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            raw = r.read().decode("utf-8")
            data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"HTTP: {e}")
    finally:
        time.sleep(sleep_ms/1000.0)

    # Erwartetes Schema: { s: "ok", t: [...], c: [...] }
    if not isinstance(data, dict) or data.get("s") != "ok" or not data.get("t") or not data.get("c"):
        raise RuntimeError(f"bad status: {data.get('s')}")
    df = pd.DataFrame({"date": pd.to_datetime(pd.Series(data["t"], dtype="int64"), unit="s"),
                       "close": pd.to_numeric(pd.Series(data["c"]), errors="coerce")})
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Provider: yfinance
# ──────────────────────────────────────────────────────────────────────────────

def yf_fetch(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    import yfinance as yf
    df = yf.download(symbol, start=start, end=end, interval="1d", progress=False,
                     auto_adjust=False, prepost=False)
    if df is None or df.empty:
        raise RuntimeError("yfinance: empty")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Close"]].copy()
    df.index = pd.to_datetime(df.index)
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Provider: Stooq (pandas_datareader)
# ──────────────────────────────────────────────────────────────────────────────

def stooq_fetch(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
    from pandas_datareader import data as pdr
    df = pdr.DataReader(symbol, "stooq", start, end)
    if df is None or df.empty:
        raise RuntimeError("stooq: empty")
    df = df.sort_index()
    df = df[["Close"]].copy()
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True)
    ap.add_argument("--days", type=int, default=750)
    ap.add_argument("--sleep-ms", type=int, default=200)
    ap.add_argument("--outdir", default="data/prices")
    ap.add_argument("--provider", choices=["auto", "finnhub", "yf", "stooq"], default="auto")
    ap.add_argument("--token", default=os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY"))
    args = ap.parse_args()

    syms = load_watchlist(args.watchlist)
    if not syms:
        print(f"WARN: Watchlist {args.watchlist} leer?", file=sys.stderr)

    end = datetime.utcnow()
    start = end - timedelta(days=args.days + 10)
    fr_ts = int((start).timestamp())
    to_ts = int(end.timestamp())

    os.makedirs(args.outdir, exist_ok=True)
    rep = {"ok": [], "err": []}

    for sym in syms:
        try:
            df = None

            def try_finnhub():
                return finnhub_get_candles(sym, fr_ts, to_ts, args.token, sleep_ms=max(args.sleep_ms, 1200))

            def try_yf():
                return yf_fetch(sym, start, end)

            def try_stooq():
                return stooq_fetch(sym, start, end)

            if args.provider == "finnhub":
                df = try_finnhub()
            elif args.provider == "yf":
                df = try_yf()
            elif args.provider == "stooq":
                df = try_stooq()
            else:  # auto: Finnhub -> yfinance -> Stooq
                for fn in (try_finnhub, try_yf, try_stooq):
                    try:
                        df = fn()
                        break
                    except Exception as _:
                        df = None
                if df is None:
                    raise RuntimeError("auto: all providers failed")

            outp = os.path.join(args.outdir, f"{sym}.csv")
            write_csv(outp, df)
            print(f"✔ {sym} → {outp} ({len(df)} rows)")
            rep["ok"].append(sym)
        except Exception as e:
            print(f"✖ {sym}: {e}", file=sys.stderr)
            rep["err"].append({"symbol": sym, "error": str(e)})
        time.sleep(args.sleep_ms/1000.0)

    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/fetch_prices_report.json", "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    print(f"Done. OK={len(rep['ok'])}, ERR={len(rep['err'])}")

if __name__ == "__main__":
    main()
