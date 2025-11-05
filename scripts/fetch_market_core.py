#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_core.py — robuste Mehrfach-Fallbacks (Yahoo→Yahoo(dl)→Stooq→Yahoo(pdr))
Schreibt:
  - data/market/core/{NAME}.csv
  - data/processed/market_core.csv.gz
"""
from __future__ import annotations
from pathlib import Path
import time, math
import pandas as pd

RAW_DIR = Path("data/market/core")
OUT_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

START = "2003-01-01"
PAUSE = 0.6  # Sekunden zwischen Versuchen

# Primär-Tickers + Alternativen
SYMS = {
    "VIX":    ["^VIX"],
    "VIX3M":  ["^VIX3M"],
    "DXY":    ["DX-Y.NYB", "DX=F", "DXY", "UUP"],
    "USDJPY": ["JPY=X", "USDJPY=X"],
    "HYG":    ["HYG"],
    "LQD":    ["LQD"],
    "XLF":    ["XLF"],
    "SPY":    ["SPY"],
}

def _to_close_series(df: pd.DataFrame, name: str) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float, name=name)
    c = "Adj Close" if "Adj Close" in df.columns else ("Close" if "Close" in df.columns else None)
    if c is None:
        return pd.Series(dtype=float, name=name)
    s = pd.to_numeric(df[c], errors="coerce").dropna()
    s.index = pd.to_datetime(s.index).tz_localize(None)
    s.name = name
    return s

def _try_yf_history(ticker: str):
    import yfinance as yf
    t = yf.Ticker(ticker)
    return t.history(start=START, auto_adjust=False, actions=False)

def _try_yf_download(ticker: str):
    import yfinance as yf
    return yf.download(tickers=ticker, start=START, progress=False)

def _try_pdr_stooq(ticker: str):
    from pandas_datareader import data as pdr
    df = pdr.DataReader(ticker, "stooq", start=pd.to_datetime(START))
    return df.sort_index() if not df.empty else df

def _try_pdr_yahoo(ticker: str):
    from pandas_datareader import data as pdr
    df = pdr.DataReader(ticker, "yahoo", start=pd.to_datetime(START))
    return df  # Spalten heißen Close/Adj Close → _to_close_series kümmert sich

def save_raw(s: pd.Series):
    if s.empty: return
    out = RAW_DIR / f"{s.name}.csv"
    s.to_frame("value").rename_axis("date").to_csv(out, index=True)
    print(f"OK: {s.name} rows={len(s)} → {out}")

def pull_one(name: str, alts: list[str]) -> pd.Series:
    attempts = []
    for t in alts:
        attempts.append(("yf.history", t, _try_yf_history))
        attempts.append(("yf.download", t, _try_yf_download))
        # Für ETFs/Index-ETFs zusätzlich Stooq
        if name in ("SPY","HYG","LQD","XLF"):
            attempts.append(("pdr.stooq", name, _try_pdr_stooq))
        attempts.append(("pdr.yahoo", t, _try_pdr_yahoo))

    seen = set()
    for src, tick, fn in attempts:
        key = (src, tick)
        if key in seen: 
            continue
        seen.add(key)
        try:
            df = fn(tick)
            s  = _to_close_series(df, name)
            if not s.empty:
                print(f"OK: {name} via {src} ← {tick}  rows={len(s)}")
                save_raw(s)
                return s
            else:
                print(f"WARN: leer für {name} via {src} ← {tick}")
        except Exception as e:
            print(f"WARN: {name} via {src} ← {tick} Fehler: {e}")
        time.sleep(PAUSE)

    return pd.Series(dtype=float, name=name)

def _write_empty_gzip(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame().to_csv(path, index=False, compression="gzip")

def main() -> int:
    try:
        import yfinance  # noqa
        from pandas_datareader import data as _pdr  # noqa
    except Exception as e:
        print("WARN: Abhängigkeiten fehlen:", e)
        _write_empty_gzip(OUT_DIR/"market_core.csv.gz")
        return 0

    series = []
    for k, v in SYMS.items():
        s = pull_one(k, v)
        if not s.empty:
            series.append(s)

    # VIX-Proxy wenn nötig
    have = {s.name for s in series}
    if "VIX" not in have:
        spy = next((s for s in series if s.name == "SPY"), None)
        if spy is not None and not spy.empty:
            r = spy.pct_change()
            hv20 = r.rolling(20).std() * math.sqrt(252) * 100.0
            vix_proxy = hv20.rename("VIX").dropna()
            if not vix_proxy.empty:
                print("INFO: VIX-Proxy aus SPY-HV20 erzeugt.")
                save_raw(vix_proxy)
                series.append(vix_proxy)

    if not series:
        print("ERROR: keine Market-Reihen erhalten – schreibe leere .gz")
        _write_empty_gzip(OUT_DIR/"market_core.csv.gz")
        return 0

    df = pd.concat(series, axis=1).sort_index()
    df.index.name = "date"
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    out = OUT_DIR / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print("nonnull counts:", df.notna().sum().to_dict())
    print(f"✔ wrote {out}  cols={list(df.columns)} rows={len(df)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
