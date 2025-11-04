#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_core.py  — robust, mit Fallbacks
Schreibt:
  data/market/core/{NAME}.csv
  data/processed/market_core.csv.gz
"""
from __future__ import annotations
from pathlib import Path
import sys, time, math
import pandas as pd

RAW_DIR = Path("data/market/core")
OUT_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

START = "2003-01-01"
PAUSE = 0.6  # Sekunden zwischen Versuchen

# Primär Yahoo, mit Alternativen
SYMS = {
    "VIX":    ["^VIX"],                       # CBOE VIX
    "VIX3M":  ["^VIX3M"],                     # 3-Monats VIX
    "DXY":    ["DX-Y.NYB", "DX=F", "DXY", "UUP"],   # DXY bzw. Futures/ETF
    "USDJPY": ["JPY=X", "USDJPY=X"],          # FX (Yahoo: JPY=X)
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

def pull_yahoo_history(ticker: str, start=START) -> pd.DataFrame:
    # history() ist robuster als download()
    import yfinance as yf
    try:
        t = yf.Ticker(ticker)
        df = t.history(start=start, auto_adjust=False, actions=False)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        raise RuntimeError(str(e))

def pull_stooq(ticker: str, start=START) -> pd.DataFrame:
    # Stooq für große US-ETFs funktioniert oft (SPY/HYG/LQD/XLF)
    try:
        from pandas_datareader import data as pdr
        df = pdr.DataReader(ticker, "stooq", start=pd.to_datetime(start))
        # Stooq liefert neu→alt; drehen
        if not df.empty:
            df = df.sort_index()
        return df
    except Exception as e:
        raise RuntimeError(str(e))

def save_raw(s: pd.Series):
    if s.empty: return
    out = RAW_DIR / f"{s.name}.csv"
    s.to_frame("value").rename_axis("date").to_csv(out, index=True)
    print(f"OK: {s.name} rows={len(s)} → {out}")

def pull_one(name: str, alts: list[str]) -> pd.Series:
    # 1) Yahoo-Versuche
    for i, t in enumerate(alts, 1):
        try:
            df = pull_yahoo_history(t, START)
            s  = _to_close_series(df, name)
            if not s.empty:
                save_raw(s)
                return s
            else:
                print(f"WARN: leer für {name} ({t}) [Yahoo]")
        except Exception as e:
            print(f"WARN: Download-Fehler {name} ({t}) [Yahoo]: {e}")
        time.sleep(PAUSE)

    # 2) Stooq-Fallback für ETFs
    if name in ("SPY","HYG","LQD","XLF"):
        try:
            df = pull_stooq(name, START)
            s  = _to_close_series(df, name)
            if not s.empty:
                save_raw(s)
                return s
        except Exception as e:
            print(f"WARN: Fallback Stooq {name}: {e}")

    # 3) leer
    return pd.Series(dtype=float, name=name)

def main() -> int:
    try:
        import yfinance  # noqa - sicherstellen, dass es installiert ist
    except Exception as e:
        print("WARN: yfinance nicht installiert → überspringe Market-Core. Fehler:", e)
        # schreibe MINIMALDATEI, damit Workflow weiterläuft
        (OUT_DIR/"market_core.csv.gz").write_bytes(b"")
        return 0

    series = []
    for k, v in SYMS.items():
        s = pull_one(k, v)
        if not s.empty:
            series.append(s)

    # VIX-Proxy als Notnagel, falls VIX fehlt aber SPY vorhanden ist
    have = {s.name for s in series}
    if "VIX" not in have:
        spy = next((s for s in series if s.name=="SPY"), None)
        if spy is not None and not spy.empty:
            r = spy.pct_change()
            hv20 = r.rolling(20).std() * math.sqrt(252) * 100.0
            vix_proxy = hv20.rename("VIX")
            save_raw(vix_proxy.dropna())
            series.append(vix_proxy)
            print("INFO: VIX-Proxy aus SPY-HV20 erzeugt.")

    if not series:
        print("ERROR: keine Market-Reihen erhalten – RiskIndex nutzt dann nur FRED.")
        # leere Datei anlegen, damit Folge-Steps nicht scheitern
        (OUT_DIR/"market_core.csv.gz").write_bytes(b"")
        return 0

    df = pd.concat(series, axis=1).sort_index()
    df.index.name = "date"
    # auf Tagesgitter + FFill (vereinheitlicht für spätere Joins)
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    # schreiben
    out = OUT_DIR / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print(f"✔ wrote {out}  cols={list(df.columns)}  rows={len(df)}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
