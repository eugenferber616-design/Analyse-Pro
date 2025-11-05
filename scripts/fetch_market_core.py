#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_core.py — robust mit Fallbacks (Yahoo → Stooq), gültige leere .gz
Schreibt:
  - data/market/core/{NAME}.csv
  - data/processed/market_core.csv.gz
"""

from __future__ import annotations

from pathlib import Path
import sys
import time
import math
import pandas as pd

RAW_DIR = Path("data/market/core")
OUT_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

START = "2003-01-01"
PAUSE = 0.6  # Sekunden zwischen Versuchen

# Primär Yahoo, mit Alternativen
SYMS = {
    "VIX":    ["^VIX"],                           # CBOE VIX
    "VIX3M":  ["^VIX3M"],                         # 3-Monats VIX
    "DXY":    ["DX-Y.NYB", "DX=F", "DXY", "UUP"], # DXY bzw. Futures/ETF
    "USDJPY": ["JPY=X", "USDJPY=X"],              # JPY pro USD (Yahoo: JPY=X)
    "HYG":    ["HYG"],
    "LQD":    ["LQD"],
    "XLF":    ["XLF"],
    "SPY":    ["SPY"],
}

# -------------------- Helpers --------------------

def _to_close_series(df: pd.DataFrame, name: str) -> pd.Series:
    """Extrahiere Close/Adj Close als 1-d Serie (tz-naiv, float)."""
    if df is None or df.empty:
        return pd.Series(dtype=float, name=name)
    col = "Adj Close" if "Adj Close" in df.columns else ("Close" if "Close" in df.columns else None)
    if col is None:
        return pd.Series(dtype=float, name=name)
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    idx = pd.to_datetime(s.index)
    try:
        # falls tz-aware → tz entfernen
        s.index = idx.tz_convert(None)
    except (AttributeError, TypeError):
        # bereits tz-naiv oder älteres pandas
        s.index = idx
    s.name = name
    return s

def pull_yahoo_history(ticker: str, start: str = START) -> pd.DataFrame:
    """Yahoo Finance: Ticker.history ist meist robuster als download()."""
    import yfinance as yf
    t = yf.Ticker(ticker)
    df = t.history(start=start, auto_adjust=False, actions=False)
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

def pull_stooq(ticker: str, start: str = START) -> pd.DataFrame:
    """Stooq-Fallback (funktioniert für große US-ETFs)."""
    try:
        from pandas_datareader import data as pdr
    except Exception:
        raise RuntimeError("pandas_datareader fehlt – kein Stooq-Fallback möglich")
    df = pdr.DataReader(ticker, "stooq", start=pd.to_datetime(start))
    if not df.empty:
        df = df.sort_index()  # Stooq liefert neu→alt
    return df

def save_raw(s: pd.Series) -> None:
    if s.empty:
        return
    out = RAW_DIR / f"{s.name}.csv"
    s.to_frame("value").rename_axis("date").to_csv(out, index=True)
    print(f"OK: {s.name} rows={len(s)} → {out}")

def write_empty_gzip_csv(path: Path) -> None:
    """Erzeugt eine gültige (leere) gzip-CSV, damit Folgeschritte nicht brechen."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame().to_csv(path, index=False, compression="gzip")

def pull_one(name: str, alts: list[str]) -> pd.Series:
    # 1) Yahoo-Versuche
    for i, t in enumerate(alts, 1):
        try:
            df = pull_yahoo_history(t, START)
            s = _to_close_series(df, name)
            if not s.empty:
                save_raw(s)
                return s
            else:
                print(f"WARN: leer für {name} ({t}) [Yahoo]")
        except Exception as e:
            print(f"WARN: Download-Fehler {name} ({t}) [Yahoo]: {e}")
        if i < len(alts):
            time.sleep(PAUSE)

    # 2) Stooq-Fallback für ETFs
    if name in ("SPY", "HYG", "LQD", "XLF"):
        try:
            df = pull_stooq(name, START)
            s = _to_close_series(df, name)
            if not s.empty:
                save_raw(s)
                return s
            else:
                print(f"WARN: Fallback Stooq leer für {name}")
        except Exception as e:
            print(f"WARN: Fallback Stooq {name}: {e}")

    # 3) leer
    return pd.Series(dtype=float, name=name)

# -------------------- Main --------------------

def main() -> int:
    # Sicherstellen, dass yfinance vorhanden ist
    try:
        import yfinance  # noqa: F401
    except Exception as e:
        print("WARN: yfinance nicht installiert → überspringe Market-Core. Fehler:", e)
        write_empty_gzip_csv(OUT_DIR / "market_core.csv.gz")
        return 0

    series: list[pd.Series] = []
    for k, v in SYMS.items():
        s = pull_one(k, v)
        if not s.empty:
            series.append(s)

    # VIX-Proxy als Notnagel, falls VIX fehlt aber SPY vorhanden ist
    have = {s.name for s in series}
    if "VIX" not in have:
        spy = next((s for s in series if s.name == "SPY"), None)
        if spy is not None and not spy.empty:
            r = spy.pct_change()
            hv20 = r.rolling(20).std() * math.sqrt(252) * 100.0
            vix_proxy = hv20.rename("VIX").dropna()
            if not vix_proxy.empty:
                save_raw(vix_proxy)
                series.append(vix_proxy)
                print("INFO: VIX-Proxy aus SPY-HV20 erzeugt.")

    if not series:
        print("ERROR: keine Market-Reihen erhalten – RiskIndex nutzt dann nur FRED.")
        write_empty_gzip_csv(OUT_DIR / "market_core.csv.gz")
        return 0

    df = pd.concat(series, axis=1).sort_index()
    df.index.name = "date"

    # auf Tagesgitter + FFill (vereinheitlicht für spätere Joins)
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    out = OUT_DIR / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print(f"✔ wrote {out}  cols={list(df.columns)}  rows={len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
