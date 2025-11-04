#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_market_core.py
Zieht Markt-Proxy-Serien (yfinance) EINZELN & robust.

Schreibt:
  - data/market/core/{NAME}.csv          (Roh, Spalten: date,value)
  - data/processed/market_core.csv.gz    (Merge aller verfügbaren Reihen)

Fixes:
- Einzel-Downloads → keine MultiIndex/tuple-Probleme
- Richtige Ticker (DXY & USDJPY) inkl. Fallbacks
- VIX3M-Fallback auf ^VXV
- Sanfte Retries bei Netzwerkfehlern
"""

from __future__ import annotations
from pathlib import Path
import os, sys, time
from typing import List
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("WARN: yfinance nicht installiert → überspringe Market-Core. Fehler:", e)
    sys.exit(0)

RAW_DIR = Path("data/market/core")
OUT_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

START  = os.getenv("MARKET_START", "2007-01-01")
PAUSE  = float(os.getenv("MARKET_PAUSE_SEC", "0.6"))   # Pause zwischen Versuchen
RETRIES = int(os.getenv("MARKET_RETRIES", "2"))        # zusätzliche Versuche je Ticker

# Ticker + Fallbacks
SYMS = {
    "VIX":    ["^VIX"],
    "VIX3M":  ["^VIX3M", "^VXV"],           # Fallback
    # DXY: je nach Yahoo-Umgebung mal DX-Y.NYB, mal DXY – UUP als Proxy
    "DXY":    ["DX-Y.NYB", "DXY", "UUP"],
    # FX: korrekt ist JPY=X (USD pro 1 JPY). Alternativ USDJPY=X.
    "USDJPY": ["JPY=X", "USDJPY=X"],
    "HYG":    ["HYG"],
    "LQD":    ["LQD"],
    "XLF":    ["XLF"],
    "SPY":    ["SPY"],
}

def pull_one(name: str, alts: List[str]) -> pd.Series:
    """Lädt eine Serie mit Fallback-Tickern und Retries, gibt Series(date-index) zurück."""
    for t in alts:
        for attempt in range(RETRIES + 1):
            try:
                df = yf.download(
                    t, start=START, progress=False, auto_adjust=False, threads=False
                )
                if df is None or df.empty:
                    print(f"WARN: leer für {name} ({t})")
                    break  # nächster Fallback-Ticker
                col = "Adj Close" if "Adj Close" in df.columns else (
                      "Close"     if "Close"     in df.columns else None)
                if col is None:
                    print(f"WARN: keine Close-Spalte für {name} ({t}) → {list(df.columns)}")
                    break
                s = pd.to_numeric(df[col], errors="coerce").dropna()
                if s.empty:
                    print(f"WARN: nur NaN für {name} ({t})")
                    break
                # Index glattziehen (naiv, ohne TZ)
                s.index = pd.to_datetime(s.index).tz_localize(None)
                s.name = name
                # Roh speichern
                out_raw = RAW_DIR / f"{name}.csv"
                s.to_frame("value").rename_axis("date").to_csv(out_raw, index=True)
                print(f"OK: {name} via {t} (rows={len(s)}) → {out_raw}")
                return s
            except Exception as e:
                if attempt < RETRIES:
                    print(f"WARN: Download-Fehler {name} ({t}) Versuch {attempt+1}/{RETRIES}: {e}")
                    time.sleep(PAUSE)
                    continue
                print(f"WARN: Download endgültig fehlgeschlagen für {name} ({t}): {e}")
            finally:
                time.sleep(PAUSE)
        # nächster Fallback-Ticker
    # Nichts bekommen
    return pd.Series(dtype=float, name=name)

def main() -> int:
    series = []
    for k, v in SYMS.items():
        s = pull_one(k, v)
        if not s.empty:
            series.append(s)

    if not series:
        print("ERROR: keine Market-Reihen erhalten – kein Merge möglich. (RiskIndex nutzt dann FRED.)")
        # Kein harter Fehler → Exit 0, damit Pipeline weiterlaufen kann
        return 0

    # Zusammenführen
    df = pd.concat(series, axis=1).sort_index()
    df.index.name = "date"

    # Tägliches Grid + FFill (für spätere Joins)
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    out = OUT_DIR / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print(f"✔ wrote {out}  cols={list(df.columns)}  rows={len(df)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
