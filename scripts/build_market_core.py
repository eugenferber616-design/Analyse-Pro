#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_market_core.py
Lädt robuste Markt-Proxy-Serien (yfinance) und schreibt:
  data/processed/market_core.csv.gz  (date,VIX,VIX3M,DXY,USDJPY,HYG,LQD,XLF,SPY)
- Lädt JE Symbol einzeln (robust gegen 1-2 Ausfälle)
- Mehrere Fallback-Symbole (DXY, USDJPY)
- Bricht NICHT hart ab: schreibt Stub, wenn gar nichts kommt
"""

from __future__ import annotations
import sys, math, time
from pathlib import Path
import pandas as pd

try:
    import yfinance as yf
except Exception as e:
    print("ERROR: yfinance fehlt. `pip install yfinance`", e)
    sys.exit(0)  # weich raus; build_riskindex.py hat Fallback

PROCESSED = Path("data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

# --------- Symbol-Mapping mit Fallbacks ---------
# VIX / VIX3M sind ^-Ticker (Index)
SYMS = {
    "VIX":    ["^VIX"],
    "VIX3M":  ["^VIX3M"],
    # DXY: ICE DX-Y.NYB (häufig ok); Fallback: UUP ETF (Proxy)
    "DXY":    ["DX-Y.NYB", "DXY", "UUP"],
    # USDJPY in yfinance: "JPY=X" (NICHT USDJPY=X); Fallback: "USDJPY=X" (einige Mirrors)
    "USDJPY": ["JPY=X", "USDJPY=X"],
    "HYG":    ["HYG"],
    "LQD":    ["LQD"],
    "XLF":    ["XLF"],
    "SPY":    ["SPY"],
}

START = "2007-01-01"
TIMEOUT_SLEEP = 0.6  # Sekunden zwischen Requests

def load_one(name: str, tickers: list[str]) -> pd.Series:
    """Lädt Close/Adj Close für das erste funktionierende Ticker-Alias."""
    for t in tickers:
        try:
            # Einzelsymbol-Load; group_by ist egal, wir nehmen .Close/.Adj Close
            df = yf.download(t, start=START, progress=False, auto_adjust=False, threads=False)
            # yfinance kann leere Frames mit richtigen Columns liefern -> prüfen
            if df is None or df.empty:
                print(f"WARN: leer für {name} ({t})")
                continue
            # Spalten-Auswahl: bevorzugt Adj Close, sonst Close
            col = "Adj Close" if "Adj Close" in df.columns else ("Close" if "Close" in df.columns else None)
            if col is None:
                print(f"WARN: keine Close-Spalte für {name} ({t}) → Columns={list(df.columns)}")
                continue
            s = pd.to_numeric(df[col], errors="coerce").dropna()
            if s.empty:
                print(f"WARN: nur NaN für {name} ({t})")
                continue
            s.name = name
            print(f"OK: {name} via {t} (rows={len(s)})")
            return s
        except Exception as e:
            print(f"WARN: Download-Fehler {name} ({t}): {e}")
        finally:
            time.sleep(TIMEOUT_SLEEP)
    return pd.Series(dtype=float, name=name)

def main() -> int:
    series = []
    for name, alts in SYMS.items():
        s = load_one(name, alts)
        if not s.empty:
            series.append(s)

    if not series:
        # Stub schreiben, aber OK zurück (RiskIndex hat Fallback/Stub)
        out = PROCESSED / "market_core.csv.gz"
        out.write_text("date\n", encoding="utf-8")
        print("ERROR: Keine Marktserien geladen. Stub geschrieben →", out)
        return 0

    df = pd.concat(series, axis=1).sort_index()
    df.index.name = "date"
    # auf Tagesfreq auffüllen (für spätere Joins sauber)
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    out = PROCESSED / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print("✔ wrote", out, "cols:", list(df.columns), "rows:", len(df))
    return 0

if __name__ == "__main__":
    sys.exit(main())
