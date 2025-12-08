#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_market_core.py
Mergt vorhandene data/market/core/*.csv zu data/processed/market_core.csv.gz
– nur verwenden, wenn fetch_market_core.py die Rohdateien bereits erzeugt hat.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

RAW = Path("data/market/core")
OUT = Path("data/processed")
OUT.mkdir(parents=True, exist_ok=True)

def main() -> int:
    parts = []
    for p in sorted(RAW.glob("*.csv")):
        try:
            s = pd.read_csv(p, parse_dates=["date"])
            if "value" not in s.columns: 
                continue
            name = p.stem
            s = s[["date", "value"]].dropna()
            s = s.rename(columns={"value": name}).set_index("date")
            parts.append(s)
        except Exception as e:
            print("WARN:", p, e)

    if not parts:
        print("WARN: keine Rohdateien in", RAW)
        return 0

    df = pd.concat(parts, axis=1).sort_index()
    df.index.name = "date"
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    out = OUT / "market_core.csv.gz"
    df.to_csv(out, index=True, float_format="%.6f", compression="gzip")
    print("✔ wrote", out, "cols=", list(df.columns), "rows=", len(df))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
