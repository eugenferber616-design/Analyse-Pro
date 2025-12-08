#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_hv_summary.py
-------------------
Berechnet annualisierte HV20 / HV60 aus lokalen Price-Files:

Input:
- data/prices/{SYMBOL}.csv  (Spalten: Date/Adj Close oder Date/Close)

Watchlist:
- per --watchlist (Fallback: ENV WATCHLIST_STOCKS oder watchlists/mylist.txt)

Output:
- data/processed/hv_summary.csv.gz  (oder .csv, je nach --out)
  Spalten: symbol, hv20, hv60, asof
"""

import argparse
import csv
import os
from datetime import datetime

import numpy as np
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--watchlist",
        default=os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt"),
        help="Pfad zur Watchlist (TXT/CSV, erste Spalte = Symbol)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=252,
        help="Maximale Anzahl Tage für die Historie (wird hier nur für Info genutzt)",
    )
    p.add_argument(
        "--out",
        default="data/processed/hv_summary.csv.gz",
        help="Output-Datei (.csv oder .csv.gz)",
    )
    return p.parse_args()


def load_symbols(path):
    syms = []
    if not os.path.exists(path):
        return syms

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(",")
            sym = parts[0].strip()
            if sym and sym.upper() != "SYMBOL":
                syms.append(sym)
    return sorted(set(syms))


def realized_vol(returns: pd.Series, window: int) -> float:
    """Annualisierte Realized Vol in %, basierend auf daily log-returns."""
    returns = returns.dropna()
    if len(returns) < window:
        return float("nan")
    r = returns.iloc[-window:]
    # 252 Trading-Tage, ddof=1 für Sample-Std
    hv = np.sqrt(252.0) * np.std(r, ddof=1) * 100.0
    return float(hv)


def compute_hv_for_symbol(sym: str):
    price_path = os.path.join("data", "prices", f"{sym}.csv")
    if not os.path.exists(price_path):
        # Kein lokaler Preis -> None zurück (wird später übersprungen)
        return None

    try:
        df = pd.read_csv(price_path)
    except Exception:
        return None

    # Spalten-Normalisierung
    cols_lower = {c.lower(): c for c in df.columns}
    if "date" not in cols_lower:
        return None

    date_col = cols_lower["date"]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)

    # Close/Adj Close finden
    close_col = None
    for key in ["adj close", "adj_close", "close"]:
        if key in cols_lower:
            close_col = cols_lower[key]
            break

    if close_col is None:
        return None

    close = df[close_col].astype(float)
    if close.dropna().empty:
        return None

    # Log-Returns
    log_ret = np.log(close / close.shift(1))

    hv20 = realized_vol(log_ret, 20)
    hv60 = realized_vol(log_ret, 60)

    asof = df[date_col].iloc[-1].date().isoformat()

    return {
        "symbol": sym,
        "hv20": round(hv20, 2) if not np.isnan(hv20) else None,
        "hv60": round(hv60, 2) if not np.isnan(hv60) else None,
        "asof": asof,
    }


def main():
    args = parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    symbols = load_symbols(args.watchlist)
    print(f"Calculating HV for {len(symbols)} symbols from local files...")

    results = []
    errors = []

    for sym in symbols:
        try:
            rec = compute_hv_for_symbol(sym)
            if rec is not None:
                results.append(rec)
                print(f"[HV] {sym}: hv20={rec['hv20']}, hv60={rec['hv60']}")
            else:
                errors.append(sym)
                print(f"[HV] {sym}: SKIP (keine gültigen Daten)")
        except Exception as e:
            errors.append(sym)
            print(f"[HV] {sym}: ERROR: {e}")

    df_out = pd.DataFrame(results)

    # Schreiben mit oder ohne GZIP abhängig von der Endung
    if args.out.endswith(".gz"):
        df_out.to_csv(args.out, index=False, compression="gzip")
    else:
        df_out.to_csv(args.out, index=False)

    print(f"Done. Wrote {len(df_out)} rows to {args.out}")

    # Kleiner Report
    rep_path = os.path.join("data", "reports", "hv_report.json")
    os.makedirs(os.path.dirname(rep_path), exist_ok=True)
    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "symbols_total": len(symbols),
        "symbols_ok": len(df_out),
        "symbols_error": errors,
        "out": args.out,
    }
    try:
        import json

        with open(rep_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
    except Exception:
        pass

    if errors:
        print(f"Errors: {len(errors)} (See {rep_path})")
    else:
        print("No HV errors.")


if __name__ == "__main__":
    main()
