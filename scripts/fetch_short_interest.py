#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Short Interest + Float + Borrow von Finnhub
und speichere sie als data/processed/short_interest.csv.

Voraussetzungen:
- ENV: FINNHUB_TOKEN oder FINNHUB_API_KEY
- ENV: WATCHLIST_STOCKS, WATCHLIST_ETF (CSV oder TXT)
"""

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

BASE = "https://finnhub.io/api/v1"
TOKEN = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY")


def q(url, params):
    """Kleiner Request-Wrapper mit Retry."""
    params = dict(params or {})
    if TOKEN:
        params["token"] = TOKEN
    for i in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(1 + 2 * i)
    return None


def read_symbols(path):
    """Liest Symbole aus CSV (Spalte 'symbol' oder erste Spalte) oder TXT."""
    if not path or not os.path.exists(path):
        return []

    # einfache Heuristik: CSV vs. TXT
    _, ext = os.path.splitext(path)
    ext = ext.lower()

    if ext in (".csv", ".tsv"):
        df = pd.read_csv(path)
        col = "symbol" if "symbol" in df.columns else df.columns[0]
        return [str(x).strip().upper() for x in df[col].dropna().tolist()]

    # TEXT: eine Zeile = ein Symbol
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip().upper() for line in f if line.strip()]


def main():
    if not TOKEN:
        raise SystemExit("FINNHUB_TOKEN / FINNHUB_API_KEY fehlt in den Umgebungsvariablen")

    wl_stocks = read_symbols(os.getenv("WATCHLIST_STOCKS"))
    wl_etf    = read_symbols(os.getenv("WATCHLIST_ETF"))
    wl        = sorted(set(wl_stocks + wl_etf))

    if not wl:
        print("WARN: Watchlist leer – keine Symbole gefunden.")
        return

    out_path = "data/processed/short_interest.csv"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    rows = []

    fr = (datetime.utcnow() - timedelta(days=400)).strftime("%Y-%m-%d")
    to = datetime.utcnow().strftime("%Y-%m-%d")

    print("== Short Interest Pull (Finnhub) für", len(wl), "Symbole ==")

    for sym in wl:
        print("  ->", sym)
        try:
            # 1) Short Interest Historie
            si = q(BASE + "/stock/short-interest", {"symbol": sym, "from": fr, "to": to}) or {}
            si_data = si.get("data") if isinstance(si, dict) else None
            si_last = (si_data or [{}])[-1] if si_data else {}

            # 2) Borrow (Leihgebühr / verfügbare Shares)
            br = q(BASE + "/stock/borrow", {"symbol": sym}) or {}
            br_last = (br.get("data") or [{}])[-1] if isinstance(br, dict) else {}

            # 3) Float (frei handelbare Aktien)
            fl = q(BASE + "/stock/float", {"symbol": sym}) or {}
            float_sh = fl.get("floatShares") if isinstance(fl, dict) else None

            # Rohwerte
            si_sh = si_last.get("shortInterest") or si_last.get("short_interest")
            si_dt = si_last.get("date") or si_last.get("t")
            br_dt = br_last.get("date") or br_last.get("t")

            # Prozent vom Float berechnen
            pct_float = None
            try:
                if float_sh and si_sh:
                    pct_float = 100.0 * float(si_sh) / float(float_sh)
            except Exception:
                pct_float = None

            rows.append({
                "symbol":       sym,
                "si_source":    "finnhub",
                "si_date":      si_dt,
                "si_shares":    si_sh,
                "float_shares": float_sh,
                "si_pct_float": pct_float,
                "borrow_date":  br_dt,
                "borrow_rate":  br_last.get("rate") or br_last.get("feeRate") or br_last.get("fr"),
                "borrow_avail": br_last.get("available") or br_last.get("shares"),
            })
        except Exception as e:
            print("   !! Fehler bei", sym, ":", e)

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    print("wrote", out_path, "rows", len(df))


if __name__ == "__main__":
    main()
