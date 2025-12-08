#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_short_interest.py (LIGHT)
-------------------------------
Vereinfachte Version:

- KEIN Finnhub-Short-Interest, KEIN Float mehr.
- Nur noch Borrow-/Fee-Daten von iBorrowDesk für US-Ticker.

Output:
  data/processed/short_interest.csv

Spalten (Schema kompatibel zu vorher, damit Equity Master & Scanner nicht brechen):
  symbol, si_date, si_shares, float_shares, si_pct_float,
  borrow_date, borrow_rate, borrow_avail,
  si_source,
  ibd_rebate, ibd_high_available, ibd_low_available,
  ibd_high_fee, ibd_low_fee, ibd_high_rebate, ibd_low_rebate,
  ibd_status, fh_si_status, fh_borrow_status, fh_float_status
"""

import os
import time
from typing import List, Optional, Dict, Any

import pandas as pd
import requests

IBD_BASE = "https://iborrowdesk.com/api/ticker/"
OUT_PATH = "data/processed/short_interest.csv"

EU_SUFFIXES = (
    ".DE", ".EU", ".AS", ".BR", ".BE", ".PA", ".MI", ".SW", ".L", ".VX", ".VI",
    ".TO", ".V", ".ME", ".HE", ".ST", ".CO", ".OL", ".SS", ".SZ", ".HK"
)

# ---------------------------------------------------------------------------
# Watchlist-Helfer
# ---------------------------------------------------------------------------

def read_watchlist_csv(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    col = "symbol" if "symbol" in df.columns else df.columns[0]
    return (
        df[col]
        .dropna()
        .astype(str)
        .tolist()
    )


def read_watchlist_txt(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            if s.lower().startswith("symbol"):
                continue
            out.append(s)
    return out


def clean_to_us_symbol(raw: str) -> Optional[str]:
    """
    Wandelt Roh-Einträge wie
      'AAPL,US_IG'
      'EUNL.DE # ISHARES CORE MSCI WORLD UCITS'
    in saubere US-Ticker um oder wirft sie weg (EU/sonstiges).
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None
    # Kommentare entfernen
    if "#" in s:
        s = s.split("#", 1)[0].strip()
    # interne Suffixe wie ,US_IG / ,EU_IG entfernen
    if "," in s:
        s = s.split(",", 1)[0].strip()
    if not s:
        return None
    # EU-/Nicht-US-Listings hart rausfiltern
    for suf in EU_SUFFIXES:
        if s.endswith(suf):
            return None
    # sehr einfache Plausibilitätsprüfung: nur A–Z/0–9/.-_ zulassen
    for ch in s:
        if ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._":
            return None
    return s


def build_universe() -> List[str]:
    wl_stocks = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.csv")
    wl_etf    = os.getenv("WATCHLIST_ETF",    "watchlists/etf_sample.txt")

    raw: List[str] = []

    if wl_stocks.endswith(".txt"):
        raw.extend(read_watchlist_txt(wl_stocks))
    else:
        raw.extend(read_watchlist_csv(wl_stocks))

    if wl_etf.endswith(".txt"):
        raw.extend(read_watchlist_txt(wl_etf))
    else:
        raw.extend(read_watchlist_csv(wl_etf))

    us = set()
    for r in raw:
        sym = clean_to_us_symbol(r)
        if sym:
            us.add(sym)

    uni = sorted(us)
    print("US-Universum für Borrow/Sentiment:", uni)
    return uni

# ---------------------------------------------------------------------------
# iBorrowDesk-Wrapper
# ---------------------------------------------------------------------------

def fetch_ibd(sym: str) -> Dict[str, Any]:
    url = IBD_BASE + sym
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Referer": "https://www.iborrowdesk.com/",
    }
    base_row: Dict[str, Any] = {
        "ibd_status": "none",
        "ibd_date": None,
        "ibd_available": None,
        "ibd_fee": None,
        "ibd_rebate": None,
        "ibd_high_available": None,
        "ibd_low_available": None,
        "ibd_high_fee": None,
        "ibd_low_fee": None,
        "ibd_high_rebate": None,
        "ibd_low_rebate": None,
    }

    try:
        r = requests.get(url, headers=headers, timeout=20)
    except Exception as e:
        base_row["ibd_status"] = f"error_request:{e}"
        return base_row

    if not r.ok:
        base_row["ibd_status"] = f"http_{r.status_code}"
        return base_row

    try:
        j = r.json()
    except Exception as e:
        base_row["ibd_status"] = f"json_error:{e}"
        return base_row

    daily = j.get("daily") or []
    if not daily:
        base_row["ibd_status"] = "no_daily_data"
        return base_row

    last = daily[-1] or {}
    base_row.update({
        "ibd_status": "ok",
        "ibd_date": last.get("date"),
        "ibd_available": last.get("available"),
        "ibd_fee": last.get("fee"),
        "ibd_rebate": last.get("rebate"),
        "ibd_high_available": last.get("high_available"),
        "ibd_low_available": last.get("low_available"),
        "ibd_high_fee": last.get("high_fee"),
        "ibd_low_fee": last.get("low_fee"),
        "ibd_high_rebate": last.get("high_rebate"),
        "ibd_low_rebate": last.get("low_rebate"),
    })
    return base_row

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    universe = build_universe()
    if not universe:
        print("Keine US-Symbole für Borrow/Short-Sentiment gefunden.")
        return

    rows = []

    print("== Borrow / Fee Pull (iBorrowDesk ONLY) für", len(universe), "US-Symbole ==")

    for i, sym in enumerate(universe, start=1):
        print(f"[{i}/{len(universe)}] {sym} …")

        ibd = fetch_ibd(sym)

        borrow_date  = ibd.get("ibd_date")
        borrow_rate  = ibd.get("ibd_fee")
        borrow_avail = ibd.get("ibd_available")

        row = {
            "symbol":       sym,
            # Short-Interest/Float (NICHT mehr befüllt)
            "si_source":    "ibd_only" if ibd.get("ibd_status") == "ok" else "none",
            "si_date":      None,
            "si_shares":    None,
            "float_shares": None,
            "si_pct_float": None,
            # Borrow
            "borrow_date":  borrow_date,
            "borrow_rate":  borrow_rate,
            "borrow_avail": borrow_avail,
            # Diagnose-Felder
            "ibd_rebate":          ibd.get("ibd_rebate"),
            "ibd_high_available":  ibd.get("ibd_high_available"),
            "ibd_low_available":   ibd.get("ibd_low_available"),
            "ibd_high_fee":        ibd.get("ibd_high_fee"),
            "ibd_low_fee":         ibd.get("ibd_low_fee"),
            "ibd_high_rebate":     ibd.get("ibd_high_rebate"),
            "ibd_low_rebate":      ibd.get("ibd_low_rebate"),
            "ibd_status":          ibd.get("ibd_status"),
            # Finnhub-Felder deaktiviert, aber fürs Schema drin
            "fh_si_status":        "disabled",
            "fh_borrow_status":    "disabled",
            "fh_float_status":     "disabled",
        }
        rows.append(row)

        # kleiner Delay, um iBorrowDesk nicht zu stressen
        time.sleep(0.3)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_PATH, index=False)
    print(f"wrote {OUT_PATH} rows={len(df)}")


if __name__ == "__main__":
    main()
