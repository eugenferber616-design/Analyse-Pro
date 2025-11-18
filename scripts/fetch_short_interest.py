#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Short Interest + Float + Borrow von Finnhub
PLUS Borrow-Daten von iBorrowDesk (Website-Scrape, nur US-Ticker).

Ergebnis:
  data/processed/short_interest.csv

Spalten (wichtig für equity_master):
  symbol, si_date, si_shares, float_shares, si_pct_float,
  borrow_date, borrow_rate, borrow_avail

Zusatzspalten zur Diagnose:
  si_source, ibd_rebate, ibd_high_available, ibd_low_available,
  ibd_high_fee, ibd_low_fee, ibd_high_rebate, ibd_low_rebate,
  ibd_status, fh_si_status, fh_borrow_status, fh_float_status
"""

import os
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

import pandas as pd
import requests

FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_TOKEN = os.getenv("FINNHUB_TOKEN") or os.getenv("FINNHUB_API_KEY")

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
    print("US-Universum für Short Interest:", uni)
    return uni

# ---------------------------------------------------------------------------
# Finnhub-Wrapper
# ---------------------------------------------------------------------------

def fh_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = dict(params or {})
    if FINNHUB_TOKEN:
        params["token"] = FINNHUB_TOKEN
    url = FINNHUB_BASE + path
    for i in range(3):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                return r.json() or {}
        except Exception:
            pass
        time.sleep(1 + 2 * i)
    return {}


def fetch_finnhub_blocks(sym: str, fr: str, to: str) -> Dict[str, Any]:
    """Holt Short-Interest, Borrow, Float von Finnhub für ein Symbol."""
    out: Dict[str, Any] = {
        "fh_si_status": "none",
        "fh_borrow_status": "none",
        "fh_float_status": "none",
        "si_shares": None,
        "si_date": None,
        "float_shares": None,
        "borrow_date_fh": None,
        "borrow_rate_fh": None,
        "borrow_avail_fh": None,
    }

    # Short interest
    try:
        si = fh_get("/stock/short-interest", {"symbol": sym, "from": fr, "to": to})
        data = si.get("data") if isinstance(si, dict) else None
        last = (data or [{}])[-1] if data else {}
        if last:
            out["fh_si_status"] = "ok"
            out["si_shares"] = last.get("shortInterest") or last.get("short_interest")
            out["si_date"] = last.get("date") or last.get("t")
        else:
            out["fh_si_status"] = "empty"
    except Exception as e:
        out["fh_si_status"] = f"error:{e}"

    # Borrow
    try:
        br = fh_get("/stock/borrow", {"symbol": sym})
        data = br.get("data") if isinstance(br, dict) else None
        last = (data or [{}])[-1] if data else {}
        if last:
            out["fh_borrow_status"] = "ok"
            out["borrow_date_fh"] = last.get("date") or last.get("t")
            out["borrow_rate_fh"] = last.get("rate") or last.get("feeRate") or last.get("fr")
            out["borrow_avail_fh"] = last.get("available") or last.get("shares")
        else:
            out["fh_borrow_status"] = "empty"
    except Exception as e:
        out["fh_borrow_status"] = f"error:{e}"

    # Float
    try:
        fl = fh_get("/stock/float", {"symbol": sym})
        if fl:
            out["fh_float_status"] = "ok"
            out["float_shares"] = fl.get("floatShares")
        else:
            out["fh_float_status"] = "empty"
    except Exception as e:
        out["fh_float_status"] = f"error:{e}"

    return out

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
    if not FINNHUB_TOKEN:
        print("WARN: FINNHUB_TOKEN / FINNHUB_API_KEY fehlt – Finnhub-Teil wird leer sein.")
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    universe = build_universe()
    if not universe:
        print("Keine US-Symbole für Short Interest gefunden.")
        return

    fr = (datetime.utcnow() - timedelta(days=400)).strftime("%Y-%m-%d")
    to = datetime.utcnow().strftime("%Y-%m-%d")

    rows = []

    print("== Short Interest Pull (Finnhub + iBorrowDesk) für", len(universe), "US-Symbole ==")

    for i, sym in enumerate(universe, start=1):
        print(f"[{i}/{len(universe)}] {sym} …")
        # Finnhub
        fh = fetch_finnhub_blocks(sym, fr, to) if FINNHUB_TOKEN else {
            "fh_si_status": "no_token",
            "fh_borrow_status": "no_token",
            "fh_float_status": "no_token",
            "si_shares": None,
            "si_date": None,
            "float_shares": None,
            "borrow_date_fh": None,
            "borrow_rate_fh": None,
            "borrow_avail_fh": None,
        }
        # iBorrowDesk
        ibd = fetch_ibd(sym)

        # si_pct_float berechnen (nur Finnhub-Teil)
        si_pct = None
        try:
            if fh.get("si_shares") is not None and fh.get("float_shares") not in (None, 0):
                si_pct = 100.0 * float(fh["si_shares"]) / float(fh["float_shares"])
        except Exception:
            si_pct = None

        # Quelle zusammensetzen
        sources = []
        if fh.get("fh_si_status") not in ("none", "empty", "no_token"):
            sources.append("fh_si")
        if fh.get("fh_borrow_status") not in ("none", "empty", "no_token"):
            sources.append("fh_borrow")
        if fh.get("fh_float_status") not in ("none", "empty", "no_token"):
            sources.append("fh_float")
        if ibd.get("ibd_status") == "ok":
            sources.append("ibd")
        si_source = "+".join(sources) if sources else "none"

        # Borrow-Felder: Priorität Finnhub, Fallback iBorrowDesk
        borrow_date = fh.get("borrow_date_fh") or ibd.get("ibd_date")
        borrow_rate = fh.get("borrow_rate_fh") or ibd.get("ibd_fee")
        borrow_avail = fh.get("borrow_avail_fh") or ibd.get("ibd_available")

        row = {
            "symbol":       sym,
            "si_source":    si_source,
            # Short-Interest/Float (Finnhub-basiert)
            "si_date":      fh.get("si_date") or ibd.get("ibd_date"),
            "si_shares":    fh.get("si_shares"),
            "float_shares": fh.get("float_shares"),
            "si_pct_float": si_pct,
            # Borrow kombiniert
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
            "fh_si_status":        fh.get("fh_si_status"),
            "fh_borrow_status":    fh.get("fh_borrow_status"),
            "fh_float_status":     fh.get("fh_float_status"),
        }
        rows.append(row)
        # leichter Delay, um iBorrowDesk nicht zu stressen
        time.sleep(0.3)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_PATH, index=False)
    print(f"wrote {OUT_PATH} rows={len(df)}")


if __name__ == "__main__":
    main()
