#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch options OI + simple IV/HV summary via yfinance.

Output
- CSV:  data/processed/options_oi_summary.csv
- JSON: data/reports/options_oi_report.json
"""

import os, sys, json, math
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd
import yfinance as yf

# ------------ helpers ------------
def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

def read_watchlist(path: str) -> List[str]:
    """Accepts txt or csv. If csv: expects a 'symbol' column."""
    if not os.path.exists(path):
        return []
    if path.lower().endswith(".csv"):
        try:
            df = pd.read_csv(path)
            if "symbol" in df.columns:
                syms = [str(s).strip() for s in df["symbol"].dropna().tolist()]
                return [s for s in syms if s]
        except Exception:
            pass
    # fallback: txt (one symbol per line), ignore header named 'symbol'
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and s.lower() != "symbol":
                out.append(s)
    return out

def annualize_vol(returns: pd.Series) -> float:
    """Annualized stdev of daily log returns (252 trading days)."""
    if returns is None or returns.empty:
        return None
    return float(returns.std(ddof=0) * math.sqrt(252))

def compute_hv(hist: pd.DataFrame, win: int) -> float:
    if hist is None or hist.empty or "Close" not in hist.columns or len(hist) < max(5, win+1):
        return None
    lr = np.log(hist["Close"]).diff().dropna()
    if len(lr) < win:
        return None
    return annualize_vol(lr.tail(win))

def wavg_iv(df: pd.DataFrame) -> float:
    """Weighted IV by openInterest; falls back to simple mean."""
    if df is None or df.empty:
        return None
    if "impliedVolatility" not in df.columns:
        return None
    d = df.dropna(subset=["impliedVolatility"])
    if d.empty:
        return None
    if "openInterest" in d.columns and d["openInterest"].sum() > 0:
        w = d["openInterest"].astype(float)
        return float((d["impliedVolatility"] * w).sum() / w.sum())
    return float(d["impliedVolatility"].mean())

def top_strikes(df: pd.DataFrame, k: int) -> str:
    if df is None or df.empty or "openInterest" not in df.columns or "strike" not in df.columns:
        return ""
    d = df[["strike", "openInterest"]].copy()
    d = d.sort_values("openInterest", ascending=False).head(max(1, k))
    return ",".join(str(x) for x in d["strike"].tolist())

# ------------ main ------------
def main():
    ensure_dirs()

    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries = int(os.getenv("OPTIONS_MAX_EXPIRIES", "4"))
    topk          = int(os.getenv("OPTIONS_TOPK", "3"))
    hv_win_short  = int(os.getenv("HV_WIN_SHORT", "20"))
    hv_win_long   = int(os.getenv("HV_WIN_LONG", "60"))

    symbols = read_watchlist(wl_path)
    if not symbols:
        print(f"watchlist empty: {wl_path}")
        symbols = ["AAPL"]

    rows = []
    errors = []
    for sym in symbols:
        try:
            tk = yf.Ticker(sym)

            # underlying history for HV
            hist = tk.history(period="400d", interval="1d", auto_adjust=False)
            hv20 = compute_hv(hist, hv_win_short)
            hv60 = compute_hv(hist, hv_win_long)
            spot = float(hist["Close"].dropna().iloc[-1]) if ("Close" in hist and not hist["Close"].dropna().empty) else None

            expiries = []
            try:
                expiries = list(tk.options or [])
            except Exception as e:
                errors.append({"symbol": sym, "stage": "options_list", "msg": str(e)})

            if not expiries:
                rows.append({
                    "symbol": sym, "expiry": "", "spot": spot,
                    "call_oi": 0, "put_oi": 0, "put_call_ratio": None,
                    "call_iv_w": None, "put_iv_w": None,
                    "call_top_strikes": "", "put_top_strikes": "",
                    "hv20": hv20, "hv60": hv60,
                })
                continue

            for exp in expiries[:max_expiries]:
                try:
                    ch = tk.option_chain(exp)
                    calls = ch.calls if hasattr(ch, "calls") else pd.DataFrame()
                    puts  = ch.puts  if hasattr(ch, "puts")  else pd.DataFrame()

                    c_oi = int(calls["openInterest"].fillna(0).sum()) if "openInterest" in calls.columns else 0
                    p_oi = int(puts["openInterest"].fillna(0).sum())  if "openInterest" in puts.columns  else 0
                    pcr  = (float(p_oi) / float(c_oi)) if c_oi > 0 else (float("inf") if p_oi > 0 else None)

                    c_iv = wavg_iv(calls)
                    p_iv = wavg_iv(puts)

                    c_top = top_strikes(calls, topk)
                    p_top = top_strikes(puts,  topk)

                    rows.append({
                        "symbol": sym, "expiry": exp, "spot": spot,
                        "call_oi": c_oi, "put_oi": p_oi, "put_call_ratio": pcr,
                        "call_iv_w": c_iv, "put_iv_w": p_iv,
                        "call_top_strikes": c_top, "put_top_strikes": p_top,
                        "hv20": hv20, "hv60": hv60,
                    })
                except Exception as e:
                    errors.append({"symbol": sym, "stage": f"chain:{exp}", "msg": str(e)})

        except Exception as e:
            errors.append({"symbol": sym, "stage": "ticker", "msg": str(e)})

    # write outputs
    out_csv = "data/processed/options_oi_summary.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    print("wrote", out_csv, "rows=", len(rows))

    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "watchlist": wl_path,
        "symbols": len(symbols),
        "rows": len(rows),
        "errors": errors,
    }
    with open("data/reports/options_oi_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    if errors:
        print("option errors:", len(errors))

    return 0

if __name__ == "__main__":
    sys.exit(main())
