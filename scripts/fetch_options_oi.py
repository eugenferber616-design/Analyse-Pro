#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch options OI + simple IV/HV summary via yfinance.

Outputs
- CSV:  data/processed/options_oi_summary.csv      (per Symbol & Verfall; Call/Put OI, IV_w, ATM-IV, Top-Strikes, HV)
- CSV:  data/processed/options_oi_by_expiry.csv    (per Symbol & Verfall; OI gesamt, Anteil, Rang)
- CSV:  data/processed/options_oi_totals.csv       (per Symbol; Totals & Verfall mit max. OI)
- JSON: data/reports/options_oi_report.json        (Fehler/Meta)
"""

import os, sys, json, math
from datetime import datetime
from typing import List, Optional

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

def annualize_vol(returns: pd.Series) -> Optional[float]:
    """Annualized stdev of daily log returns (252 trading days)."""
    if returns is None or len(returns) == 0:
        return None
    return float(returns.std(ddof=0) * math.sqrt(252))

def compute_hv(hist: pd.DataFrame, win: int) -> Optional[float]:
    if hist is None or hist.empty or "Close" not in hist.columns or len(hist) < max(5, win+1):
        return None
    lr = np.log(hist["Close"]).diff().dropna()
    if len(lr) < win:
        return None
    return annualize_vol(lr.tail(win))

def wavg_iv(df: pd.DataFrame) -> Optional[float]:
    """Weighted IV by openInterest; falls back to simple mean."""
    if df is None or df.empty or "impliedVolatility" not in df.columns:
        return None
    d = df.dropna(subset=["impliedVolatility"]).copy()
    if d.empty:
        return None
    if "openInterest" in d.columns and d["openInterest"].fillna(0).sum() > 0:
        w = d["openInterest"].fillna(0).astype(float)
        return float((d["impliedVolatility"] * w).sum() / w.sum())
    return float(d["impliedVolatility"].mean())

def atm_iv(df: pd.DataFrame, spot: Optional[float]) -> Optional[float]:
    """IV am Strike, der dem Spot am nächsten liegt."""
    if spot is None or df is None or df.empty:
        return None
    if "strike" not in df.columns or "impliedVolatility" not in df.columns:
        return None
    d = df.dropna(subset=["strike", "impliedVolatility"]).copy()
    if d.empty:
        return None
    d["dist"] = (d["strike"] - float(spot)).abs()
    row = d.sort_values("dist").head(1)
    if row.empty:
        return None
    return float(row["impliedVolatility"].iloc[0])

def top_strikes(df: pd.DataFrame, k: int) -> str:
    if df is None or df.empty or "openInterest" not in df.columns or "strike" not in df.columns:
        return ""
    d = df[["strike", "openInterest"]].copy()
    d = d.sort_values("openInterest", ascending=False).head(max(1, k))
    return ",".join(str(x) for x in d["strike"].tolist())

# ------------ main ------------
def main():
    ensure_dirs()

    wl_path       = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries  = int(os.getenv("OPTIONS_MAX_EXPIRIES", "4"))
    topk          = int(os.getenv("OPTIONS_TOPK", "3"))
    hv_win_short  = int(os.getenv("HV_WIN_SHORT", "20"))
    hv_win_long   = int(os.getenv("HV_WIN_LONG", "60"))

    symbols = read_watchlist(wl_path)
    if not symbols:
        print(f"watchlist empty: {wl_path}")
        symbols = ["AAPL"]

    summary_rows = []   # per symbol & expiry
    errors = []

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)

            # underlying history for HV
            hist = tk.history(period="400d", interval="1d", auto_adjust=False)
            hv20 = compute_hv(hist, hv_win_short)
            hv60 = compute_hv(hist, hv_win_long)
            spot = float(hist["Close"].dropna().iloc[-1]) if ("Close" in hist and not hist["Close"].dropna().empty) else None

            try:
                expiries = list(tk.options or [])
            except Exception as e:
                expiries = []
                errors.append({"symbol": sym, "stage": "options_list", "msg": str(e)})

            if not expiries:
                summary_rows.append({
                    "symbol": sym, "expiry": "", "spot": spot,
                    "call_oi": 0, "put_oi": 0, "oi_total": 0, "put_call_ratio": None,
                    "call_iv_w": None, "put_iv_w": None,
                    "call_iv_atm": None, "put_iv_atm": None,
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
                    tot  = c_oi + p_oi
                    pcr  = (float(p_oi) / float(c_oi)) if c_oi > 0 else (float("inf") if p_oi > 0 else None)

                    c_iv_w   = wavg_iv(calls)
                    p_iv_w   = wavg_iv(puts)
                    c_iv_atm = atm_iv(calls, spot)
                    p_iv_atm = atm_iv(puts,  spot)

                    c_top = top_strikes(calls, topk)
                    p_top = top_strikes(puts,  topk)

                    summary_rows.append({
                        "symbol": sym, "expiry": exp, "spot": spot,
                        "call_oi": c_oi, "put_oi": p_oi, "oi_total": tot, "put_call_ratio": pcr,
                        "call_iv_w": c_iv_w, "put_iv_w": p_iv_w,
                        "call_iv_atm": c_iv_atm, "put_iv_atm": p_iv_atm,
                        "call_top_strikes": c_top, "put_top_strikes": p_top,
                        "hv20": hv20, "hv60": hv60,
                    })
                except Exception as e:
                    errors.append({"symbol": sym, "stage": f"chain:{exp}", "msg": str(e)})
        except Exception as e:
            errors.append({"symbol": sym, "stage": "ticker", "msg": str(e)})

    # -------- write summary (per symbol & expiry)
    summary_df = pd.DataFrame(summary_rows)
    out_summary = "data/processed/options_oi_summary.csv"
    summary_df.to_csv(out_summary, index=False)
    print("wrote", out_summary, "rows=", len(summary_df))

    # -------- build per-expiry aggregation (rank/share)
    by_expiry_df = pd.DataFrame()
    totals_df    = pd.DataFrame()
    if not summary_df.empty:
        tmp = summary_df.copy()
        # Sicherstellen, dass oi_total vorhanden ist (für Altlauf)
        if "oi_total" not in tmp.columns:
            tmp["oi_total"] = tmp[["call_oi","put_oi"]].sum(axis=1)

        # Per Symbol Gesamt-OI je Verfall + Rang/Share
        g = tmp.groupby(["symbol","expiry"], dropna=False, as_index=False).agg(
            oi_total=("oi_total","sum"),
            call_oi=("call_oi","sum"),
            put_oi=("put_oi","sum"),
            spot=("spot","last"),
            hv20=("hv20","last"),
            hv60=("hv60","last"),
        )
        # Rang & Anteil pro Symbol
        g["rank_in_symbol"] = g.sort_values(["symbol","oi_total"], ascending=[True, False]) \
                                .groupby("symbol").cumcount()+1
        g["oi_share_pct"] = 100 * g["oi_total"] / g.groupby("symbol")["oi_total"].transform("sum")
        by_expiry_df = g.sort_values(["symbol","rank_in_symbol"])
        out_byexp = "data/processed/options_oi_by_expiry.csv"
        by_expiry_df.to_csv(out_byexp, index=False)
        print("wrote", out_byexp, "rows=", len(by_expiry_df))

        # Totals pro Symbol + max OI Expiry
        idx = g.groupby("symbol")["oi_total"].idxmax()
        max_rows = g.loc[idx, ["symbol","expiry","oi_total"]].rename(columns={
            "expiry":"max_oi_expiry","oi_total":"max_oi_value"
        })
        totals = g.groupby("symbol", as_index=False).agg(
            oi_total=("oi_total","sum"),
            spot=("spot","last"),
            hv20=("hv20","last"),
            hv60=("hv60","last"),
            expiries=("expiry","nunique"),
        ).merge(max_rows, on="symbol", how="left")
        totals_df = totals[["symbol","spot","hv20","hv60","expiries","oi_total","max_oi_expiry","max_oi_value"]]
        out_tot = "data/processed/options_oi_totals.csv"
        totals_df.to_csv(out_tot, index=False)
        print("wrote", out_tot, "rows=", len(totals_df))

    # -------- report
    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "watchlist": wl_path,
        "symbols": len(symbols),
        "rows_summary": int(len(summary_df)),
        "rows_by_expiry": int(len(by_expiry_df)),
        "rows_totals": int(len(totals_df)),
        "errors": errors,
    }
    with open("data/reports/options_oi_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    if errors:
        print("option errors:", len(errors))

    return 0

if __name__ == "__main__":
    sys.exit(main())
