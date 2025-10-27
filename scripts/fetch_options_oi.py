#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch options OI + simple IV/HV summary via yfinance.

Outputs
- data/processed/options_oi_summary.csv     (Zeile je Symbol x Verfall, inkl. Top-Strikes, IV, HV)
- data/processed/options_oi_by_expiry.csv   (Aggregat je Symbol x Verfall, inkl. OI-Anteile & Ranking)
- data/processed/options_oi_totals.csv      (Aggregat je Symbol: total OI & dominanter Verfall)
- data/reports/options_oi_report.json       (Laufbericht)
"""

import os, sys, json, math
from datetime import datetime
from typing import List, Tuple

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

def annualize_vol(returns: pd.Series) -> float | None:
    """Annualized stdev of daily log returns (252 trading days)."""
    if returns is None or returns.empty:
        return None
    return float(returns.std(ddof=0) * math.sqrt(252))

def compute_hv(hist: pd.DataFrame, win: int) -> float | None:
    if hist is None or hist.empty or "Close" not in hist.columns or len(hist) < max(5, win+1):
        return None
    lr = np.log(hist["Close"]).diff().dropna()
    if len(lr) < win:
        return None
    return annualize_vol(lr.tail(win))

def wavg_iv(df: pd.DataFrame) -> float | None:
    """Weighted IV by openInterest; falls back to simple mean."""
    if df is None or df.empty:
        return None
    if "impliedVolatility" not in df.columns:
        return None
    d = df.dropna(subset=["impliedVolatility"]).copy()
    if d.empty:
        return None
    if "openInterest" in d.columns and d["openInterest"].fillna(0).sum() > 0:
        w = d["openInterest"].fillna(0).astype(float)
        return float((d["impliedVolatility"] * w).sum() / w.sum())
    return float(d["impliedVolatility"].mean())

def top_strikes(df: pd.DataFrame, k: int) -> str:
    if df is None or df.empty or "openInterest" not in df.columns or "strike" not in df.columns:
        return ""
    d = df[["strike", "openInterest"]].copy()
    d["openInterest"] = d["openInterest"].fillna(0)
    d = d.sort_values("openInterest", ascending=False).head(max(1, k))
    return ",".join(str(x) for x in d["strike"].tolist())

def parse_max_exp(raw: str) -> int:
    raw = (raw or "4").strip().lower()
    if raw in ("all", "*"):
        return 10**9
    return int(raw)

# ------------ main ------------
def main() -> int:
    ensure_dirs()

    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries = parse_max_exp(os.getenv("OPTIONS_MAX_EXPIRIES", "4"))
    topk          = int(os.getenv("OPTIONS_TOPK", "3"))
    hv_win_short  = int(os.getenv("HV_WIN_SHORT", "20"))
    hv_win_long   = int(os.getenv("HV_WIN_LONG", "60"))

    symbols = read_watchlist(wl_path)
    if not symbols:
        print(f"watchlist empty: {wl_path}")
        symbols = ["AAPL"]

    rows = []
    by_exp_rows = []   # Aggregat je Symbol x Verfall
    errors = []

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)

            # underlying history for HV
            try:
                hist = tk.history(period="400d", interval="1d", auto_adjust=False)
            except Exception as e:
                hist = pd.DataFrame()
                errors.append({"symbol": sym, "stage": "history", "msg": str(e)})

            hv20 = compute_hv(hist, hv_win_short)
            hv60 = compute_hv(hist, hv_win_long)
            spot = float(hist["Close"].dropna().iloc[-1]) if ("Close" in hist and not hist["Close"].dropna().empty) else None

            # expiries
            try:
                expiries = list(tk.options or [])
            except Exception as e:
                expiries = []
                errors.append({"symbol": sym, "stage": "options_list", "msg": str(e)})

            if not expiries:
                # write a minimal row so Symbol erscheint
                rows.append({
                    "symbol": sym, "expiry": "", "spot": spot,
                    "call_oi": 0, "put_oi": 0, "put_call_ratio": None,
                    "call_iv_w": None, "put_iv_w": None,
                    "call_top_strikes": "", "put_top_strikes": "",
                    "hv20": hv20, "hv60": hv60,
                })
                continue

            # chains per expiry
            for exp in expiries[:max_expiries]:
                try:
                    ch = tk.option_chain(exp)
                    calls = ch.calls if hasattr(ch, "calls") else pd.DataFrame()
                    puts  = ch.puts  if hasattr(ch, "puts")  else pd.DataFrame()

                    # ensure numeric OI
                    for d in (calls, puts):
                        if not d.empty and "openInterest" in d.columns:
                            d["openInterest"] = pd.to_numeric(d["openInterest"], errors="coerce").fillna(0)

                    c_oi = int(calls["openInterest"].sum()) if "openInterest" in calls.columns else 0
                    p_oi = int(puts["openInterest"].sum())  if "openInterest" in puts.columns  else 0
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

                    by_exp_rows.append({
                        "symbol": sym,
                        "expiry": exp,
                        "call_oi": c_oi,
                        "put_oi": p_oi,
                        "total_oi": int(c_oi + p_oi),
                    })

                except Exception as e:
                    errors.append({"symbol": sym, "stage": f"chain:{exp}", "msg": str(e)})

        except Exception as e:
            errors.append({"symbol": sym, "stage": "ticker", "msg": str(e)})

    # write outputs
    out_summary = "data/processed/options_oi_summary.csv"
    pd.DataFrame(rows).to_csv(out_summary, index=False)
    print("wrote", out_summary, "rows=", len(rows))

    # aggregates je Verfall (incl. OI-Anteile & Ranking je Symbol)
    if by_exp_rows:
        ag = pd.DataFrame(by_exp_rows)
        ag["total_oi"] = pd.to_numeric(ag["total_oi"], errors="coerce").fillna(0).astype(int)
        # Anteil und Ranking je Symbol
        ag["oi_share_pct"] = ag.groupby("symbol")["total_oi"].apply(lambda s: 100 * s / s.sum().replace(0, 1))
        ag["rank_in_symbol"] = ag.groupby("symbol")["total_oi"].rank(ascending=False, method="min").astype(int)
        out_by_exp = "data/processed/options_oi_by_expiry.csv"
        ag.sort_values(["symbol", "rank_in_symbol", "expiry"]).to_csv(out_by_exp, index=False)
        print("wrote", out_by_exp, "rows=", len(ag))

        # totals je Symbol
        tot = (
            ag.sort_values(["symbol", "total_oi"], ascending=[True, False])
              .groupby("symbol", as_index=False)
              .agg(total_oi=("total_oi", "sum"),
                   max_oi_expiry=("expiry", "first"),
                   max_oi_value=("total_oi", "max"))
        )
        out_tot = "data/processed/options_oi_totals.csv"
        tot.to_csv(out_tot, index=False)
        print("wrote", out_tot, "rows=", len(tot))
    else:
        out_by_exp = "data/processed/options_oi_by_expiry.csv"
        out_tot = "data/processed/options_oi_totals.csv"
        pd.DataFrame(columns=["symbol","expiry","call_oi","put_oi","total_oi","oi_share_pct","rank_in_symbol"]).to_csv(out_by_exp, index=False)
        pd.DataFrame(columns=["symbol","total_oi","max_oi_expiry","max_oi_value"]).to_csv(out_tot, index=False)
        print("wrote empty", out_by_exp, "and", out_tot)

    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "watchlist": wl_path,
        "symbols": len(symbols),
        "rows_summary": len(rows),
        "rows_by_expiry": len(by_exp_rows),
        "errors": errors,
    }
    with open("data/reports/options_oi_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    if errors:
        print("option errors:", len(errors))

    return 0

if __name__ == "__main__":
    sys.exit(main())
