#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch options OI + simple IV/HV summary via yfinance.

Writes:
- data/processed/options_oi_summary.csv      (eine Zeile je Symbol*Expiry)
- data/processed/options_oi_by_expiry.csv    (aggregiert pro Verfall, mit Anteil & Rang)
- data/processed/options_oi_totals.csv       (Totals + dominanter Verfall je Symbol)
- data/processed/options_oi_by_strike.csv    (kollabiert über alle Verfälle; NUR Max-OI-Ranking)
- data/reports/options_oi_report.json        (Fehler/Diagnose)

ENV (optional):
- WATCHLIST_STOCKS        Pfad zu Watchlist (txt oder csv mit Spalte 'symbol')
- OPTIONS_MAX_EXPIRIES    Zahl | "all"
- OPTIONS_TOPK            Top-K Strike-Strings im Summary (nur Infofeld)
- HV_WIN_10, HV_WIN_SHORT, HV_WIN_30
"""

import os, sys, json, math
from datetime import datetime
from typing import List, Dict

import numpy as np
import pandas as pd
import yfinance as yf


# ---------- helpers ----------
def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)


def read_watchlist(path: str) -> List[str]:
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
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and s.lower() != "symbol":
                out.append(s)
    return out


def annualize_vol(returns: pd.Series):
    if returns is None or returns.empty:
        return None
    return float(returns.std(ddof=0) * math.sqrt(252))


def compute_hv(hist: pd.DataFrame, win: int):
    if hist is None or hist.empty or "Close" not in hist.columns or len(hist) < max(5, win + 1):
        return None
    lr = np.log(hist["Close"]).diff().dropna()
    if len(lr) < win:
        return None
    return annualize_vol(lr.tail(win))


def wavg_iv(df: pd.DataFrame):
    if df is None or df.empty or "impliedVolatility" not in df.columns:
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
    d["openInterest"] = pd.to_numeric(d["openInterest"], errors="coerce").fillna(0)
    d = d.sort_values("openInterest", ascending=False).head(max(1, k))
    return ",".join(str(x) for x in d["strike"].tolist())


def parse_max_exp(raw: str) -> int:
    raw = (raw or "4").strip().lower()
    if raw in ("all", "*"):
        return 10 ** 9
    return int(raw)


# ---------- main ----------
def main() -> int:
    ensure_dirs()

    wl_path        = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries   = parse_max_exp(os.getenv("OPTIONS_MAX_EXPIRIES", "4"))
    topk           = int(os.getenv("OPTIONS_TOPK", "3"))
    hv_win_10      = int(os.getenv("HV_WIN_10", "10"))
    hv_win_20      = int(os.getenv("HV_WIN_SHORT", "20"))
    hv_win_30      = int(os.getenv("HV_WIN_30", "30"))

    symbols = read_watchlist(wl_path) or ["AAPL"]

    rows: List[Dict] = []
    by_exp_rows: List[Dict] = []
    by_strike_rows: List[Dict] = []
    errors: List[Dict] = []

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)
            # --- price history (for HV & spot)
            try:
                hist = tk.history(period="400d", interval="1d", auto_adjust=False)
            except Exception as e:
                hist = pd.DataFrame()
                errors.append({"symbol": sym, "stage": "history", "msg": str(e)})

            hv10 = compute_hv(hist, hv_win_10)
            hv20 = compute_hv(hist, hv_win_20)
            hv30 = compute_hv(hist, hv_win_30)
            spot = float(hist["Close"].dropna().iloc[-1]) if ("Close" in hist and not hist["Close"].dropna().empty) else None

            # --- expiries
            try:
                expiries = list(tk.options or [])
            except Exception as e:
                expiries = []
                errors.append({"symbol": sym, "stage": "options_list", "msg": str(e)})

            # accumulate strike OI across expiries for this symbol
            strike_map: Dict[float, float] = {}

            if not expiries:
                rows.append({
                    "symbol": sym, "expiry": "", "spot": spot,
                    "call_oi": 0, "put_oi": 0, "put_call_ratio": None,
                    "call_iv_w": None, "put_iv_w": None,
                    "call_top_strikes": "", "put_top_strikes": "",
                    "hv10": hv10, "hv20": hv20, "hv30": hv30,
                })
            else:
                for exp in expiries[:max_expiries]:
                    try:
                        ch = tk.option_chain(exp)
                        calls = ch.calls if hasattr(ch, "calls") else pd.DataFrame()
                        puts  = ch.puts  if hasattr(ch, "puts")  else pd.DataFrame()

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
                            "hv10": hv10, "hv20": hv20, "hv30": hv30,
                        })

                        by_exp_rows.append({
                            "symbol": sym,
                            "expiry": exp,
                            "call_oi": c_oi,
                            "put_oi": p_oi,
                            "total_oi": int(c_oi + p_oi),
                        })

                        # accumulate by strike for this expiry
                        for d in (calls, puts):
                            if not d.empty and {"strike", "openInterest"} <= set(d.columns):
                                tmp = d[["strike", "openInterest"]].copy()
                                tmp["openInterest"] = pd.to_numeric(tmp["openInterest"], errors="coerce").fillna(0)
                                for s, oi in zip(tmp["strike"].tolist(), tmp["openInterest"].tolist()):
                                    try:
                                        s_float = float(s)
                                    except Exception:
                                        continue
                                    strike_map[s_float] = strike_map.get(s_float, 0.0) + float(oi)

                    except Exception as e:
                        errors.append({"symbol": sym, "stage": f"chain:{exp}", "msg": str(e)})

            # flush strike rows for this symbol
            if strike_map:
                for s, oi in strike_map.items():
                    by_strike_rows.append({"symbol": sym, "strike": s, "total_oi": int(round(oi))})

        except Exception as e:
            errors.append({"symbol": sym, "stage": "ticker", "msg": str(e)})

    # --- per-expiry summary (one row per symbol*expiry) ---
    out_summary = "data/processed/options_oi_summary.csv"
    pd.DataFrame(rows).to_csv(out_summary, index=False)
    print("wrote", out_summary, "rows=", len(rows))

    # --- aggregates & rankings by expiry ---
    if by_exp_rows:
        ag = pd.DataFrame(by_exp_rows)
        ag["total_oi"] = pd.to_numeric(ag["total_oi"], errors="coerce").fillna(0).astype(int)

        grp = ag.groupby("symbol")["total_oi"]
        ag["oi_share_pct"] = grp.transform(lambda s: (s / max(float(s.sum()), 1.0)) * 100.0)
        ag["rank_in_symbol"] = grp.rank(ascending=False, method="min").astype(int)

        out_by_exp = "data/processed/options_oi_by_expiry.csv"
        ag.sort_values(["symbol", "rank_in_symbol", "expiry"]).to_csv(out_by_exp, index=False)
        print("wrote", out_by_exp, "rows=", len(ag))

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
        pd.DataFrame(columns=["symbol","expiry","call_oi","put_oi","total_oi","oi_share_pct","rank_in_symbol"])\
          .to_csv("data/processed/options_oi_by_expiry.csv", index=False)
        pd.DataFrame(columns=["symbol","total_oi","max_oi_expiry","max_oi_value"])\
          .to_csv("data/processed/options_oi_totals.csv", index=False)
        print("wrote empty aggregates")

    # --- by strike (alle Verfälle je Symbol kollabiert) → NUR Max-OI-Ranking ---
    out_by_strike = "data/processed/options_oi_by_strike.csv"
    if by_strike_rows:
        bsd = pd.DataFrame(by_strike_rows)  # columns: symbol,strike,total_oi
        bsd["total_oi"] = pd.to_numeric(bsd["total_oi"], errors="coerce").fillna(0).astype(int)

        # OI je (symbol,strike) aufsummieren
        bsd = bsd.groupby(["symbol", "strike"], as_index=False)["total_oi"].sum()

        # Reines Max-OI-Ranking pro Symbol; Tiebreak nur zur Stabilität nach strike ASC
        bsd = bsd.sort_values(["symbol", "total_oi", "strike"], ascending=[True, False, True])
        bsd["rank_in_symbol"] = bsd.groupby("symbol")["total_oi"] \
                                   .rank(ascending=False, method="first").astype(int)

        bsd[["symbol", "strike", "total_oi", "rank_in_symbol"]].to_csv(out_by_strike, index=False)
        print("wrote", out_by_strike, "rows=", len(bsd))
    else:
        pd.DataFrame(columns=["symbol","strike","total_oi","rank_in_symbol"]).to_csv(out_by_strike, index=False)
        print("wrote empty", out_by_strike)

    # --- report ---
    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "watchlist": wl_path,
        "symbols": len(symbols),
        "rows_summary": len(rows),
        "rows_by_expiry": len(by_exp_rows),
        "rows_by_strike": len(by_strike_rows),
        "errors": errors,
    }
    with open("data/reports/options_oi_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if errors:
        print("option errors:", len(errors))
    return 0


if __name__ == "__main__":
    sys.exit(main())
