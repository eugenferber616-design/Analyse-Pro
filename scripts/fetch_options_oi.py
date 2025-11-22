#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch options OI + IV/HV summary + Whale Alerts via yfinance.

Features:
1. Aggregates OI by Expiry and Strike.
2. Calculates 'Expected Move' based on Implied Volatility.
3. Detects 'Whale Activity' (Volume > Open Interest).

Writes:
- data/processed/options_oi_summary.csv
- data/processed/options_oi_by_expiry.csv
- data/processed/options_oi_totals.csv
- data/processed/options_oi_by_strike.csv
- data/processed/whale_alerts.csv  <-- NEU
- data/reports/options_oi_report.json
"""
import os, sys, json, math
from datetime import datetime
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import yfinance as yf


# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────
def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)


def _normalize_symbol(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    s = s.split("#", 1)[0].strip()
    for sep in [",", ";", "\t", " "]:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
    for suf in ["_US_IG", "_EU_IG", "_IG", "_EU"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s


def read_watchlist(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    syms: List[str] = []
    if path.lower().endswith(".csv"):
        try:
            df = pd.read_csv(path)
            if "symbol" in df.columns:
                col = df["symbol"].astype(str).tolist()
                syms = [s for s in map(_normalize_symbol, col) if s]
            else:
                first_col = df.columns[0]
                col = df[first_col].astype(str).tolist()
                syms = [s for s in map(_normalize_symbol, col) if s]
        except Exception:
            syms = []
    else:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = _normalize_symbol(line)
                if s and s.lower() != "symbol":
                    syms.append(s)
    seen = set()
    out = []
    for s in syms:
        if s and s not in seen:
            seen.add(s)
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
        return 10**9
    return int(raw)

# ── NEU: Expected Move Berechnung ──
def calc_expected_move(spot_price: float, iv: float, days_to_exp: int) -> float:
    """
    Berechnet den erwarteten Move (+/-) basierend auf IV.
    Formula: Price * IV * sqrt(Days / 365)
    """
    if not spot_price or not iv or days_to_exp is None:
        return 0.0
    # IV kommt oft als 0.25 (25%) oder selten 25.0. YF liefert meist 0.xx
    # Sicherheitshalber:
    if iv > 50: iv = iv / 100.0 
    
    return spot_price * iv * math.sqrt(max(1, days_to_exp) / 365.0)


# ────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────
def main() -> int:
    ensure_dirs()

    wl_path      = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries = parse_max_exp(os.getenv("OPTIONS_MAX_EXPIRIES", "4"))
    topk         = int(os.getenv("OPTIONS_TOPK", "3"))
    
    # Whale Threshold: Nur Optionen mit Volume > 500 beachten
    whale_min_vol = int(os.getenv("WHALE_MIN_VOL", "500")) 

    hv_win_10    = int(os.getenv("HV_WIN_10", "10"))
    hv_win_20    = int(os.getenv("HV_WIN_SHORT", "20"))
    hv_win_30    = int(os.getenv("HV_WIN_30", "30"))

    symbols = read_watchlist(wl_path) or ["AAPL"]

    rows: List[Dict] = []
    by_exp_rows: List[Dict] = []
    by_strike_rows: List[Dict] = []
    whale_alerts: List[Dict] = []  # NEU
    errors: List[Dict] = []

    for raw in symbols:
        sym = _normalize_symbol(raw)
        if not sym:
            continue

        try:
            tk = yf.Ticker(sym)

            try:
                expiries = list(set(tk.options or []))
            except Exception as e:
                expiries = []
                errors.append({"symbol": sym, "stage": "options_list", "msg": str(e)})

            if not expiries:
                errors.append({"symbol": sym, "stage": "options_list", "msg": "no options available; skipped"})
                continue

            # Price history (für HV & Spot)
            try:
                hist = tk.history(period="400d", interval="1d", auto_adjust=False)
            except Exception as e:
                hist = pd.DataFrame()
                errors.append({"symbol": sym, "stage": "history", "msg": str(e)})

            hv10 = compute_hv(hist, hv_win_10)
            hv20 = compute_hv(hist, hv_win_20)
            hv30 = compute_hv(hist, hv_win_30)
            
            # Spot Price holen
            spot = None
            if "Close" in hist and not hist["Close"].dropna().empty:
                spot = float(hist["Close"].dropna().iloc[-1])

            strike_map: Dict[float, float] = {}
            any_oi = 0

            # Datum heute für Restlaufzeit-Berechnung
            now = datetime.utcnow()

            for exp_str in sorted(expiries)[:max_expiries]:
                try:
                    # Days to expiry berechnen
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d")
                    days_to_exp = (exp_date - now).days
                    if days_to_exp < 0: days_to_exp = 0

                    ch = tk.option_chain(exp_str)
                    calls = ch.calls if hasattr(ch, "calls") else pd.DataFrame()
                    puts  = ch.puts  if hasattr(ch, "puts")  else pd.DataFrame()

                    # Daten säubern
                    for d in (calls, puts):
                        if not d.empty:
                            if "openInterest" in d.columns:
                                d["openInterest"] = pd.to_numeric(d["openInterest"], errors="coerce").fillna(0)
                            if "volume" in d.columns:
                                d["volume"] = pd.to_numeric(d["volume"], errors="coerce").fillna(0)
                            if "impliedVolatility" in d.columns:
                                d["impliedVolatility"] = pd.to_numeric(d["impliedVolatility"], errors="coerce").fillna(0)

                    # ── Whale Scanner (NEU) ──
                    # Wir suchen nach: Volume > OpenInterest (Aggressives Positioning)
                    for opt_type, df in [("CALL", calls), ("PUT", puts)]:
                        if df.empty: continue
                        
                        # Filter: Volume > OI  UND  Volume > Threshold
                        # VORSICHT: Bei yfinance ist OI oft vom Vortag, Volume von heute (15min delay).
                        whale_mask = (df["volume"] > df["openInterest"]) & (df["volume"] >= whale_min_vol)
                        whales = df[whale_mask]
                        
                        for _, w_row in whales.iterrows():
                            whale_alerts.append({
                                "symbol": sym,
                                "expiry": exp_str,
                                "type": opt_type,
                                "strike": w_row["strike"],
                                "volume": int(w_row["volume"]),
                                "oi": int(w_row["openInterest"]),
                                "vol_oi_ratio": round(w_row["volume"] / max(1, w_row["openInterest"]), 2),
                                "iv": round(w_row["impliedVolatility"], 4),
                                "lastPrice": w_row.get("lastPrice", 0),
                                "spot_at_detection": spot
                            })

                    # ── Summary Berechnung ──
                    c_oi = int(calls["openInterest"].sum()) if "openInterest" in calls.columns else 0
                    p_oi = int(puts["openInterest"].sum())  if "openInterest" in puts.columns  else 0
                    any_oi += (c_oi + p_oi)

                    c_iv = wavg_iv(calls)
                    p_iv = wavg_iv(puts)
                    
                    # Overall IV für Expected Move (Mittelwert aus Call/Put IV oder gewichtet)
                    avg_iv = 0.0
                    if c_iv and p_iv: avg_iv = (c_iv + p_iv) / 2.0
                    elif c_iv: avg_iv = c_iv
                    elif p_iv: avg_iv = p_iv
                    
                    # Expected Move
                    exp_move = calc_expected_move(spot, avg_iv, days_to_exp)
                    upper_bound = (spot + exp_move) if spot else None
                    lower_bound = (spot - exp_move) if spot else None

                    rows.append({
                        "symbol": sym, "expiry": exp_str, "spot": spot,
                        "call_oi": c_oi, "put_oi": p_oi,
                        "put_call_ratio": (float(p_oi)/float(c_oi)) if c_oi>0 else None,
                        "call_iv_w": c_iv, "put_iv_w": p_iv,
                        "expected_move": round(exp_move, 2),
                        "upper_bound": round(upper_bound, 2) if upper_bound else None,
                        "lower_bound": round(lower_bound, 2) if lower_bound else None,
                        "days_to_exp": days_to_exp,
                        "call_top_strikes": top_strikes(calls, topk),
                        "put_top_strikes": top_strikes(puts,  topk),
                        "hv10": hv10, "hv20": hv20, "hv30": hv30,
                    })

                    by_exp_rows.append({
                        "symbol": sym, "expiry": exp_str,
                        "call_oi": c_oi, "put_oi": p_oi, "total_oi": int(c_oi + p_oi),
                    })

                    # By strike sammeln
                    for d in (calls, puts):
                        if not d.empty and {"strike","openInterest"} <= set(d.columns):
                            for s, oi in zip(d["strike"], d["openInterest"]):
                                try: s_float = float(s)
                                except: continue
                                strike_map[s_float] = strike_map.get(s_float, 0.0) + float(oi)

                except Exception as e:
                    errors.append({"symbol": sym, "stage": f"chain:{exp_str}", "msg": str(e)})

            if any_oi <= 0:
                rows = [r for r in rows if r.get("symbol") != sym]
                continue

            if strike_map:
                for s, oi in strike_map.items():
                    by_strike_rows.append({"symbol": sym, "strike": s, "total_oi": int(round(oi))})

        except Exception as e:
            errors.append({"symbol": sym, "stage": "ticker", "msg": str(e)})

    # ── Persist Summary ──
    out_summary = "data/processed/options_oi_summary.csv"
    pd.DataFrame(rows).to_csv(out_summary, index=False)
    print("wrote", out_summary, "rows=", len(rows))

    # ── Persist Whale Alerts ──
    out_whales = "data/processed/whale_alerts.csv"
    if whale_alerts:
        wd = pd.DataFrame(whale_alerts)
        # Sortieren nach Ratio für die krassesten Ausreißer
        wd = wd.sort_values("vol_oi_ratio", ascending=False)
        wd.to_csv(out_whales, index=False)
        print("wrote", out_whales, "rows=", len(wd))
    else:
        pd.DataFrame(columns=["symbol","expiry","type","strike","volume","oi","vol_oi_ratio","iv","spot_at_detection"]).to_csv(out_whales, index=False)
        print("wrote empty", out_whales)

    # ── Persist By Expiry ──
    if by_exp_rows:
        ag = pd.DataFrame(by_exp_rows)
        ag["total_oi"] = pd.to_numeric(ag["total_oi"], errors="coerce").fillna(0).astype(int)
        grp = ag.groupby("symbol")["total_oi"]
        ag["rank_in_symbol"] = grp.rank(ascending=False, method="min").astype(int)
        ag.sort_values(["symbol","rank_in_symbol","expiry"]).to_csv("data/processed/options_oi_by_expiry.csv", index=False)

        tot = ag.groupby("symbol", as_index=False).agg(total_oi=("total_oi","sum"))
        tot.to_csv("data/processed/options_oi_totals.csv", index=False)
    else:
        # Leere Files schreiben
        pd.DataFrame(columns=["symbol","expiry","call_oi","put_oi","total_oi","rank_in_symbol"]).to_csv("data/processed/options_oi_by_expiry.csv", index=False)
        pd.DataFrame(columns=["symbol","total_oi"]).to_csv("data/processed/options_oi_totals.csv", index=False)

    # ── Persist By Strike ──
    out_by_strike = "data/processed/options_oi_by_strike.csv"
    if by_strike_rows:
        bsd = pd.DataFrame(by_strike_rows)
        bsd = bsd.groupby(["symbol","strike"], as_index=False)["total_oi"].sum().sort_values(["symbol","total_oi"], ascending=[True, False])
        bsd.to_csv(out_by_strike, index=False)
    else:
        pd.DataFrame(columns=["symbol","strike","total_oi"]).to_csv(out_by_strike, index=False)

    # Report
    report = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "watchlist": wl_path,
        "symbols": len(symbols),
        "rows_summary": len(rows),
        "whale_alerts": len(whale_alerts),
        "errors": errors[:20], # Limit error log
    }
    with open("data/reports/options_oi_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return 0


if __name__ == "__main__":
    sys.exit(main())
