#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch options OI + Smart Walls + IV/HV summary.

Optimized for AI + Overlay:
- Smart Walls: Call/Put-Walls mit OTM-Logik.
- Expected Move (IV-basiert) pro Verfall.
- Whale Activity (Unusual Options Volume).
- HV10/20/30 aus Daily-Returns.

Writes:
- data/processed/options_oi_summary.csv
- data/processed/options_oi_totals.csv
- data/processed/whale_alerts.csv

Modes:
- Live-Only  (default):   Nur zukünftige Expiries (dt_exp > now).
- Historical (Env-Flag):  OPTIONS_HISTORICAL_MODE=1 → ALLE Expiries,
                          d.h. days_to_exp kann auch negativ sein.
"""

import os
import sys
import json
import math
from datetime import datetime, timedelta
from typing import List, Dict

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
    s = str(raw).strip().upper()
    s = s.split("#", 1)[0].strip()
    # Common cleanup
    for sep in [",", ";", "\t"]:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
    # Suffixes
    for suf in ["_US_IG", "_EU_IG", "_IG", "_EU"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s


def read_watchlist(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    syms: List[str] = []
    try:
        # Try CSV
        df = pd.read_csv(path)
        cols = [c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower()]
        if cols:
            syms = df[cols[0]].dropna().astype(str).tolist()
        else:
            # Fallback first column
            syms = df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception:
        # Try Text
        with open(path, "r", encoding="utf-8") as f:
            syms = [line.strip() for line in f if line.strip()]

    out = []
    for s in syms:
        n = _normalize_symbol(s)
        if n:
            out.append(n)
    # eindeutige, sortierte Liste
    return sorted(list(set(out)))

# ────────────────────────────────────────────────────────────────────
# Math Helpers
# ────────────────────────────────────────────────────────────────────

def annualize_vol(returns: pd.Series):
    """Calculates annualized volatility from daily log returns (decimal, e.g. 0.20)."""
    if returns is None or returns.empty or len(returns) < 5:
        return None
    return float(returns.std(ddof=1) * math.sqrt(252.0))


def wavg_iv(df: pd.DataFrame):
    """Calculates Open-Interest weighted IV (decimal)."""
    if df is None or df.empty or "impliedVolatility" not in df.columns:
        return None
    d = df.dropna(subset=["impliedVolatility", "openInterest"]).copy()
    if d.empty:
        return None

    # Filter offensichtlicher Müll: IV sehr klein oder extrem hoch
    d = d[(d["impliedVolatility"] > 0.01) & (d["impliedVolatility"] < 5.0)]
    if d.empty:
        return None

    total_oi = d["openInterest"].sum()
    if total_oi > 0:
        return float((d["impliedVolatility"] * d["openInterest"]).sum() / total_oi)
    return float(d["impliedVolatility"].mean())


def get_smart_walls(calls: pd.DataFrame, puts: pd.DataFrame, spot_price: float):
    """
    Finds the 'Real' Walls (OTM) + Magnet.

    Call Wall = Max OI Strike > Spot (Resistance)
    Put Wall  = Max OI Strike < Spot (Support)
    Magnet    = Absolute Max OI Strike (Call or Put)
    """
    if not spot_price:
        return None, None, None

    # 1. Magnet: Strike mit max. total OI (Call+Put)
    magnet_strike = None
    try:
        all_opts = pd.concat([
            calls[["strike", "openInterest"]].assign(type="C"),
            puts[["strike", "openInterest"]].assign(type="P")
        ], ignore_index=True)
    except Exception:
        all_opts = pd.DataFrame(columns=["strike", "openInterest"])

    if not all_opts.empty:
        strike_oi = all_opts.groupby("strike")["openInterest"].sum()
        if not strike_oi.empty:
            magnet_strike = float(strike_oi.idxmax())

    # 2. Call Wall (OTM Calls)
    call_wall = None
    try:
        otm_calls = calls[calls["strike"] > spot_price]
    except Exception:
        otm_calls = pd.DataFrame()

    if not otm_calls.empty:
        call_wall = float(otm_calls.sort_values("openInterest", ascending=False).iloc[0]["strike"])
    else:
        # Fallback: max OI Call (auch ITM möglich)
        if not calls.empty:
            call_wall = float(calls.sort_values("openInterest", ascending=False).iloc[0]["strike"])

    # 3. Put Wall (OTM Puts)
    put_wall = None
    try:
        otm_puts = puts[puts["strike"] < spot_price]
    except Exception:
        otm_puts = pd.DataFrame()

    if not otm_puts.empty:
        put_wall = float(otm_puts.sort_values("openInterest", ascending=False).iloc[0]["strike"])
    else:
        if not puts.empty:
            put_wall = float(puts.sort_values("openInterest", ascending=False).iloc[0]["strike"])

    return call_wall, put_wall, magnet_strike


def calc_expected_move(price: float, iv: float, days: int):
    """Expected Move (Einseitig) als Betrag (in $), IV dezimal, days Kalendertage."""
    if not price or not iv or days is None:
        return 0.0
    return float(price * iv * math.sqrt(max(1, days) / 365.0))

# ────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────

def main() -> int:
    ensure_dirs()

    # Env Vars
    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries = 6  # Look ahead max N expiries
    # Historical Mode:
    # 0 (default) = nur zukünftige Expiries
    # 1           = auch vergangene Expiries (Backtest / Historie)
    historical_mode = os.getenv("OPTIONS_HISTORICAL_MODE", "0").strip() == "1"

    # Load Symbols
    symbols = read_watchlist(wl_path)
    if not symbols:
        # Fallback defaults if no file
        symbols = ["SPY", "QQQ", "IWM", "AAPL", "MSFT",
                   "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD"]

    rows_summary: List[Dict] = []
    rows_totals: List[Dict] = []
    rows_whales: List[Dict] = []
    errors: List[str] = []

    print(f"Fetching Options Data for {len(symbols)} symbols...")
    if historical_mode:
        print("Mode: HISTORICAL (alle Expiries, days_to_exp kann negativ sein)")
    else:
        print("Mode: LIVE-ONLY (nur zukünftige Expiries)")

    now = datetime.utcnow()

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)

            # A. Get Spot & HV
            # Fetch 1 year history
            hist = tk.history(period="1y", interval="1d", auto_adjust=False)
            if hist.empty:
                errors.append(f"{sym}: No history")
                continue

            spot = float(hist["Close"].iloc[-1])

            # Calculate Log Returns
            hist["LogRet"] = np.log(hist["Close"] / hist["Close"].shift(1))

            # HV Windows
            hv10 = annualize_vol(hist["LogRet"].tail(10))
            hv20 = annualize_vol(hist["LogRet"].tail(20))
            hv30 = annualize_vol(hist["LogRet"].tail(30))

            # B. Options Chain
            try:
                expirations = tk.options
            except Exception as e:
                expirations = []
                errors.append(f"{sym}: options() failed: {e}")

            if not expirations:
                # Kein Optionsmarkt → Totals trotzdem mit HV/Spot
                rows_totals.append({
                    "symbol": sym,
                    "total_call_oi": 0,
                    "total_put_oi": 0,
                    "total_oi": 0,
                    "spot": spot,
                    "hv20": round(hv20 if hv20 else 0.0, 4)
                })
                continue

            total_call_oi = 0
            total_put_oi = 0

            for exp_date_str in expirations[:max_expiries]:
                try:
                    # Parsing des Verfallsdatums
                    dt_exp = datetime.strptime(exp_date_str, "%Y-%m-%d")

                    # LIVE-ONLY: Vergangene Verfallstage überspringen
                    if (not historical_mode) and (dt_exp <= now):
                        continue

                    chain = tk.option_chain(exp_date_str)
                    calls = chain.calls.copy()
                    puts = chain.puts.copy()

                    # Basic Cleaning
                    for df in (calls, puts):
                        if "openInterest" in df.columns:
                            df["openInterest"] = df["openInterest"].fillna(0).astype(int)
                        else:
                            df["openInterest"] = 0

                        if "volume" in df.columns:
                            df["volume"] = df["volume"].fillna(0).astype(int)
                        else:
                            df["volume"] = 0

                        if "impliedVolatility" in df.columns:
                            df["impliedVolatility"] = df["impliedVolatility"].fillna(0.0)
                        else:
                            df["impliedVolatility"] = 0.0

                    # 1. Smart Walls
                    call_wall, put_wall, magnet = get_smart_walls(calls, puts, spot)

                    # 2. Aggregates for this Expiry
                    exp_c_oi = int(calls["openInterest"].sum())
                    exp_p_oi = int(puts["openInterest"].sum())
                    total_call_oi += exp_c_oi
                    total_put_oi += exp_p_oi

                    # 3. Weighted IV
                    iv_c = wavg_iv(calls)
                    iv_p = wavg_iv(puts)
                    valid_ivs = [x for x in (iv_c, iv_p) if x is not None and x > 0]
                    term_iv = float(sum(valid_ivs) / len(valid_ivs)) if valid_ivs else 0.0

                    # 4. Expected Move
                    days = (dt_exp - now).days  # kann im Historical-Mode negativ sein
                    exp_move = calc_expected_move(spot, term_iv, days)
                    upper = spot + exp_move
                    lower = spot - exp_move

                    # 5. Whale Alerts (Volume > OI & Volume > 500)
                    for opt_type, df in [("CALL", calls), ("PUT", puts)]:
                        whales = df[
                            (df["volume"] > df["openInterest"]) &
                            (df["volume"] > 500)
                        ]
                        for _, row in whales.iterrows():
                            rows_whales.append({
                                "symbol": sym,
                                "expiry": exp_date_str,
                                "type": opt_type,
                                "strike": float(row.get("strike", 0.0)),
                                "volume": int(row.get("volume", 0)),
                                "oi": int(row.get("openInterest", 0)),
                                "vol_oi_ratio": round(
                                    float(row.get("volume", 0)) /
                                    max(1, float(row.get("openInterest", 0))), 2
                                ),
                                "iv": round(float(row.get("impliedVolatility", 0.0)), 4),
                                "spot_at_detection": spot
                            })

                    # 6. Top Strikes List (für C#-Indikator)
                    # Wir erzwingen, dass die Smart Wall vorne steht.
                    top_c = calls.sort_values(
                        "openInterest", ascending=False
                    ).head(5)["strike"].tolist()
                    if call_wall and call_wall in top_c:
                        top_c.remove(call_wall)
                        top_c.insert(0, call_wall)
                    elif call_wall:
                        top_c.insert(0, call_wall)

                    top_p = puts.sort_values(
                        "openInterest", ascending=False
                    ).head(5)["strike"].tolist()
                    if put_wall and put_wall in top_p:
                        top_p.remove(put_wall)
                        top_p.insert(0, put_wall)
                    elif put_wall:
                        top_p.insert(0, put_wall)

                    # 7. Summary-Zeile für diesen Verfall
                    rows_summary.append({
                        "symbol": sym,
                        "expiry": exp_date_str,
                        "spot": spot,
                        "call_oi": exp_c_oi,
                        "put_oi": exp_p_oi,
                        "put_call_ratio": round(
                            float(exp_p_oi) / max(1.0, float(exp_c_oi)), 2
                        ),
                        "call_iv_w": round(iv_c if iv_c else 0.0, 4),  # decimal
                        "put_iv_w": round(iv_p if iv_p else 0.0, 4),
                        "expected_move": round(exp_move, 2),
                        "upper_bound": round(upper, 2),
                        "lower_bound": round(lower, 2),
                        "days_to_exp": int(days),
                        "call_top_strikes": ",".join(map(str, top_c)),
                        "put_top_strikes": ",".join(map(str, top_p)),
                        "magnet_strike": magnet,
                        "hv10": round(hv10 if hv10 else 0.0, 4),
                        "hv20": round(hv20 if hv20 else 0.0, 4),
                        "hv30": round(hv30 if hv30 else 0.0, 4)
                    })

                except Exception as e:
                    errors.append(f"{sym}: error in expiry {exp_date_str}: {e}")
                    # wir machen weiter mit nächsten Expiry
                    continue

            # Totals per Symbol (auch wenn keine Expiries im Live-Mode übrig blieben)
            rows_totals.append({
                "symbol": sym,
                "total_call_oi": int(total_call_oi),
                "total_put_oi": int(total_put_oi),
                "total_oi": int(total_call_oi + total_put_oi),
                "spot": spot,
                "hv20": round(hv20 if hv20 else 0.0, 4)
            })

        except Exception as e:
            msg = f"Failed to fetch {sym}: {e}"
            print(msg)
            errors.append(msg)

    # ────────────────────────────────────────────────────────────────
    # Save Files
    # ────────────────────────────────────────────────────────────────

    if rows_summary:
        pd.DataFrame(rows_summary).to_csv(
            "data/processed/options_oi_summary.csv", index=False
        )
        print(f"Saved summary with {len(rows_summary)} rows.")

    if rows_totals:
        pd.DataFrame(rows_totals).to_csv(
            "data/processed/options_oi_totals.csv", index=False
        )
        print(f"Saved totals with {len(rows_totals)} rows.")

    if rows_whales:
        pd.DataFrame(rows_whales).to_csv(
            "data/processed/whale_alerts.csv", index=False
        )
        print(f"Saved {len(rows_whales)} whale alerts.")
    else:
        # Leere Struktur erzeugen, damit dein C#-Code nicht crasht
        pd.DataFrame(
            columns=[
                "symbol", "expiry", "type", "strike",
                "volume", "oi", "vol_oi_ratio", "iv",
                "spot_at_detection"
            ]
        ).to_csv("data/processed/whale_alerts.csv", index=False)
        print("No whale alerts found – wrote empty whale_alerts.csv.")

    # Optional: einfacher Report (nur zur Info)
    report = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "mode": "historical" if historical_mode else "live_only",
        "n_symbols": len(symbols),
        "n_summary_rows": len(rows_summary),
        "n_totals_rows": len(rows_totals),
        "n_whale_alerts": len(rows_whales),
        "errors": errors[:50]  # nicht zu viel
    }
    try:
        with open("data/reports/options_oi_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print("Wrote data/reports/options_oi_report.json")
    except Exception as e:
        print(f"Failed to write report: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
