#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Options Data V38 - Unified options data builder.

- Lädt für alle Symbole der Watchlist die Optionsketten via yfinance.
- Berechnet Spot, HV10/20/30.
- Erzeugt konsistente Dateien:
    * data/processed/options_oi_summary.csv
    * data/processed/options_oi_totals.csv
    * data/processed/options_oi_by_expiry.csv
    * data/processed/whale_alerts.csv
"""

import os
import sys
import math
from datetime import datetime, timedelta
from typing import List, Dict

import numpy as np
import pandas as pd
import yfinance as yf


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)


def _normalize_symbol(raw: str) -> str:
    if raw is None:
        return ""
    s = str(raw).strip().upper()
    # Kommentar / zusätzliche Infos entfernen
    s = s.split("#", 1)[0].strip()
    # Erstes Feld nehmen, falls Separatoren drin sind
    for sep in [",", ";", "\t"]:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
    # evtl. Suffices von anderen Pipelines
    for suf in ["_US_IG", "_EU_IG", "_IG", "_EU"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s


def read_watchlist(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    syms: List[str] = []
    try:
        df = pd.read_csv(path)
        cols = [c for c in df.columns if "symbol" in c.lower() or "ticker" in c.lower()]
        if cols:
            syms = df[cols[0]].dropna().astype(str).tolist()
        else:
            syms = df.iloc[:, 0].dropna().astype(str).tolist()
    except Exception:
        with open(path, "r", encoding="utf-8") as f:
            syms = [line.strip() for line in f if line.strip()]

    out = []
    for s in syms:
        n = _normalize_symbol(s)
        if n:
            out.append(n)
    return sorted(list(set(out)))


def annualize_vol(returns: pd.Series):
    if returns is None or returns.empty or len(returns) < 5:
        return None
    return float(returns.std(ddof=1) * math.sqrt(252.0))


def wavg_iv(df: pd.DataFrame):
    if df is None or df.empty or "impliedVolatility" not in df.columns:
        return None
    d = df.dropna(subset=["impliedVolatility", "openInterest"]).copy()
    if d.empty:
        return None
    # Trash-Filter
    d = d[(d["impliedVolatility"] > 0.01) & (d["impliedVolatility"] < 5.0)]
    if d.empty:
        return None
    total_oi = d["openInterest"].sum()
    if total_oi > 0:
        return float((d["impliedVolatility"] * d["openInterest"]).sum() / total_oi)
    return float(d["impliedVolatility"].mean())


def get_smart_walls(calls: pd.DataFrame, puts: pd.DataFrame, spot_price: float):
    """Call-Wall, Put-Wall, Magnet-Strike (max OI) berechnen."""
    if not spot_price:
        return None, None, None

    # Magnet = Strike mit maximaler Gesamt-OI (Calls+Puts)
    try:
        all_opts = pd.concat(
            [
                calls[["strike", "openInterest"]].assign(type="C"),
                puts[["strike", "openInterest"]].assign(type="P"),
            ],
            ignore_index=True,
        )
    except Exception:
        all_opts = pd.DataFrame(columns=["strike", "openInterest"])

    magnet_strike = None
    if not all_opts.empty:
        strike_oi = all_opts.groupby("strike")["openInterest"].sum()
        if not strike_oi.empty:
            magnet_strike = float(strike_oi.idxmax())

    # Call-Wall (OTM-Priorität)
    call_wall = None
    if not calls.empty:
        otm_calls = calls[calls["strike"] > spot_price]
        if not otm_calls.empty:
            call_wall = float(
                otm_calls.sort_values("openInterest", ascending=False).iloc[0]["strike"]
            )
        else:
            call_wall = float(
                calls.sort_values("openInterest", ascending=False).iloc[0]["strike"]
            )

    # Put-Wall (OTM-Priorität)
    put_wall = None
    if not puts.empty:
        otm_puts = puts[puts["strike"] < spot_price]
        if not otm_puts.empty:
            put_wall = float(
                otm_puts.sort_values("openInterest", ascending=False).iloc[0]["strike"]
            )
        else:
            put_wall = float(
                puts.sort_values("openInterest", ascending=False).iloc[0]["strike"]
            )

    return call_wall, put_wall, magnet_strike


def calc_expected_move(price: float, iv: float, days: int):
    if not price or not iv or days is None:
        return 0.0
    d_eff = max(1.0, float(days))
    return float(price * iv * math.sqrt(d_eff / 365.0))


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main() -> int:
    ensure_dirs()

    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    max_expiries = 8          # wie viele Verfälle je Symbol
    historical_mode = os.getenv("OPTIONS_HISTORICAL_MODE", "0").strip() == "1"

    symbols = read_watchlist(wl_path)
    if not symbols:
        symbols = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA", "AMD"]

    rows_summary: List[Dict] = []
    rows_totals: List[Dict] = []
    rows_by_expiry: List[Dict] = []
    rows_whales: List[Dict] = []
    errors: List[str] = []

    print(f"Fetching Options Data for {len(symbols)} symbols...")
    print(f"Mode: {'HISTORICAL' if historical_mode else 'LIVE (Forward looking only)'}")

    now = datetime.utcnow()

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)

            # A. Spot & HV
            try:
                hist = tk.history(period="1y", interval="1d", auto_adjust=False)
            except Exception:
                hist = pd.DataFrame()

            if hist.empty:
                continue

            spot = float(hist["Close"].iloc[-1])

            hist["LogRet"] = np.log(hist["Close"] / hist["Close"].shift(1))
            hv10 = annualize_vol(hist["LogRet"].tail(10))
            hv20 = annualize_vol(hist["LogRet"].tail(20))
            hv30 = annualize_vol(hist["LogRet"].tail(30))

            # B. Expiries
            try:
                expirations = tk.options
            except Exception:
                expirations = []

            if not expirations:
                # Symbol ohne Optionsdaten
                rows_totals.append(
                    {
                        "symbol": sym,
                        "total_call_oi": 0,
                        "total_put_oi": 0,
                        "total_oi": 0,
                        "spot": spot,
                        "hv20": round(hv20 if hv20 else 0.0, 4),
                    }
                )
                continue

            total_call_oi_ticker = 0
            total_put_oi_ticker = 0

            # Loop über Verfallstage
            for exp_date_str in expirations[:max_expiries]:
                try:
                    dt_exp = datetime.strptime(exp_date_str, "%Y-%m-%d")

                    # Im LIVE-Modus nur künftige / aktuelle Verfälle
                    if (not historical_mode) and (dt_exp < (now - timedelta(days=1))):
                        continue

                    chain = tk.option_chain(exp_date_str)
                    calls = chain.calls.copy()
                    puts = chain.puts.copy()

                    # Spalten robust machen
                    for df in (calls, puts):
                        if "openInterest" not in df.columns:
                            df["openInterest"] = 0
                        if "volume" not in df.columns:
                            df["volume"] = 0
                        if "impliedVolatility" not in df.columns:
                            df["impliedVolatility"] = 0.0

                        df["openInterest"] = df["openInterest"].fillna(0).astype(int)
                        df["volume"] = df["volume"].fillna(0).astype(int)
                        df["impliedVolatility"] = df["impliedVolatility"].fillna(0.0)

                    call_wall, put_wall, magnet = get_smart_walls(calls, puts, spot)

                    exp_c_oi = int(calls["openInterest"].sum())
                    exp_p_oi = int(puts["openInterest"].sum())
                    exp_total_oi = exp_c_oi + exp_p_oi

                    total_call_oi_ticker += exp_c_oi
                    total_put_oi_ticker += exp_p_oi

                    iv_c = wavg_iv(calls)
                    iv_p = wavg_iv(puts)
                    valid_ivs = [x for x in (iv_c, iv_p) if x is not None and x > 0]
                    term_iv = float(sum(valid_ivs) / len(valid_ivs)) if valid_ivs else 0.0

                    days = (dt_exp - now).days
                    exp_move = calc_expected_move(spot, term_iv, days)
                    upper = spot + exp_move
                    lower = spot - exp_move

                    # Whale-Detection (Volume >> OI)
                    for opt_type, df in [("CALL", calls), ("PUT", puts)]:
                        whales = df[
                            (df["volume"] > df["openInterest"])
                            & (df["volume"] > 500)
                            & (df["openInterest"] > 10)
                        ]
                        for _, row in whales.iterrows():
                            rows_whales.append(
                                {
                                    "symbol": sym,
                                    "expiry": exp_date_str,
                                    "type": opt_type,
                                    "strike": float(row.get("strike", 0)),
                                    "volume": int(row.get("volume", 0)),
                                    "oi": int(row.get("openInterest", 0)),
                                    "vol_oi_ratio": round(
                                        float(row["volume"])
                                        / max(1, float(row["openInterest"])),
                                        2,
                                    ),
                                    "iv": round(
                                        float(row.get("impliedVolatility", 0)), 4
                                    ),
                                    "spot_at_detection": spot,
                                }
                            )

                    # Helper: Top-Strikes-Liste (inkl. Wall zuerst)
                    def get_top_strikes(df_in, wall_price):
                        tmp = df_in.sort_values("openInterest", ascending=False).head(5)
                        strikes = tmp["strike"].tolist()
                        if wall_price and wall_price in strikes:
                            strikes.remove(wall_price)
                            strikes.insert(0, wall_price)
                        elif wall_price:
                            strikes.insert(0, wall_price)
                        return ",".join(map(str, strikes))

                    top_c_str = get_top_strikes(calls, call_wall)
                    top_p_str = get_top_strikes(puts, put_wall)

                    # SUMMARY pro Symbol+Expiry (für OptionsData_Scanner, AI, etc.)
                    rows_summary.append(
                        {
                            "symbol": sym,
                            "expiry": exp_date_str,
                            "spot": spot,
                            "call_oi": exp_c_oi,
                            "put_oi": exp_p_oi,
                            "total_oi": exp_total_oi,
                            "put_call_ratio": round(
                                float(exp_p_oi) / max(1.0, float(exp_c_oi)), 2
                            ),
                            "call_iv_w": round(iv_c if iv_c else 0.0, 4),
                            "put_iv_w": round(iv_p if iv_p else 0.0, 4),
                            "expected_move": round(exp_move, 2),
                            "upper_bound": round(upper, 2),
                            "lower_bound": round(lower, 2),
                            "days_to_exp": int(days),
                            "call_top_strikes": top_c_str,
                            "put_top_strikes": top_p_str,
                            "magnet_strike": magnet,
                            "hv10": round(hv10 if hv10 else 0.0, 4),
                            "hv20": round(hv20 if hv20 else 0.0, 4),
                            "hv30": round(hv30 if hv30 else 0.0, 4),
                        }
                    )

                    # BY-EXPIRY Tabelle (für Max-OI-Scanner usw.)
                    rows_by_expiry.append(
                        {
                            "symbol": sym,
                            "expiry": exp_date_str,
                            "total_call_oi": exp_c_oi,
                            "total_put_oi": exp_p_oi,
                            "total_oi": exp_total_oi,
                            "spot": spot,
                            "hv20": round(hv20 if hv20 else 0.0, 4),
                        }
                    )

                except Exception:
                    # Fehler bei einem bestimmten Verfall → nächsten Verfall probieren
                    continue

            # Totals pro Symbol
            rows_totals.append(
                {
                    "symbol": sym,
                    "total_call_oi": int(total_call_oi_ticker),
                    "total_put_oi": int(total_put_oi_ticker),
                    "total_oi": int(total_call_oi_ticker + total_put_oi_ticker),
                    "spot": spot,
                    "hv20": round(hv20 if hv20 else 0.0, 4),
                }
            )

            sys.stdout.write(".")
            sys.stdout.flush()

        except Exception as e:
            errors.append(f"Error fetching {sym}: {e}")
            continue

    print("\nProcessing complete.")

    # ──────────────────────────────────────────────────────────
    # Write CSVs
    # ──────────────────────────────────────────────────────────

    if rows_summary:
        df_sum = pd.DataFrame(rows_summary)
        df_sum.to_csv("data/processed/options_oi_summary.csv", index=False)
        print(f"Saved summary: {len(df_sum)} rows.")
    else:
        print("Warning: No summary data generated.")

    if rows_totals:
        pd.DataFrame(rows_totals).to_csv(
            "data/processed/options_oi_totals.csv", index=False
        )

    if rows_by_expiry:
        df_by = pd.DataFrame(rows_by_expiry)
        df_by.to_csv("data/processed/options_oi_by_expiry.csv", index=False)
        print(f"Saved by_expiry: {len(df_by)} rows.")

    cols_whales = [
        "symbol",
        "expiry",
        "type",
        "strike",
        "volume",
        "oi",
        "vol_oi_ratio",
        "iv",
        "spot_at_detection",
    ]
    if rows_whales:
        pd.DataFrame(rows_whales).to_csv(
            "data/processed/whale_alerts.csv", index=False
        )
        print(f"Saved {len(rows_whales)} Whale Alerts.")
    else:
        pd.DataFrame(columns=cols_whales).to_csv(
            "data/processed/whale_alerts.csv", index=False
        )
        print("No whales found. Created empty alert file.")

    if errors:
        with open("data/reports/options_errors.log", "w", encoding="utf-8") as f:
            for line in errors:
                f.write(line + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
