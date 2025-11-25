#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Options Data V50 Pro - Smart Walls "like the big boys"

3-Stufen-Rakete mit Profi-Heuristiken:

1. TACTICAL (0-14 Tage):
   - Nächster Verfall
   - Call-/Put-Walls nur im sinnvollen Moneyness-Band
   - Deckel = Calls über Spot, Unterstützung = Puts unter Spot

2. MEDIUM (15-120 Tage):
   - Aggregierte "Swing-Magneten" (Call/Put) mit Notional- & DTE-Gewichtung
   - Medium-Put/Call-Ratio & Bias

3. STRATEGIC (>120 Tage):
   - LEAPS-Ziele (Call/Put) mit Notional- & DTE-Gewichtung
   - Strategic-Bias

4. GLOBAL:
   - Globale Call-/Put-Walls über alle Verfallstage mit DTE-Gewichtung
"""

import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

# ──────────────────────────────────────────────────────────────
# Settings
# ──────────────────────────────────────────────────────────────
RISK_FREE_RATE = 0.045             # Reserviert, falls du später echte Gamma-Berechnungen einbaust
DAYS_TACTICAL_MAX = 14
DAYS_MEDIUM_MAX = 120              # Alles darüber ist Strategic
MONEYNESS_BAND_PCT = 0.35          # +/- 35% um Spot für sinnvolle Walls


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def load_symbols():
    """Liest die Watchlist; Fallback auf ein paar bekannte Namen."""
    wl_path = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")

    symbols = []
    if os.path.exists(wl_path):
        with open(wl_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Kommentare mit # entfernen
                sym = line.split("#")[0].strip()
                if sym:
                    symbols.append(sym)

    if not symbols:
        symbols = ["SPY", "QQQ", "NVDA", "TSLA", "MSFT", "AAPL", "AMD"]

    return symbols


def clean_chain(df):
    """Säubert eine Optionskette: strike/openInterest numerisch & ohne NaNs."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["strike", "openInterest"])

    out = df.copy()

    if "strike" not in out.columns:
        out["strike"] = 0.0
    if "openInterest" not in out.columns:
        out["openInterest"] = 0.0

    out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
    out["openInterest"] = pd.to_numeric(out["openInterest"], errors="coerce").fillna(0)

    out = out.dropna(subset=["strike"])
    return out[["strike", "openInterest"]]


def compute_wall(df, spot, kind="call", max_pct=0.35, use_dte_weight=True):
    """
    Berechnet eine "Wall" (Deckel/Unterstützung) mit Profi-Heuristik:

    - Nur Strikes im Band [spot*(1-max_pct), spot*(1+max_pct)]
    - Calls: nur Strikes >= Spot (Deckel)
      Puts:  nur Strikes <= Spot (Unterstützung)
    - Score = notional_OI * DTE-Gewichtung
      notional_OI = openInterest * strike
      DTE-Gewichtung = exp(-dte/60) (kurzfristiges OI wiegt stärker)
    """
    if df is None or df.empty:
        return None

    df2 = df.copy()
    df2 = df2[np.isfinite(df2["strike"])]

    if df2.empty:
        return None

    # Moneyness-Fenster um den Spot
    low = spot * (1.0 - max_pct)
    high = spot * (1.0 + max_pct)
    df2 = df2[(df2["strike"] >= low) & (df2["strike"] <= high)]

    # Directional: Deckel über Spot, Unterstützung unter Spot
    if kind == "call":
        df2 = df2[df2["strike"] >= spot]
    else:
        df2 = df2[df2["strike"] <= spot]

    if df2.empty:
        return None

    # Nur sinnvolle OI
    df2["openInterest"] = df2["openInterest"].clip(lower=0)

    # Notional OI
    df2["notional_oi"] = df2["openInterest"] * df2["strike"].abs()

    # DTE-Gewichtung (optional)
    if use_dte_weight and "dte" in df2.columns:
        df2["w"] = np.exp(-df2["dte"] / 60.0)
    else:
        df2["w"] = 1.0

    df2["score"] = df2["notional_oi"] * df2["w"]

    grp = (
        df2.groupby("strike", as_index=False)
        .agg(score=("score", "sum"), total_oi=("openInterest", "sum"))
        .sort_values("score", ascending=False)
    )

    if grp.empty:
        return None

    strike = float(grp.iloc[0]["strike"])
    score = float(grp.iloc[0]["score"])
    total_oi = float(grp.iloc[0]["total_oi"])

    # Welche Expiry trägt am meisten zu diesem Strike bei?
    sub = df2[df2["strike"] == strike].sort_values("score", ascending=False).iloc[0]
    expiry = sub.get("expiry", None)
    dte = int(sub["dte"]) if "dte" in sub else None

    expiry_str = ""
    if isinstance(expiry, datetime):
        expiry_str = expiry.strftime("%Y-%m-%d")
    elif isinstance(expiry, str):
        expiry_str = expiry

    return {
        "strike": strike,
        "score": score,
        "total_oi": total_oi,
        "expiry": expiry_str,
        "dte": dte,
    }


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
def main():
    os.makedirs("data/processed", exist_ok=True)
    symbols = load_symbols()

    print(f"Processing V50 Pro (Smart Walls) for {len(symbols)} symbols...")

    now = datetime.utcnow()
    rows_out = []

    for sym in symbols:
        try:
            tk = yf.Ticker(sym)

            # Spot bestimmen
            try:
                hist = tk.history(period="5d", interval="1d")
                if hist.empty:
                    print(f"\n{sym}: keine History, skip.")
                    continue
                spot = float(hist["Close"].iloc[-1])
            except Exception as e:
                print(f"\n{sym}: History-Fehler ({e}), skip.")
                continue

            try:
                exps = tk.options
            except Exception as e:
                print(f"\n{sym}: Options-Fehler ({e}), skip.")
                continue

            if not exps:
                print(f"\n{sym}: keine Optionen, skip.")
                continue

            # Alle Options-Reihen (Calls & Puts) mit Expiry & DTE sammeln
            rows = []
            for e_str in exps:
                try:
                    dt = datetime.strptime(e_str, "%Y-%m-%d")
                except Exception:
                    continue

                dte = (dt - now).days
                if dte < 0:
                    # Vergangene Verfälle ignorieren
                    continue

                try:
                    chain = tk.option_chain(e_str)
                except Exception:
                    continue

                c_raw = getattr(chain, "calls", None)
                p_raw = getattr(chain, "puts", None)

                c = clean_chain(c_raw)
                p = clean_chain(p_raw)

                if c.empty and p.empty:
                    continue

                if not c.empty:
                    tmp_c = c[["strike", "openInterest"]].copy()
                    tmp_c["expiry"] = dt
                    tmp_c["dte"] = dte
                    tmp_c["kind"] = "call"
                    rows.append(tmp_c)

                if not p.empty:
                    tmp_p = p[["strike", "openInterest"]].copy()
                    tmp_p["expiry"] = dt
                    tmp_p["dte"] = dte
                    tmp_p["kind"] = "put"
                    rows.append(tmp_p)

            if not rows:
                print(f"\n{sym}: keine verwertbaren Optionsdaten.")
                continue

            df_all = pd.concat(rows, ignore_index=True)

            # Grund-Separation Calls / Puts
            df_calls = df_all[df_all["kind"] == "call"].copy()
            df_puts = df_all[df_all["kind"] == "put"].copy()

            if df_calls.empty and df_puts.empty:
                print(f"\n{sym}: weder Calls noch Puts nach Filter.")
                continue

            # ──────────────────────────────────────────────
            # GLOBAL WALLS (über alle DTE, mit DTE-Gewichtung)
            # ──────────────────────────────────────────────
            global_call_wall = compute_wall(df_calls, spot, "call",
                                            max_pct=MONEYNESS_BAND_PCT,
                                            use_dte_weight=True)
            global_put_wall = compute_wall(df_puts, spot, "put",
                                           max_pct=MONEYNESS_BAND_PCT,
                                           use_dte_weight=True)

            # ──────────────────────────────────────────────
            # TACTICAL (0–14 Tage) – nächster Verfall
            # ──────────────────────────────────────────────
            df_tac = df_all[(df_all["dte"] >= 0) & (df_all["dte"] <= DAYS_TACTICAL_MAX)].copy()
            tactical_call_wall = None
            tactical_put_wall = None
            tac_expiry_str = ""
            tac_dte = None

            if not df_tac.empty:
                # Nächsten Verfall finden
                min_dte = df_tac["dte"].min()
                earliest_expiries = df_tac[df_tac["dte"] == min_dte]["expiry"].drop_duplicates()
                tac_expiry = earliest_expiries.iloc[0]
                tac_dte = int(min_dte)

                tac_expiry_str = tac_expiry.strftime("%Y-%m-%d") if isinstance(tac_expiry, datetime) else str(tac_expiry)

                df_tac_calls = df_tac[(df_tac["kind"] == "call") & (df_tac["expiry"] == tac_expiry)].copy()
                df_tac_puts = df_tac[(df_tac["kind"] == "put") & (df_tac["expiry"] == tac_expiry)].copy()

                tactical_call_wall = compute_wall(
                    df_tac_calls, spot, "call",
                    max_pct=MONEYNESS_BAND_PCT,
                    use_dte_weight=False  # innerhalb eines Verfalls
                )
                tactical_put_wall = compute_wall(
                    df_tac_puts, spot, "put",
                    max_pct=MONEYNESS_BAND_PCT,
                    use_dte_weight=False
                )

            # ──────────────────────────────────────────────
            # MEDIUM (15–120 Tage) – Swing-Magneten
            # ──────────────────────────────────────────────
            df_med = df_all[(df_all["dte"] >= DAYS_TACTICAL_MAX + 1) &
                            (df_all["dte"] <= DAYS_MEDIUM_MAX)].copy()
            medium_call_wall = None
            medium_put_wall = None
            medium_pcr = None
            medium_bias = ""

            if not df_med.empty:
                df_med_calls = df_med[df_med["kind"] == "call"].copy()
                df_med_puts = df_med[df_med["kind"] == "put"].copy()

                medium_call_wall = compute_wall(
                    df_med_calls, spot, "call",
                    max_pct=MONEYNESS_BAND_PCT,
                    use_dte_weight=True
                )
                medium_put_wall = compute_wall(
                    df_med_puts, spot, "put",
                    max_pct=MONEYNESS_BAND_PCT,
                    use_dte_weight=True
                )

                total_c_oi_med = float(df_med_calls["openInterest"].sum()) if not df_med_calls.empty else 0.0
                total_p_oi_med = float(df_med_puts["openInterest"].sum()) if not df_med_puts.empty else 0.0

                if total_c_oi_med > 0:
                    medium_pcr = total_p_oi_med / total_c_oi_med
                else:
                    medium_pcr = None

                if total_c_oi_med > total_p_oi_med:
                    medium_bias = "Bullish"
                elif total_c_oi_med < total_p_oi_med:
                    medium_bias = "Bearish"
                else:
                    medium_bias = "Neutral"

            # ──────────────────────────────────────────────
            # STRATEGIC (>120 Tage) – LEAPS
            # ──────────────────────────────────────────────
            df_strat = df_all[df_all["dte"] > DAYS_MEDIUM_MAX].copy()
            strat_call_wall = None
            strat_put_wall = None
            strat_bias = ""

            if not df_strat.empty:
                df_strat_calls = df_strat[df_strat["kind"] == "call"].copy()
                df_strat_puts = df_strat[df_strat["kind"] == "put"].copy()

                strat_call_wall = compute_wall(
                    df_strat_calls, spot, "call",
                    max_pct=MONEYNESS_BAND_PCT,
                    use_dte_weight=True
                )
                strat_put_wall = compute_wall(
                    df_strat_puts, spot, "put",
                    max_pct=MONEYNESS_BAND_PCT,
                    use_dte_weight=True
                )

                total_c_oi_strat = float(df_strat_calls["openInterest"].sum()) if not df_strat_calls.empty else 0.0
                total_p_oi_strat = float(df_strat_puts["openInterest"].sum()) if not df_strat_puts.empty else 0.0

                if total_c_oi_strat > total_p_oi_strat:
                    strat_bias = "Bullish"
                elif total_c_oi_strat < total_p_oi_strat:
                    strat_bias = "Bearish"
                else:
                    strat_bias = "Neutral"

            # ──────────────────────────────────────────────
            # Output-Zeile für dieses Symbol bauen
            # ──────────────────────────────────────────────
            row = {
                "Symbol": sym,
                "Spot": round(spot, 4),

                # Tactical
                "Tac_Expiry": tac_expiry_str,
                "Tac_DTE": tac_dte,
                "Tac_Call_Wall": tactical_call_wall["strike"] if tactical_call_wall else 0.0,
                "Tac_Call_Wall_OI": tactical_call_wall["total_oi"] if tactical_call_wall else 0.0,
                "Tac_Put_Wall": tactical_put_wall["strike"] if tactical_put_wall else 0.0,
                "Tac_Put_Wall_OI": tactical_put_wall["total_oi"] if tactical_put_wall else 0.0,

                # Global Walls über alle DTE
                "Global_Call_Wall": global_call_wall["strike"] if global_call_wall else 0.0,
                "Global_Call_Wall_Expiry": global_call_wall["expiry"] if global_call_wall else "",
                "Global_Call_Wall_DTE": global_call_wall["dte"] if global_call_wall else None,
                "Global_Call_Wall_OI": global_call_wall["total_oi"] if global_call_wall else 0.0,

                "Global_Put_Wall": global_put_wall["strike"] if global_put_wall else 0.0,
                "Global_Put_Wall_Expiry": global_put_wall["expiry"] if global_put_wall else "",
                "Global_Put_Wall_DTE": global_put_wall["dte"] if global_put_wall else None,
                "Global_Put_Wall_OI": global_put_wall["total_oi"] if global_put_wall else 0.0,

                # Medium (Swing)
                "Medium_Call_Magnet": medium_call_wall["strike"] if medium_call_wall else 0.0,
                "Medium_Call_Magnet_Expiry": medium_call_wall["expiry"] if medium_call_wall else "",
                "Medium_Put_Magnet": medium_put_wall["strike"] if medium_put_wall else 0.0,
                "Medium_Put_Magnet_Expiry": medium_put_wall["expiry"] if medium_put_wall else "",
                "Medium_PCR": round(medium_pcr, 3) if medium_pcr is not None else None,
                "Medium_Bias": medium_bias,

                # Strategic (LEAPS)
                "Strategic_Call_Target": strat_call_wall["strike"] if strat_call_wall else 0.0,
                "Strategic_Call_Target_Expiry": strat_call_wall["expiry"] if strat_call_wall else "",
                "Strategic_Put_Target": strat_put_wall["strike"] if strat_put_wall else 0.0,
                "Strategic_Put_Target_Expiry": strat_put_wall["expiry"] if strat_put_wall else "",
                "Strategic_Bias": strat_bias,
            }

            rows_out.append(row)
            sys.stdout.write(".")
            sys.stdout.flush()

        except Exception as e:
            print(f"\n{sym}: Fehler im Hauptloop: {e}")
            continue

    print("\nSaving Pro 3-Stage Report...")

    if rows_out:
        df_out = pd.DataFrame(rows_out)
        df_out.to_csv("data/processed/options_3stage_pro.csv", index=False)
        print("✔ Saved data/processed/options_3stage_pro.csv")
    else:
        print("Keine Daten zum Speichern.")

    print("Done.")


if __name__ == "__main__":
    main()
