#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
options_3stage_pro.py
---------------------
Baut eine kompakte 3-Stufen-Datei pro Symbol aus options_oi_summary:

Output: data/processed/options_3stage_pro.csv

Spalten (pro Symbol):
- Tac_Call_Wall, Tac_Put_Wall, Tac_Expiry, Tac_DTE
- Medium_Call_Magnet, Medium_Put_Magnet,
  Medium_Call_Magnet_Expiry, Medium_Put_Magnet_Expiry,
  Medium_PCR, Medium_Bias
- Strategic_Call_Target, Strategic_Put_Target,
  Strategic_Call_Target_Expiry, Strategic_Put_Target_Expiry,
  Strategic_Bias
- Global_Call_Wall, Global_Put_Wall,
  Global_Call_Wall_Expiry, Global_Put_Wall_Expiry,
  Global_Call_Wall_DTE, Global_Put_Wall_DTE

Heuristik:
- Basis: options_oi_summary.csv(.gz)
  erwartet Felder (Case-Insensitive):
    symbol, expiry, call_oi, put_oi,
    call_top_strikes, put_top_strikes
- DTE wird aus expiry - heute berechnet.
- Tact:  0–14 Tage
- Med:   15–120 Tage
- Strat: >120 Tage
- Global: alle DTE >= 0
"""

import os
import math
from datetime import datetime, timezone

import numpy as np
import pandas as pd


BASE = "data/processed"
OUT_PATH = os.path.join(BASE, "options_3stage_pro.csv")


def find_summary_path() -> str:
    """Sucht options_oi_summary als .csv oder .csv.gz."""
    candidates = [
        os.path.join(BASE, "options_oi_summary.csv"),
        os.path.join(BASE, "options_oi_summary.csv.gz"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    raise SystemExit("options_oi_summary.csv(.gz) nicht gefunden unter data/processed")


def first_strike(raw):
    """Extrahiert den ERSTEN Strike aus einem call_top_strikes/put_top_strikes String."""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return np.nan
    s = str(raw)
    # Klammern entfernen
    for ch in "[]()":
        s = s.replace(ch, "")
    # Split an Kommas
    parts = [p.strip(" '\"") for p in s.split(",")]
    for p in parts:
        if not p:
            continue
        try:
            return float(p)
        except Exception:
            continue
    return np.nan


def load_summary() -> pd.DataFrame:
    p = find_summary_path()
    print("Lese", p)
    df = pd.read_csv(p, compression="infer")

    # Spaltennamen vereinheitlichen (lowercase)
    df.columns = [c.lower() for c in df.columns]

    # Pflichtfelder prüfen
    required = ["symbol", "expiry", "call_oi", "put_oi"]
    for col in required:
        if col not in df.columns:
            raise SystemExit(f"Column '{col}' fehlt in options_oi_summary")

    # Expiry in Datum konvertieren
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    df = df.dropna(subset=["expiry"])

    # DTE berechnen
    today = datetime.now(timezone.utc).date()
    df["dte"] = (df["expiry"].dt.date - today).apply(lambda x: x.days)

    # Nur zukünftige / aktuelle Verfälle
    df = df[df["dte"] >= 0].copy()
    if df.empty:
        raise SystemExit("options_oi_summary enthält keine künftigen Verfälle (dte>=0).")

    # Primäre Strikes aus den Top-Listen ziehen (falls vorhanden)
    df["primary_call_strike"] = np.nan
    df["primary_put_strike"] = np.nan

    if "call_top_strikes" in df.columns:
        df["primary_call_strike"] = df["call_top_strikes"].map(first_strike)
    if "put_top_strikes" in df.columns:
        df["primary_put_strike"] = df["put_top_strikes"].map(first_strike)

    # Fallback: wenn primary_* NaN -> nimm ggf. "focus_strike" falls vorhanden
    if "focus_strike" in df.columns:
        m_call = df["primary_call_strike"].isna()
        m_put = df["primary_put_strike"].isna()
        df.loc[m_call, "primary_call_strike"] = df.loc[m_call, "focus_strike"]
        df.loc[m_put, "primary_put_strike"] = df.loc[m_put, "focus_strike"]

    # Symbol Uppercase
    df["Symbol"] = df["symbol"].astype(str).str.upper()

    return df


def pick_max_oi(rowset: pd.DataFrame, side: str):
    """Wählt die Zeile mit maximalem Call-/Put-OI aus einem Subset."""
    if rowset is None or rowset.empty:
        return None

    oi_col = "call_oi" if side == "call" else "put_oi"
    if oi_col not in rowset.columns:
        return None

    # Nur Zeilen mit OI > 0
    rs = rowset[rowset[oi_col] > 0]
    if rs.empty:
        return None

    idx = rs[oi_col].idxmax()
    return rs.loc[idx]


def stage_for_symbol(g: pd.DataFrame) -> dict:
    """Erzeugt den 3-Stufen-Datensatz für EIN Symbol."""
    out = {
        "Symbol": g["Symbol"].iloc[0],
        # Tactical
        "Tac_Call_Wall": np.nan,
        "Tac_Put_Wall": np.nan,
        "Tac_Expiry": "",
        "Tac_DTE": np.nan,
        # Medium
        "Medium_Call_Magnet": np.nan,
        "Medium_Put_Magnet": np.nan,
        "Medium_Call_Magnet_Expiry": "",
        "Medium_Put_Magnet_Expiry": "",
        "Medium_PCR": np.nan,
        "Medium_Bias": "",
        # Strategic
        "Strategic_Call_Target": np.nan,
        "Strategic_Put_Target": np.nan,
        "Strategic_Call_Target_Expiry": "",
        "Strategic_Put_Target_Expiry": "",
        "Strategic_Bias": "",
        # Global
        "Global_Call_Wall": np.nan,
        "Global_Put_Wall": np.nan,
        "Global_Call_Wall_Expiry": "",
        "Global_Put_Wall_Expiry": "",
        "Global_Call_Wall_DTE": np.nan,
        "Global_Put_Wall_DTE": np.nan,
    }

    # Bucket-Aufteilung
    tac = g[(g["dte"] >= 0) & (g["dte"] <= 14)]
    med = g[(g["dte"] >= 15) & (g["dte"] <= 120)]
    strat = g[g["dte"] > 120]
    all_fut = g[g["dte"] >= 0]

    # ---------- TACTICAL ----------
    rc = pick_max_oi(tac, "call")
    rp = pick_max_oi(tac, "put")

    if rc is not None:
        out["Tac_Call_Wall"] = float(rc.get("primary_call_strike", np.nan))
        out["Tac_Expiry"] = rc["expiry"].date().isoformat()
        out["Tac_DTE"] = int(rc["dte"])
    if rp is not None:
        out["Tac_Put_Wall"] = float(rp.get("primary_put_strike", np.nan))
        # Wenn noch kein Expiry gesetzt ist, nimm das Put-Datum
        if not out["Tac_Expiry"]:
            out["Tac_Expiry"] = rp["expiry"].date().isoformat()
            out["Tac_DTE"] = int(rp["dte"])

    # ---------- MEDIUM ----------
    rc_m = pick_max_oi(med, "call")
    rp_m = pick_max_oi(med, "put")

    if rc_m is not None:
        out["Medium_Call_Magnet"] = float(rc_m.get("primary_call_strike", np.nan))
        out["Medium_Call_Magnet_Expiry"] = rc_m["expiry"].date().isoformat()
    if rp_m is not None:
        out["Medium_Put_Magnet"] = float(rp_m.get("primary_put_strike", np.nan))
        out["Medium_Put_Magnet_Expiry"] = rp_m["expiry"].date().isoformat()

    # PCR & Bias (Medium)
    if not med.empty:
        sum_call = float(med["call_oi"].sum())
        sum_put = float(med["put_oi"].sum())
        if sum_call > 0:
            pcr = sum_put / sum_call
            out["Medium_PCR"] = round(pcr, 2)
        else:
            out["Medium_PCR"] = np.nan

        if sum_call > sum_put * 1.1:
            out["Medium_Bias"] = "Bullish"
        elif sum_put > sum_call * 1.1:
            out["Medium_Bias"] = "Bearish"
        else:
            out["Medium_Bias"] = "Neutral"

    # ---------- STRATEGIC ----------
    rc_s = pick_max_oi(strat, "call")
    rp_s = pick_max_oi(strat, "put")

    if rc_s is not None:
        out["Strategic_Call_Target"] = float(rc_s.get("primary_call_strike", np.nan))
        out["Strategic_Call_Target_Expiry"] = rc_s["expiry"].date().isoformat()
    if rp_s is not None:
        out["Strategic_Put_Target"] = float(rp_s.get("primary_put_strike", np.nan))
        out["Strategic_Put_Target_Expiry"] = rp_s["expiry"].date().isoformat()

    if not strat.empty:
        sum_call_s = float(strat["call_oi"].sum())
        sum_put_s = float(strat["put_oi"].sum())
        if sum_call_s > sum_put_s * 1.1:
            out["Strategic_Bias"] = "Bullish"
        elif sum_put_s > sum_call_s * 1.1:
            out["Strategic_Bias"] = "Bearish"
        else:
            out["Strategic_Bias"] = "Neutral"

    # ---------- GLOBAL ----------
    rc_g = pick_max_oi(all_fut, "call")
    rp_g = pick_max_oi(all_fut, "put")

    if rc_g is not None:
        out["Global_Call_Wall"] = float(rc_g.get("primary_call_strike", np.nan))
        out["Global_Call_Wall_Expiry"] = rc_g["expiry"].date().isoformat()
        out["Global_Call_Wall_DTE"] = int(rc_g["dte"])

    if rp_g is not None:
        out["Global_Put_Wall"] = float(rp_g.get("primary_put_strike", np.nan))
        out["Global_Put_Wall_Expiry"] = rp_g["expiry"].date().isoformat()
        out["Global_Put_Wall_DTE"] = int(rp_g["dte"])

    return out


def main():
    os.makedirs(BASE, exist_ok=True)
    df = load_summary()

    rows = []
    for sym, g in df.groupby("Symbol"):
        rows.append(stage_for_symbol(g))

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values("Symbol").reset_index(drop=True)
    out_df.to_csv(OUT_PATH, index=False)

    print("wrote", OUT_PATH, "rows=", len(out_df))
    print(out_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
