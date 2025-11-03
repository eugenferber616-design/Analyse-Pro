#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_direction_signal.py
Ziel: Erzeuge
  data/processed/direction_signal.csv.gz
mit folgendem, stabilen Schema:
  symbol,dir,strength,next_expiry,nearest_dte,
  focus_strike,focus_strike_7,focus_strike_30,focus_strike_60

Datenquellen (Priorität):
1) options_signals.csv(.gz)      → dir/strength und ggf. focus_strike_* / expiry / dte
2) options_oi_by_strike.csv(.gz) → Fallback: focus_strike + nearest expiry/dte
3) options_oi_by_expiry.csv(.gz) → Fallback: nearest expiry/dte
4) Minimal-Heuristik, falls kein dir/strength vorhanden (dir=0, strength=0)

Alle Reader sind robust gg. alternative Spaltennamen (…7 / …7d, next_expiry / nearest_expiry etc.)
"""

import os, io, gzip, sys
import pandas as pd
from pathlib import Path

PROC = Path("data/processed")

def _read_any(base: Path):
    """Liest .csv oder .csv.gz; gibt DataFrame oder None zurück."""
    csv = base.with_suffix(".csv")
    gz  = base.with_suffix(".csv.gz")
    if csv.exists():
        return pd.read_csv(csv)
    if gz.exists():
        with gzip.open(gz, "rt", encoding="utf-8") as f:
            return pd.read_csv(f)
    return None

def _pick_first_col(df, names, default=None):
    for c in names:
        if c in df.columns:
            return c
    return default

def _coerce_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _coerce_int(x):
    try:
        v = int(float(x))
        return v
    except Exception:
        return None

def _norm_symbol(s):
    try:
        return str(s).strip()
    except Exception:
        return s

def main():
    PROC.mkdir(parents=True, exist_ok=True)

    df_sig = _read_any(PROC / "options_signals")           # optional
    df_bs  = _read_any(PROC / "options_oi_by_strike")      # optional
    df_ex  = _read_any(PROC / "options_oi_by_expiry")      # optional

    # ---- Basis-Tabelle (alle Symbole) bestimmen ----
    symbols = set()
    for df in (df_sig, df_bs, df_ex):
        if df is not None:
            sym_col = _pick_first_col(df, ["symbol","ticker","sym"], None)
            if sym_col:
                symbols |= set(map(_norm_symbol, df[sym_col].dropna().unique().tolist()))
    symbols = sorted(list(symbols))

    if not symbols:
        print("WARN: Keine Symbolbasis gefunden. Schreibe leere Datei.", file=sys.stderr)
        out_gz = PROC / "direction_signal.csv.gz"
        with gzip.open(out_gz, "wt", encoding="utf-8", newline="") as f:
            pd.DataFrame(columns=[
                "symbol","dir","strength","next_expiry","nearest_dte",
                "focus_strike","focus_strike_7","focus_strike_30","focus_strike_60"
            ]).to_csv(f, index=False)
        print("wrote", out_gz)
        return

    # ---- Spalten in options_signals erkennen ----
    if df_sig is not None:
        sc_sym   = _pick_first_col(df_sig, ["symbol","ticker","sym"])
        sc_dir   = _pick_first_col(df_sig, ["dir","direction","signal_dir"])
        sc_str   = _pick_first_col(df_sig, ["strength","score","signal_strength"])
        sc_exp   = _pick_first_col(df_sig, ["next_expiry","nearest_expiry","expiry"])
        sc_dte   = _pick_first_col(df_sig, ["nearest_dte","dte","days_to_expiry"])
        sc_fs    = _pick_first_col(df_sig, ["focus_strike","strike","target_strike","focus"])
        sc_fs7   = _pick_first_col(df_sig, ["focus_strike_7","focus_strike_7d","strike_7d"])
        sc_fs30  = _pick_first_col(df_sig, ["focus_strike_30","focus_strike_30d","strike_30d"])
        sc_fs60  = _pick_first_col(df_sig, ["focus_strike_60","focus_strike_60d","strike_60d"])
    else:
        sc_sym=sc_dir=sc_str=sc_exp=sc_dte=sc_fs=sc_fs7=sc_fs30=sc_fs60=None

    # ---- Spalten in by_strike erkennen ----
    if df_bs is not None:
        bs_sym  = _pick_first_col(df_bs, ["symbol","ticker","sym"])
        bs_exp  = _pick_first_col(df_bs, ["expiry","next_expiry","nearest_expiry"])
        bs_dte  = _pick_first_col(df_bs, ["dte","nearest_dte","days_to_expiry","days"])
        bs_fs   = _pick_first_col(df_bs, ["focus_strike","strike_focus","max_oi_strike","strike_poc"])
    else:
        bs_sym=bs_exp=bs_dte=bs_fs=None

    # ---- Spalten in by_expiry erkennen ----
    if df_ex is not None:
        ex_sym  = _pick_first_col(df_ex, ["symbol","ticker","sym"])
        ex_exp  = _pick_first_col(df_ex, ["expiry","next_expiry","nearest_expiry"])
        ex_dte  = _pick_first_col(df_ex, ["dte","nearest_dte","days_to_expiry","days"])
    else:
        ex_sym=ex_exp=ex_dte=None

    rows = []
    for sym in symbols:
        out = {
            "symbol": sym,
            "dir": 0,
            "strength": 0,
            "next_expiry": "",
            "nearest_dte": None,
            "focus_strike": None,
            "focus_strike_7": None,
            "focus_strike_30": None,
            "focus_strike_60": None
        }

        # 1) options_signals → dir/strength/expiry/dte/focus_strikes
        if df_sig is not None and sc_sym:
            hit = df_sig[df_sig[sc_sym].astype(str).str.upper().eq(sym.upper())]
            if hit.empty and "." in sym:
                # EU-Suffix-Heuristik
                base = sym.split(".", 1)[0]
                hit = df_sig[df_sig[sc_sym].astype(str).str.upper().isin([sym.upper(), base.upper(), (base+".DE").upper(), (base+".PA").upper()])]
            if not hit.empty:
                r = hit.iloc[0]
                if sc_dir: out["dir"] = _coerce_int(r.get(sc_dir)) or 0
                if sc_str: out["strength"] = _coerce_int(r.get(sc_str)) or 0
                if sc_exp: out["next_expiry"] = pd.to_datetime(r.get(sc_exp), errors="coerce").strftime("%Y-%m-%d") if pd.notnull(r.get(sc_exp)) else ""
                if sc_dte: out["nearest_dte"] = _coerce_int(r.get(sc_dte))
                if sc_fs:  out["focus_strike"] = _coerce_float(r.get(sc_fs))
                if sc_fs7: out["focus_strike_7"] = _coerce_float(r.get(sc_fs7))
                if sc_fs30:out["focus_strike_30"] = _coerce_float(r.get(sc_fs30))
                if sc_fs60:out["focus_strike_60"] = _coerce_float(r.get(sc_fs60))

        # 2) Fallback: by_strike für expiry/dte + general focus_strike
        if (not out["next_expiry"] or out["nearest_dte"] is None or out["focus_strike"] is None) and df_bs is not None and bs_sym:
            hit = df_bs[df_bs[bs_sym].astype(str).str.upper().eq(sym.upper())]
            if not hit.empty:
                r = hit.iloc[0]
                if (not out["next_expiry"]) and bs_exp:
                    out["next_expiry"] = pd.to_datetime(r.get(bs_exp), errors="coerce").strftime("%Y-%m-%d") if pd.notnull(r.get(bs_exp)) else out["next_expiry"]
                if (out["nearest_dte"] is None) and bs_dte:
                    out["nearest_dte"] = _coerce_int(r.get(bs_dte)) or out["nearest_dte"]
                if out["focus_strike"] is None and bs_fs:
                    out["focus_strike"] = _coerce_float(r.get(bs_fs))

        # 3) Fallback: by_expiry für expiry/dte
        if (not out["next_expiry"] or out["nearest_dte"] is None) and df_ex is not None and ex_sym:
            hit = df_ex[df_ex[ex_sym].astype(str).str.upper().eq(sym.upper())]
            if not hit.empty:
                r = hit.sort_values(by=ex_dte if ex_dte else ex_exp, ascending=True).iloc[0]
                if not out["next_expiry"] and ex_exp:
                    out["next_expiry"] = pd.to_datetime(r.get(ex_exp), errors="coerce").strftime("%Y-%m-%d") if pd.notnull(r.get(ex_exp)) else out["next_expiry"]
                if out["nearest_dte"] is None and ex_dte:
                    out["nearest_dte"] = _coerce_int(r.get(ex_dte)) or out["nearest_dte"]

        # 4) Dir/Strength minimal absichern
        if out["dir"] not in (-1,0,1):
            out["dir"] = 0
        if out["strength"] is None or out["strength"] < 0:
            out["strength"] = 0
        if out["nearest_dte"] is not None and out["nearest_dte"] < 0:
            out["nearest_dte"] = 0

        rows.append(out)

    out_df = pd.DataFrame(rows, columns=[
        "symbol","dir","strength","next_expiry","nearest_dte",
        "focus_strike","focus_strike_7","focus_strike_30","focus_strike_60"
    ])

    # konsistente Typen/Formats
    out_df["next_expiry"] = pd.to_datetime(out_df["next_expiry"], errors="coerce").dt.strftime("%Y-%m-%d")
    out_df["nearest_dte"] = pd.to_numeric(out_df["nearest_dte"], errors="coerce").fillna("").astype("Int64")

    out_gz = PROC / "direction_signal.csv.gz"
    with gzip.open(out_gz, "wt", encoding="utf-8", newline="") as f:
        out_df.to_csv(f, index=False)
    print("wrote", out_gz, "rows=", len(out_df))

if __name__ == "__main__":
    main()
