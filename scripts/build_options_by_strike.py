#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_options_by_strike.py
Ziel: Eine robuste, standardisierte Datei
  data/processed/options_oi_by_strike.csv
erzeugen, die (mindestens) diese Spalten enthält:
  symbol, expiry, dte, focus_strike, focus_side, method

- Liest bevorzugt:
    data/processed/options_oi_summary.csv(.gz)       (für Top-/Focus-Strikes je Horizont)
    data/processed/options_oi_by_expiry.csv(.gz)     (für nearest expiry / dte)
- Fallbacks:
    data/processed/options_oi_totals.csv(.gz)
- Alle Leser sind fehlertolerant, Spaltennamen werden heuristisch erkannt.

Hinweis:
Dieses Script schreibt eine "general"-Zeile (pro Symbol die nächste relevante Expiry
+ ein Focus-Strike), damit Overlays/Scanner immer etwas anzeigen können.
"""

import os, sys, gzip, io, datetime as dt
import pandas as pd
from pathlib import Path

PROC = Path("data/processed")

def _read_any(path_csv: Path):
    """Liest .csv oder .csv.gz wenn vorhanden, sonst None."""
    if path_csv.with_suffix(".csv").exists():
        return pd.read_csv(path_csv.with_suffix(".csv"))
    if path_csv.with_suffix(".csv.gz").exists():
        with gzip.open(path_csv.with_suffix(".csv.gz"), "rt", encoding="utf-8") as f:
            return pd.read_csv(f)
    return None

def _to_date(s):
    try:
        return pd.to_datetime(s, errors="coerce").dt.date
    except Exception:
        return pd.to_datetime(pd.Series(s), errors="coerce").dt.date

def _first_notnull(series, candidates):
    for c in candidates:
        if c in series.index and pd.notnull(series[c]):
            return series[c]
    return None

def _norm_symbol(s):
    try:
        return str(s).strip()
    except Exception:
        return s

def main():
    PROC.mkdir(parents=True, exist_ok=True)

    df_sum  = _read_any(PROC / "options_oi_summary")     # optional
    df_exp  = _read_any(PROC / "options_oi_by_expiry")   # optional
    df_tot  = _read_any(PROC / "options_oi_totals")      # optional

    if df_sum is None and df_exp is None and df_tot is None:
        print("WARN: Keine Eingabedateien gefunden (summary/expiry/totals). Abbruch.", file=sys.stderr)
        sys.exit(0)

    # ---- nearest expiry/dte je Symbol bestimmen ----
    nearest_map = {}
    if df_exp is not None:
        # Spalten-Heuristik
        sym_col = "symbol" if "symbol" in df_exp.columns else df_exp.columns[0]
        exp_col = "expiry" if "expiry" in df_exp.columns else None
        dte_col = None
        for c in ["dte", "days_to_expiry", "nearest_dte", "days"]:
            if c in df_exp.columns:
                dte_col = c; break

        # normalize
        if exp_col:
            df_exp["__exp"] = _to_date(df_exp[exp_col])
        else:
            df_exp["__exp"] = pd.NaT

        if dte_col:
            df_exp["__dte"] = pd.to_numeric(df_exp[dte_col], errors="coerce")
        else:
            # falls kein DTE: nehme min positive (oder kleinste) Expiry je Symbol
            df_exp["__dte"] = None

        # Für jedes Symbol: die kleinste positive DTE; sonst kleinste DTE
        for sym, grp in df_exp.groupby(sym_col):
            g = grp.copy()
            if g["__dte"].notna().any():
                gpos = g[g["__dte"].fillna(0) >= 0]
                if len(gpos):
                    row = gpos.sort_values(["__dte", "__exp"], ascending=True).iloc[0]
                else:
                    row = g.sort_values("__dte").iloc[0]
            else:
                # Fallback über Datum
                g = g[g["__exp"].notna()]
                if len(g):
                    row = g.sort_values("__exp").iloc[0]
                else:
                    row = grp.iloc[0]

            nearest_map[_norm_symbol(sym)] = {
                "expiry": row["__exp"],
                "dte":    int(row["__dte"]) if pd.notnull(row["__dte"]) else None
            }

    # ---- Focus-Strike bestimmen ----
    # Quellenpriorität:
    # 1) options_oi_summary: Spalten wie focus_strike, max_oi_strike, top_call_strike, top_put_strike, ...
    # 2) totals: evtl. keine strike-Info -> None
    focus_rows = []

    if df_sum is not None:
        # Heuristische Spaltennamen sammeln
        candidates_general = [
            "focus_strike", "max_oi_strike", "strike_focus", "strike_poc",
            "top_strike", "best_strike"
        ]
        # (falls summary mehrere Horizonte enthält, ist das okay — wir schreiben "method=summary")
        sym_col = "symbol" if "symbol" in df_sum.columns else df_sum.columns[0]
        for sym, grp in df_sum.groupby(sym_col):
            sym = _norm_symbol(sym)
            row = grp.iloc[0]
            fs = _first_notnull(row, candidates_general)
            side = _first_notnull(row, ["focus_side", "bias_side", "side"])
            # normalisieren
            try:
                fs = float(fs) if fs is not None else None
            except Exception:
                fs = None
            side = str(side).lower() if side is not None else ""

            # nearest expiry/dte mergen
            nexp = nearest_map.get(sym, {})
            focus_rows.append({
                "symbol": sym,
                "expiry": nexp.get("expiry"),
                "dte":    nexp.get("dte"),
                "focus_strike": fs,
                "focus_side": side if side in ("call","put","mixed","") else "",
                "method": "summary"
            })

    if not focus_rows and df_tot is not None:
        # Fallback: totals hat keine Strike-Details — wir liefern nur expiry/dte
        sym_col = "symbol" if "symbol" in df_tot.columns else df_tot.columns[0]
        for sym, _grp in df_tot.groupby(sym_col):
            sym = _norm_symbol(sym)
            nexp = nearest_map.get(sym, {})
            focus_rows.append({
                "symbol": sym,
                "expiry": nexp.get("expiry"),
                "dte":    nexp.get("dte"),
                "focus_strike": None,
                "focus_side": "",
                "method": "totals_fallback"
            })

    if not focus_rows:
        print("WARN: Konnte keine Focus-Strikes bestimmen. Schreibe leere Datei.", file=sys.stderr)
        out = PROC / "options_oi_by_strike.csv"
        pd.DataFrame(columns=["symbol","expiry","dte","focus_strike","focus_side","method"]).to_csv(out, index=False)
        print("wrote", out)
        return

    out_df = pd.DataFrame(focus_rows)
    # Datumsformat standardisieren
    if "expiry" in out_df.columns:
        out_df["expiry"] = pd.to_datetime(out_df["expiry"], errors="coerce").dt.strftime("%Y-%m-%d")

    out_df = out_df[["symbol","expiry","dte","focus_strike","focus_side","method"]]

    out_path = PROC / "options_oi_by_strike.csv"
    out_df.to_csv(out_path, index=False)
    print("wrote", out_path, "rows=", len(out_df))

if __name__ == "__main__":
    main()
