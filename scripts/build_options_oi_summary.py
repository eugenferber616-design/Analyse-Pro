#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_options_oi_summary.py
---------------------------
Re-enriches options_oi_summary.csv so that the AgenaTrader
OptionsData_Scanner can use it directly.

Input (from pipeline):
  - data/processed/options_oi_summary.csv   (basic v60 summary)
  - data/processed/options_oi_totals.csv   (optional, for max_oi_expiry)
  - data/processed/options_oi_by_expiry.csv (optional fallback for expiry)

Output (overwrite):
  - data/processed/options_oi_summary.csv   (enriched)

Added/derived columns:
  - hv20, hv_20              ← hv_current
  - call_oi, put_oi          ← total_call_oi / total_put_oi
  - total_oi                 ← call_oi + put_oi
  - put_call_ratio           ← pcr_total
  - upper_bound              ← first element of call_top_strikes
  - lower_bound              ← first element of put_top_strikes
  - expiry                   ← max_oi_expiry (oder bestes expiry aus options_oi_by_expiry)
  - iv                       ← bevorzugt call_iv_w / iv_atm / iv30d, sonst hv_current
  - expected_move            ← spot * hv_current * sqrt(DTE / 365)
"""

from pathlib import Path
from datetime import datetime, date
import math
import numpy as np
import pandas as pd


BASE = Path("data/processed")


def _first_from_list_string(s: str):
    """
    Nimmt z.B. "[700.0, 710.0, 680.0]" und gibt 700.0 zurück.
    Falls Parsing fehlschlägt → np.nan.
    """
    if not isinstance(s, str):
        return np.nan
    s = s.strip()
    if not s:
        return np.nan
    # Klammern entfernen
    if s[0] == "[" and s[-1] == "]":
        s = s[1:-1]
    first = s.split(",")[0].strip()
    if not first:
        return np.nan
    first = first.strip('"').strip("'")
    try:
        return float(first)
    except Exception:
        try:
            return float(first.replace(",", "."))
        except Exception:
            return np.nan


def _load_totals():
    """
    Nutzt options_oi_totals.csv, falls vorhanden:
      symbol, total_oi, max_oi_expiry, max_oi_value
    """
    path = BASE / "options_oi_totals.csv"
    if path.exists():
        df = pd.read_csv(path)
        if not {"symbol", "max_oi_expiry"}.issubset(df.columns):
            return None
        df = df[["symbol", "max_oi_expiry"]].rename(columns={"max_oi_expiry": "expiry"})
        return df
    return None


def _load_expiry_fallback():
    """
    Fallback, falls options_oi_totals.csv fehlt:
    Nimm je Symbol das expiry mit dem höchsten total_oi
    aus options_oi_by_expiry.csv.

    Erlaubt zwei Varianten:
      - total_oi ist schon vorhanden
      - total_call_oi + total_put_oi werden zu total_oi addiert
    """
    path = BASE / "options_oi_by_expiry.csv"
    if not path.exists():
        return None

    df = pd.read_csv(path)

    if not {"symbol", "expiry"}.issubset(df.columns):
        return None

    # total_oi zur Not aus total_call_oi + total_put_oi bauen
    if "total_oi" not in df.columns:
        if {"total_call_oi", "total_put_oi"}.issubset(df.columns):
            df["total_oi"] = (
                pd.to_numeric(df["total_call_oi"], errors="coerce").fillna(0)
                + pd.to_numeric(df["total_put_oi"], errors="coerce").fillna(0)
            )
        else:
            return None

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")

    df = (
        df.dropna(subset=["expiry", "total_oi"])
          .sort_values(["symbol", "total_oi"], ascending=[True, False])
          .groupby("symbol", as_index=False)
          .first()[["symbol", "expiry"]]
    )

    df["expiry"] = df["expiry"].dt.strftime("%Y-%m-%d")
    return df


def _calc_expected_move(row, today):
    """
    Einfache Expected-Move-Annäherung:
      EM ≈ spot * hv_current * sqrt(DTE / 365)
    hv_current ist annualisierte Volatilität (0.15 = 15%).
    """
    spot = row.get("spot", np.nan)
    hv = row.get("hv_current", np.nan)
    if pd.isna(spot) or pd.isna(hv):
        return np.nan

    exp = row.get("expiry", None)
    if isinstance(exp, str):
        try:
            exp_dt = pd.to_datetime(exp).date()
        except Exception:
            exp_dt = None
    elif isinstance(exp, pd.Timestamp):
        exp_dt = exp.date()
    elif isinstance(exp, date):
        exp_dt = exp
    else:
        exp_dt = None

    if exp_dt is not None:
        dte = max((exp_dt - today).days, 1)
    else:
        # Fallback: 30 Tage
        dte = 30

    return float(spot) * float(hv) * math.sqrt(float(dte) / 365.0)


def main():
    base_path = BASE / "options_oi_summary.csv"
    if not base_path.exists():
        raise SystemExit("options_oi_summary.csv not found under {}".format(base_path))

    df = pd.read_csv(base_path)

    # --- Basismappings ---
    if "hv_current" in df.columns:
        df["hv20"] = df["hv_current"]
        df["hv_20"] = df["hv_current"]

    if {"total_call_oi", "total_put_oi"}.issubset(df.columns):
        df["call_oi"] = df["total_call_oi"]
        df["put_oi"] = df["total_put_oi"]
        df["total_oi"] = df["total_call_oi"] + df["total_put_oi"]

    if "pcr_total" in df.columns:
        df["put_call_ratio"] = df["pcr_total"]

    # Ober-/Untergrenze aus den Top-Strikes
    if "call_top_strikes" in df.columns:
        df["upper_bound"] = df["call_top_strikes"].apply(_first_from_list_string)
    if "put_top_strikes" in df.columns:
        df["lower_bound"] = df["put_top_strikes"].apply(_first_from_list_string)

    # --- Expiry-Mapping ---
    expiry_map = _load_totals()
    if expiry_map is None:
        expiry_map = _load_expiry_fallback()

    if expiry_map is not None:
        # Cleanup potential duplicate columns before merge
        cols_to_drop = [c for c in ["expiry", "expiry_x", "expiry_y"] if c in df.columns]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)
            
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df = df.merge(expiry_map, on="symbol", how="left")

    # --- IV-Spalte erzeugen ---
    if "iv" not in df.columns:
        iv_source = None
        for cand in ["call_iv_w", "iv_atm", "iv30d", "iv_30d"]:
            if cand in df.columns:
                iv_source = cand
                break

        if iv_source is not None:
            df["iv"] = pd.to_numeric(df[iv_source], errors="coerce")
        elif "hv_current" in df.columns:
            # Fallback: HV als Proxy-IV (besser als gar nichts)
            df["iv"] = pd.to_numeric(df["hv_current"], errors="coerce")
        else:
            df["iv"] = np.nan

    # --- Expected Move berechnen ---
    today = datetime.utcnow().date()
    df["expected_move"] = df.apply(_calc_expected_move, axis=1, today=today)

    df["summary_enriched_at"] = datetime.utcnow().isoformat()

    # Alles wieder rausschreiben
    df.to_csv(base_path, index=False)
    print("✔ options_oi_summary.csv angereichert und gespeichert:", base_path)


if __name__ == "__main__":
    main()
