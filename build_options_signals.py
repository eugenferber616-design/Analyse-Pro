#!/usr/bin/env python3
# scripts/build_options_signals.py
#
# Erzeugt eine schlanke Signaltabelle für den AgenaTrader-Scanner:
#   symbol, direction(-1/0/+1), expiry(YYYY-MM-DD), side, focus_strike,
#   call_strike_top, put_strike_top
#
# Logik:
# - Quelle: data/processed/options_oi_summary.csv(.gz)
# - Pro Symbol den Verfall mit dem höchsten (call_oi + put_oi) wählen.
# - direction = sign(call_oi - put_oi).
# - side = "C" (Calls > Puts), "P" (Puts > Calls), "N" (neutral/gleich/keine Daten).
# - call_strike_top / put_strike_top = erster Wert aus call_top_strikes / put_top_strikes.
# - focus_strike = call_strike_top, wenn side="C";
#                  put_strike_top,  wenn side="P";
#                  sonst NaN.
#
# Optional: --with-metrics fügt call_oi_expiry, put_oi_expiry, total_oi_expiry an.

import argparse
import gzip
import sys
from pathlib import Path
from typing import Tuple, List, Optional

import numpy as np
import pandas as pd


# ----------------------------- Helper ----------------------------------------

def read_csv_auto(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        # try gz-Variante
        if p.suffix != ".gz" and (p.with_suffix(p.suffix + ".gz")).exists():
            p = p.with_suffix(p.suffix + ".gz")
        elif p.suffix == ".gz" and p.with_suffix("").exists():
            p = p.with_suffix("")
    if not p.exists():
        raise FileNotFoundError(f"Input not found: {path}")

    if str(p).endswith(".gz"):
        with gzip.open(p, "rt", encoding="utf-8") as f:
            return pd.read_csv(f)
    return pd.read_csv(p)


def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def to_dt(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    if "symbol" not in df.columns:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def parse_first_strike(val) -> float:
    """
    Versucht aus call_top_strikes / put_top_strikes den ersten Strike als float zu holen.
    Akzeptiert Formate wie:
      "[150, 155, 160]"
      "150,155,160"
      "150; 155; 160"
      "150|155|160"
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return np.nan

    s = str(val).strip()
    if not s:
        return np.nan

    # Klammern entfernen
    for ch in "[](){}'\"":
        s = s.replace(ch, " ")

    # Trennzeichen vereinheitlichen
    for sep in [";", "|"]:
        s = s.replace(sep, ",")

    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return np.nan

    for p in parts:
        try:
            return float(p)
        except Exception:
            continue
    return np.nan


# ----------------------------- Kernlogik -------------------------------------

def build_options_signals(
    summary_path: str,
    out_path: str,
    horizon_days: int,
    with_metrics: bool
) -> None:
    # 1) Summary laden
    try:
        summ = read_csv_auto(summary_path)
    except Exception as e:
        print(f"[ERR] cannot read {summary_path}: {e}", file=sys.stderr)
        sys.exit(2)

    if summ is None or summ.empty:
        print("[WARN] options_oi_summary leer; schreibe leere options_signals.csv")
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["symbol", "direction", "expiry", "side",
                              "focus_strike", "call_strike_top", "put_strike_top"]
                     ).to_csv(out_path, index=False)
        return

    df = summ.copy()
    df = ensure_cols(df)

    # Normalisieren
    df["symbol"] = df.get("symbol", pd.Series(dtype="object")).astype(str).str.upper().str.strip()
    df["expiry"] = to_dt(df.get("expiry", pd.Series(dtype="datetime64[ns]")))

    df["call_oi"] = to_num(df.get("call_oi", pd.Series(dtype="float64"))).fillna(0.0)
    df["put_oi"]  = to_num(df.get("put_oi",  pd.Series(dtype="float64"))).fillna(0.0)
    df["total_oi"] = df["call_oi"] + df["put_oi"]

    df = df[df["expiry"].notna()]
    today = pd.Timestamp.today().normalize()
    df["days"] = (df["expiry"] - today).dt.days
    df = df[df["days"] >= 0]

    if horizon_days > 0:
        df = df[df["days"] <= horizon_days]

    if df.empty:
        print("[WARN] keine künftigen Expiries im Horizont; schreibe leere Datei")
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["symbol", "direction", "expiry", "side",
                              "focus_strike", "call_strike_top", "put_strike_top"]
                     ).to_csv(out_path, index=False)
        return

    # 2) Pro Symbol den Verfall mit max(total_oi) wählen
    idx = df.groupby("symbol")["total_oi"].idxmax()
    top = df.loc[idx].copy()

    # 3) direction & side
    top["direction"] = np.sign(top["call_oi"] - top["put_oi"]).astype(int)

    def side_fn(row) -> str:
        d = row.get("direction", 0)
        try:
            d = int(d)
        except Exception:
            d = 0
        call_oi = float(row.get("call_oi", 0.0) or 0.0)
        put_oi  = float(row.get("put_oi",  0.0) or 0.0)
        if d > 0 and call_oi > 0:
            return "C"
        if d < 0 and put_oi > 0:
            return "P"
        return "N"

    top["side"] = top.apply(side_fn, axis=1)

    # 4) Top-Strikes aus call_top_strikes / put_top_strikes parsen
    top["call_strike_top"] = top.get("call_top_strikes", pd.Series(dtype=object)).apply(parse_first_strike)
    top["put_strike_top"]  = top.get("put_top_strikes",  pd.Series(dtype=object)).apply(parse_first_strike)

    # 5) focus_strike je nach side bestimmen
    def focus_fn(row) -> float:
        s = row.get("side", "N")
        if s == "C":
            return float(row.get("call_strike_top", np.nan))
        if s == "P":
            return float(row.get("put_strike_top", np.nan))
        return np.nan

    top["focus_strike"] = top.apply(focus_fn, axis=1)

    # 6) Output vorbereiten
    out_rows = []
    for _, r in top.iterrows():
        sym = str(r["symbol"]).upper().strip()
        exp_ts = r["expiry"]
        exp_str = exp_ts.strftime("%Y-%m-%d") if pd.notna(exp_ts) else ""

        out_row = {
            "symbol": sym,
            "direction": int(max(-1, min(1, int(r.get("direction", 0))))),
            "expiry": exp_str,
            "side": r.get("side", "N"),
            "focus_strike": (np.nan if pd.isna(r.get("focus_strike"))
                             else float(r.get("focus_strike"))),
            "call_strike_top": (np.nan if pd.isna(r.get("call_strike_top"))
                                else float(r.get("call_strike_top"))),
            "put_strike_top": (np.nan if pd.isna(r.get("put_strike_top"))
                               else float(r.get("put_strike_top"))),
        }

        if with_metrics:
            out_row["call_oi_expiry"] = float(r.get("call_oi", np.nan))
            out_row["put_oi_expiry"]  = float(r.get("put_oi", np.nan))
            out_row["total_oi_expiry"] = float(r.get("total_oi", np.nan))

        out_rows.append(out_row)

    out_df = pd.DataFrame(out_rows)

    if not with_metrics:
        cols = ["symbol", "direction", "expiry", "side",
                "focus_strike", "call_strike_top", "put_strike_top"]
        out_df = out_df[cols]

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print("wrote", out_path, "rows=", len(out_df), "cols=", len(out_df.columns))


# ----------------------------- CLI -------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Build compact options_signals.csv for AgenaTrader scanner (inkl. Strikes aus options_oi_summary).")
    ap.add_argument("--summary", default="data/processed/options_oi_summary.csv",
                    help="Pfad zu options_oi_summary.csv oder .csv.gz")
    ap.add_argument("--out", default="data/processed/options_signals.csv",
                    help="Output CSV (Workflow gzippt später)")
    ap.add_argument("--horizon-days", type=int, default=365,
                    help="Look-ahead window in days (0/negativ = kein Limit nach oben)")
    ap.add_argument("--with-metrics", action="store_true",
                    help="call_oi_expiry / put_oi_expiry / total_oi_expiry anhängen")
    args = ap.parse_args()

    horizon = args.horizon_days
    if horizon is None or horizon <= 0:
        horizon = 365 * 10  # praktisch 'kein Limit'

    build_options_signals(
        summary_path=args.summary,
        out_path=args.out,
        horizon_days=horizon,
        with_metrics=args.with_metrics
    )

if __name__ == "__main__":
    main()
