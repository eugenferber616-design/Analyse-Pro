# scripts/build_options_signals.py
# Erzeugt eine schlanke Signaltabelle für den AgenaTrader-Scanner:
#   symbol, direction(-1/0/+1), expiry(YYYY-MM-DD), strike
#
# Logik:
# - Wähle pro Symbol den Verfall mit dem höchsten total_OI innerhalb des Horizonts.
# - direction = sign(call_OI - put_OI) am gewählten Verfall.
# - strike = bei direction>0: Strike mit max(call_oi),
#           bei direction<0: Strike mit max(put_oi),
#           sonst NaN; Fallback: strike_max (wenn vorhanden).
#
# Akzeptiert sowohl .csv als auch .csv.gz.
# Optional: --with-metrics fügt Diagnosefelder an (call_oi_expiry, put_oi_expiry, total_oi_expiry).

import os
import sys
import gzip
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Tuple

def read_csv_auto(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        # try gz variant
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

def coalesce(df: pd.DataFrame, names: Tuple[str, ...]) -> pd.Series:
    for c in names:
        if c in df.columns:
            return df[c]
    # return empty series if none found
    return pd.Series(index=df.index, dtype="float64")

def ensure_cols(df: pd.DataFrame):
    # Normalisiere wichtige Spaltennamen; akzeptiere verschiedene Varianten aus deinem Pipeline-Verlauf
    # by_expiry: symbol, expiry, total_call_oi|call_oi, total_put_oi|put_oi, total_oi
    if "symbol" not in df.columns:
        # Versuche Lowercase-Trim
        df.columns = [str(c).strip() for c in df.columns]
    return df

def pick_best_expiry(byex: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    """Wählt pro Symbol den Verfall mit maximalem total_OI innerhalb des Horizonts."""
    df = byex.copy()
    df = ensure_cols(df)

    # Alias-Spalten
    call_col = coalesce(df, ("total_call_oi", "call_oi"))
    put_col  = coalesce(df, ("total_put_oi", "put_oi"))
    tot_col  = coalesce(df, ("total_oi",))

    # Casts
    df["__call"] = to_num(call_col).fillna(0)
    df["__put"]  = to_num(put_col).fillna(0)
    df["__tot"]  = to_num(tot_col).fillna(df["__call"] + df["__put"])
    df["expiry"] = to_dt(df.get("expiry", pd.Series(dtype="datetime64[ns]")))
    df["symbol"] = df.get("symbol", pd.Series(dtype="object")).astype(str).str.upper().str.strip()

    today = pd.Timestamp.today().normalize()
    df = df[df["expiry"].notna()]
    df["days"] = (df["expiry"] - today).dt.days
    df = df[(df["days"] >= 0) & (df["days"] <= int(horizon_days))]

    if df.empty:
        return pd.DataFrame(columns=["symbol", "expiry", "call_oi_expiry", "put_oi_expiry", "total_oi_expiry"])

    # Top expiry nach total OI
    idx = df.groupby("symbol")["__tot"].idxmax()
    top = df.loc[idx, ["symbol", "expiry", "__call", "__put", "__tot"]].copy()
    top.rename(columns={
        "__call": "call_oi_expiry",
        "__put":  "put_oi_expiry",
        "__tot":  "total_oi_expiry"
    }, inplace=True)
    return top

def choose_strike(bystr: pd.DataFrame, sym: str, expiry: pd.Timestamp, direction: int) -> float:
    """Wählt den dominanten Strike für (sym, expiry) abhängig von der Richtung."""
    if expiry is None or pd.isna(expiry):
        return np.nan
    df = bystr.copy()
    df = ensure_cols(df)
    df["symbol"] = df.get("symbol", pd.Series(dtype="object")).astype(str).str.upper().str.strip()
    df = df[df["symbol"] == sym]
    df["expiry"] = to_dt(df.get("expiry", pd.Series(dtype="datetime64[ns]")))
    df = df[df["expiry"].dt.date == expiry.date()]
    if df.empty:
        return np.nan

    df["strike"]  = to_num(df.get("strike", pd.Series(dtype=float)))
    call_col = coalesce(df, ("call_oi", "total_call_oi"))
    put_col  = coalesce(df, ("put_oi", "total_put_oi"))
    df["__call"]  = to_num(call_col).fillna(0)
    df["__put"]   = to_num(put_col).fillna(0)

    if direction > 0:
        # Bullisch → größtes Call OI
        row = df.loc[df["__call"].idxmax()] if df["__call"].notna().any() else None
    elif direction < 0:
        # Bärisch → größtes Put OI
        row = df.loc[df["__put"].idxmax()] if df["__put"].notna().any() else None
    else:
        return np.nan

    if row is None:
        return np.nan
    return float(row.get("strike", np.nan))

def main():
    ap = argparse.ArgumentParser(description="Build compact options_signals.csv for AgenaTrader scanner.")
    ap.add_argument("--by-expiry", default="data/processed/options_oi_by_expiry.csv", help="Path to by_expiry CSV or CSV.GZ")
    ap.add_argument("--by-strike", default="data/processed/options_oi_by_strike.csv", help="Path to by_strike CSV or CSV.GZ")
    ap.add_argument("--strike-max", default="data/processed/options_oi_strike_max.csv", help="Optional fallback CSV/CSV.GZ for strike")
    ap.add_argument("--out", default="data/processed/options_signals.csv", help="Output CSV (plain; Workflow gzipt ggf. später)")
    ap.add_argument("--horizon-days", type=int, default=30, help="Look-ahead window in days for picking best expiry")
    ap.add_argument("--with-metrics", action="store_true", help="Append diagnostic columns (call_oi_expiry, put_oi_expiry, total_oi_expiry)")
    args = ap.parse_args()

    # Laden
    try:
        byex = read_csv_auto(args.by_expiry)
    except Exception as e:
        print(f"[ERR] cannot read by_expiry: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        bystr = read_csv_auto(args.by_strike)
    except Exception as e:
        print(f"[ERR] cannot read by_strike: {e}", file=sys.stderr)
        sys.exit(2)

    # optional strike_max (Fallback)
    strike_max_df = None
    try:
        strike_max_df = read_csv_auto(args.strike_max)
    except Exception:
        strike_max_df = None  # ok if absent

    # 1) Bester Verfall pro Symbol
    top = pick_best_expiry(byex, args.horizon_days)
    if top.empty:
        # wenn nichts im Horizont → wähle global besten Verfall (max __tot) ohne Horizon-Filter
        df = byex.copy()
        df = ensure_cols(df)
        df["__call"] = to_num(coalesce(df, ("total_call_oi", "call_oi"))).fillna(0)
        df["__put"]  = to_num(coalesce(df, ("total_put_oi", "put_oi"))).fillna(0)
        df["__tot"]  = to_num(coalesce(df, ("total_oi",))).fillna(df["__call"] + df["__put"])
        df["expiry"] = to_dt(df.get("expiry", pd.Series(dtype="datetime64[ns]")))
        df["symbol"] = df.get("symbol", pd.Series(dtype="object")).astype(str).str.upper().str.strip()
        if not df.empty:
            idx = df.groupby("symbol")["__tot"].idxmax()
            top = df.loc[idx, ["symbol", "expiry", "__call", "__put", "__tot"]].copy()
            top.rename(columns={"__call":"call_oi_expiry","__put":"put_oi_expiry","__tot":"total_oi_expiry"}, inplace=True)

    if top.empty:
        print("[WARN] no rows selected; writing empty output")
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["symbol","direction","expiry","strike"]).to_csv(args.out, index=False)
        print("wrote", args.out, "rows=0")
        return

    # 2) Richtung bestimmen
    top["direction"] = np.sign(to_num(top["call_oi_expiry"]) - to_num(top["put_oi_expiry"])).astype(int)

    # 3) Strike je Symbol bestimmen
    out_rows = []
    # Schneller Zugriff auf by_strike
    bystr["_sym"] = bystr.get("symbol", pd.Series(dtype="object")).astype(str).str.upper().str.strip()
    bystr["_exp"] = to_dt(bystr.get("expiry", pd.Series(dtype="datetime64[ns]"))).dt.date

    # optionales Mapping aus strike_max (Fallback)
    strike_max_map = {}
    if strike_max_df is not None and not strike_max_df.empty:
        smax = strike_max_df.copy()
        smax["symbol"] = smax.get("symbol", pd.Series(dtype="object")).astype(str).str.upper().str.strip()
        smax["expiry"] = to_dt(smax.get("expiry", pd.Series(dtype="datetime64[ns]"))).dt.date
        # bevorzugt: separate Spalten call_strike_max / put_strike_max falls vorhanden
        call_sm = smax.get("call_strike_max")
        put_sm  = smax.get("put_strike_max")
        if call_sm is not None or put_sm is not None:
            for _, r in smax.iterrows():
                strike_max_map[(r["symbol"], r["expiry"])] = {
                    "call": float(pd.to_numeric(r.get("call_strike_max"), errors="coerce")) if "call_strike_max" in smax.columns else np.nan,
                    "put":  float(pd.to_numeric(r.get("put_strike_max"), errors="coerce")) if "put_strike_max"  in smax.columns else np.nan
                }

    for _, r in top.iterrows():
        sym = str(r["symbol"]).upper().strip()
        exp = pd.to_datetime(r["expiry"]).date() if pd.notna(r["expiry"]) else None
        d   = int(r["direction"])
        strike = choose_strike(bystr, sym, pd.Timestamp(exp) if exp else None, d)

        if (np.isnan(strike) or strike == 0.0) and strike_max_map:
            fm = strike_max_map.get((sym, exp))
            if fm:
                strike = fm["call"] if d > 0 else (fm["put"] if d < 0 else np.nan)

        out_rows.append({
            "symbol": sym,
            "direction": int(max(-1, min(1, d))),
            "expiry": pd.Timestamp(exp).strftime("%Y-%m-%d") if exp else "",
            "strike": np.nan if (strike is None or np.isnan(strike)) else float(strike),
            # optionale Diagnostik:
            "call_oi_expiry": float(r.get("call_oi_expiry", np.nan)),
            "put_oi_expiry":  float(r.get("put_oi_expiry",  np.nan)),
            "total_oi_expiry":float(r.get("total_oi_expiry",np.nan)),
        })

    out = pd.DataFrame(out_rows)

    if not args.with_metrics:
        out = out[["symbol","direction","expiry","strike"]]

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print("wrote", args.out, "rows=", len(out))

if __name__ == "__main__":
    main()
