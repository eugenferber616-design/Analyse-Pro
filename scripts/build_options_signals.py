#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/build_options_signals.py

Output (AgenaTrader):
  symbol, direction(-1/0/+1), expiry(YYYY-MM-DD), side(C/P/N),
  focus_strike, call_strike_top, put_strike_top
Optional --with-metrics:
  call_oi_expiry, put_oi_expiry, total_oi_expiry

Data Sources:
  - options_oi_by_expiry.csv: symbol, expiry, total_call_oi, total_put_oi, total_oi
  - options_oi_by_strike.csv: symbol, expiry, strike, call_oi, put_oi (optional, for focus_strike)

NOTE: direction = sign(call_oi - put_oi) is NOT a true directional signal!
      It only shows "where is more interest" (OI ≠ Flow).
"""

import argparse
import gzip
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def read_csv_auto(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
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


def pick_side_and_dir(call_oi: float, put_oi: float):
    d = int(np.sign(call_oi - put_oi))
    if d > 0 and call_oi > 0:
        return d, "C"
    if d < 0 and put_oi > 0:
        return d, "P"
    return 0, "N"


def build_options_signals(
    by_expiry_path: str,
    by_strike_path: str,
    out_path: str,
    horizon_days: int,
    with_metrics: bool
) -> None:
    # ----------------- load by_expiry -----------------
    try:
        exp = read_csv_auto(by_expiry_path)
    except Exception as e:
        print(f"[ERR] cannot read {by_expiry_path}: {e}", file=sys.stderr)
        sys.exit(2)

    if exp is None or exp.empty:
        print("[WARN] options_oi_by_expiry leer; schreibe leere options_signals.csv")
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=[
            "symbol","direction","expiry","side","focus_strike","call_strike_top","put_strike_top"
        ]).to_csv(out_path, index=False)
        return

    df = exp.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # required columns: symbol, expiry, total_call_oi, total_put_oi (total_oi optional)
    df["symbol"] = df.get("symbol", "").astype(str).str.upper().str.strip()
    df["expiry"] = to_dt(df.get("expiry", pd.Series(dtype="datetime64[ns]")))

    df["call_oi"] = to_num(df.get("total_call_oi", df.get("call_oi", 0))).fillna(0.0)
    df["put_oi"]  = to_num(df.get("total_put_oi",  df.get("put_oi",  0))).fillna(0.0)
    if "total_oi" in df.columns:
        df["total_oi"] = to_num(df["total_oi"]).fillna(df["call_oi"] + df["put_oi"])
    else:
        df["total_oi"] = df["call_oi"] + df["put_oi"]

    df = df[df["symbol"].notna() & (df["symbol"] != "")]
    df = df[df["expiry"].notna()]

    # Use tz-naive timestamp to match expiry dates from CSV
    today = pd.Timestamp.today().normalize()
    df["days"] = (df["expiry"] - today).dt.days
    df = df[df["days"] >= 0]

    if horizon_days > 0:
        df = df[df["days"] <= horizon_days]

    if df.empty:
        print("[WARN] keine künftigen Expiries im Horizont; schreibe leere Datei")
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=[
            "symbol","direction","expiry","side","focus_strike","call_strike_top","put_strike_top"
        ]).to_csv(out_path, index=False)
        return

    # ----------------- pick max total_oi expiry per symbol -----------------
    idx = df.groupby("symbol")["total_oi"].idxmax()
    top = df.loc[idx, ["symbol","expiry","call_oi","put_oi","total_oi"]].copy()

    top["direction"] = 0
    top["side"] = "N"
    for i, r in top.iterrows():
        d, s = pick_side_and_dir(float(r["call_oi"]), float(r["put_oi"]))
        top.at[i, "direction"] = d
        top.at[i, "side"] = s

    # defaults (in case by_strike missing)
    top["call_strike_top"] = np.nan
    top["put_strike_top"]  = np.nan
    top["focus_strike"]    = np.nan

    # ----------------- optional: enrich strikes from by_strike -----------------
    bs_ok = False
    try:
        if by_strike_path and Path(by_strike_path).exists():
            bs = read_csv_auto(by_strike_path)
            bs_ok = (bs is not None and not bs.empty)
        else:
            bs = None
    except Exception:
        bs = None

    if bs_ok:
        bs = bs.copy()
        bs.columns = [str(c).strip() for c in bs.columns]
        
        # Check if required columns exist
        required_cols = ["symbol", "expiry", "strike"]
        if not all(c in bs.columns for c in required_cols):
            print(f"[INFO] by_strike file missing required columns {required_cols}, skipping strike enrichment")
            bs_ok = False
        else:
            bs["symbol"] = bs.get("symbol", "").astype(str).str.upper().str.strip()
            bs["expiry"] = to_dt(bs.get("expiry", pd.Series(dtype="datetime64[ns]")))
            bs["strike"] = to_num(bs.get("strike", np.nan))
            
            # Flexible column mapping for OI
            if "call_oi" in bs.columns:
                bs["call_oi"] = to_num(bs["call_oi"]).fillna(0.0)
            elif "total_call_oi" in bs.columns:
                bs["call_oi"] = to_num(bs["total_call_oi"]).fillna(0.0)
            else:
                bs["call_oi"] = 0.0
                
            if "put_oi" in bs.columns:
                bs["put_oi"] = to_num(bs["put_oi"]).fillna(0.0)
            elif "total_put_oi" in bs.columns:
                bs["put_oi"] = to_num(bs["total_put_oi"]).fillna(0.0)
            else:
                bs["put_oi"] = 0.0
                
            bs["total_oi"] = bs["call_oi"] + bs["put_oi"]

            want_syms = set(top["symbol"].tolist())
            want_exps = set(top["expiry"].dt.normalize().tolist())

            bs = bs[bs["symbol"].isin(want_syms)]
            bs = bs[bs["expiry"].dt.normalize().isin(want_exps)]
            bs = bs[bs["strike"].notna()]

            if not bs.empty:
                # precompute top call/put strikes per (symbol, expiry)
                def top_strike(sub, col):
                    sub2 = sub.sort_values(col, ascending=False)
                    if sub2.empty:
                        return np.nan
                    return float(sub2.iloc[0]["strike"])

                # Create normalized expiry for grouping
                bs["expiry_norm"] = bs["expiry"].dt.normalize()
                grp = bs.groupby(["symbol", "expiry_norm"])
                
                call_top = grp.apply(lambda x: top_strike(x, "call_oi"), include_groups=False).reset_index(name="call_strike_top")
                put_top = grp.apply(lambda x: top_strike(x, "put_oi"), include_groups=False).reset_index(name="put_strike_top")
                total_top = grp.apply(lambda x: top_strike(x, "total_oi"), include_groups=False).reset_index(name="total_strike_top")

                # Merge all strike data
                strikes_df = call_top.merge(put_top, on=["symbol", "expiry_norm"]).merge(total_top, on=["symbol", "expiry_norm"])
                
                # Add normalized expiry to top for merging
                top["expiry_norm"] = top["expiry"].dt.normalize()
                
                # Merge strikes back to top
                top = top.merge(strikes_df, on=["symbol", "expiry_norm"], how="left", suffixes=("", "_new"))
                
                # Update columns if merge was successful
                if "call_strike_top_new" in top.columns:
                    top["call_strike_top"] = top["call_strike_top_new"].fillna(top["call_strike_top"])
                    top["put_strike_top"] = top["put_strike_top_new"].fillna(top["put_strike_top"])
                    top = top.drop(columns=["call_strike_top_new", "put_strike_top_new", "total_strike_top"], errors="ignore")
                elif "call_strike_top" not in top.columns:
                    top["call_strike_top"] = strikes_df.set_index(["symbol", "expiry_norm"]).reindex(
                        pd.MultiIndex.from_arrays([top["symbol"], top["expiry_norm"]])
                    )["call_strike_top"].values
                    top["put_strike_top"] = strikes_df.set_index(["symbol", "expiry_norm"]).reindex(
                        pd.MultiIndex.from_arrays([top["symbol"], top["expiry_norm"]])
                    )["put_strike_top"].values

                # focus based on side
                def focus_row(side, c, p, t):
                    if side == "C":
                        return c
                    if side == "P":
                        return p
                    return t

                total_strike_map = strikes_df.set_index(["symbol", "expiry_norm"])["total_strike_top"]
                top["focus_strike"] = [
                    focus_row(s, c, p, total_strike_map.get((sym, exp), np.nan))
                    for sym, exp, s, c, p in zip(top["symbol"], top["expiry_norm"], top["side"], top["call_strike_top"], top["put_strike_top"])
                ]
                
                # Clean up temp column
                top = top.drop(columns=["expiry_norm"], errors="ignore")

    # ----------------- output -----------------
    out_rows = []
    for _, r in top.iterrows():
        exp_str = r["expiry"].strftime("%Y-%m-%d") if pd.notna(r["expiry"]) else ""
        out_row = {
            "symbol": str(r["symbol"]).upper().strip(),
            "direction": int(max(-1, min(1, int(r.get("direction", 0))))),
            "expiry": exp_str,
            "side": str(r.get("side", "N")),
            "focus_strike": (np.nan if pd.isna(r.get("focus_strike")) else float(r.get("focus_strike"))),
            "call_strike_top": (np.nan if pd.isna(r.get("call_strike_top")) else float(r.get("call_strike_top"))),
            "put_strike_top": (np.nan if pd.isna(r.get("put_strike_top")) else float(r.get("put_strike_top"))),
        }
        if with_metrics:
            out_row["call_oi_expiry"] = float(r.get("call_oi", np.nan))
            out_row["put_oi_expiry"] = float(r.get("put_oi", np.nan))
            out_row["total_oi_expiry"] = float(r.get("total_oi", np.nan))
        out_rows.append(out_row)

    out_df = pd.DataFrame(out_rows)

    cols = ["symbol","direction","expiry","side","focus_strike","call_strike_top","put_strike_top"]
    if with_metrics:
        cols += ["call_oi_expiry","put_oi_expiry","total_oi_expiry"]
    out_df = out_df[cols]

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print(f"wrote {out_path} rows={len(out_df)} cols={len(out_df.columns)} (strikes_from_by_strike={bs_ok})")


def main():
    ap = argparse.ArgumentParser(description="Build compact options_signals.csv for AgenaTrader scanner.")
    ap.add_argument("--by-expiry", default="data/processed/options_oi_by_expiry.csv",
                    help="Pfad zu options_oi_by_expiry.csv oder .csv.gz")
    ap.add_argument("--by-strike", default="data/processed/options_oi_by_strike.csv",
                    help="Pfad zu options_oi_by_strike.csv oder .csv.gz (optional, für focus_strike)")
    ap.add_argument("--out", default="data/processed/options_signals.csv",
                    help="Output CSV (Workflow gzippt später)")
    ap.add_argument("--horizon-days", type=int, default=365,
                    help="Look-ahead window in days (0/negativ = kein Limit nach oben)")
    ap.add_argument("--with-metrics", action="store_true",
                    help="call_oi_expiry / put_oi_expiry / total_oi_expiry anhängen")
    args = ap.parse_args()

    horizon = args.horizon_days if args.horizon_days and args.horizon_days > 0 else (365 * 10)

    build_options_signals(
        by_expiry_path=args.by_expiry,
        by_strike_path=args.by_strike,
        out_path=args.out,
        horizon_days=horizon,
        with_metrics=args.with_metrics
    )


if __name__ == "__main__":
    main()
