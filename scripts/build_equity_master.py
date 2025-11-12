#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds one wide master file per equity symbol by merging all processed datasets.
Input (best-effort, optional): direction_signal, options_signals, options_oi_by_strike,
options_oi_strike_max, hv_summary, fundamentals_core, earnings_next.json, cds_proxy, revisions.
Output: data/processed/equity_master.csv  (later gzipped by workflow)
"""

import os, json, argparse, pandas as pd
from datetime import datetime

PROC = "data/processed"
DOCS = "docs"

def _read_csv_any(*candidates):
    for p in candidates:
        if not p: 
            continue
        if os.path.exists(p):
            try:
                return pd.read_csv(p, compression="infer")
            except Exception:
                pass
    return None

def _read_json_to_df(*candidates, records_key=None):
    for p in candidates:
        if os.path.exists(p):
            try:
                data = json.load(open(p, "r", encoding="utf-8"))
                if isinstance(data, list):
                    return pd.DataFrame(data)
                if isinstance(data, dict):
                    if records_key and records_key in data:
                        return pd.DataFrame(data[records_key])
                    return pd.DataFrame([data])
            except Exception:
                pass
    return None

def _left(master, df, on="symbol", cols=None, rename=None):
    if df is None or df.empty:
        return master
    if rename:
        df = df.rename(columns=rename)
    if cols:
        keep = [c for c in cols if c in df.columns]
        df = df[keep].copy()
    return master.merge(df, how="left", on=on)

def _norm_symbol(df, col="symbol"):
    if df is not None and col in df.columns:
        df[col] = df[col].astype(str).str.upper().str.strip()
    return df

def _stance_from_dir(row):
    d = row.get("dir")
    if pd.isna(d): return "neutral"
    try:
        d = int(d)
    except Exception:
        return "neutral"
    return "bullish" if d > 0 else ("bearish" if d < 0 else "neutral")

def build(out):
    # --- Collect symbols from available sets
    sets = []

    dirsig = _read_csv_any(f"{PROC}/direction_signal.csv", f"{PROC}/direction_signal.csv.gz")
    _norm_symbol(dirsig); sets.append(dirsig[["symbol"]]) if dirsig is not None else None

    optsig = _read_csv_any(f"{PROC}/options_signals.csv", f"{PROC}/options_signals.csv.gz")
    _norm_symbol(optsig); sets.append(optsig[["symbol"]]) if optsig is not None else None

    bystrike = _read_csv_any(f"{PROC}/options_oi_by_strike.csv", f"{PROC}/options_oi_by_strike.csv.gz")
    _norm_symbol(bystrike); sets.append(bystrike[["symbol"]]) if bystrike is not None else None

    hv = _read_csv_any(f"{PROC}/hv_summary.csv", f"{PROC}/hv_summary.csv.gz")
    _norm_symbol(hv); sets.append(hv[["symbol"]]) if hv is not None else None

    fnda = _read_csv_any(f"{PROC}/fundamentals_core.csv", f"{PROC}/fundamentals_core.csv.gz")
    _norm_symbol(fnda); sets.append(fnda[["symbol"]]) if fnda is not None else None

    cds = _read_csv_any(f"{PROC}/cds_proxy.csv", f"{PROC}/cds_proxy.csv.gz", f"{PROC}/cds_proxy_v3.csv", f"{PROC}/cds_proxy_v3.csv.gz")
    _norm_symbol(cds); sets.append(cds[["symbol"]]) if cds is not None else None

    rev = _read_csv_any(f"{PROC}/revisions.csv", f"{PROC}/revisions.csv.gz")
    _norm_symbol(rev); sets.append(rev[["symbol"]]) if rev is not None else None

    earn = _read_json_to_df(f"{DOCS}/earnings_next.json", f"{DOCS}/earnings_next.json.gz")
    _norm_symbol(earn); sets.append(earn[["symbol"]]) if earn is not None else None

    if not sets:
        raise SystemExit("Keine Eingabedateien gefunden – Master kann nicht gebaut werden.")

    universe = pd.concat(sets, ignore_index=True).dropna().drop_duplicates()
    universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
    master = universe.drop_duplicates("symbol").copy()

    # --- direction_signal (bevorzugt)
    if dirsig is not None:
        ren = {
            "nearest_dte":"nearest_dte",
            "next_expiry":"next_expiry",
            "focus_strike_7":"fs7",
            "focus_strike_30":"fs30",
            "focus_strike_60":"fs60",
        }
        # tolerate variants
        for a,b in list(ren.items()):
            if a not in dirsig.columns and b in dirsig.columns:
                ren[b] = b
        # dir/strength field names
        if "dir" not in dirsig.columns and "direction" in dirsig.columns:
            ren["direction"] = "dir"
        if "strength" not in dirsig.columns and "score" in dirsig.columns:
            ren["score"] = "strength"

        dir_cols = ["symbol","dir","strength","next_expiry","nearest_dte","focus_strike_7","focus_strike_30","focus_strike_60"]
        master = _left(master, dirsig, cols=[c for c in dir_cols if c in dirsig.columns], rename=ren)

    # --- options_signals (Fallback für dir/strength)
    if optsig is not None:
        ren = {}
        if "direction" in optsig.columns: ren["direction"] = "dir"
        sig_cols = [c for c in ["symbol","dir","strength","expiry","strike"] if c in optsig.columns or (c=="dir" and "direction" in optsig.columns)]
        master = _left(master, optsig, cols=sig_cols, rename=ren)

    # --- by_strike (kompakt: focus_strike; detail: strike,call_oi,put_oi,dte/expiry)
    if bystrike is not None:
        bs = bystrike.copy()
        # prefer compact columns if present
        if "focus_strike" in bs.columns:
            bs = bs.rename(columns={"focus_strike":"focus_strike_general"})
            cols = ["symbol","focus_strike_general","expiry","dte"]
        else:
            # detail schema -> keep expiry/dte if provided by your builder
            cols = ["symbol","expiry","dte"]
        cols = [c for c in cols if c in bs.columns]
        if cols:
            master = _left(master, bs, cols=cols)

    # --- hv summary
    if hv is not None:
        hv = hv.rename(columns={c:c.lower() for c in hv.columns})
        cols = [c for c in ["symbol","hv20","hv60","hv10","hv30"] if c in hv.columns]
        master = _left(master, hv, cols=cols)

    # --- fundamentals (pick common fields only if present)
    if fnda is not None:
        keep = [c for c in [
            "symbol","name","sector","industry","currency",
            "marketcap","sharesoutstanding","pe","pb","ps","ev_ebitda","beta"
        ] if c in fnda.columns]
        master = _left(master, fnda, cols=keep)

    # --- earnings next
    if earn is not None:
        rn = {}
        if "next_date" in earn.columns: rn["next_date"] = "earnings_next"
        elif "earnings_next" in earn.columns: rn = None
        cols = [c for c in ["symbol","earnings_next","next_date"] if c in earn.columns]
        master = _left(master, earn, cols=cols, rename=rn)

    # --- CDS proxy
    if cds is not None:
        rn = {}
        if "proxy_spread" in cds.columns: rn["proxy_spread"] = "cds_proxy"
        keep = [c for c in ["symbol","cds_proxy","proxy_spread"] if c in cds.columns]
        master = _left(master, cds, cols=keep, rename=rn)

    # --- Revisions (optional fields)
    if rev is not None:
        keep = [c for c in [
            "symbol","eps_rev_3m","rev_rev_3m","eps_surprise","rev_surprise"
        ] if c in rev.columns]
        master = _left(master, rev, cols=keep)

    # --- derived: stance
    master["stance"] = master.apply(_stance_from_dir, axis=1)

    # --- tidy & types
    for c in ["next_expiry","expiry","earnings_next"]:
        if c in master.columns:
            try:
                master[c] = pd.to_datetime(master[c], errors="coerce").dt.date
            except Exception:
                pass

    # order columns
    preferred = [
        "symbol","name","sector","industry","stance","dir","strength",
        "next_expiry","nearest_dte","fs7","fs30","fs60","focus_strike_general",
        "earnings_next","hv10","hv20","hv30","hv60","cds_proxy","marketcap","pe","pb","ps","ev_ebitda","beta","currency"
    ]
    cols = [c for c in preferred if c in master.columns] + [c for c in master.columns if c not in preferred]
    master = master[cols]

    os.makedirs(os.path.dirname(out), exist_ok=True)
    master.to_csv(out, index=False)
    print(f"✅ wrote {out} with {len(master)} rows, {len(master.columns)} cols")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=f"{PROC}/equity_master.csv")
    args = ap.parse_args()
    build(args.out)
