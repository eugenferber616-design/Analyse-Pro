#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds one wide master file per equity symbol by merging all processed datasets.
Inputs (best-effort, optional):
  - direction_signal, options_signals, options_oi_by_strike, options_oi_summary
  - hv_summary, fundamentals_core, earnings_next.json, cds_proxy, revisions
  - short_interest (iBorrowDesk Website)
Output:
  - data/processed/equity_master.csv  (later gzipped by workflow)
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
    if pd.isna(d):
        return "neutral"
    try:
        d = int(d)
    except Exception:
        return "neutral"
    return "bullish" if d > 0 else ("bearish" if d < 0 else "neutral")


# --- NEU: Borrow-Stress 0–4 -----------------------------------------------
def _borrow_stress(row):
    """
    Berechnet einen einfachen Borrow-Stress-Level 0–4 aus
    borrow_rate (% p.a.) und borrow_avail (verfügbare Shares).

    0 = keine Daten
    1 = entspannt (billig & reichlich verfügbar)
    2 = leicht angespannt
    3 = angespannt
    4 = extrem / Squeeze-Gefahr
    """
    rate = row.get("borrow_rate")
    avail = row.get("borrow_avail")

    # 0 = keine Daten / unlesbare Werte
    if pd.isna(rate) or pd.isna(avail):
        return 0
    try:
        r = float(rate)
        a = float(avail)
    except Exception:
        return 0

    # Rate-Score (1–4)
    if r <= 0.5:
        rate_score = 1
    elif r <= 2.0:
        rate_score = 2
    elif r <= 10.0:
        rate_score = 3
    else:
        rate_score = 4

    # Availability-Score (1–4)
    if a >= 1_000_000:
        avail_score = 1
    elif a >= 200_000:
        avail_score = 2
    elif a >= 50_000:
        avail_score = 3
    else:
        avail_score = 4

    return int(max(rate_score, avail_score))


def build(out):
    # --- Collect symbols from available sets
    sets = []

    dirsig = _read_csv_any(f"{PROC}/direction_signal.csv", f"{PROC}/direction_signal.csv.gz")
    _norm_symbol(dirsig); sets.append(dirsig[["symbol"]]) if dirsig is not None else None

    optsig = _read_csv_any(f"{PROC}/options_signals.csv", f"{PROC}/options_signals.csv.gz")
    _norm_symbol(optsig); sets.append(optsig[["symbol"]]) if optsig is not None else None

    bystrike = _read_csv_any(f"{PROC}/options_oi_by_strike.csv", f"{PROC}/options_oi_by_strike.csv.gz")
    _norm_symbol(bystrike); sets.append(bystrike[["symbol"]]) if bystrike is not None else None

    # NEU: Options-Summary (verfall-/strikebasierte OI-Daten)
    optsum = _read_csv_any(f"{PROC}/options_oi_summary.csv", f"{PROC}/options_oi_summary.csv.gz")
    _norm_symbol(optsum); sets.append(optsum[["symbol"]]) if optsum is not None else None

    hv = _read_csv_any(f"{PROC}/hv_summary.csv", f"{PROC}/hv_summary.csv.gz")
    _norm_symbol(hv); sets.append(hv[["symbol"]]) if hv is not None else None

    fnda = _read_csv_any(f"{PROC}/fundamentals_core.csv", f"{PROC}/fundamentals_core.csv.gz")
    _norm_symbol(fnda); sets.append(fnda[["symbol"]]) if fnda is not None else None

    cds = _read_csv_any(f"{PROC}/cds_proxy.csv", f"{PROC}/cds_proxy.csv.gz",
                        f"{PROC}/cds_proxy_v3.csv", f"{PROC}/cds_proxy_v3.csv.gz")
    _norm_symbol(cds); sets.append(cds[["symbol"]]) if cds is not None else None

    rev = _read_csv_any(f"{PROC}/revisions.csv", f"{PROC}/revisions.csv.gz")
    _norm_symbol(rev); sets.append(rev[["symbol"]]) if rev is not None else None

    earn = _read_json_to_df(f"{DOCS}/earnings_next.json", f"{DOCS}/earnings_next.json.gz")
    _norm_symbol(earn); sets.append(earn[["symbol"]]) if earn is not None else None

    # NEU: Short-Interest/Borrow (iBorrowDesk)
    shorti = _read_csv_any(f"{PROC}/short_interest.csv", f"{PROC}/short_interest.csv.gz")
    _norm_symbol(shorti); sets.append(shorti[["symbol"]]) if shorti is not None else None

    if not sets:
        raise SystemExit("Keine Eingabedateien gefunden – Master kann nicht gebaut werden.")

    universe = pd.concat(sets, ignore_index=True).dropna().drop_duplicates()
    universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
    master = universe.drop_duplicates("symbol").copy()

    # --- direction_signal (bevorzugt)
    if dirsig is not None:
        ren = {
            "nearest_dte": "nearest_dte",
            "next_expiry": "next_expiry",
            "focus_strike_7": "fs7",
            "focus_strike_30": "fs30",
            "focus_strike_60": "fs60",
        }
        # tolerate variants
        for a, b in list(ren.items()):
            if a not in dirsig.columns and b in dirsig.columns:
                ren[b] = b
        # dir/strength field names
        if "dir" not in dirsig.columns and "direction" in dirsig.columns:
            ren["direction"] = "dir"
        if "strength" not in dirsig.columns and "score" in dirsig.columns:
            ren["score"] = "strength"

        dir_cols = [
            "symbol", "dir", "strength", "next_expiry", "nearest_dte",
            "focus_strike_7", "focus_strike_30", "focus_strike_60"
        ]
        master = _left(master, dirsig,
                       cols=[c for c in dir_cols if c in dirsig.columns],
                       rename=ren)

    # --- options_signals (Fallback für dir/strength)
    if optsig is not None:
        ren = {}
        if "direction" in optsig.columns:
            ren["direction"] = "dir"
        sig_cols = [c for c in ["symbol", "dir", "strength", "expiry", "strike"]
                    if c in optsig.columns or (c == "dir" and "direction" in optsig.columns)]
        master = _left(master, optsig, cols=sig_cols, rename=ren)

    # --- by_strike (kompakt: focus_strike; detail: expiry/dte)
    if bystrike is not None:
        bs = bystrike.copy()
        if "focus_strike" in bs.columns:
            bs = bs.rename(columns={"focus_strike": "focus_strike_general"})
            cols = ["symbol", "focus_strike_general", "expiry", "dte"]
        else:
            cols = ["symbol", "expiry", "dte"]
        cols = [c for c in cols if c in bs.columns]
        if cols:
            master = _left(master, bs, cols=cols)

    # --- NEU: options_oi_summary → opt_* Felder (dominanter Verfall pro Symbol)
    if optsum is not None and not optsum.empty:
        s = optsum.copy()

        # sicherstellen, dass call_oi/put_oi numerisch sind
        if "call_oi" in s.columns:
            s["call_oi"] = pd.to_numeric(s["call_oi"], errors="coerce")
        if "put_oi" in s.columns:
            s["put_oi"] = pd.to_numeric(s["put_oi"], errors="coerce")

        if "call_oi" in s.columns and "put_oi" in s.columns:
            s["__tot_oi"] = s["call_oi"].fillna(0.0) + s["put_oi"].fillna(0.0)
            s = s.sort_values(["symbol", "__tot_oi"], ascending=[True, False]).drop_duplicates("symbol")
        else:
            s = s.drop_duplicates("symbol")

        ren = {
            "expiry":           "opt_expiry",
            "call_oi":          "opt_call_oi",
            "put_oi":           "opt_put_oi",
            "put_call_ratio":   "opt_put_call_ratio",
            "call_top_strikes": "opt_call_top_strikes",
            "put_top_strikes":  "opt_put_top_strikes",
        }
        cols = ["symbol"] + [c for c in ren.keys() if c in s.columns]
        master = _left(master, s, cols=cols, rename=ren)

    # --- hv summary
    if hv is not None:
        hv = hv.rename(columns={c: c.lower() for c in hv.columns})
        cols = [c for c in ["symbol", "hv20", "hv60", "hv10", "hv30"] if c in hv.columns]
        master = _left(master, hv, cols=cols)

    # --- fundamentals (pick common fields only if present)
    if fnda is not None:
        keep = [c for c in [
            "symbol", "name", "sector", "industry", "currency",
            "marketcap", "sharesoutstanding", "pe", "pb", "ps", "ev_ebitda", "beta"
        ] if c in fnda.columns]
        master = _left(master, fnda, cols=keep)

    # --- earnings next
    if earn is not None:
        rn = {}
        if "next_date" in earn.columns:
            rn["next_date"] = "earnings_next"
        elif "earnings_next" in earn.columns:
            rn = None
        cols = [c for c in ["symbol", "earnings_next", "next_date"] if c in earn.columns]
        master = _left(master, earn, cols=cols, rename=rn)

    # --- CDS proxy
    if cds is not None:
        rn = {}
        if "proxy_spread" in cds.columns:
            rn["proxy_spread"] = "cds_proxy"
        keep = [c for c in ["symbol", "cds_proxy", "proxy_spread"] if c in cds.columns]
        master = _left(master, cds, cols=keep, rename=rn)

    # --- Revisions (optional fields)
    if rev is not None:
        keep = [c for c in [
            "symbol", "eps_rev_3m", "rev_rev_3m", "eps_surprise", "rev_surprise"
        ] if c in rev.columns]
        master = _left(master, rev, cols=keep)

    # --- NEU: Short-Interest / Borrow + Stress 0–4
    if shorti is not None:
        keep = [c for c in [
            "symbol", "si_date", "si_shares", "float_shares",
            "si_pct_float", "borrow_rate", "borrow_avail", "ibd_status"
        ] if c in shorti.columns]
        master = _left(master, shorti, cols=keep)

        if "borrow_rate" in master.columns or "borrow_avail" in master.columns:
            master["borrow_stress"] = master.apply(_borrow_stress, axis=1)

    # --- derived: stance
    master["stance"] = master.apply(_stance_from_dir, axis=1)

    # --- tidy & types
    for c in ["next_expiry", "expiry", "earnings_next", "opt_expiry", "si_date"]:
        if c in master.columns:
            try:
                master[c] = pd.to_datetime(master[c], errors="coerce").dt.date
            except Exception:
                pass

    # order columns (inkl. neuer opt_* und Borrow-Felder)
    preferred = [
        "symbol", "name", "sector", "industry",
        "stance", "dir", "strength",
        "next_expiry", "nearest_dte",
        "fs7", "fs30", "fs60", "focus_strike_general",
        "opt_expiry", "opt_call_oi", "opt_put_oi",
        "opt_put_call_ratio", "opt_call_top_strikes", "opt_put_top_strikes",
        "earnings_next",
        "hv10", "hv20", "hv30", "hv60",
        "cds_proxy",
        "marketcap", "pe", "pb", "ps", "ev_ebitda", "beta",
        "currency",
        "si_date", "si_shares", "float_shares", "si_pct_float",
        "borrow_rate", "borrow_avail", "borrow_stress", "ibd_status",
    ]
    cols = [c for c in preferred if c in master.columns] + \
           [c for c in master.columns if c not in preferred]
    master = master[cols]

    os.makedirs(os.path.dirname(out), exist_ok=True)
    master.to_csv(out, index=False)
    print(f"✅ wrote {out} with {len(master)} rows, {len(master.columns)} cols")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=f"{PROC}/equity_master.csv")
    args = ap.parse_args()
    build(args.out)
