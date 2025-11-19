#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds one wide master file per equity symbol by merging all processed datasets.

WICHTIG:
- KEINE Options- oder Richtungsdaten mehr (dir/strength/stance/next_expiry/Strikes).
- Fokus: Fundamentals, HV, CDS-Proxy, Revisions, Earnings-Nächster Termin,
         Short Interest + Borrow-Stress, Peers, Dividenden, Splits, Insider.

Input (best effort, alle optional):
  data/processed/fundamentals_core.csv(.gz)
  data/processed/hv_summary.csv(.gz)
  data/processed/cds_proxy.csv(.gz) oder cds_proxy_v3.csv(.gz)
  data/processed/revisions.csv(.gz)
  docs/earnings_next.json(.gz)
  data/processed/short_interest.csv(.gz)
  data/processed/peers.csv(.gz)
  data/processed/dividends.csv(.gz)
  data/processed/splits.csv(.gz)
  data/processed/insider_tx.csv(.gz)

Output:
  data/processed/equity_master.csv  (wird im Workflow später gezippt)
"""

import os
import json
import argparse
from datetime import datetime, timedelta

import pandas as pd

PROC = "data/processed"
DOCS = "docs"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def rd_csv(*candidates):
    """Liest die erste existierende CSV (auch .gz) als DataFrame."""
    for p in candidates:
        if not p:
            continue
        if os.path.exists(p):
            try:
                return pd.read_csv(p, compression="infer")
            except Exception:
                try:
                    return pd.read_csv(p)
                except Exception:
                    pass
    return None


def rd_json_to_df(*candidates, records_key=None):
    """Liest JSON/JSON.GZ und gibt DataFrame zurück."""
    for p in candidates:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    return pd.DataFrame(data)
                if isinstance(data, dict):
                    if records_key and records_key in data:
                        return pd.DataFrame(data[records_key])
                    return pd.DataFrame([data])
            except Exception:
                pass
    return None


def norm_symbol(df, col="symbol"):
    """Symbol-Spalte vereinheitlichen."""
    if df is not None and col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.upper()
            .str.strip()
        )
    return df


def left(master, df, on="symbol", cols=None, rename=None):
    """Left-Join auf symbol mit optionaler Spaltenauswahl/Umbenennung."""
    if df is None or df.empty:
        return master
    if rename:
        df = df.rename(columns=rename)
    if cols:
        keep = [c for c in cols if c in df.columns]
        df = df[keep].copy()
    return master.merge(df, how="left", on=on)


# Borrow-Stress 0–4 aus Short Interest / Borrow
def borrow_stress(row):
    """
    0 = keine Daten
    1 = entspannt (billig & viel verfügbar)
    2 = leicht angespannt
    3 = angespannt
    4 = extrem / Squeeze-Gefahr
    """
    rate = row.get("borrow_rate")
    avail = row.get("borrow_avail")

    if pd.isna(rate) or pd.isna(avail):
        return 0
    try:
        r = float(rate)
        a = float(avail)
    except Exception:
        return 0

    if r <= 0.5:
        rate_score = 1
    elif r <= 2.0:
        rate_score = 2
    elif r <= 10.0:
        rate_score = 3
    else:
        rate_score = 4

    if a >= 1_000_000:
        avail_score = 1
    elif a >= 200_000:
        avail_score = 2
    elif a >= 50_000:
        avail_score = 3
    else:
        avail_score = 4

    return int(max(rate_score, avail_score))


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #

def build(out_path: str):
    # ------------------ Rohdaten laden ------------------
    fnda   = norm_symbol(rd_csv(f"{PROC}/fundamentals_core.csv",
                                f"{PROC}/fundamentals_core.csv.gz"))
    hv     = norm_symbol(rd_csv(f"{PROC}/hv_summary.csv",
                                f"{PROC}/hv_summary.csv.gz"))
    cds    = norm_symbol(rd_csv(f"{PROC}/cds_proxy.csv",
                                f"{PROC}/cds_proxy.csv.gz",
                                f"{PROC}/cds_proxy_v3.csv",
                                f"{PROC}/cds_proxy_v3.csv.gz"))
    rev    = norm_symbol(rd_csv(f"{PROC}/revisions.csv",
                                f"{PROC}/revisions.csv.gz"))
    earn   = norm_symbol(rd_json_to_df(f"{DOCS}/earnings_next.json",
                                       f"{DOCS}/earnings_next.json.gz"))
    shorti = norm_symbol(rd_csv(f"{PROC}/short_interest.csv",
                                f"{PROC}/short_interest.csv.gz"))
    peers  = norm_symbol(rd_csv(f"{PROC}/peers.csv",
                                f"{PROC}/peers.csv.gz"))
    divs   = norm_symbol(rd_csv(f"{PROC}/dividends.csv",
                                f"{PROC}/dividends.csv.gz"))
    splits = norm_symbol(rd_csv(f"{PROC}/splits.csv",
                                f"{PROC}/splits.csv.gz"))
    ins    = norm_symbol(rd_csv(f"{PROC}/insider_tx.csv",
                                f"{PROC}/insider_tx.csv.gz"))

    sets = []
    for df in (fnda, hv, cds, rev, earn, shorti, peers, divs, splits, ins):
        if df is not None and "symbol" in df.columns:
            sets.append(df[["symbol"]])

    if not sets:
        raise SystemExit("Keine Inputs gefunden – equity_master kann nicht gebaut werden.")

    universe = (
        pd.concat(sets, ignore_index=True)
        .dropna()
        .drop_duplicates()
    )
    universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
    master = universe.drop_duplicates("symbol").copy()

    # ------------------ Fundamentals --------------------
    if fnda is not None:
        keep = [c for c in [
            "symbol", "name", "sector", "industry", "currency",
            "marketcap", "sharesoutstanding",
            "pe", "pb", "ps", "ev_ebitda", "beta"
        ] if c in fnda.columns]
        master = left(master, fnda, cols=keep)

    # ------------------ HV summary ----------------------
    if hv is not None:
        hv2 = hv.rename(columns={c: c.lower() for c in hv.columns})
        cols = [c for c in ["symbol", "hv10", "hv20", "hv30", "hv60"] if c in hv2.columns]
        master = left(master, hv2, cols=cols)

    # ------------------ CDS proxy -----------------------
    if cds is not None:
        rn = {}
        if "proxy_spread" in cds.columns:
            rn["proxy_spread"] = "cds_proxy"
        cols = [c for c in ["symbol", "cds_proxy", "proxy_spread"] if c in cds.columns]
        master = left(master, cds, cols=cols, rename=rn)

    # ------------------ Revisions -----------------------
    if rev is not None:
        keep = [c for c in [
            "symbol", "eps_rev_3m", "rev_rev_3m",
            "eps_surprise", "rev_surprise"
        ] if c in rev.columns]
        master = left(master, rev, cols=keep)

    # ------------------ Earnings next -------------------
    if earn is not None:
        rn = {}
        if "next_date" in earn.columns:
            rn["next_date"] = "earnings_next"
        cols = [c for c in ["symbol", "earnings_next", "next_date"] if c in earn.columns]
        master = left(master, earn, cols=cols, rename=rn)

    # ------------------ Short Interest + Borrow ---------
    if shorti is not None:
        keep = [c for c in [
            "symbol", "si_date", "si_shares", "float_shares",
            "si_pct_float", "borrow_rate", "borrow_avail",
            "si_source"
        ] if c in shorti.columns]
        master = left(master, shorti, cols=keep)

        if "borrow_rate" in master.columns or "borrow_avail" in master.columns:
            master["borrow_stress"] = master.apply(borrow_stress, axis=1)

    # ------------------ Peers (Anzahl) ------------------
    if peers is not None and "peer" in peers.columns:
        pc = (
            peers.groupby("symbol")["peer"]
            .nunique()
            .reset_index()
            .rename(columns={"peer": "peers_count"})
        )
        master = left(master, pc, cols=["symbol", "peers_count"])

    # ------------------ Dividenden-Aggregate ------------
    if divs is not None and not divs.empty and "date" in divs.columns:
        d = divs.copy()
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        agg_map = {
            "last_div_date": ("date", "max"),
            "div_count_total": ("date", "count"),
        }
        if "dividend" in d.columns:
            agg_map["last_div_amount"] = ("dividend", "last")

        agg = (
            d.sort_values("date")
             .groupby("symbol")
             .agg(**{k: pd.NamedAgg(*v) for k, v in agg_map.items()})
             .reset_index()
        )
        master = left(master, agg, cols=agg.columns.tolist())

    # ------------------ Splits-Aggregate ----------------
    if splits is not None and not splits.empty and "date" in splits.columns:
        s = splits.copy()
        s["date"] = pd.to_datetime(s["date"], errors="coerce")
        agg_map = {
            "last_split_date": ("date", "max"),
            "split_count_total": ("date", "count"),
        }
        if "split_ratio" in s.columns:
            agg_map["last_split_ratio"] = ("split_ratio", "last")

        agg = (
            s.sort_values("date")
             .groupby("symbol")
             .agg(**{k: pd.NamedAgg(*v) for k, v in agg_map.items()})
             .reset_index()
        )
        master = left(master, agg, cols=agg.columns.tolist())

    # ------------------ Insider-Aggregate ----------------
    if ins is not None and not ins.empty:
        df_ins = ins.copy()

        # Datum: Transaction-Date bevorzugen, sonst Filing-Date
        tx_date_col = None
        if "transaction_date" in df_ins.columns:
            tx_date_col = "transaction_date"
        elif "filing_date" in df_ins.columns:
            tx_date_col = "filing_date"

        if tx_date_col is not None:
            # alles als UTC-Timestamps parsen (timezone-aware)
            df_ins["tx_date"] = pd.to_datetime(
                df_ins[tx_date_col],
                errors="coerce",
                utc=True
            )

            # Letzte Transaktion je Symbol
            last_idx = (
                df_ins
                .sort_values("tx_date")
                .groupby("symbol")["tx_date"]
                .idxmax()
            )
            last = df_ins.loc[last_idx].copy()

            last_cols = ["symbol"]
            if "tx_date" in last.columns:
                # als Naive-Date (ohne TZ) in Master schreiben
                last["insider_last_tx_date"] = last["tx_date"].dt.tz_convert(None).dt.date
                last_cols.append("insider_last_tx_date")
            if "transaction_code" in last.columns:
                last = last.rename(columns={"transaction_code": "insider_last_tx_code"})
                last_cols.append("insider_last_tx_code")
            if "change" in last.columns:
                last = last.rename(columns={"change": "insider_last_tx_shares"})
                last_cols.append("insider_last_tx_shares")
            elif "share" in last.columns:
                last = last.rename(columns={"share": "insider_last_tx_shares"})
                last_cols.append("insider_last_tx_shares")
            if "transaction_price" in last.columns:
                last = last.rename(columns={"transaction_price": "insider_last_tx_price"})
                last_cols.append("insider_last_tx_price")

            master = left(master, last[last_cols], cols=last_cols)

            # 12M-Fenster für Buy/Sell-Summen
            cutoff = pd.Timestamp.now(tz="UTC") - timedelta(days=365)
            w = df_ins[df_ins["tx_date"] >= cutoff].copy()
            if not w.empty and "transaction_code" in w.columns:
                w["code"] = w["transaction_code"].astype(str).str.upper()
                is_buy = w["code"].str.startswith("P")   # Purchase
                is_sell = w["code"].str.startswith("S")  # Sale

                if "change" in w.columns:
                    shares_col = "change"
                elif "share" in w.columns:
                    shares_col = "share"
                else:
                    shares_col = None

                agg_dict = {
                    "insider_buy_trades_12m": ("code", lambda x: (x.str.startswith("P")).sum()),
                    "insider_sell_trades_12m": ("code", lambda x: (x.str.startswith("S")).sum()),
                }

                if shares_col is not None:
                    w["_shares"] = pd.to_numeric(w[shares_col], errors="coerce")
                    agg_dict["insider_buy_shares_12m"] = (
                        "_shares",
                        lambda x: x[is_buy.loc[x.index]].sum()
                    )
                    agg_dict["insider_sell_shares_12m"] = (
                        "_shares",
                        lambda x: x[is_sell.loc[x.index]].sum()
                    )

                agg = (
                    w.groupby("symbol")
                     .agg(**{k: pd.NamedAgg(col, func) for k, (col, func) in agg_dict.items()})
                     .reset_index()
                )
                master = left(master, agg, cols=agg.columns.tolist())

    # ------------------ Spalten sortieren ----------------
    preferred = [
        "symbol", "name", "sector", "industry",
        # HV
        "hv10", "hv20", "hv30", "hv60",
        # Earnings/Revisions
        "earnings_next", "eps_rev_3m", "rev_rev_3m",
        "eps_surprise", "rev_surprise",
        # CDS / Fundamentals
        "cds_proxy",
        "marketcap", "pe", "pb", "ps", "ev_ebitda", "beta", "currency",
        # Short Interest
        "si_date", "si_shares", "float_shares", "si_pct_float",
        "borrow_rate", "borrow_avail", "borrow_stress", "si_source",
        # Peers
        "peers_count",
        # Dividenden
        "last_div_date", "last_div_amount", "div_count_total",
        # Splits
        "last_split_date", "last_split_ratio", "split_count_total",
        # Insider
        "insider_last_tx_date", "insider_last_tx_code",
        "insider_last_tx_shares", "insider_last_tx_price",
        "insider_buy_shares_12m", "insider_sell_shares_12m",
        "insider_buy_trades_12m", "insider_sell_trades_12m",
    ]
    cols = [c for c in preferred if c in master.columns] + \
           [c for c in master.columns if c not in preferred]
    master = master[cols]

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    master.to_csv(out_path, index=False)
    print(f"✅ wrote {out_path} with {len(master)} rows, {len(master.columns)} cols")


# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=f"{PROC}/equity_master.csv")
    args = ap.parse_args()
    build(args.out)
