#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Builds one wide master file per equity symbol by merging all processed datasets.
Inputs (best-effort, optional):
  - direction_signal, options_signals, options_oi_by_strike, options_oi_summary, options_oi_totals
  - hv_summary, fundamentals_core, earnings_next.json, cds_proxy, revisions
  - short_interest (iBorrowDesk Website), peers
  - dividends, splits, insider_tx
Output:
  - data/processed/equity_master.csv  (later gzipped by workflow)
"""

import os, json, argparse, pandas as pd
from datetime import timedelta

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
                try:
                    return pd.read_csv(p)
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


# --- Borrow-Stress 0–4 ------------------------------------------------------
def _borrow_stress(row):
    """
    Einfacher Borrow-Stress-Level 0–4 aus borrow_rate (% p.a.) und borrow_avail.
    0 = keine Daten
    1 = entspannt, 4 = extrem angespannt (Squeeze-Gefahr)
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


def build(out):
    # --- Collect symbols from all available sets -----------------------------
    sets = []

    dirsig = _read_csv_any(f"{PROC}/direction_signal.csv", f"{PROC}/direction_signal.csv.gz")
    _norm_symbol(dirsig)
    if dirsig is not None:
        sets.append(dirsig[["symbol"]])

    optsig = _read_csv_any(f"{PROC}/options_signals.csv", f"{PROC}/options_signals.csv.gz")
    _norm_symbol(optsig)
    if optsig is not None:
        sets.append(optsig[["symbol"]])

    bystrike = _read_csv_any(f"{PROC}/options_oi_by_strike.csv", f"{PROC}/options_oi_by_strike.csv.gz")
    _norm_symbol(bystrike)
    if bystrike is not None:
        sets.append(bystrike[["symbol"]])

    optsum = _read_csv_any(f"{PROC}/options_oi_summary.csv", f"{PROC}/options_oi_summary.csv.gz")
    _norm_symbol(optsum)
    if optsum is not None:
        sets.append(optsum[["symbol"]])

    opttot = _read_csv_any(f"{PROC}/options_oi_totals.csv", f"{PROC}/options_oi_totals.csv.gz")
    _norm_symbol(opttot)
    if opttot is not None:
        sets.append(opttot[["symbol"]])

    hv = _read_csv_any(f"{PROC}/hv_summary.csv", f"{PROC}/hv_summary.csv.gz")
    _norm_symbol(hv)
    if hv is not None:
        sets.append(hv[["symbol"]])

    fnda = _read_csv_any(f"{PROC}/fundamentals_core.csv", f"{PROC}/fundamentals_core.csv.gz")
    _norm_symbol(fnda)
    if fnda is not None:
        sets.append(fnda[["symbol"]])

    cds = _read_csv_any(
        f"{PROC}/cds_proxy.csv", f"{PROC}/cds_proxy.csv.gz",
        f"{PROC}/cds_proxy_v3.csv", f"{PROC}/cds_proxy_v3.csv.gz"
    )
    _norm_symbol(cds)
    if cds is not None:
        sets.append(cds[["symbol"]])

    rev = _read_csv_any(f"{PROC}/revisions.csv", f"{PROC}/revisions.csv.gz")
    _norm_symbol(rev)
    if rev is not None:
        sets.append(rev[["symbol"]])

    earn = _read_json_to_df(f"{DOCS}/earnings_next.json", f"{DOCS}/earnings_next.json.gz")
    _norm_symbol(earn)
    if earn is not None:
        sets.append(earn[["symbol"]])

    shorti = _read_csv_any(f"{PROC}/short_interest.csv", f"{PROC}/short_interest.csv.gz")
    _norm_symbol(shorti)
    if shorti is not None:
        sets.append(shorti[["symbol"]])

    peers = _read_csv_any(f"{PROC}/peers.csv", f"{PROC}/peers.csv.gz")
    _norm_symbol(peers)
    if peers is not None:
        sets.append(peers[["symbol"]])

    divs = _read_csv_any(f"{PROC}/dividends.csv", f"{PROC}/dividends.csv.gz")
    _norm_symbol(divs)
    if divs is not None:
        sets.append(divs[["symbol"]])

    splits = _read_csv_any(f"{PROC}/splits.csv", f"{PROC}/splits.csv.gz")
    _norm_symbol(splits)
    if splits is not None:
        sets.append(splits[["symbol"]])

    insider = _read_csv_any(f"{PROC}/insider_tx.csv", f"{PROC}/insider_tx.csv.gz")
    _norm_symbol(insider)
    if insider is not None:
        sets.append(insider[["symbol"]])

    if not sets:
        raise SystemExit("Keine Eingabedateien gefunden – Master kann nicht gebaut werden.")

    universe = pd.concat(sets, ignore_index=True).dropna().drop_duplicates()
    universe["symbol"] = universe["symbol"].astype(str).str.upper().str.strip()
    master = universe.drop_duplicates("symbol").copy()

    # --- direction_signal (bevorzugt) ---------------------------------------
    if dirsig is not None:
        ren = {
            "nearest_dte": "nearest_dte",
            "next_expiry": "next_expiry",
            "focus_strike_7": "fs7",
            "focus_strike_30": "fs30",
            "focus_strike_60": "fs60",
        }
        if "dir" not in dirsig.columns and "direction" in dirsig.columns:
            ren["direction"] = "dir"
        if "strength" not in dirsig.columns and "score" in dirsig.columns:
            ren["score"] = "strength"

        dir_cols = [
            "symbol", "dir", "strength", "next_expiry", "nearest_dte",
            "focus_strike_7", "focus_strike_30", "focus_strike_60", "direction", "score"
        ]
        cols = [c for c in dir_cols if c in dirsig.columns]
        master = _left(master, dirsig, cols=cols, rename=ren)

    # --- options_signals (Fallback für dir/strength/Strike) ------------------
    if optsig is not None:
        ren = {}
        if "direction" in optsig.columns:
            ren["direction"] = "dir"
        sig_cols = [c for c in ["symbol", "dir", "strength", "expiry", "strike", "direction"]
                    if c in optsig.columns]
        master = _left(master, optsig, cols=sig_cols, rename=ren)

    # --- options_oi_by_strike (Focus-Strike + dte) ---------------------------
    if bystrike is not None:
        bs = bystrike.copy()
        if "focus_strike" in bs.columns:
            bs = bs.rename(columns={"focus_strike": "focus_strike_general"})
        cols = [c for c in ["symbol", "focus_strike_general", "expiry", "dte"] if c in bs.columns]
        master = _left(master, bs, cols=cols)

    # --- options_oi_summary (dominanter Verfall pro Symbol) ------------------
    if optsum is not None and not optsum.empty:
        s = optsum.copy()
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
            "spot":             "opt_spot",
            "call_oi":          "opt_call_oi",
            "put_oi":           "opt_put_oi",
            "put_call_ratio":   "opt_put_call_ratio",
            "call_iv_w":        "opt_call_iv_w",
            "put_iv_w":         "opt_put_iv_w",
            "call_top_strikes": "opt_call_top_strikes",
            "put_top_strikes":  "opt_put_top_strikes",
        }
        cols = ["symbol"] + [c for c in ren.keys() if c in s.columns]
        master = _left(master, s, cols=cols, rename=ren)

    # --- options_oi_totals (Summe über alle Verfälle) ------------------------
    if opttot is not None and not opttot.empty:
        t = opttot.copy()
        cl = {c.lower(): c for c in t.columns}

        call_candidates = ["call_oi", "total_call_oi", "call_oi_all", "calloi", "call_oi_total"]
        put_candidates  = ["put_oi", "total_put_oi", "put_oi_all", "putoi", "put_oi_total"]

        call_col = next((cl[x] for x in call_candidates if x in cl), None)
        put_col  = next((cl[x] for x in put_candidates  if x in cl), None)

        if call_col and put_col:
            t[call_col] = pd.to_numeric(t[call_col], errors="coerce")
            t[put_col]  = pd.to_numeric(t[put_col],  errors="coerce")

            if "symbol" in t.columns and t["symbol"].nunique() < len(t):
                agg = t.groupby("symbol", as_index=False).agg({
                    call_col: "sum",
                    put_col:  "sum",
                })
            else:
                agg = t[["symbol", call_col, put_col]].drop_duplicates("symbol")

            agg = agg.rename(columns={
                call_col: "tot_call_oi_all",
                put_col:  "tot_put_oi_all",
            })
            if "tot_call_oi_all" in agg.columns and "tot_put_oi_all" in agg.columns:
                agg["tot_put_call_ratio_all"] = agg["tot_put_oi_all"] / agg["tot_call_oi_all"].replace(0, pd.NA)

            master = _left(master, agg, cols=agg.columns.tolist())

    # --- HV summary ----------------------------------------------------------
    if hv is not None:
        hv2 = hv.rename(columns={c: c.lower() for c in hv.columns})
        cols = [c for c in ["symbol", "hv10", "hv20", "hv30", "hv60"] if c in hv2.columns]
        master = _left(master, hv2, cols=cols)

    # --- Fundamentals --------------------------------------------------------
    if fnda is not None:
        keep = [c for c in [
            "symbol", "name", "sector", "industry", "currency",
            "marketcap", "sharesoutstanding", "pe", "pb", "ps", "ev_ebitda", "beta"
        ] if c in fnda.columns]
        master = _left(master, fnda, cols=keep)

    # --- Earnings next -------------------------------------------------------
    if earn is not None:
        rn = {}
        if "next_date" in earn.columns:
            rn["next_date"] = "earnings_next"
        cols = [c for c in ["symbol", "earnings_next", "next_date"] if c in earn.columns]
        master = _left(master, earn, cols=cols, rename=rn if rn else None)

    # --- CDS proxy -----------------------------------------------------------
    if cds is not None:
        rn = {}
        if "proxy_spread" in cds.columns:
            rn["proxy_spread"] = "cds_proxy"
        keep = [c for c in ["symbol", "cds_proxy", "proxy_spread"] if c in cds.columns]
        master = _left(master, cds, cols=keep, rename=rn if rn else None)

    # --- Revisions -----------------------------------------------------------
    if rev is not None:
        keep = [c for c in [
            "symbol", "eps_rev_3m", "rev_rev_3m", "eps_surprise", "rev_surprise"
        ] if c in rev.columns]
        master = _left(master, rev, cols=keep)

    # --- Short-Interest / Borrow + Stress -----------------------------------
    if shorti is not None:
        keep = [c for c in [
            "symbol", "si_date", "si_shares", "float_shares",
            "si_pct_float", "borrow_rate", "borrow_avail", "si_source"
        ] if c in shorti.columns]
        master = _left(master, shorti, cols=keep)

        if "borrow_rate" in master.columns or "borrow_avail" in master.columns:
            master["borrow_stress"] = master.apply(_borrow_stress, axis=1)

    # --- Peers: count der Peer-Symbole --------------------------------------
    if peers is not None and "peer" in peers.columns:
        pc = (peers.groupby("symbol")["peer"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"peer": "peers_count"}))
        master = _left(master, pc, cols=["symbol", "peers_count"])

    # --- Dividenden: letzte Dividende + Count -------------------------------
    if divs is not None and "date" in divs.columns:
        d = divs.copy()
        d["date"] = pd.to_datetime(d["date"], errors="coerce")
        d = d.dropna(subset=["date"])
        if not d.empty:
            d_last = d.sort_values(["symbol", "date"]).groupby("symbol").tail(1)
            d_last = d_last.rename(columns={"date": "last_div_date", "dividend": "last_div_amount"})
            d_last = d_last[["symbol", "last_div_date", "last_div_amount"]]

            d_cnt = (d.groupby("symbol")["date"]
                       .count()
                       .reset_index()
                       .rename(columns={"date": "div_count_total"}))

            d_agg = d_last.merge(d_cnt, on="symbol", how="left")
            master = _left(master, d_agg, cols=["symbol", "last_div_date", "last_div_amount", "div_count_total"])

    # --- Splits: letzter Split + Count --------------------------------------
    if splits is not None and "date" in splits.columns:
        s = splits.copy()
        s["date"] = pd.to_datetime(s["date"], errors="coerce")
        s = s.dropna(subset=["date"])
        if not s.empty:
            s_last = s.sort_values(["symbol", "date"]).groupby("symbol").tail(1)
            s_last = s_last.rename(columns={"date": "last_split_date", "split_ratio": "last_split_ratio"})
            keep_cols = ["symbol", "last_split_date"]
            if "last_split_ratio" in s_last.columns:
                keep_cols.append("last_split_ratio")
            s_last = s_last[keep_cols]

            s_cnt = (s.groupby("symbol")["date"]
                       .count()
                       .reset_index()
                       .rename(columns={"date": "split_count_total"}))

            s_agg = s_last.merge(s_cnt, on="symbol", how="left")
            master = _left(master, s_agg, cols=["symbol", "last_split_date", "last_split_ratio", "split_count_total"])

    # --- Insider-Transaktionen: letzte + 12M-Summen -------------------------
    if insider is not None and "transaction_date" in insider.columns:
        ins = insider.copy()
        ins["transaction_date"] = pd.to_datetime(ins["transaction_date"], errors="coerce")
        ins = ins.dropna(subset=["transaction_date"])
        if not ins.empty:
            last_tx = (ins.sort_values(["symbol", "transaction_date"])
                          .groupby("symbol")
                          .tail(1))
            last_tx = last_tx.rename(columns={
                "transaction_date": "insider_last_tx_date",
                "transaction_code": "insider_last_tx_code",
                "share": "insider_last_tx_shares",
                "transaction_price": "insider_last_tx_price",
            })
            keep_cols = ["symbol", "insider_last_tx_date", "insider_last_tx_code",
                         "insider_last_tx_shares", "insider_last_tx_price"]
            last_tx = last_tx[[c for c in keep_cols if c in last_tx.columns]]

            cutoff = pd.Timestamp.today() - timedelta(days=365)
            recent = ins[ins["transaction_date"] >= cutoff].copy()

            def _is_buy(code):
                c = str(code).upper().strip()
                return c.startswith("P")  # Purchase

            def _is_sell(code):
                c = str(code).upper().strip()
                return c.startswith("S")  # Sale

            recent["is_buy"] = recent.get("transaction_code", "").apply(_is_buy)
            recent["is_sell"] = recent.get("transaction_code", "").apply(_is_sell)

            recent["share"] = pd.to_numeric(recent.get("share"), errors="coerce").fillna(0.0)

            gb = recent.groupby("symbol")
            agg_ins = gb.agg(
                insider_buy_shares_12m=("share", lambda x: float(x[recent.loc[x.index, "is_buy"]].sum())),
                insider_sell_shares_12m=("share", lambda x: float(x[recent.loc[x.index, "is_sell"]].sum())),
                insider_buy_trades_12m=("is_buy", "sum"),
                insider_sell_trades_12m=("is_sell", "sum"),
            ).reset_index()

            ins_agg = last_tx.merge(agg_ins, on="symbol", how="left")
            master = _left(master, ins_agg)

    # --- stance aus dir ------------------------------------------------------
    master["stance"] = master.apply(_stance_from_dir, axis=1)

    # --- Datumsfelder schön machen ------------------------------------------
    for c in ["next_expiry", "opt_expiry", "earnings_next",
              "si_date", "last_div_date", "last_split_date",
              "insider_last_tx_date"]:
        if c in master.columns:
            try:
                master[c] = pd.to_datetime(master[c], errors="coerce").dt.date
            except Exception:
                pass

    # --- Spalten sortieren ---------------------------------------------------
    preferred = [
        "symbol", "name", "sector", "industry",
        "stance", "dir", "strength",
        "next_expiry", "nearest_dte",
        "fs7", "fs30", "fs60", "focus_strike_general",
        "opt_expiry", "opt_spot",
        "opt_call_oi", "opt_put_oi", "opt_put_call_ratio",
        "opt_call_iv_w", "opt_put_iv_w",
        "opt_call_top_strikes", "opt_put_top_strikes",
        "tot_call_oi_all", "tot_put_oi_all", "tot_put_call_ratio_all",
        "earnings_next",
        "hv10", "hv20", "hv30", "hv60",
        "cds_proxy",
        "marketcap", "pe", "pb", "ps", "ev_ebitda", "beta", "currency",
        "si_date", "si_shares", "float_shares", "si_pct_float",
        "borrow_rate", "borrow_avail", "borrow_stress", "si_source",
        "peers_count",
        "last_div_date", "last_div_amount", "div_count_total",
        "last_split_date", "last_split_ratio", "split_count_total",
        "insider_last_tx_date", "insider_last_tx_code",
        "insider_last_tx_shares", "insider_last_tx_price",
        "insider_buy_shares_12m", "insider_sell_shares_12m",
        "insider_buy_trades_12m", "insider_sell_trades_12m",
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
