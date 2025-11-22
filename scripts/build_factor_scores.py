#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_factor_scores.py – ABSOLUTE Version (Updated for Subdirectories)
-----------------------------------------
"""

import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


BASE_PROC = os.path.join("data", "processed")
BASE_PRICES = os.path.join("data", "prices")


# ------------------------------------------------------------------ #
# Helper: robustes Einlesen
# ------------------------------------------------------------------ #

def rd(path: str, **kwargs) -> Optional[pd.DataFrame]:
    """Robustes Einlesen von CSV/CSV.GZ aus data/processed."""
    full = os.path.join(BASE_PROC, path)
    if not os.path.exists(full):
        return None
    try:
        return pd.read_csv(full, **kwargs)
    except Exception:
        try:
            return pd.read_csv(full, compression="infer", **kwargs)
        except Exception:
            return None


def ensure_cols(df: pd.DataFrame, need: List[str]) -> pd.DataFrame:
    for c in need:
        if c not in df.columns:
            df[c] = np.nan
    return df


# ------------------------------------------------------------------ #
# Helper: Skalierungsfunktionen (absolut)
# ------------------------------------------------------------------ #

def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def scale_linear(x: pd.Series, low_bad: float, high_good: float) -> pd.Series:
    v = to_num(x)
    out = (v - low_bad) / float(high_good - low_bad) * 100.0
    return out.clip(0.0, 100.0)


def scale_inverse(x: pd.Series, low_good: float, high_bad: float) -> pd.Series:
    v = to_num(x)
    out = (high_bad - v) / float(high_bad - low_good) * 100.0
    return out.clip(0.0, 100.0)


def as_percent(x: pd.Series) -> pd.Series:
    v = to_num(x)
    med = v.median(skipna=True)
    if pd.notna(med) and -1.0 < med < 1.0:
        v = v * 100.0
    return v


def score_to_grade(score: pd.Series) -> pd.Series:
    s = to_num(score)
    out = pd.Series(np.nan, index=s.index, dtype="object")
    mask = s.notna()
    if not mask.any():
        return out

    out.loc[(mask) & (s >= 75.0)] = "A"
    out.loc[(mask) & (s >= 55.0) & (s < 75.0)] = "B"
    out.loc[(mask) & (s >= 35.0) & (s < 55.0)] = "C"
    out.loc[(mask) & (s < 35.0)] = "D"
    return out


# ------------------------------------------------------------------ #
# Price-Features (MIT NEUER ORDNER-LOGIK)
# ------------------------------------------------------------------ #

def compute_price_features(sym: str) -> Dict[str, float]:
    """
    Liest data/prices/{SUBDIR}/{sym}.csv und berechnet Features.
    """
    # --- NEUE LOGIK START ---
    if not sym: 
        return {}
        
    first_char = sym[0].upper()
    # Falls erstes Zeichen keine Buchstabe ist (z.B. '1'), Ordner '#' nutzen
    if not first_char.isalpha():
        first_char = "#"

    f = os.path.join(BASE_PRICES, first_char, f"{sym}.csv")
    # --- NEUE LOGIK ENDE ---

    if not os.path.exists(f):
        return {}

    try:
        df = pd.read_csv(f)
    except Exception:
        return {}

    if "date" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date")
        except Exception:
            pass

    if "close" not in df.columns:
        if "Close" in df.columns:
            df["close"] = df["Close"]
        else:
            return {}

    close = to_num(df["close"]).dropna()
    if len(close) < 30:
        return {}

    out: Dict[str, float] = {}
    horizons = [(21, "rtn_1m"), (63, "rtn_3m"), (126, "rtn_6m"), (252, "rtn_12m")]
    for d, name in horizons:
        if len(close) > d:
            try:
                r = float(close.iloc[-1] / close.iloc[-1 - d] - 1.0) * 100.0
                out[name] = r
            except Exception:
                pass

    try:
        if len(close) >= 50:
            window = close.iloc[-252:] if len(close) >= 252 else close
            hi = float(window.max())
            last = float(close.iloc[-1])
            if hi > 0:
                out["near_52w_high"] = 100.0 * (1.0 - (hi - last) / hi)
    except Exception:
        pass

    return out


# ------------------------------------------------------------------ #
# Hauptfunktion
# ------------------------------------------------------------------ #

def main():
    os.makedirs(BASE_PROC, exist_ok=True)

    # 1) FUNDAMENTALS
    fund = rd("fundamentals_core.csv")
    if fund is None or fund.empty:
        raise SystemExit("fundamentals_core.csv fehlt oder ist leer")

    fund = ensure_cols(fund, ["symbol", "sector", "industry", "marketcap"])
    fund["symbol"] = fund["symbol"].astype(str).str.upper()

    # 2) HV / CREDIT / SHORT-RISK MERGEN
    hv = rd("hv_summary.csv.gz")
    if hv is not None and not hv.empty:
        hv = ensure_cols(hv, ["symbol", "hv20", "hv60"])
        hv["symbol"] = hv["symbol"].astype(str).str.upper()
        fund = fund.merge(
            hv[["symbol", "hv20", "hv60"]].drop_duplicates("symbol"),
            on="symbol",
            how="left",
        )

    cds = rd("cds_proxy.csv")
    if cds is None or cds.empty:
        cds = rd("cds_proxy.csv.gz")

    if cds is not None and not cds.empty:
        cds["symbol"] = cds["symbol"].astype(str).str.upper()
        cand = [c for c in cds.columns if "spread" in c.lower() or "proxy" in c.lower()]
        credit_col = cand[0] if cand else None
        if credit_col:
            fund = fund.merge(
                cds[["symbol", credit_col]].drop_duplicates("symbol"),
                on="symbol",
                how="left",
            )
            fund = fund.rename(columns={credit_col: "credit_spread"})

    si = rd("short_interest.csv")
    if si is None or si.empty:
        si = rd("short_interest.csv.gz")

    if si is not None and not si.empty:
        si["symbol"] = si["symbol"].astype(str).str.upper()
        cols = [c for c in si.columns if c in ("si_pct_float", "borrow_rate")]
        if cols:
            fund = fund.merge(
                si[["symbol"] + cols].drop_duplicates("symbol"),
                on="symbol",
                how="left",
            )

    # 3) PRICE-FEATURES
    all_syms = fund["symbol"].dropna().astype(str).unique().tolist()
    price_rows = []
    for sym in all_syms:
        feats = compute_price_features(sym)
        if feats:
            row = {"symbol": sym}
            row.update(feats)
            price_rows.append(row)

    if price_rows:
        price_df = pd.DataFrame(price_rows)
        fund = fund.merge(price_df, on="symbol", how="left")

    # 4) VALUE-SCORE
    for c in ["pe", "pb", "ps", "ev_ebitda", "ev_sales", "fcf_yield"]:
        if c not in fund.columns: fund[c] = np.nan

    sc_val_pe = scale_inverse(fund["pe"], low_good=10.0, high_bad=40.0)
    sc_val_pb = scale_inverse(fund["pb"], low_good=1.0, high_bad=6.0)
    sc_val_ps = scale_inverse(fund["ps"], low_good=1.0, high_bad=10.0)
    sc_val_ev_ebitda = scale_inverse(fund["ev_ebitda"], low_good=6.0, high_bad=20.0)
    sc_val_ev_sales = scale_inverse(fund["ev_sales"], low_good=1.5, high_bad=10.0)
    sc_val_fcf = scale_linear(fund["fcf_yield"], low_bad=0.0, high_good=8.0)

    value_mat = np.vstack([sc_val_pe, sc_val_pb, sc_val_ps, sc_val_ev_ebitda, sc_val_ev_sales, sc_val_fcf])
    fund["value_score"] = np.nanmean(value_mat, axis=0)

    # 5) QUALITY-SCORE
    for c in ["gross_margin", "oper_margin", "fcf_margin", "roe", "roic"]:
        if c not in fund.columns: fund[c] = np.nan

    sc_q_gm = scale_linear(as_percent(fund["gross_margin"]), low_bad=5.0, high_good=40.0)
    sc_q_om = scale_linear(as_percent(fund["oper_margin"]), low_bad=0.0, high_good=30.0)
    sc_q_fm = scale_linear(as_percent(fund["fcf_margin"]), low_bad=0.0, high_good=20.0)
    sc_q_roe = scale_linear(as_percent(fund["roe"]), low_bad=5.0, high_good=25.0)
    sc_q_roic = scale_linear(as_percent(fund["roic"]), low_bad=5.0, high_good=25.0)

    q_mat = np.vstack([sc_q_gm, sc_q_om, sc_q_fm, sc_q_roe, sc_q_roic])
    fund["quality_score"] = np.nanmean(q_mat, axis=0)

    # 6) GROWTH-SCORE
    growth_cols = []
    for cname in fund.columns:
        lc = cname.lower()
        if "rev_yoy" in lc or "revenue_yoy" in lc or "rev_cagr" in lc or "revenue_cagr" in lc or "eps_yoy" in lc:
            growth_cols.append(cname)
    growth_cols = sorted(set(growth_cols))

    g_parts = []
    for c in growth_cols:
        sc = scale_linear(as_percent(fund[c]), low_bad=-10.0, high_good=20.0)
        g_parts.append(sc)

    if g_parts:
        fund["growth_score"] = np.nanmean(np.vstack(g_parts), axis=0)
    else:
        fund["growth_score"] = np.nan

    # 7) MOMENTUM-SCORE
    for c in ["rtn_1m", "rtn_3m", "rtn_6m", "rtn_12m", "near_52w_high"]:
        if c not in fund.columns: fund[c] = np.nan

    sc_m_1m = scale_linear(fund["rtn_1m"], low_bad=-20.0, high_good=15.0)
    sc_m_3m = scale_linear(fund["rtn_3m"], low_bad=-25.0, high_good=25.0)
    sc_m_6m = scale_linear(fund["rtn_6m"], low_bad=-30.0, high_good=40.0)
    sc_m_12m = scale_linear(fund["rtn_12m"], low_bad=-40.0, high_good=60.0)
    sc_m_hi = scale_linear(fund["near_52w_high"], low_bad=0.0, high_good=100.0)

    fund["momentum_score"] = np.nanmean(np.vstack([sc_m_1m, sc_m_3m, sc_m_6m, sc_m_12m, sc_m_hi]), axis=0)

    # 8) RISK-SCORE
    for c in ["beta", "hv60", "net_debt_ebitda", "credit_spread", "si_pct_float", "borrow_rate"]:
        if c not in fund.columns: fund[c] = np.nan

    r_parts = [
        scale_inverse(fund["beta"], 0.8, 2.0),
        scale_inverse(fund["hv60"], 15.0, 60.0),
        scale_inverse(fund["net_debt_ebitda"], 0.0, 5.0),
        scale_inverse(fund["credit_spread"], 50.0, 600.0),
        scale_inverse(fund["si_pct_float"], 0.0, 20.0),
        scale_inverse(fund["borrow_rate"], 0.0, 15.0)
    ]
    fund["risk_score"] = np.nanmean(np.vstack(r_parts), axis=0)

    # 9) FUNDAMENTAL & GRADES
    fund["fundamental_score"] = 0.50 * fund["quality_score"] + 0.50 * fund["growth_score"]
    fund["composite_score"] = fund["fundamental_score"]

    fund["global_grade"] = score_to_grade(fund["fundamental_score"])
    fund["value_grade"] = score_to_grade(fund["value_score"])
    fund["momentum_grade"] = score_to_grade(fund["momentum_score"])
    fund["risk_grade"] = score_to_grade(fund["risk_score"])

    # R2K Growth
    idx_df = rd("index_membership.csv")
    mask_r2k = pd.Series(False, index=fund.index)
    if idx_df is not None and not idx_df.empty and "symbol" in idx_df.columns and "index" in idx_df.columns:
        idx_df["symbol"] = idx_df["symbol"].astype(str).str.upper()
        idx_df["index"] = idx_df["index"].astype(str).str.upper()
        r2k_syms = idx_df.loc[idx_df["index"].str.contains("RUSSELL"), "symbol"].unique()
        mask_r2k = fund["symbol"].isin(r2k_syms)
    else:
        mc = to_num(fund["marketcap"])
        if mc.notna().sum() > 10: mask_r2k = mc <= mc.quantile(0.40)

    fund["r2k_growth_grade"] = np.nan
    if mask_r2k.any():
        fund.loc[mask_r2k, "r2k_growth_grade"] = score_to_grade(fund.loc[mask_r2k, "growth_score"])

    # OUTPUT
    out_cols = [
        "symbol", "sector", "industry", "marketcap",
        "pe", "pb", "ps", "ev_ebitda", "ev_sales", "gross_margin", "oper_margin", "fcf_margin",
        "beta", "hv20", "hv60", "net_debt_ebitda", "current_ratio", "credit_spread",
        "si_pct_float", "borrow_rate", "rtn_1m", "rtn_3m", "rtn_6m", "rtn_12m", "near_52w_high",
        "value_score", "quality_score", "growth_score", "momentum_score", "risk_score",
        "fundamental_score", "composite_score",
        "global_grade", "value_grade", "momentum_grade", "risk_grade", "r2k_growth_grade",
    ]
    out = fund[[c for c in out_cols if c in fund.columns]].copy()
    
    out_path = os.path.join(BASE_PROC, "factor_scores.csv")
    out.to_csv(out_path, index=False)
    print("wrote", out_path, "rows:", len(out))

if __name__ == "__main__":
    main()
