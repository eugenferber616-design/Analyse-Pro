#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_factor_scores.py
----------------------
Baut Value-/Quality-/Growth-/Momentum-/Risk-Scores (0–100) pro Aktie.

WICHTIG (dein gewünschtes Design):
- Value = eigener Score + eigener Grade (value_score, value_grade)
- Fundamental/Global = NUR Quality + Growth (fundamental_score, global_grade)
- Risk = eigener Score + Grade (risk_score, risk_grade)
- Momentum = relativ (vs. Universum), wie vorher
- Alle Grades A–D basieren auf festen Score-Grenzen (nicht mehr Perzentile):

    A: Score >= 80
    B: 60–79
    C: 40–59
    D: < 40

Input:
- data/processed/fundamentals_core.csv
- data/processed/hv_summary.csv.gz
- data/processed/cds_proxy.csv (oder .csv.gz)
- data/processed/short_interest.csv(.gz) (optional)
- data/processed/index_membership.csv (optional, für R2K)
- data/prices/{SYMBOL}.csv (Returns 1/3/6/12M, 52W-High-Nähe)

Output:
- data/processed/factor_scores.csv
"""

import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


BASE_PROC = os.path.join("data", "processed")
BASE_PRICES = os.path.join("data", "prices")


# ------------------------------------------------------------------ #
# Helpers
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


def global_percentile(
    df: pd.DataFrame, col: str, higher_is_better: bool
) -> pd.Series:
    """
    Globales Perzentil über das ganze Universum (0–100).
    Wird noch für Momentum verwendet (relative Performance),
    aber NICHT mehr für Value/Quality/Risk/Growth.
    """
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index)
    x = pd.to_numeric(df[col], errors="coerce")
    mask = x.notna()
    if mask.sum() <= 1:
        return pd.Series(np.nan, index=df.index)
    ranks = x[mask].rank(method="average", pct=True)
    if not higher_is_better:
        ranks = 1.0 - ranks
    out = pd.Series(np.nan, index=df.index)
    out[mask] = (ranks * 100.0).clip(0.0, 100.0)
    return out


# ------------------------------------------------------------------ #
# ABSOLUTE SCORING-FUNKTIONEN (0–100)
# ------------------------------------------------------------------ #
def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def score_low_is_good_abs(
    series: pd.Series, best: float, ok: float, bad: float
) -> pd.Series:
    """
    Für Kennzahlen, bei denen "niedriger = besser" ist (z.B. KGV, Beta, HV).

    best: bis hier (inkl.) -> 100 Punkte
    ok:   bis hier noch ok -> ~70 Punkte
    bad:  ab hier und darüber -> fällt auf 0

    Dazwischen wird linear gemappt.
    """
    x = _to_num(series)
    out = pd.Series(np.nan, index=x.index)
    mask = x.notna()
    if mask.sum() == 0:
        return out

    xx = x[mask]

    # sehr gut
    out.loc[xx <= best] = 100.0

    # zwischen best und ok → 100 -> 70
    m = (xx > best) & (xx <= ok)
    if (ok - best) != 0:
        out.loc[m] = 70.0 + (ok - xx[m]) / (ok - best) * 30.0

    # zwischen ok und bad → 70 -> 10
    m = (xx > ok) & (xx <= bad)
    if (bad - ok) != 0:
        out.loc[m] = 10.0 + (bad - xx[m]) / (bad - ok) * 60.0

    # schlechter als bad → 0
    out.loc[xx > bad] = 0.0

    return out.clip(0.0, 100.0)


def score_high_is_good_abs(
    series: pd.Series, bad: float, ok: float, best: float
) -> pd.Series:
    """
    Für Kennzahlen, bei denen "höher = besser" ist (z.B. Marge, ROE, Growth).

    bad:  darunter -> 0 Punkte
    ok:   ab hier noch ok -> ~60 Punkte
    best: ab hier (inkl.) -> 100 Punkte
    """
    x = _to_num(series)
    out = pd.Series(np.nan, index=x.index)
    mask = x.notna()
    if mask.sum() == 0:
        return out

    xx = x[mask]

    # sehr schlecht
    out.loc[xx <= bad] = 0.0

    # zwischen bad und ok → 0 -> 60
    m = (xx > bad) & (xx <= ok)
    if (ok - bad) != 0:
        out.loc[m] = (xx[m] - bad) / (ok - bad) * 60.0

    # zwischen ok und best → 60 -> 90
    m = (xx > ok) & (xx <= best)
    if (best - ok) != 0:
        out.loc[m] = 60.0 + (xx[m] - ok) / (best - ok) * 30.0

    # über best → 100
    out.loc[xx > best] = 100.0

    return out.clip(0.0, 100.0)


# ------------------------------------------------------------------ #
# Preis-Features (Momentum, 52W-High)
# ------------------------------------------------------------------ #
def compute_price_features(sym: str) -> Dict[str, float]:
    """
    Liest data/prices/{sym}.csv und berechnet:
    - rtn_1m, rtn_3m, rtn_6m, rtn_12m (in %)
    - near_52w_high (0–100, 100 = am Hoch)
    """
    f = os.path.join(BASE_PRICES, f"{sym}.csv")
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

    close = pd.to_numeric(df["close"], errors="coerce").dropna()
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

    # 52W-High-Nähe (letzte 252 Tage)
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
# Grades A–D aus Score (0–100) – ABSOLUT
# ------------------------------------------------------------------ #
def scores_to_grade(series: pd.Series) -> pd.Series:
    """
    Mappt eine Score-Spalte (0–100) direkt auf Letter-Grades A–D:

      A: Score >= 80
      B: 60–79
      C: 40–59
      D: < 40
    """
    s = pd.to_numeric(series, errors="coerce")
    grades = pd.Series(np.nan, index=s.index, dtype="object")
    mask = s.notna()
    if mask.sum() == 0:
        return grades

    xx = s[mask]
    grades.loc[xx >= 80.0] = "A"
    grades.loc[(xx >= 60.0) & (xx < 80.0)] = "B"
    grades.loc[(xx >= 40.0) & (xx < 60.0)] = "C"
    grades.loc[(xx < 40.0)] = "D"

    return grades


# ------------------------------------------------------------------ #
# MAIN
# ------------------------------------------------------------------ #
def main():
    os.makedirs(BASE_PROC, exist_ok=True)

    # 1) FUNDAMENTALS LADEN
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

    # CDS robust laden
    cds = rd("cds_proxy.csv")
    if cds is None or cds.empty:
        cds = rd("cds_proxy.csv.gz")

    if cds is not None and not cds.empty:
        cds["symbol"] = cds["symbol"].astype(str).str.upper()
        cand = []
        for c in cds.columns:
            if "spread" in c.lower() or "proxy" in c.lower():
                cand.append(c)
        credit_col = cand[0] if cand else None
        if credit_col:
            fund = fund.merge(
                cds[["symbol", credit_col]].drop_duplicates("symbol"),
                on="symbol",
                how="left",
            )
            fund = fund.rename(columns={credit_col: "credit_spread"})

    # Short Interest (optional)
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

    # 3) FELDER-KANDIDATEN
    value_low_is_good = ["pe", "pb", "ps", "ev_ebitda", "ev_sales"]
    value_high_is_good = ["fcf_yield"]

    quality_high_is_good = [
        "gross_margin",
        "oper_margin",
        "fcf_margin",
        "roe",
        "roic",
    ]

    risk_high_is_bad = [
        "beta",
        "hv60",
        "net_debt_ebitda",
        "credit_spread",
        "si_pct_float",
        "borrow_rate",
    ]

    momo_cols = ["rtn_1m", "rtn_3m", "rtn_6m", "rtn_12m", "near_52w_high"]

    # 4) PRICE-FEATURES je SYMBOL
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

    # 5) VALUE-SCORE (ABSOLUT, EIGENER SCORE)
    for c in value_low_is_good + value_high_is_good:
        if c not in fund.columns:
            fund[c] = np.nan

    value_parts = []

    fund["value_pe"] = score_low_is_good_abs(
        fund["pe"], best=12.0, ok=20.0, bad=40.0
    )
    value_parts.append("value_pe")

    fund["value_pb"] = score_low_is_good_abs(
        fund["pb"], best=1.5, ok=3.0, bad=6.0
    )
    value_parts.append("value_pb")

    fund["value_ps"] = score_low_is_good_abs(
        fund["ps"], best=1.5, ok=3.0, bad=6.0
    )
    value_parts.append("value_ps")

    fund["value_ev_ebitda"] = score_low_is_good_abs(
        fund["ev_ebitda"], best=8.0, ok=12.0, bad=25.0
    )
    value_parts.append("value_ev_ebitda")

    fund["value_ev_sales"] = score_low_is_good_abs(
        fund["ev_sales"], best=1.5, ok=3.0, bad=6.0
    )
    value_parts.append("value_ev_sales")

    fund["value_fcf_yield"] = score_high_is_good_abs(
        fund["fcf_yield"], bad=0.0, ok=3.0, best=8.0
    )
    value_parts.append("value_fcf_yield")

    fund["value_score"] = (
        fund[value_parts].mean(axis=1, skipna=True) if value_parts else np.nan
    )

    # 6) QUALITY-SCORE (ABSOLUT)
    for c in quality_high_is_good:
        if c not in fund.columns:
            fund[c] = np.nan

    q_parts = []

    fund["quality_gross_margin"] = score_high_is_good_abs(
        fund["gross_margin"], bad=10.0, ok=30.0, best=50.0
    )
    q_parts.append("quality_gross_margin")

    fund["quality_oper_margin"] = score_high_is_good_abs(
        fund["oper_margin"], bad=5.0, ok=15.0, best=30.0
    )
    q_parts.append("quality_oper_margin")

    fund["quality_fcf_margin"] = score_high_is_good_abs(
        fund["fcf_margin"], bad=0.0, ok=5.0, best=15.0
    )
    q_parts.append("quality_fcf_margin")

    fund["quality_roe"] = score_high_is_good_abs(
        fund["roe"], bad=5.0, ok=12.0, best=20.0
    )
    q_parts.append("quality_roe")

    fund["quality_roic"] = score_high_is_good_abs(
        fund["roic"], bad=5.0, ok=10.0, best=20.0
    )
    q_parts.append("quality_roic")

    fund["quality_score"] = (
        fund[q_parts].mean(axis=1, skipna=True) if q_parts else np.nan
    )

    # 7) MOMENTUM-SCORE (RELATIV, Perzentile)
    for c in momo_cols:
        if c not in fund.columns:
            fund[c] = np.nan

    momo_parts = []
    for c in momo_cols:
        s = global_percentile(fund, c, higher_is_better=True)
        fund[f"momo_{c}_pctl"] = s
        if s.notna().any():
            momo_parts.append(f"momo_{c}_pctl")

    if momo_parts:
        fund["momentum_score"] = fund[momo_parts].mean(axis=1, skipna=True)
    else:
        fund["momentum_score"] = np.nan

    # 8) RISK-SCORE (ABSOLUT: hohe Werte = hohes Risiko → niedriger Score)
    for c in risk_high_is_bad:
        if c not in fund.columns:
            fund[c] = np.nan

    risk_parts = []

    fund["risk_beta"] = score_low_is_good_abs(
        fund["beta"], best=0.7, ok=1.0, bad=1.5
    )
    risk_parts.append("risk_beta")

    fund["risk_hv60"] = score_low_is_good_abs(
        fund["hv60"], best=15.0, ok=25.0, bad=40.0
    )
    risk_parts.append("risk_hv60")

    fund["risk_net_debt_ebitda"] = score_low_is_good_abs(
        fund["net_debt_ebitda"], best=0.0, ok=2.0, bad=4.0
    )
    risk_parts.append("risk_net_debt_ebitda")

    fund["risk_credit_spread"] = score_low_is_good_abs(
        fund["credit_spread"], best=100.0, ok=300.0, bad=600.0
    )
    risk_parts.append("risk_credit_spread")

    fund["risk_si_pct_float"] = score_low_is_good_abs(
        fund["si_pct_float"], best=2.0, ok=8.0, bad=20.0
    )
    risk_parts.append("risk_si_pct_float")

    fund["risk_borrow_rate"] = score_low_is_good_abs(
        fund["borrow_rate"], best=1.0, ok=5.0, bad=15.0
    )
    risk_parts.append("risk_borrow_rate")

    fund["risk_score"] = (
        fund[risk_parts].mean(axis=1, skipna=True) if risk_parts else np.nan
    )

    # 9) GROWTH-SCORE (ABSOLUT – EPS/Revenue Growth)
    growth_cols = []
    for cname in fund.columns:
        lc = cname.lower()
        if "rev_yoy" in lc or "revenue_yoy" in lc:
            growth_cols.append(cname)
        if "rev_cagr" in lc or "revenue_cagr" in lc:
            growth_cols.append(cname)
        if "eps_yoy" in lc or ("eps" in lc and "growth" in lc):
            growth_cols.append(cname)

    growth_cols = sorted(set(growth_cols))

    g_parts = []
    for c in growth_cols:
        # Growth in %: -10 = schlecht, 0 = neutral, >=20 sehr gut
        fund[f"growth_{c}_pctl"] = score_high_is_good_abs(
            fund[c], bad=-10.0, ok=0.0, best=20.0
        )
        g_parts.append(f"growth_{c}_pctl")

    fund["growth_score"] = (
        fund[g_parts].mean(axis=1, skipna=True) if g_parts else np.nan
    )

    # 10) FUNDAMENTAL-COMPOSITE (NUR Quality + Growth, OHNE Value)
    q = fund["quality_score"].astype(float)
    g = fund["growth_score"].astype(float)

    # z.B. 60% Quality, 40% Growth – kannst du später anpassen
    fundamental_score = 0.60 * q + 0.40 * g
    fund["fundamental_score"] = fundamental_score
    fund["composite_score"] = fundamental_score

    # 11) LETTER-GRADES (ABSOLUT)
    fund["value_grade"] = scores_to_grade(fund["value_score"])
    fund["global_grade"] = scores_to_grade(fund["fundamental_score"])
    fund["momentum_grade"] = scores_to_grade(fund["momentum_score"])
    fund["risk_grade"] = scores_to_grade(fund["risk_score"])

    # Russell-2000-ähnliches Growth-Subset (wie vorher)
    idx_df = rd("index_membership.csv")
    mask_r2k = pd.Series(False, index=fund.index)

    if (
        idx_df is not None
        and not idx_df.empty
        and "symbol" in idx_df.columns
        and "index" in idx_df.columns
    ):
        idx_df["symbol"] = idx_df["symbol"].astype(str).str.upper()
        idx_df["index"] = idx_df["index"].astype(str).str.upper()
        r2k_syms = idx_df.loc[
            idx_df["index"].str.contains("RUSSELL"), "symbol"
        ].unique()
        mask_r2k = fund["symbol"].isin(r2k_syms)
    else:
        mc = pd.to_numeric(fund["marketcap"], errors="coerce")
        if mc.notna().sum() > 10:
            thresh = mc.quantile(0.40)
            mask_r2k = mc <= thresh

    fund["r2k_growth_grade"] = np.nan
    if mask_r2k.any():
        subset = fund.loc[mask_r2k, "growth_score"]
        grades_r2k = scores_to_grade(subset)
        fund.loc[mask_r2k, "r2k_growth_grade"] = grades_r2k

    # 12) OUTPUT
    out_cols = [
        "symbol",
        "sector",
        "industry",
        "marketcap",
        # Basis-Features (optional)
        "pe",
        "pb",
        "ps",
        "ev_ebitda",
        "ev_sales",
        "gross_margin",
        "oper_margin",
        "fcf_margin",
        "beta",
        "hv20",
        "hv60",
        "net_debt_ebitda",
        "current_ratio",
        "credit_spread",
        "si_pct_float",
        "borrow_rate",
        "rtn_1m",
        "rtn_3m",
        "rtn_6m",
        "rtn_12m",
        "near_52w_high",
        # Scores
        "value_score",
        "quality_score",
        "growth_score",
        "momentum_score",
        "risk_score",
        "fundamental_score",
        "composite_score",
        # Grades
        "value_grade",
        "global_grade",
        "momentum_grade",
        "risk_grade",
        "r2k_growth_grade",
    ]

    out_cols = [c for c in out_cols if c in fund.columns]

    out = fund[out_cols].copy()
    out_path = os.path.join(BASE_PROC, "factor_scores.csv")
    out.to_csv(out_path, index=False)
    print("wrote", out_path, "rows:", len(out))


if __name__ == "__main__":
    main()
