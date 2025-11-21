#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_factor_scores.py
----------------------
Baut Value-/Quality-/Growth-/Momentum-/Risk-Scores (0–100) pro Aktie
auf Basis deiner bestehenden Dateien und leitet daraus mehrere Rankings ab:

Input:
- data/processed/fundamentals_core.csv
- data/processed/hv_summary.csv.gz
- data/processed/cds_proxy.csv (oder .csv.gz)
- data/processed/short_interest.csv(.gz) (optional)
- data/processed/index_membership.csv (optional, für echtes R2K)
- data/prices/{SYMBOL}.csv (Returns 1/3/6/12M, 52W-High-Nähe)

Output:
- data/processed/factor_scores.csv

Scores (0–100):
- value_score, quality_score, growth_score, momentum_score, risk_score, composite_score

Letter-Grades A–D:
- global_grade      (A–D, globales Ranking von **fundamental_score = Quality+Growth**, OHNE Value)
- momentum_grade    (A–D, globales Ranking von momentum_score)
- r2k_growth_grade  (A–D, Ranking von growth_score NUR innerhalb Russell-Subset)
- risk_grade        (A–D, Ranking von risk_score: hoch = stabil, wenig Risiko)

Mapping (Perzentile):
- A: oberste 20 %
- B: 50–80 %
- C: 20–50 %
- D: unterste 20 %
"""

import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


BASE_PROC = os.path.join("data", "processed")
BASE_PRICES = os.path.join("data", "prices")


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


def sector_percentile(
    df: pd.DataFrame, col: str, higher_is_better: bool
) -> pd.Series:
    """
    Sektor-neutralisierte Perzentile (0–100).
    Annahme: df hat Spalte 'sector'.
    """
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index)

    def _p(s: pd.Series) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce")
        mask = x.notna()
        if mask.sum() <= 1:
            return pd.Series(np.nan, index=x.index)
        ranks = x[mask].rank(method="average", pct=True)
        if not higher_is_better:
            ranks = 1.0 - ranks
        out = pd.Series(np.nan, index=x.index)
        out[mask] = (ranks * 100.0).clip(0.0, 100.0)
        return out

    return df.groupby(df["sector"].fillna("UNK"), group_keys=False)[col].apply(_p)


def global_percentile(
    df: pd.DataFrame, col: str, higher_is_better: bool
) -> pd.Series:
    """Globales Perzentil über das ganze Universum (0–100)."""
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


def compute_price_features(sym: str) -> Dict[str, float]:
    """
    Liest data/prices/{sym}.csv und berechnet:
    - rtn_1m, rtn_3m, rtn_6m, rtn_12m (in %)
    - near_52w_high (0–100, 100 = am Hoch)
    Wenn Datei fehlt oder zu wenig Daten: leere dict.
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
        # yfinance-Format: "Close"?
        if "Close" in df.columns:
            df["close"] = df["Close"]
        else:
            return {}

    close = pd.to_numeric(df["close"], errors="coerce")
    close = close.dropna()
    if len(close) < 30:
        return {}

    out: Dict[str, float] = {}
    # Handels-Tage: grob 21/63/126/252
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
                # 100 = am Hoch; 0 = sehr weit weg
                out["near_52w_high"] = 100.0 * (1.0 - (hi - last) / hi)
    except Exception:
        pass

    return out


def scores_to_grade(series: pd.Series) -> pd.Series:
    """
    Mappt eine Score-Spalte (0–100) auf Letter-Grades A–D anhand
    globaler Perzentile:
      A: oberste 20%
      B: 50–80%
      C: 20–50%
      D: unterste 20%
    """
    s = pd.to_numeric(series, errors="coerce")
    grades = pd.Series(np.nan, index=s.index, dtype="object")
    mask = s.notna()
    if mask.sum() == 0:
        return grades

    ranks = s[mask].rank(method="average", pct=True)

    for idx, r in ranks.items():
        if r >= 0.80:
            g = "A"
        elif r >= 0.50:
            g = "B"
        elif r >= 0.20:
            g = "C"
        else:
            g = "D"
        grades.at[idx] = g

    return grades


def main():
    os.makedirs(BASE_PROC, exist_ok=True)

    # ------------------------------------------------------------------ #
    # 1) FUNDAMENTALS LADEN
    # ------------------------------------------------------------------ #
    fund = rd("fundamentals_core.csv")
    if fund is None or fund.empty:
        raise SystemExit("fundamentals_core.csv fehlt oder ist leer")

    # Sicherstellen, dass Basis-Spalten existieren
    fund = ensure_cols(fund, ["symbol", "sector", "industry", "marketcap"])
    fund["symbol"] = fund["symbol"].astype(str).str.upper()

    # ------------------------------------------------------------------ #
    # 2) HV / CREDIT / SHORT-RISK MERGEN
    # ------------------------------------------------------------------ #
    hv = rd("hv_summary.csv.gz")
    if hv is not None and not hv.empty:
        hv = ensure_cols(hv, ["symbol", "hv20", "hv60"])
        hv["symbol"] = hv["symbol"].astype(str).str.upper()
        fund = fund.merge(
            hv[["symbol", "hv20", "hv60"]].drop_duplicates("symbol"),
            on="symbol",
            how="left",
        )

    # CDS robust laden (kein or auf DataFrame)
    cds = rd("cds_proxy.csv")
    if cds is None or cds.empty:
        cds = rd("cds_proxy.csv.gz")

    if cds is not None and not cds.empty:
        # Annahme: Spalten symbol, proxy_spread o.ä.
        # Wir nehmen die erste passende Spalte als Credit-Prox
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

    # Short Interest (optional) – nur für Risk-Score, wenn vorhanden
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

    # ------------------------------------------------------------------ #
    # 3) FIELDS, DIE WIR FÜR SCORES NUTZEN WOLLEN
    # ------------------------------------------------------------------ #
    # Value-Kandidaten (je niedriger, desto besser – außer fcf_yield)
    value_low_is_good = ["pe", "pb", "ps", "ev_ebitda", "ev_sales"]
    value_high_is_good = ["fcf_yield"]

    # Quality-Kandidaten (je höher, desto besser)
    quality_high_is_good = [
        "gross_margin",
        "oper_margin",
        "fcf_margin",
        "roe",
        "roic",
    ]

    # Risk-Stabilitäts-Faktoren:
    # hohe Werte = HOHES Risiko (schlecht), niedrige Werte = stabil (gut)
    # => wir rechnen sie so um, dass risk_score HOCH = stabil/wenig Risiko ist.
    risk_high_is_bad = [
        "beta",
        "hv60",
        "net_debt_ebitda",
        "credit_spread",
        "si_pct_float",
        "borrow_rate",
    ]

    # Momentum (Returns hoch = gut, Nähe 52W-High hoch = gut)
    momo_cols = ["rtn_1m", "rtn_3m", "rtn_6m", "rtn_12m", "near_52w_high"]

    # ------------------------------------------------------------------ #
    # 4) PRICE-FEATURES je SYMBOL REINMERGEN
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # 5) VALUE-SCORE
    # ------------------------------------------------------------------ #
    for c in value_low_is_good + value_high_is_good:
        if c not in fund.columns:
            fund[c] = np.nan

    value_parts = []

    for c in value_low_is_good:
        s = sector_percentile(fund, c, higher_is_better=False)
        fund[f"value_{c}_pctl"] = s
        if s.notna().any():
            value_parts.append(f"value_{c}_pctl")

    for c in value_high_is_good:
        s = sector_percentile(fund, c, higher_is_better=True)
        fund[f"value_{c}_pctl"] = s
        if s.notna().any():
            value_parts.append(f"value_{c}_pctl")

    if value_parts:
        fund["value_score"] = fund[value_parts].mean(axis=1, skipna=True)
    else:
        fund["value_score"] = np.nan

    # ------------------------------------------------------------------ #
    # 6) QUALITY-SCORE
    # ------------------------------------------------------------------ #
    for c in quality_high_is_good:
        if c not in fund.columns:
            fund[c] = np.nan

    q_parts = []
    for c in quality_high_is_good:
        s = sector_percentile(fund, c, higher_is_better=True)
        fund[f"quality_{c}_pctl"] = s
        if s.notna().any():
            q_parts.append(f"quality_{c}_pctl")

    if q_parts:
        fund["quality_score"] = fund[q_parts].mean(axis=1, skipna=True)
    else:
        fund["quality_score"] = np.nan

    # ------------------------------------------------------------------ #
    # 7) MOMENTUM-SCORE (global Perzentile)
    # ------------------------------------------------------------------ #
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

    # ------------------------------------------------------------------ #
    # 8) RISK-SCORE (Stabilität: hoch = gut, wenig Risiko)
    # ------------------------------------------------------------------ #
    for c in risk_high_is_bad:
        if c not in fund.columns:
            fund[c] = np.nan

    risk_parts = []
    for c in risk_high_is_bad:
        # WICHTIG: higher_is_better=False, weil hohe Werte schlecht sind.
        # Dadurch bekommen niedrige Risiko-Werte HOHE Perzentile.
        s = sector_percentile(fund, c, higher_is_better=False)
        fund[f"risk_{c}_pctl"] = s
        if s.notna().any():
            risk_parts.append(f"risk_{c}_pctl")

    if risk_parts:
        fund["risk_score"] = fund[risk_parts].mean(axis=1, skipna=True)
    else:
        fund["risk_score"] = np.nan

    # ------------------------------------------------------------------ #
    # 9) GROWTH-SCORE (LIGHT) – OPTIONAL
    # ------------------------------------------------------------------ #
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
        s = sector_percentile(fund, c, higher_is_better=True)
        fund[f"growth_{c}_pctl"] = s
        if s.notna().any():
            g_parts.append(f"growth_{c}_pctl")

    if g_parts:
        fund["growth_score"] = fund[g_parts].mean(axis=1, skipna=True)
    else:
        fund["growth_score"] = np.nan

    # ------------------------------------------------------------------ #
    # 10) FUNDAMENTAL-COMPOSITE (OHNE Momentum & Risk, OHNE Value)
    #      -> jetzt NUR Quality + Growth (Business-Qualität)
    # ------------------------------------------------------------------ #
    v = fund["value_score"].astype(float)    # bleibt erhalten, aber geht NICHT in fundamental_score
    q = fund["quality_score"].astype(float)
    g = fund["growth_score"].astype(float)
    # m = fund["momentum_score"].astype(float)  # separat
    # r = fund["risk_score"].astype(float)      # separat

    # Neuer Fundamental-Score:
    #  - 60 % Quality
    #  - 40 % Growth
    #  - KEIN Value (Value bleibt eigener Score)
    fundamental_score = (
        0.60 * q +
        0.40 * g
    )

    fund["fundamental_score"] = fundamental_score
    # Für Kompatibilität: composite_score = Fundamental-Score
    fund["composite_score"] = fundamental_score

    # ------------------------------------------------------------------ #
    # 11) LETTER-GRADES (Global Fundamental, Momentum, Risk, R2K-Growth)
    # ------------------------------------------------------------------ #
    # Globaler Fundamental-Grade: jetzt nur noch Quality+Growth
    fund["global_grade"] = scores_to_grade(fund["fundamental_score"])
    # Separater Momentum-Grade
    fund["momentum_grade"] = scores_to_grade(fund["momentum_score"])
    # Separater Risk-Grade (hoch = stabil)
    fund["risk_grade"] = scores_to_grade(fund["risk_score"])

    # --- Russell-2000-Subset bestimmen ---
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
        # Fallback: Small-Cap-Proxy über Marketcap (untere 40% = "R2K-ähnlich")
        mc = pd.to_numeric(fund["marketcap"], errors="coerce")
        if mc.notna().sum() > 10:
            thresh = mc.quantile(0.40)
            mask_r2k = mc <= thresh

    # R2K-Growth-Grade nur für dieses Subset
    fund["r2k_growth_grade"] = np.nan
    if mask_r2k.any():
        subset = fund.loc[mask_r2k, "growth_score"]
        grades_r2k = scores_to_grade(subset)
        fund.loc[mask_r2k, "r2k_growth_grade"] = grades_r2k

    # ------------------------------------------------------------------ #
    # 12) OUTPUT REDUZIEREN & SCHREIBEN
    # ------------------------------------------------------------------ #
    out_cols = [
        "symbol",
        "sector",
        "industry",
        "marketcap",
        # Basis-Features (optional mitgeben)
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
        # Grades / Rankings
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
