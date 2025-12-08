#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex.py  (robust, NA-/Type-safe)

Liest (so viele wie vorhanden):
  - data/processed/fred_core.csv.gz        (FRED-Makroserien)
  - data/processed/market_core.csv.gz      (VIX/DXY/USDJPY/HYG/LQD/XLF/SPY …)
  - data/processed/fred_oas.csv.gz         (optional; IG/HY OAS)

Schreibt:
  - data/processed/riskindex_snapshot.json
  - data/processed/riskindex_timeseries.csv

Wichtig:
- Alle numerischen Reihen werden mit errors="coerce" konvertiert.
- Logische Verknüpfungen nutzen ausschließlich bool-Masken (kein float | bool).
- Enthält _series_from_preds_df(): rule-based Gates → risk_gates (0..N) + risk_index_bin (0..100).
"""

from __future__ import annotations
import json, math, sys
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd

# ───────────────────────────────────────────────────────────────
# Pfade
F_FRED   = Path("data/processed/fred_core.csv.gz")
F_OAS    = Path("data/processed/fred_oas.csv.gz")
F_MARKET = Path("data/processed/market_core.csv.gz")
OUTDIR   = Path("data/processed")

# ───────────────────────────────────────────────────────────────
# IO & Basis-Helper

def _read_df(p: Path) -> pd.DataFrame:
    """CSV robust lesen: 'date' parsen, Index=DatetimeIndex, Spalten trimmen."""
    if not p.exists():
        print(f"WARN: Datei fehlt → {p}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, compression="infer")
    except Exception as e:
        print(f"WARN: CSV-Read fehlgeschlagen ({p}): {e}")
        return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    date_col = None
    for cand in ("date", "Date", "DATE"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        date_col = df.columns[0]
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_localize(None)
    except Exception:
        pass
    df = df.rename(columns={date_col: "date"}).set_index("date").sort_index()
    # alle numerisch (coerce → NaN)
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _daily_ffill(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    idx = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.reindex(idx).ffill()

def _zscore(s: pd.Series, win: int = 252) -> pd.Series | None:
    if s is None:
        return None
    s = pd.to_numeric(s, errors="coerce")
    mu = s.rolling(win, min_periods=max(20, win//4)).mean()
    sd = s.rolling(win, min_periods=max(20, win//4)).std(ddof=0)
    z = (s - mu) / sd
    return z

def _score_from_z(z: float | None, invert: bool=False) -> float | None:
    """0..100 aus z (einfach linear; außerhalb 0..100 gecappt)."""
    if z is None or pd.isna(z):
        return None
    s = 50 + 10 * ((-z) if invert else z)
    return float(max(0, min(100, s)))

def _last(s: pd.Series | None) -> float | None:
    if s is None or s.empty:
        return None
    v = s.iloc[-1]
    return None if pd.isna(v) else float(v)

# ───────────────────────────────────────────────────────────────
# NA-/Type-sichere bool-Utilities für Gates

def _num(x):
    return pd.to_numeric(x, errors="coerce")

def _gt(a, b):
    return (_num(a) > _num(b)).fillna(False)

def _ge(a, b):
    return (_num(a) >= _num(b)).fillna(False)

def _lt(a, b):
    return (_num(a) < _num(b)).fillna(False)

def _le(a, b):
    return (_num(a) <= _num(b)).fillna(False)

def _or(*masks):
    m = None
    for mk in masks:
        mk = pd.Series(mk).fillna(False)
        m = mk if m is None else (m | mk)
    return m.fillna(False)

def _and(*masks):
    m = None
    for mk in masks:
        mk = pd.Series(mk).fillna(False)
        m = mk if m is None else (m & mk)
    return m.fillna(False)

# ───────────────────────────────────────────────────────────────
# Rule-based Gates (keine float|bool Fehler mehr)

def _series_from_preds_df(df: pd.DataFrame) -> pd.Series:
    """
    Baut eine integer-Serie 'risk_gates' aus boolschen Teilregeln:
      - VIX-Term: VIX >= VIX3M  (Stress)
      - Curve invertiert: (10y-2y < 0) oder (10y-3m < 0)
      - Credit (HYG/LQD 30d Δ < 0) → Risk
      - USD stärker (DXY 30d Δ > 0) → Risk
      - UST10-Vol hoch (rolling std der Δ > Schwelle) → Risk
      - Rel. Financials schwach (XLF/SPY < SMA200 und < Tief50) → Risk
    Passe die Regeln bei Bedarf an deinen Pine exakt an.
    """
    vix   = _num(df.get("VIX"))
    vix3  = _num(df.get("VIX3M"))
    dgs10 = _num(df.get("DGS10"))
    dgs2  = _num(df.get("DGS2"))
    dgs3m = _num(df.get("DGS3MO"))
    hyg   = _num(df.get("HYG"))
    lqd   = _num(df.get("LQD"))
    dxy   = _num(df.get("DXY"))
    xlf   = _num(df.get("XLF"))
    spy   = _num(df.get("SPY"))

    # 1) VIX-Term (Stress wenn Spot >= 3M)
    m_vixterm = _ge(vix, vix3)

    # 2) Kurve invertiert
    curve_10s2  = dgs10 - dgs2
    curve_10s3m = dgs10 - dgs3m
    m_curve_inv = _or(_lt(curve_10s2, 0.0), _lt(curve_10s3m, 0.0))

    # 3) Credit: HYG/LQD Momentum 30d < 0 → Risk
    cr = hyg / lqd
    cr_chg30 = (cr - cr.shift(30)) * 100.0
    m_credit = _lt(cr_chg30, 0.0)

    # 4) USD-Stärke (DXY) 30d Δ > 0 → Risk
    dxy_chg30 = (dxy - dxy.shift(30)) * 100.0
    m_usd = _gt(dxy_chg30, 0.0)

    # 5) UST10-Vol hoch
    ust10v = dgs10.diff().rolling(20, min_periods=10).std() * math.sqrt(252.0)
    m_ust10v = _gt(ust10v, 0.05)  # Schwelle frei

    # 6) Rel. Financials schwach
    rel = xlf / spy
    rel_sma200 = rel.rolling(200, min_periods=50).mean()
    rel_low50  = rel.rolling(50,  min_periods=20).min()
    m_relfin   = _and(_lt(rel, rel_sma200), _lt(rel, rel_low50))

    score = (
        m_vixterm.astype(int)
      + m_curve_inv.astype(int)
      + m_credit.astype(int)
      + m_usd.astype(int)
      + m_ust10v.astype(int)
      + m_relfin.astype(int)
    )
    score.name = "risk_gates"
    return score

# ───────────────────────────────────────────────────────────────
def main() -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)

    dfF = _read_df(F_FRED)
    dfM = _read_df(F_MARKET)
    dfO = _read_df(F_OAS) if F_OAS.exists() else pd.DataFrame()

    if dfF.empty and dfM.empty and dfO.empty:
        print("ERROR: Keine Eingabedateien gefunden – breche ohne Snapshot ab.")
        return 0  # weich abort

    # Tagesfrequenz & ffill
    if not dfF.empty: dfF = _daily_ffill(dfF)
    if not dfM.empty: dfM = _daily_ffill(dfM)
    if not dfO.empty: dfO = _daily_ffill(dfO)

    # Spaltennamen vereinheitlichen (UPPER)
    if not dfF.empty: dfF.columns = [str(c).strip().upper() for c in dfF.columns]
    if not dfM.empty: dfM.columns = [str(c).strip().upper() for c in dfM.columns]
    if not dfO.empty: dfO.columns = [str(c).strip().upper() for c in dfO.columns]

    # Join
    dfs = [d for d in (dfF, dfM, dfO) if not d.empty]
    df  = pd.concat(dfs, axis=1).sort_index().ffill()
    if df.empty:
        print("WARN: Nach Join keine Daten – Snapshot übersprungen.")
        return 0

    cols_available = set(df.columns)

    # ── Z-Serien (ähnlich Pine-Komponenten) ──
    W = 252
    S = df.get  # kurz

    z_dgs30  = _zscore(S("DGS30"), W) if "DGS30" in df.columns else None

    if {"DGS30","DGS2"}.issubset(df.columns):
        z_2s30s = _zscore(_num(S("DGS30")) - _num(S("DGS2")), W)
    else:
        z_2s30s = None

    z_sofr30 = _zscore((_num(S("SOFR")) - _num(S("SOFR")).shift(30)) * 100, W) if "SOFR" in df.columns else None
    rrp_pct  = df["RRPONTSYD"].rank(pct=True) if "RRPONTSYD" in df.columns else None
    z_stlfsi = _zscore(S("STLFSI4"), W) if "STLFSI4" in df.columns else None

    z_vix    = _zscore(S("VIX"), W) if "VIX" in df.columns else None
    z_vxterm = _zscore(_num(S("VIX3M")) - _num(S("VIX")), W) if {"VIX3M","VIX"}.issubset(df.columns) else None
    z_dxy    = _zscore(S("DXY"), W) if "DXY" in df.columns else None

    if "USDJPY" in df.columns:
        usdvol = _num(S("USDJPY")).pct_change().rolling(20).std() * math.sqrt(252) * 100
        z_usdvol = _zscore(usdvol, W)
    else:
        z_usdvol = None

    if {"HYG","LQD"}.issubset(df.columns):
        rel = _num(S("HYG")) / _num(S("LQD"))
        z_cr30 = _zscore((rel - rel.shift(30)) * 100, W)
    else:
        z_cr30 = None

    z_10s2  = _zscore(_num(S("DGS10")) - _num(S("DGS2")),   W) if {"DGS10","DGS2"}.issubset(df.columns) else None
    z_10s3m = _zscore(_num(S("DGS10")) - _num(S("DGS3MO")), W) if {"DGS10","DGS3MO"}.issubset(df.columns) else None

    if {"XLF","SPY"}.issubset(df.columns):
        relfs = _num(S("XLF")) / _num(S("SPY"))
        z_relfin30 = _zscore((relfs - relfs.shift(30)) * 100, W)
    else:
        z_relfin30 = None

    if "DGS10" in df.columns:
        ust10v = _num(S("DGS10")).diff().rolling(20, min_periods=10).std() * math.sqrt(252)
        z_ust10v = _zscore(ust10v, W)
    else:
        z_ust10v = None

    if {"WALCL","WTREGEN","RRPONTSYD","WRESBAL"}.issubset(df.columns):
        netliq = (_num(S("WALCL")) - _num(S("WTREGEN")) - _num(S("RRPONTSYD")) - _num(S("WRESBAL"))) / 1e3
        z_netliq30 = _zscore(netliq - netliq.shift(30), W)
    else:
        z_netliq30 = None

    z_ig_oas = _zscore(S("IG_OAS"), W) if "IG_OAS" in df.columns else None
    z_hy_oas = _zscore(S("HY_OAS"), W) if "HY_OAS" in df.columns else None

    # ── Rule-based Gates (fix) → risk_index_bin ──
    try:
        gates = _series_from_preds_df(df)
        # 0..100 skalieren (bei 6 Gates → * 100/6)
        max_g = max(1, int(gates.max(skipna=True)) if pd.notna(gates.max()) else 6)
        risk_index_bin = (gates * (100.0 / max(1, max_g))).clip(0, 100)
    except Exception as e:
        print("WARN: _series_from_preds_df() failed:", e)
        gates = None
        risk_index_bin = None

    # ── Einzel-Scores (Snapshot) ──
    scores = {
        "dgs30"  : _score_from_z(_last(z_dgs30)),
        "2s30s"  : _score_from_z(_last(z_2s30s)),
        "sofr"   : _score_from_z(_last(z_sofr30)),
        "rrp"    : (1.0 - float(_last(rrp_pct))) * 100.0 if rrp_pct is not None and _last(rrp_pct) is not None else None,
        "stlfsi" : _score_from_z(_last(z_stlfsi)),

        "vix"    : _score_from_z(_last(z_vix)),
        "usdvol" : _score_from_z(_last(z_usdvol)),
        "dxy"    : _score_from_z(_last(z_dxy)),
        "cr"     : _score_from_z(_last(z_cr30)),
        "vxterm" : _score_from_z(_last(z_vxterm)),

        "10s2s"  : _score_from_z(_last(z_10s2),  invert=True),
        "10s3m"  : _score_from_z(_last(z_10s3m), invert=True),
        "relfin" : _score_from_z(_last(z_relfin30), invert=True),
        "ust10v" : _score_from_z(_last(z_ust10v)),
        "netliq" : _score_from_z(_last(z_netliq30), invert=True),

        "ig_oas" : _score_from_z(_last(z_ig_oas)) if z_ig_oas is not None else None,
        "hy_oas" : _score_from_z(_last(z_hy_oas)) if z_hy_oas is not None else None,
    }

    sc_vals = [v for v in scores.values() if v is not None]
    sc_comp = float(sum(sc_vals)/len(sc_vals)) if sc_vals else None

    # einfache Regime-Heuristik (wie vorher)
    def is_red(v): return v is not None and v >= 70
    gate_hits = sum(is_red(scores.get(k)) for k in ["cr","vix","vxterm","ust10v","relfin","10s2s","10s3m"])
    fs_score  = 2 if (is_red(scores.get("vix")) and is_red(scores.get("cr"))) else (1 if is_red(scores.get("vix")) else 0)
    rg_tip = 70.0 - (10.0 if fs_score >= 2 else 0.0) - (3.0 if gate_hits >= 3 else 0.0) - (3.0 if gate_hits >= 5 else 0.0)
    rg_tip = max(50.0, min(90.0, rg_tip))
    rg_d   = (sc_comp - rg_tip) if sc_comp is not None else 0.0

    if sc_comp is None:
        regime = "NEUTRAL"
    else:
        regime = "RISK-OFF" if (rg_d >= 0 and (gate_hits >= 4 or fs_score >= 2)) else \
                 "CAUTION"  if (rg_d >= 0) else \
                 "RISK-ON"  if (gate_hits <= 2 and fs_score <= 1 and rg_d <= -10) else "NEUTRAL"

    bias = "RISK-ON" if (sc_comp is not None and sc_comp < 45) else ("RISK-OFF" if (sc_comp is not None and sc_comp > 55) else "NEUTRAL")
    size = "klein" if (fs_score >= 2 or (scores.get("netliq") or 0) > 60) else "moderat"
    dur  = "↓" if ((scores.get("dgs30") or 0) > 60 or (scores.get("ust10v") or 0) > 60) else ("↑" if ((scores.get("dgs30") or 50) < 40 and (scores.get("ust10v") or 50) < 40) else "≙")
    one_liner = f"Bias: {bias} | Größe: {size} | Dur {dur}"

    risks = []
    if (scores.get("netliq") or 0) > 60: risks.append("Liquidität: knapp → Drawdowns können verstärkt werden.")
    if (scores.get("vix") or 0)   > 60: risks.append("Volatilität erhöht → Risiko für High-Beta.")
    if (scores.get("cr")  or 0)   > 60: risks.append("Credit Spreads weit → HY/ZYK anfällig.")
    if (scores.get("dxy") or 0)   > 60: risks.append("USD stark → Gegenwind für EM/Gold.")

    snap = {
        "asof": datetime.now(timezone.utc).isoformat(),
        "composite": sc_comp,
        "regime": regime,
        "fs_score": float(fs_score),
        "scores": scores,
        "has_risk_index_bin": risk_index_bin is not None,
        "one_liner": one_liner,
        "risks": risks,
        "available_columns": sorted(list(cols_available)),
        "notes": [
            "Snapshot nutzt alle verfügbaren Reihen; fehlende Inputs werden ignoriert.",
            "risk_index_bin stammt aus festen Gates (VIXTerm, Curve, Credit, USD, UST10Vol, RelFin)."
        ],
    }

    (OUTDIR / "riskindex_snapshot.json").write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print("✔ wrote data/processed/riskindex_snapshot.json")

    # ── Timeseries bauen ──
    zlist = [z for z in (z_dgs30, z_2s30s, z_sofr30, z_stlfsi, z_vix, z_usdvol, z_dxy, z_cr30,
                         z_vxterm, z_10s2, z_10s3m, z_relfin30, z_ust10v, z_netliq30) if z is not None]
    rows = []
    if zlist or (risk_index_bin is not None):
        idx = df.index
        for dt in idx:
            vals = []
            for z in zlist:
                try:
                    v = z.loc[dt]
                    if pd.notna(v):
                        vals.append(50 + 10 * v)
                except KeyError:
                    pass
            rec = {"date": dt.date().isoformat()}
            if len(vals) >= max(6, len(zlist)//3):
                rec["sc_comp"] = float(sum(vals)/len(vals))
            if risk_index_bin is not None:
                rec["risk_index_bin"] = float(risk_index_bin.loc[dt]) if dt in risk_index_bin.index and pd.notna(risk_index_bin.loc[dt]) else np.nan
            rows.append(rec)

    if rows:
        out = pd.DataFrame(rows)
        out.to_csv(OUTDIR / "riskindex_timeseries.csv", index=False)
        print("✔ wrote data/processed/riskindex_timeseries.csv rows:", len(out))
    else:
        print("timeseries skipped (insufficient overlap)")

    return 0

# ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sys.exit(main())
