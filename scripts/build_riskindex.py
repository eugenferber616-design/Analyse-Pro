#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex.py

Inputs (so viele wie vorhanden):
  - data/processed/fred_core.csv.gz        (FRED-Makroserien)
  - data/processed/market_core.csv.gz      (VIX/DXY/USDJPY/HYG/LQD/XLF/SPY …)
  - data/processed/fred_oas.csv.gz         (optional; IG/HY OAS)

Outputs:
  - data/processed/riskindex_snapshot.json
  - data/processed/riskindex_timeseries.csv  (falls genügend Overlap)
"""

from __future__ import annotations
import os, math, json, sys
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

F_FRED   = Path("data/processed/fred_core.csv.gz")
F_OAS    = Path("data/processed/fred_oas.csv.gz")
F_MARKET = Path("data/processed/market_core.csv.gz")
OUTDIR   = Path("data/processed")

# --------- Helpers ---------
def _read_df(p: Path) -> pd.DataFrame:
    """CSV robust lesen: 'date' parse, Index=DatetimeIndex, Spaltennamen normalisieren."""
    if not p.exists():
        print(f"WARN: Datei fehlt → {p}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, compression="infer")
    except Exception as e:
        print(f"WARN: CSV-Read fehlgeschlagen ({p}): {e}")
        return pd.DataFrame()
    # Spaltennamen normieren
    df.columns = [str(c).strip() for c in df.columns]
    # Datums-Spalte finden
    date_col = None
    for cand in ("date", "Date", "DATE"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        # 1. Spalte als Fallback
        date_col = df.columns[0]
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_localize(None)
    except Exception:
        pass
    df = df.rename(columns={date_col: "date"}).set_index("date").sort_index()
    # Alle anderen Spalten numerisieren, wo möglich
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

def _daily_ffill(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    idx = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.reindex(idx).ffill()

def _zscore(s: pd.Series, win: int = 252) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu = s.rolling(win, min_periods=max(20, win//4)).mean()
    sd = s.rolling(win, min_periods=max(20, win//4)).std(ddof=0)
    return (s - mu) / sd

def _score_from_z(z: float | None, invert: bool=False) -> float | None:
    if z is None or pd.isna(z):
        return None
    s = 50 + 10 * ((-z) if invert else z)
    return float(max(0, min(100, s)))

def _last(s: pd.Series | None) -> float | None:
    if s is None or s.empty:
        return None
    v = s.iloc[-1]
    return None if pd.isna(v) else float(v)

def _has(df: pd.DataFrame, col: str) -> bool:
    return (not df.empty) and (col in df.columns)

def _warn_missing(cols: list[str], dfname: str):
    miss = [c for c in cols if c not in cols_available]
    if miss:
        print(f"WARN: {dfname} ohne erwartete Spalten: {', '.join(miss)}")

# --------- Main ---------
def main() -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)

    dfF = _read_df(F_FRED)
    dfM = _read_df(F_MARKET)
    dfO = _read_df(F_OAS) if F_OAS.exists() else pd.DataFrame()

    if dfF.empty and dfM.empty and dfO.empty:
        print("ERROR: Keine Eingabedateien gefunden – breche ohne Snapshot ab.")
        return 0  # weich raus, damit Workflow nicht scheitert

    # Auf Tagesfrequenz & ffill
    if not dfF.empty: dfF = _daily_ffill(dfF)
    if not dfM.empty: dfM = _daily_ffill(dfM)
    if not dfO.empty: dfO = _daily_ffill(dfO)

    # Join aller verfügbaren Quellen
    dfs = [d for d in (dfF, dfM, dfO) if not d.empty]
    df  = pd.concat(dfs, axis=1).sort_index().ffill()
    if df.empty:
        print("WARN: Nach Join keine Daten – Snapshot übersprungen.")
        return 0

    # Erwartete Spaltennamen (so wie deine Fetch-Skripte sie schreiben)
    # FRED:
    fred_cols = [
        "DGS30","DGS10","DGS2","DGS3MO","SOFR","RRPONTSYD","STLFSI4",
        "WALCL","WTREGEN","WRESBAL"
    ]
    # Market:
    mkt_cols = ["VIX","VIX3M","DXY","USDJPY","HYG","LQD","XLF","SPY"]
    # OAS:
    oas_cols = ["IG_OAS","HY_OAS"]

    # Verfügbare Cols merken
    global cols_available
    cols_available = set(df.columns)

    # --- Z-Reihen bauen (wo möglich) ---
    W = 252
    def S(c): return df[c] if c in df.columns else None

    z_dgs30  = _zscore(S("DGS30"), W)                             if S("DGS30") is not None else None
    z_2s30s  = _zscore(S("DGS30") - S("DGS2"), W)                 if S("DGS30") is not None and S("DGS2") is not None else None
    z_sofr30 = _zscore((S("SOFR") - S("SOFR").shift(30))*100, W)  if S("SOFR") is not None else None
    rrp_pct  = S("RRPONTSYD").rank(pct=True)                      if S("RRPONTSYD") is not None else None
    z_stlfsi = _zscore(S("STLFSI4"), W)                           if S("STLFSI4") is not None else None

    z_vix    = _zscore(S("VIX"), W)                               if S("VIX") is not None else None
    z_vxterm = _zscore(S("VIX") - S("VIX3M"), W)                  if S("VIX") is not None and S("VIX3M") is not None else None
    z_dxy    = _zscore(S("DXY"), W)                               if S("DXY") is not None else None

    if S("USDJPY") is not None:
        usdvol = S("USDJPY").pct_change().rolling(20).std() * math.sqrt(252) * 100
        z_usdvol = _zscore(usdvol, W)
    else:
        z_usdvol = None

    if S("HYG") is not None and S("LQD") is not None:
        rel = (S("HYG")/S("LQD"))
        z_cr30 = _zscore((rel - rel.shift(30))*100, W)
    else:
        z_cr30 = None

    z_10s2   = _zscore(S("DGS10") - S("DGS2"),   W)               if S("DGS10") is not None and S("DGS2") is not None else None
    z_10s3m  = _zscore(S("DGS10") - S("DGS3MO"), W)               if S("DGS10") is not None and S("DGS3MO") is not None else None

    if S("XLF") is not None and S("SPY") is not None:
        relfs = (S("XLF")/S("SPY"))
        z_relfin30 = _zscore((relfs - relfs.shift(30))*100, W)
    else:
        z_relfin30 = None

    if S("DGS10") is not None:
        ust10v = S("DGS10").diff().rolling(20).std() * math.sqrt(252)
        z_ust10v = _zscore(ust10v, W)
    else:
        z_ust10v = None

    if all(S(k) is not None for k in ["WALCL","WTREGEN","RRPONTSYD","WRESBAL"]):
        netliq = (S("WALCL") - S("WTREGEN") - S("RRPONTSYD") - S("WRESBAL"))/1e3
        z_netliq30 = _zscore(netliq - netliq.shift(30), W)
    else:
        z_netliq30 = None

    z_ig_oas = _zscore(S("IG_OAS"), W) if S("IG_OAS") is not None else None
    z_hy_oas = _zscore(S("HY_OAS"), W) if S("HY_OAS") is not None else None

    # --- Scores ---
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

    # --- Regime-Logik (wie gehabt, mit Guards) ---
    def is_red(v): return v is not None and v >= 70
    gate_hits = sum(is_red(scores.get(k)) for k in ["cr","vix","vxterm","ust10v","relfin","10s2s","10s3m"])
    fs_score  = 2 if (is_red(scores.get("vix")) and is_red(scores.get("cr"))) else (1 if is_red(scores.get("vix")) else 0)
    flow_sum, rebalance_on = 0, False

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
    size = "klein" if (fs_score >= 2 or (scores.get("netliq") or 0) > 60 or flow_sum >= 2) else ("moderat" if flow_sum > -2 else "moderat+")
    dur  = "↓" if ((scores.get("dgs30") or 0) > 60 or (scores.get("ust10v") or 0) > 60) else ("↑" if ((scores.get("dgs30") or 50) < 40 and (scores.get("ust10v") or 50) < 40) else "≙")
    one_liner = f"Bias: {bias} | Größe: {size} | Dur {dur}"

    risks = []
    if (scores.get("netliq") or 0) > 60: risks.append("Liquidität: knapp → Drawdowns können verstärkt werden.")
    if (scores.get("vix") or 0)   > 60: risks.append("Volatilität erhöht → Risiko für High-Beta.")
    if (scores.get("cr") or 0)    > 60: risks.append("Credit Spreads weit → HY/ZYK anfällig.")
    if (scores.get("dxy") or 0)   > 60: risks.append("USD stark → Gegenwind für EM/Gold.")

    snap = {
        "asof": datetime.now(timezone.utc).isoformat(),
        "composite": sc_comp,
        "regime": regime,
        "fs_score": float(fs_score),
        "flow_sum": int(flow_sum),
        "rebalance_on": rebalance_on,
        "scores": scores,
        "one_liner": one_liner,
        "risks": risks,
        "available_columns": sorted(list(cols_available)),
        "notes": [
            "Snapshot nutzt alle verfügbaren Reihen; fehlende Inputs werden ignoriert."
        ],
        "action_hints": [
            "Umschichten → Staples/Health/SPLV; ggf. TLT/GLD" if regime in ("CAUTION","RISK-OFF")
            else "Aufstocken → QQQ/IWM/XLF/SPHB; Defensives/Duration reduzieren." if regime == "RISK-ON"
            else "Neutral: Qual/Def mischen, Größe moderat"
        ],
    }

    OUTDIR.mkdir(parents=True, exist_ok=True)
    (OUTDIR / "riskindex_snapshot.json").write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print("✔ wrote data/processed/riskindex_snapshot.json")

    # --- Timeseries (Composite auf Basis der verfügbaren Z-Reihen) ---
    zlist = [z for z in (z_dgs30, z_2s30s, z_sofr30, z_stlfsi, z_vix, z_usdvol, z_dxy, z_cr30,
                         z_vxterm, z_10s2, z_10s3m, z_relfin30, z_ust10v, z_netliq30) if z is not None]
    if zlist:
        # Gemeinsame Index-Union und täglich füllen
        idx = df.index
        rows = []
        for dt in idx:
            vals = []
            for z in zlist:
                try:
                    v = z.loc[dt]
                    if pd.notna(v):
                        vals.append(50 + 10 * v)
                except KeyError:
                    pass
            if len(vals) >= max(6, len(zlist)//3):  # mind. 6 Reihen oder 1/3 der verfügbaren
                rows.append({"date": dt.date().isoformat(), "sc_comp": float(sum(vals)/len(vals))})
        if rows:
            pd.DataFrame(rows).to_csv(OUTDIR / "riskindex_timeseries.csv", index=False)
            print("✔ wrote data/processed/riskindex_timeseries.csv rows:", len(rows))
        else:
            print("timeseries skipped (insufficient overlap)")
    else:
        print("timeseries skipped (no z-series)")

    return 0

if __name__ == "__main__":
    sys.exit(main())
