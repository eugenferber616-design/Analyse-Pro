#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex.py
Liest:
  - data/processed/fred_core.csv.gz        (dein vorhandenes Skript)
  - data/processed/market_core.csv.gz      (neu aus yfinance)
  - (optional) data/processed/fred_oas.csv.gz (IG/HY OAS als Extras)

Schreibt:
  - data/processed/riskindex_snapshot.json
  - data/processed/riskindex_timeseries.csv (optional, wenn genug Overlap)
"""

from __future__ import annotations
import os, math, json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

F_FRED   = Path("data/processed/fred_core.csv.gz")
F_OAS    = Path("data/processed/fred_oas.csv.gz")
F_MARKET = Path("data/processed/market_core.csv.gz")

def read_df(p: Path) -> pd.DataFrame:
    return pd.read_csv(p, parse_dates=["date"], compression="infer").set_index("date")

def zscore(s: pd.Series, win: int = 252) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    return (s - s.rolling(win).mean()) / s.rolling(win).std(ddof=0)

def score_from_z(z: float | None, invert: bool=False) -> float | None:
    if z is None or pd.isna(z): return None
    s = 50 + 10 * ((-z) if invert else z)
    return float(max(0, min(100, s)))

def last(zs: pd.Series | None) -> float | None:
    if zs is None: return None
    v = zs.iloc[-1]
    return None if pd.isna(v) else float(v)

def main() -> int:
    if not F_FRED.exists() or not F_MARKET.exists():
        print("fehlende Inputs: fred_core.csv.gz oder market_core.csv.gz")
        return 1

    dfF = read_df(F_FRED)
    dfM = read_df(F_MARKET)
    df  = dfF.join(dfM, how="outer").sort_index().ffill()

    # Optional OAS (für Extras)
    dfO = read_df(F_OAS) if F_OAS.exists() else None
    if dfO is not None and not dfO.empty:
        df = df.join(dfO, how="left").ffill()

    W = 252
    def s(col): return df[col] if col in df.columns else None

    # Z-Reihen
    z_dgs30  = zscore(s("DGS30"), W)                      if s("DGS30") is not None else None
    z_2s30s  = zscore(s("DGS30") - s("DGS2"), W)          if s("DGS30") is not None and s("DGS2") is not None else None
    z_sofr30 = zscore((s("SOFR") - s("SOFR").shift(30))*100, W) if s("SOFR") is not None else None
    rrp_pct  = s("RRPONTSYD").rank(pct=True)              if s("RRPONTSYD") is not None else None
    z_stlfsi = zscore(s("STLFSI4"), W)                    if s("STLFSI4") is not None else None

    z_vix    = zscore(s("VIX"), W)                        if s("VIX") is not None else None
    z_vxterm = zscore(s("VIX") - s("VIX3M"), W)           if s("VIX") is not None and s("VIX3M") is not None else None
    z_dxy    = zscore(s("DXY"), W)                        if s("DXY") is not None else None

    if s("USDJPY") is not None:
        usdvol = s("USDJPY").pct_change().rolling(20).std() * math.sqrt(252) * 100
        z_usdvol = zscore(usdvol, W)
    else:
        z_usdvol = None

    if s("HYG") is not None and s("LQD") is not None:
        rel = (s("HYG")/s("LQD"))
        z_cr30 = zscore((rel - rel.shift(30))*100, W)
    else:
        z_cr30 = None

    z_10s2   = zscore(s("DGS10") - s("DGS2"),   W)        if s("DGS10") is not None and s("DGS2") is not None else None
    z_10s3m  = zscore(s("DGS10") - s("DGS3MO"), W)        if s("DGS10") is not None and s("DGS3MO") is not None else None

    if s("XLF") is not None and s("SPY") is not None:
        relfs = (s("XLF")/s("SPY"))
        z_relfin30 = zscore((relfs - relfs.shift(30))*100, W)
    else:
        z_relfin30 = None

    if s("DGS10") is not None:
        ust10v = s("DGS10").diff().rolling(20).std() * math.sqrt(252)
        z_ust10v = zscore(ust10v, W)
    else:
        z_ust10v = None

    if all(s(k) is not None for k in ["WALCL","WTREGEN","RRPONTSYD","WRESBAL"]):
        netliq = (s("WALCL") - s("WTREGEN") - s("RRPONTSYD") - s("WRESBAL"))/1e3
        z_netliq30 = zscore(netliq - netliq.shift(30), W)
    else:
        z_netliq30 = None

    # (optional) OAS
    z_ig_oas = zscore(s("IG_OAS"), W) if s("IG_OAS") is not None else None
    z_hy_oas = zscore(s("HY_OAS"), W) if s("HY_OAS") is not None else None

    scores = {
        "dgs30"  : score_from_z(last(z_dgs30)),
        "2s30s"  : score_from_z(last(z_2s30s)),
        "sofr"   : score_from_z(last(z_sofr30)),
        "rrp"    : (1.0 - float(last(rrp_pct))) * 100.0 if rrp_pct is not None else None,
        "stlfsi" : score_from_z(last(z_stlfsi)),
        "vix"    : score_from_z(last(z_vix)),
        "usdvol" : score_from_z(last(z_usdvol)),
        "dxy"    : score_from_z(last(z_dxy)),
        "cr"     : score_from_z(last(z_cr30)),
        "vxterm" : score_from_z(last(z_vxterm)),
        "10s2s"  : score_from_z(last(z_10s2),  invert=True),
        "10s3m"  : score_from_z(last(z_10s3m), invert=True),
        "relfin" : score_from_z(last(z_relfin30), invert=True),
        "ust10v" : score_from_z(last(z_ust10v)),
        "netliq" : score_from_z(last(z_netliq30), invert=True),
        "ig_oas" : score_from_z(last(z_ig_oas)) if z_ig_oas is not None else None,
        "hy_oas" : score_from_z(last(z_hy_oas)) if z_hy_oas is not None else None,
    }

    sc_vals = [v for v in scores.values() if v is not None]
    sc_comp = float(sum(sc_vals)/len(sc_vals)) if sc_vals else None

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
        "action_hints": [
            "Umschichten → Staples/Health/SPLV; ggf. TLT/GLD" if regime in ("CAUTION","RISK-OFF")
            else "Aufstocken → QQQ/IWM/XLF/SPHB; Defensives/Duration reduzieren." if regime == "RISK-ON"
            else "Neutral: Qual/Def mischen, Größe moderat"
        ],
    }

    outdir = Path("data/processed")
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "riskindex_snapshot.json").write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print("✔ wrote data/processed/riskindex_snapshot.json")

    # Timeseries (composite only, wenn genug Overlap)
    ts = []
    series_list = [z for z in [z_dgs30, z_2s30s, z_sofr30, z_stlfsi, z_vix, z_usdvol, z_dxy, z_cr30, z_vxterm, z_10s2, z_10s3m, z_relfin30, z_ust10v, z_netliq30] if z is not None]
    if series_list:
        idx = df.index
        for dt in idx:
            vals = []
            for z in series_list:
                if dt in z.index:
                    v = z.loc[dt]
                    if pd.notna(v):
                        vals.append(50 + 10 * v)
            if len(vals) >= 6:
                ts.append({"date": dt.date().isoformat(), "sc_comp": float(sum(vals)/len(vals))})
        if ts:
            pd.DataFrame(ts).to_csv(outdir / "riskindex_timeseries.csv", index=False)
            print("✔ wrote data/processed/riskindex_timeseries.csv rows:", len(ts))
        else:
            print("timeseries skipped (insufficient overlap)")
    else:
        print("timeseries skipped (no z-series)")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
