#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex.py
Berechnet RiskIndex (Heatmap-Kerne, Scores, Composite, Regime, One-Liner, Long-Hint – ohne Forecast)
aus:
  - data/processed/fred_core.csv.gz    (DGS30,DGS10,DGS2,DGS3MO,SOFR,RRPONTSYD,STLFSI4,WRESBAL,WTREGEN)
  - data/processed/fred_oas.csv.gz     (IG_OAS, HY_OAS)   [optional]
  - data/processed/market_core.csv.gz  (VIX,VIX3M,DXY/UUP,HYG,LQD,XLF,SPY,USDJPY)
Schreibt:
  - data/processed/riskindex_timeseries.csv
  - data/processed/riskindex_snapshot.json
"""

# scripts/build_riskindex.py
import os, json, math
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path

FRED = "data/processed/fred_core.csv"
MKT  = "data/processed/market_core.csv"

def zscore(s, win=252):
    s = pd.to_numeric(s, errors="coerce")
    r = (s - s.rolling(win).mean()) / s.rolling(win).std(ddof=0)
    return r

def score_from_z(z, invert=False):
    if z is None: return None
    s = 50 + 10 * (-z if invert else z)  # linear/kompakt
    return float(max(0, min(100, s)))

def pct_rank(s, win=200):
    return s.rank(pct=True, method="average")

def main():
    if not Path(FRED).exists() or not Path(MKT).exists():
        print("fehlende Inputs: fred_core or market_core")
        return 1

    dfF = pd.read_csv(FRED, parse_dates=["date"]).set_index("date")
    dfM = pd.read_csv(MKT,  parse_dates=["date"]).set_index("date")

    # Join auf täglicher Frequenz, ffill
    df = dfF.join(dfM, how="outer").sort_index().ffill()

    # ab hier werden Scores nach deinem Pine-Modell angenähert
    # Z-Window:
    W = 252
    out = {}

    def s(name): return df[name] if name in df.columns else None

    z_dgs30   = zscore(s("DGS30"), W)
    z_2s30s   = zscore(s("DGS30") - s("DGS2"), W) if s("DGS30") is not None and s("DGS2") is not None else None
    z_sofr30  = zscore((s("SOFR") - s("SOFR").shift(30))*100,  W) if s("SOFR") is not None else None
    rrp_pct   = pct_rank(s("RRPONTSYD").fillna(method="ffill"), 200) if s("RRPONTSYD") is not None else None
    z_stlfsi  = zscore(s("STLFSI4"), W) if s("STLFSI4") is not None else None

    z_vix     = zscore(s("VIX"), W) if s("VIX") is not None else None
    # USDJPY 20d RealVol ~ annualisiert
    if s("USDJPY") is not None:
        usdvol = (s("USDJPY").pct_change().rolling(20).std() * math.sqrt(252) * 100)
        z_usdvol = zscore(usdvol, W)
    else:
        z_usdvol = None
    z_dxy     = zscore(s("DXY"), W) if s("DXY") is not None else None

    if s("HYG") is not None and s("LQD") is not None:
        rel_hyglqd = (s("HYG")/s("LQD"))
        z_cr30 = zscore((rel_hyglqd - rel_hyglqd.shift(30))*100, W)
    else:
        z_cr30 = None

    z_vxterm  = zscore(s("VIX") - s("VIX3M"), W) if s("VIX") is not None and s("VIX3M") is not None else None
    z_10s2    = zscore(s("DGS10") - s("DGS2"),   W) if s("DGS10") is not None and s("DGS2") is not None else None
    z_10s3m   = zscore(s("DGS10") - s("DGS3MO"), W) if s("DGS10") is not None and s("DGS3MO") is not None else None

    if s("XLF") is not None and s("SPY") is not None:
        rel_xlfspy = (s("XLF")/s("SPY"))
        z_relfin30 = zscore((rel_xlfspy - rel_xlfspy.shift(30))*100, W)
    else:
        z_relfin30 = None

    if s("DGS10") is not None:
        ust10v = s("DGS10").diff().rolling(20).std() * math.sqrt(252)
        z_ust10v = zscore(ust10v, W)
    else:
        z_ust10v = None

    # Net Liquidity Proxy: (WALCL - TGA - RRP - WRESBAL) / 1e3  (wenn verfügbar)
    if all(s(x) is not None for x in ["WALCL","WTREGEN","RRPONTSYD","WRESBAL"]):
        netliq = (s("WALCL") - s("WTREGEN") - s("RRPONTSYD") - s("WRESBAL"))/1e3
        z_netliq30 = zscore(netliq - netliq.shift(30), W)
    else:
        z_netliq30 = None

    scores = {
        "dgs30":   score_from_z(z_dgs30.iloc[-1])           if z_dgs30 is not None   else None,
        "2s30s":   score_from_z(z_2s30s.iloc[-1])           if z_2s30s is not None   else None,
        "sofr_d30":score_from_z(z_sofr30.iloc[-1])          if z_sofr30 is not None  else None,
        "rrp_pct": (1.0 - float(rrp_pct.iloc[-1]))*100.0    if rrp_pct is not None   else None,
        "stlfsi":  score_from_z(z_stlfsi.iloc[-1])          if z_stlfsi is not None  else None,
        "vix":     score_from_z(z_vix.iloc[-1])             if z_vix is not None     else None,
        "usdvol":  score_from_z(z_usdvol.iloc[-1])          if z_usdvol is not None  else None,
        "dxy":     score_from_z(z_dxy.iloc[-1])             if z_dxy is not None     else None,
        "cr":      score_from_z(z_cr30.iloc[-1])            if z_cr30 is not None    else None,
        "vxterm":  score_from_z(z_vxterm.iloc[-1])          if z_vxterm is not None  else None,
        "10s2s":   score_from_z(z_10s2.iloc[-1],  invert=True)  if z_10s2 is not None  else None,
        "10s3m":   score_from_z(z_10s3m.iloc[-1], invert=True)  if z_10s3m is not None else None,
        "relfin":  score_from_z(z_relfin30.iloc[-1], invert=True)if z_relfin30 is not None else None,
        "ust10v":  score_from_z(z_ust10v.iloc[-1])          if z_ust10v is not None  else None,
        "netliq":  score_from_z(z_netliq30.iloc[-1], invert=True) if z_netliq30 is not None else None,
    }

    # Composite über alle verfügbaren Scores
    sc_vals = [v for v in scores.values() if v is not None]
    sc_comp = float(sum(sc_vals)/len(sc_vals)) if sc_vals else None

    # Simple Regime-Logik (wie Pine-Gates light)
    def is_red(x): return x is not None and x >= 70
    gate_hits = sum(is_red(scores.get(k)) for k in ["cr","vix","vxterm","ust10v","relfin","10s2s","10s3m"])
    fs_score  = 2 if (is_red(scores.get("vix")) and is_red(scores.get("cr"))) else 1 if is_red(scores.get("vix")) else 0
    flow_sum  = 0  # Platzhalter (kannst du später aus Options-/Tape-Modulen speisen)
    rebalance_on = False

    rg_tip = 70.0 - (10.0 if fs_score >= 2 else 0.0) - (3.0 if gate_hits >=3 else 0.0) - (3.0 if gate_hits >=5 else 0.0)
    rg_tip = max(50.0, min(90.0, rg_tip))
    rg_d   = (sc_comp - rg_tip) if sc_comp is not None else 0.0

    if sc_comp is None:
        regime = "NEUTRAL"
    else:
        regime = "RISK-OFF" if (rg_d >= 0 and (gate_hits >=4 or fs_score >=2)) else \
                 "CAUTION"  if (rg_d >= 0) else \
                 "RISK-ON"  if (gate_hits <=2 and fs_score <=1 and rg_d <= -10) else "NEUTRAL"

    # One-Liner & Risiken (kompakt)
    def pct(v): return "N/A" if v is None else f"{v:.0f}"
    bias = "RISK-ON" if (sc_comp is not None and sc_comp < 45) else ("RISK-OFF" if (sc_comp is not None and sc_comp > 55) else "NEUTRAL")
    size = "klein" if (fs_score >=2 or (scores.get("netliq") or 0)>60 or flow_sum >=2) else ("moderat" if flow_sum>-2 else "moderat+")
    dur  = "↓" if ((scores.get("dgs30") or 0) > 60 or (scores.get("ust10v") or 0) > 60) else ("↑" if ((scores.get("dgs30") or 50) < 40 and (scores.get("ust10v") or 50) < 40) else "≙")
    one_liner = f"Bias: {bias} | Größe: {size} | Dur {dur}"

    risks=[]
    if (scores.get("netliq") or 0)>60: risks.append("Liquidität: knapp → Drawdowns können verstärkt werden.")
    if (scores.get("vix") or 0)>60:    risks.append("Volatilität erhöht → Risiko für High-Beta.")
    if (scores.get("cr") or 0)>60:     risks.append("Credit Spreads weit → HY/ZYK anfällig.")
    if (scores.get("dxy") or 0)>60:    risks.append("USD stark → Gegenwind für EM/Gold.")

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
            "Umschichten → Staples/Health/SPLV; ggf. TLT/GLD" if regime in ("CAUTION","RISK-OFF") else
            "Aufstocken → QQQ/IWM/XLF/SPHB; Defensives/Duration reduzieren." if regime=="RISK-ON" else
            "Neutral: Qual/Def mischen, Größe moderat"
        ]
    }

    Path("data/processed").mkdir(parents=True, exist_ok=True)
    Path("data/processed/riskindex_snapshot.json").write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print("wrote data/processed/riskindex_snapshot.json")

    # Timeseries (optional, wenn genug Daten)
    # -> wir berechnen rückwirkend sc_comp pro Tag (nur wo genügend Inputs existieren)
    ts_rows=[]
    for dt, row in df.iterrows():
        vals=[]
        for z in [z_dgs30, z_2s30s, z_sofr30, z_stlfsi, z_vix, z_usdvol, z_dxy, z_cr30, z_vxterm, z_10s2, z_10s3m, z_relfin30, z_ust10v, z_netliq30]:
            if z is not None and dt in z.index and pd.notna(z.loc[dt]):
                vals.append(50+10*z.loc[dt])
        if len(vals)>=6:
            ts_rows.append({"date": dt.date().isoformat(), "sc_comp": float(sum(vals)/len(vals))})
    if ts_rows:
        pd.DataFrame(ts_rows).to_csv("data/processed/riskindex_timeseries.csv", index=False)
        print("wrote data/processed/riskindex_timeseries.csv rows=", len(ts_rows))
    else:
        print("timeseries skipped (insufficient overlap)")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())

