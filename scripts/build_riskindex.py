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

from __future__ import annotations
import os, sys, json, math
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd
import numpy as np
from datetime import datetime, timezone

from util import write_json

# --- Inputs ---
PROC = Path("data/processed")
FRED_CORE = PROC / "fred_core.csv.gz"
FRED_OAS  = PROC / "fred_oas.csv.gz"
MKT_CORE  = PROC / "market_core.csv.gz"

# --- Parameters ---
ZLEN = int(os.getenv("RISK_ZLEN", "200"))
RRP_LEN = int(os.getenv("RISK_RRP_LEN", "200"))
MIN_BARS = max(220, ZLEN + 20)

def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    df = pd.read_csv(path)
    # tolerate 'date' or 'Date'
    dcol = "date" if "date" in df.columns else "Date" if "Date" in df.columns else None
    if dcol is None:
        return pd.DataFrame()
    df[dcol] = pd.to_datetime(df[dcol]).dt.date
    df = df.set_index(dcol).sort_index()
    return df

def _norm_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    full_idx = pd.date_range(df.index.min(), df.index.max(), freq="D").date
    out = df.reindex(full_idx).ffill()
    out.index.name = "date"
    return out

def zscore(s: pd.Series, win: int) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mu = s.rolling(win, min_periods=win//2).mean()
    sd = s.rolling(win, min_periods=win//2).std()
    z = (s - mu) / sd
    return z.clip(lower=-3, upper=3)

def norm_cdf(z: pd.Series) -> pd.Series:
    # Φ(z)
    return 0.5 * (1.0 + (z / np.sqrt(2.0)).apply(math.erf))

def score_from_z(z: pd.Series, invert: bool = False) -> pd.Series:
    """Map Z -> Score 0..100 via normal CDF. invert=True kehrt das Vorzeichen um."""
    p = norm_cdf(z)
    sc = 100.0 * (1.0 - p) if invert else 100.0 * p
    return sc.clip(lower=0, upper=100)

def percent_rank(s: pd.Series, win: int) -> pd.Series:
    """Percentrank [0..1] über rollendes Fenster."""
    def _pr(x):
        if len(x) <= 1:
            return np.nan
        r = pd.Series(x).rank(pct=True).iloc[-1]
        return float(r)
    return s.rolling(win, min_periods=win//2).apply(_pr, raw=False)

def main() -> int:
    fred = _read_csv(FRED_CORE)
    oas  = _read_csv(FRED_OAS)
    mkt  = _read_csv(MKT_CORE)

    if fred.empty or mkt.empty:
        print("❌ fehlende Inputs: fred_core or market_core")
        return 1

    fred = _norm_daily(fred)
    if not oas.empty:
        oas = _norm_daily(oas)
    mkt = _norm_daily(mkt)

    # join
    df = fred.join(oas, how="left").join(mkt, how="left")
    df = df.ffill().dropna(how="all")
    if len(df) < MIN_BARS:
        print("❌ zu wenig Historie:", len(df), "<", MIN_BARS)
        return 1

    # ----- Derived series -----
    # Curves
    df["UST_30Y"] = df.get("DGS30")
    df["S10S2"]   = df.get("DGS10") - df.get("DGS2")
    df["S10S3M"]  = df.get("DGS10") - df.get("DGS3MO")
    df["S2S30S"]  = df.get("DGS2")  - df.get("DGS30")

    # SOFR Δ30d (in bp)
    df["SOFR_D30_BP"] = (df.get("SOFR") - df.get("SOFR").shift(30)) * 100.0

    # RRP percent-rank
    df["RRP_PRC"] = percent_rank(df.get("RRPONTSYD"), RRP_LEN)

    # STLFSI
    df["STLFSI"] = df.get("STLFSI4")

    # VIX / VIX3M / Term
    df["VIX"]    = df.get("VIX")
    df["VIX3M"]  = df.get("VIX3M")
    df["VX_TERM"] = df["VIX"] - df["VIX3M"]

    # DXY (fallback auf UUP, falls DXY fehlt)
    if "DXY" in df.columns and df["DXY"].notna().sum() > 20:
        df["DXY_USE"] = df["DXY"]
    elif "UUP" in df.columns:
        df["DXY_USE"] = df["UUP"]
    else:
        df["DXY_USE"] = np.nan

    # Credit: HYG/LQD Δ30d (in %-Punkten)
    if "HYG" in df.columns and "LQD" in df.columns:
        rel = df["HYG"] / df["LQD"]
        df["HYG_LQD_D30_PCT"] = (rel - rel.shift(30)) * 100.0
    else:
        df["HYG_LQD_D30_PCT"] = np.nan

    # XLF / SPY Δ30d (in %-Punkten)
    if "XLF" in df.columns and "SPY" in df.columns:
        rel2 = df["XLF"] / df["SPY"]
        df["XLF_SPY_D30_PCT"] = (rel2 - rel2.shift(30)) * 100.0
    else:
        df["XLF_SPY_D30_PCT"] = np.nan

    # UST10y Vol (stdev of daily changes, annualized)
    if "DGS10" in df.columns:
        ch = df["DGS10"].diff()
        df["UST10Y_VOL"] = ch.rolling(20, min_periods=15).std() * np.sqrt(252.0)
    else:
        df["UST10Y_VOL"] = np.nan

    # USDJPY vol (log returns, annualized)
    if "USDJPY" in df.columns:
        rj = np.log(df["USDJPY"]).diff()
        df["USDJPY_VOL"] = rj.rolling(20, min_periods=15).std() * np.sqrt(252.0) * 100.0
    else:
        df["USDJPY_VOL"] = np.nan

    # Net Liquidity Δ30d (WRESBAL − TGA − RRP), skaliert (Mrd)
    wres = df.get("WRESBAL")
    tga  = df.get("WTREGEN")
    rrp  = df.get("RRPONTSYD")
    if wres is not None and tga is not None and rrp is not None:
        nl = (wres - tga - rrp)
        df["NETLIQ_D30"] = (nl - nl.shift(30)) / 1e3
    else:
        df["NETLIQ_D30"] = np.nan

    # OAS passt direkt (IG_OAS/HY_OAS)
    # ---- Z / Scores ----
    # Z-Werte
    df["Z_DGS30"]  = zscore(df["UST_30Y"], ZLEN)
    df["Z_2S30S"]  = zscore(df["S2S30S"], ZLEN)
    df["Z_SOFR30"] = zscore(df["SOFR_D30_BP"], ZLEN)
    df["P_RRP"]    = percent_rank(df["RRP_PRC"], RRP_LEN)  # schon 0..1, aber rank of rank als Stabilisierung
    df["Z_STLFSI"] = zscore(df["STLFSI"], ZLEN)
    df["Z_VIX"]    = zscore(df["VIX"], ZLEN)
    df["Z_USDVOL"] = zscore(df["USDJPY_VOL"], ZLEN)
    df["Z_DXY"]    = zscore(df["DXY_USE"], ZLEN)
    df["Z_CR30"]   = zscore(df["HYG_LQD_D30_PCT"], ZLEN)
    df["Z_VXTERM"] = zscore(df["VX_TERM"], ZLEN)
    df["Z_10S2"]   = zscore(df["S10S2"], ZLEN)
    df["Z_10S3M"]  = zscore(df["S10S3M"], ZLEN)
    df["Z_RELFIN"] = zscore(df["XLF_SPY_D30_PCT"], ZLEN)
    df["Z_UST10V"] = zscore(df["UST10Y_VOL"], ZLEN)
    df["Z_NETLIQ"] = zscore(df["NETLIQ_D30"], ZLEN)

    # optionale OAS
    if "IG_OAS" in df.columns:
        df["Z_IG_OAS"] = zscore(df["IG_OAS"], ZLEN)
    if "HY_OAS" in df.columns:
        df["Z_HY_OAS"] = zscore(df["HY_OAS"], ZLEN)

    # Scores (invert = True, falls hohe Z "Risk-Off" bedeutet → Score hoch = rot)
    df["SC_DGS30"]  = score_from_z(df["Z_DGS30"],  False)
    df["SC_2S30S"]  = score_from_z(df["Z_2S30S"],  False)
    df["SC_SOFR"]   = score_from_z(df["Z_SOFR30"], False)
    df["SC_RRP"]    = (1.0 - df["P_RRP"]).clip(0,1) * 100.0  # hoher RRP = rot
    df["SC_STLFSI"] = score_from_z(df["Z_STLFSI"], False)
    df["SC_VIX"]    = score_from_z(df["Z_VIX"],    False)
    df["SC_USDVOL"] = score_from_z(df["Z_USDVOL"], False)
    df["SC_DXY"]    = score_from_z(df["Z_DXY"],    False)
    df["SC_CR"]     = score_from_z(df["Z_CR30"],   False)
    df["SC_VXTERM"] = score_from_z(df["Z_VXTERM"], False)
    df["SC_10S2"]   = score_from_z(df["Z_10S2"],   True)   # invert
    df["SC_10S3M"]  = score_from_z(df["Z_10S3M"],  True)   # invert
    df["SC_RELFIN"] = score_from_z(df["Z_RELFIN"], True)   # invert (RelFin↑ = pro-risk → niedriger rot-Score)
    df["SC_UST10V"] = score_from_z(df["Z_UST10V"], False)
    df["SC_NETLIQ"] = score_from_z(df["Z_NETLIQ"], True)   # mehr NetLiq Δ = gut → invert

    # optionale OAS Scores
    if "Z_IG_OAS" in df.columns:
        df["SC_IG"] = score_from_z(df["Z_IG_OAS"], False)
    if "Z_HY_OAS" in df.columns:
        df["SC_HY"] = score_from_z(df["Z_HY_OAS"], False)

    CORE_LIST = [
        "SC_DGS30","SC_2S30S","SC_SOFR","SC_RRP","SC_STLFSI","SC_VIX","SC_USDVOL",
        "SC_DXY","SC_CR","SC_VXTERM","SC_10S2","SC_10S3M","SC_RELFIN","SC_UST10V","SC_NETLIQ"
    ]
    df["SC_COMP"] = df[CORE_LIST].mean(axis=1)

    # Gate-Hits (defensive Trigger) – an Pine angelehnt
    def is_red(s): return s >= 70.0
    df["GATE_HITS"] = (
        is_red(df["SC_CR"]).astype(int) +
        is_red(df["SC_VIX"]).astype(int) +
        is_red(df["SC_VXTERM"]).astype(int) +
        is_red(df["SC_UST10V"]).astype(int) +
        is_red(df["SC_RELFIN"]).astype(int) +
        is_red(df["SC_10S2"]).astype(int) +
        is_red(df["SC_10S3M"]).astype(int)
    )

    # FS-Score (0..3) – konservativer Proxy aus Kernkomponenten
    df["FS_SCORE"] = (
        (df["SC_STLFSI"] >= 60).astype(int) +
        (df["SC_VIX"]    >= 60).astype(int) +
        (df["SC_NETLIQ"] >= 60).astype(int)
    )

    # TIP-Schwelle wie im Pine (vereinfacht)
    rg_tip = 70.0 \
             - np.where(df["FS_SCORE"] >= 2, 10.0, 0.0) \
             - np.where(df["GATE_HITS"] >= 3, 3.0, 0.0) \
             - np.where(df["GATE_HITS"] >= 5, 3.0, 0.0)
    rg_tip = np.clip(rg_tip, 50.0, 90.0)
    df["RG_D"] = df["SC_COMP"] - rg_tip

    # Regime-State
    def state_row(row) -> str:
        sc = row["SC_COMP"]; d = row["RG_D"]
        gates = row["GATE_HITS"]; fs = row["FS_SCORE"]
        if d >= 0 and (gates >= 4 or fs >= 2):
            return "RISK-OFF"
        if d >= 0:
            return "CAUTION"
        if (gates <= 2 and fs <= 1 and d <= -10):
            return "RISK-ON"
        return "NEUTRAL"
    df["RG_STATE"] = df.apply(state_row, axis=1)

    # One-Liner (vereinfachtes Pine-Äquivalent)
    def one_liner(row) -> str:
        sc_comp   = row["SC_COMP"]
        sc_netliq = row["SC_NETLIQ"]
        sc_dgs30  = row["SC_DGS30"]
        sc_ust10v = row["SC_UST10V"]
        sc_dxy    = row["SC_DXY"]
        sc_vix    = row["SC_VIX"]
        sc_cr     = row["SC_CR"]
        fs_score  = row["FS_SCORE"]
        flow_sum  = 0  # placeholder
        rebalance = False

        tG, tR = 40.0, 60.0
        bias = "RISK-ON" if sc_comp < 45 else "RISK-OFF" if sc_comp > 55 else "NEUTRAL"
        size = "klein" if (fs_score >= 2 or sc_netliq > tR or flow_sum >= 2) else ("moderat" if flow_sum > -2 else "moderat")
        dur  = "↓" if (sc_dgs30 > tR or sc_ust10v > tR) else ("↑" if (sc_dgs30 < tG and sc_ust10v < tG) else "≙")
        carry_hits = (sc_vix > tR) + (sc_dxy > tR) + (sc_cr > tR)
        kHits = (sc_netliq > tR) + (sc_vix > tR) + (sc_cr > tR) + (sc_dxy > tR) + (fs_score >= 2) + (flow_sum >= 2)
        warn = "  ⚠" if (carry_hits >= 2 or kHits >= 3) else ""
        return f"Bias: {bias} | Größe: {size} | Dur {dur}{warn}"

    df["ONE_LINER"] = df.apply(one_liner, axis=1)

    # Long-Hint (kompakte Version ohne Forecast)
    def long_hint(row) -> str:
        tG, tR = 40.0, 60.0
        sc = row
        bias = "RISK-ON" if sc["SC_COMP"] < 45 else "RISK-OFF" if sc["SC_COMP"] > 55 else "NEUTRAL"
        usd = "USD ↓: GLD/EEM +" if sc["SC_DXY"] < tG else "USD ↑: GLD −" if sc["SC_DXY"] > tR else "USD neutral"
        dur = "↑" if (sc["SC_DGS30"] < tG and sc["SC_UST10V"] < tG and sc["FS_SCORE"] < 2) else ("↓" if (sc["SC_DGS30"] > tR or sc["SC_UST10V"] > tR) else "neutral")
        cTxt = "Carry: Stress ↑ (Vol/FX/Spreads)" if ((sc["SC_VIX"] > tR) + (sc["SC_DXY"] > tR) + (sc["SC_CR"] > tR) >= 2) else ("Carry: Watch" if ((sc["SC_VIX"] > tR) + (sc["SC_DXY"] > tR) + (sc["SC_CR"] > tR) == 1) else "Carry: neutral")
        tltTxt = "TLT ↗ ok" if (sc["SC_DGS30"] < tG and sc["SC_UST10V"] < tG and sc["FS_SCORE"] < 2) else ("TLT ↘ vorsichtig" if (sc["SC_DGS30"] > tR or sc["SC_UST10V"] > tR) else "TLT neutral")
        kHits = int((sc["SC_NETLIQ"] > tR) + (sc["SC_VIX"] > tR) + (sc["SC_CR"] > tR) + (sc["SC_DXY"] > tR) + (sc["FS_SCORE"] >= 2))
        kList = " ".join([lab for lab,cond in [
            ("Liq", sc["SC_NETLIQ"] > tR), ("Vol", sc["SC_VIX"] > tR), ("Cred", sc["SC_CR"] > tR), ("USD", sc["SC_DXY"] > tR), ("FS", sc["FS_SCORE"] >= 2)
        ] if cond])
        kFlag = f"Kipp: {kList or '–'}"

        s  = "Kernaussagen:\n"
        s += f"• Zinsen/Duration: {'restriktiv (' if sc['SC_DGS30']>tR else 'locker (' if sc['SC_DGS30']<tG else 'neutral ('}{sc['SC_DGS30']:.0f}). "
        s += f"{'Dur ↑ ok.' if dur=='↑' else 'Dur vorsichtig.' if dur=='↓' else 'Dur neutral.'}\n"
        s += f"• Liquidität: Net Liquidity {'unterstützend (' if sc['SC_NETLIQ']<tG else 'bremsend (' if sc['SC_NETLIQ']>tR else 'neutral ('}{sc['SC_NETLIQ']:.0f}).\n"
        s += f"• Volatilität/Spreads: VIX {'niedrig (' if sc['SC_VIX']<tG else 'hoch (' if sc['SC_VIX']>tR else 'neutral ('}{sc['SC_VIX']:.0f}), Credit {'eng' if sc['SC_CR']<tG else 'weit' if sc['SC_CR']>tR else 'neutral'}.\n"
        s += f"• USD/FX: {usd}\n"
        s += f"• Composite: {sc['SC_COMP']:.0f} → {bias} (kein Extrem).\n"
        s += f"• {cTxt}\n"
        s += f"• {tltTxt}\n"
        s += f"• {kFlag} ({kHits} Trigger)\n"
        return s

    df["LONG_HINT"] = df.apply(long_hint, axis=1)

    # --- Outputs ---
    times = df[["SC_COMP","RG_STATE"]].reset_index().rename(columns={"index":"date"})
    times.to_csv(PROC / "riskindex_timeseries.csv", index=False)
    print("✔ wrote", PROC / "riskindex_timeseries.csv", "rows:", len(times))

    # snapshot = letzter Tag mit Daten
    last = df.iloc[-1]
    snapshot = {
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "date": str(df.index[-1]),
        "sc_comp": float(round(last["SC_COMP"], 1)),
        "rg_state": str(last["RG_STATE"]),
        "fs_score": int(last["FS_SCORE"]),
        "gate_hits": int(last["GATE_HITS"]),
        "one_liner": str(last["ONE_LINER"]),
        "long_hint": str(last["LONG_HINT"]),
        "scores": {
            "UST_30y": float(round(last["SC_DGS30"], 1)),
            "2s30s":   float(round(last["SC_2S30S"], 1)),
            "SOFR_D30":float(round(last["SC_SOFR"], 1)),
            "RRP_pct": float(round(last["SC_RRP"], 1)),
            "STLFSI":  float(round(last["SC_STLFSI"], 1)),
            "VIX":     float(round(last["SC_VIX"], 1)),
            "USDJPY_vol": float(round(last["SC_USDVOL"], 1)),
            "DXY":     float(round(last["SC_DXY"], 1)),
            "HYG/LQD_D30": float(round(last["SC_CR"], 1)),
            "VIX-VIX3M":   float(round(last["SC_VXTERM"], 1)),
            "10s2s":   float(round(last["SC_10S2"], 1)),
            "10s3m":   float(round(last["SC_10S3M"], 1)),
            "XLF/SPY_D30": float(round(last["SC_RELFIN"], 1)),
            "UST10y_Vol": float(round(last["SC_UST10V"], 1)),
            "NetLiq_D30": float(round(last["SC_NETLIQ"], 1)),
            **({"IG_OAS": float(round(last["SC_IG"], 1))} if "SC_IG" in df.columns else {}),
            **({"HY_OAS": float(round(last["SC_HY"], 1))} if "SC_HY" in df.columns else {}),
        }
    }
    write_json(PROC / "riskindex_snapshot.json", snapshot)
    print("✔ wrote", PROC / "riskindex_snapshot.json")

    return 0

if __name__ == "__main__":
    sys.exit(main())
