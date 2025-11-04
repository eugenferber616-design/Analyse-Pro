#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build RiskIndex (Heatmap + Composite + Regime) from local pipeline artifacts.

Inputs (best-effort, all optional if missing):
- Prices (EoD):   data/processed/prices.parquet  or data/prices/*.csv
                  expects columns: symbol,date,close   (parquet)  OR per-symbol CSV with date,close
- FRED/OAS:       data/processed/fred_oas.csv           (if available; IG/HY OAS series)
- Net Liquidity:  FRED WRESBAL (Reserves), RRPONTSYD (RRP daily), optional TGA (if present)
                  We derive NetLiq Δ30 = Δ(WRES) - Δ(TGA) - Δ(RRP)
- Optional JP10Y: if present in prices (symbol JP10Y proxy) or from TVC pulled elsewhere

Outputs:
- data/processed/riskindex_snapshot.json
- data/processed/riskindex_timeseries.csv
- data/reports/riskindex_report.json

Author: AnalysePro (RiskIndex core for GitHub pipeline)
"""

import os, sys, json, math, glob
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------- Config ----------------------------

# Lookbacks
ZLEN_DEFAULT   = 180     # for z-scores
DELTA_WIN_DAYS = 30      # for Δ30d transforms
CURVE_ZLEN     = 252

# Minimum rows to compute z-scores
MIN_ZROWS      = 60

# Composite members (15 core like in your Heatmap)
CORE_BLOCKS = [
    "DGS30", "SPREAD_2s30s", "SOFR_D30", "RRP_PCTL", "STLFSI",
    "VIX", "USDJPY_VOL", "DXY", "HYG_LQD_D30", "VIX_TERM",
    "10s2s", "10s3m", "XLF_SPY_D30", "UST10Y_VOL", "NET_LIQ_D30",
]

# Optional extras (only used if series exist)
OPTIONAL_BLOCKS = ["IG_OAS", "HY_OAS", "JP10Y", "US_JP10Y_SPREAD"]

# Weights for composite (equal by default)
WEIGHTS = {k: 1.0 for k in CORE_BLOCKS}

# Thresholds for regime
TH_G = 40.0
TH_R = 60.0

# ---------------------------- Helpers ---------------------------

def _ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

def _read_prices_parquet() -> Optional[pd.DataFrame]:
    p = "data/processed/prices.parquet"
    if os.path.exists(p):
        try:
            df = pd.read_parquet(p)
            # normalize
            df = df.rename(columns={"close":"Close","date":"Date","symbol":"symbol"})
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values(["symbol","Date"])
            return df
        except Exception as e:
            print("WARN: failed reading prices.parquet:", e, file=sys.stderr)
    return None

def _read_prices_csv_bulk() -> Optional[pd.DataFrame]:
    pats = glob.glob("data/prices/**/*.csv", recursive=True) + glob.glob("data/prices/*.csv")
    frames = []
    for p in pats:
        try:
            d = pd.read_csv(p)
            if {"date","close"} <= set(d.columns):
                sym = os.path.splitext(os.path.basename(p))[0]
                d = d.rename(columns={"date":"Date","close":"Close"})
                d["Date"] = pd.to_datetime(d["Date"])
                d["symbol"] = sym
                frames.append(d[["symbol","Date","Close"]])
        except Exception:
            continue
    if frames:
        big = pd.concat(frames, ignore_index=True).sort_values(["symbol","Date"])
        return big
    return None

def _pick_prices() -> pd.DataFrame:
    df = _read_prices_parquet()
    if df is None:
        df = _read_prices_csv_bulk()
    if df is None or df.empty:
        raise FileNotFoundError("No prices found (parquet or csv).")
    return df

def _series(df: pd.DataFrame, sym: str) -> pd.Series:
    s = df.loc[df["symbol"].str.upper()==sym.upper(), ["Date","Close"]].dropna()
    s = s.set_index("Date")["Close"].sort_index()
    return s

def _zscore(s: pd.Series, win: int) -> pd.Series:
    s = s.dropna()
    if len(s) < max(MIN_ZROWS, win//2):
        return pd.Series(index=s.index, dtype=float)
    r = (s - s.rolling(win, min_periods=max(20, win//4)).mean()) / s.rolling(win, min_periods=max(20, win//4)).std(ddof=0)
    return r

def _pct_rank(s: pd.Series, win: int) -> pd.Series:
    s = s.dropna()
    if len(s) < max(MIN_ZROWS, win//2):
        return pd.Series(index=s.index, dtype=float)
    return s.rolling(win, min_periods=max(20, win//4)).apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False)

def _ann_sigma_from_series(s: pd.Series, base="logret", win=20) -> pd.Series:
    if s.empty: return s
    if base == "logret":
        lr = np.log(s).diff()
    else:
        lr = s.pct_change()
    sig_d = lr.rolling(win, min_periods=max(10, win//2)).std(ddof=0)
    return sig_d * np.sqrt(252.0)

def _delta_days(s: pd.Series, days: int) -> pd.Series:
    return s - s.shift(days)

def _score_from_z(z: pd.Series, invert: bool=False) -> pd.Series:
    # map z to 0..100 via smooth logistic-ish transform; clamp
    x = z.clip(-3, 3) / 3.0  # -1..1
    if invert: x = -x
    sc = 50 + 50*x
    return sc.clip(0, 100)

def _last_non_na(s: pd.Series) -> Optional[float]:
    s = s.dropna()
    return None if s.empty else float(s.iloc[-1])

def _safe_ratio(a: pd.Series, b: pd.Series) -> pd.Series:
    j = a.to_frame("a").join(b.to_frame("b"), how="inner")
    return (j["a"] / j["b"]).replace([np.inf, -np.inf], np.nan)

def _join(*series: pd.Series) -> pd.DatetimeIndex:
    idx = None
    for s in series:
        idx = s.index if idx is None else idx.intersection(s.index)
    return idx

# ---------------------------- Load non-price helpers ------------------

def _read_fred_oas() -> Dict[str, pd.Series]:
    p = "data/processed/fred_oas.csv"
    out = {}
    if os.path.exists(p):
        try:
            d = pd.read_csv(p, parse_dates=["date"])
            d = d.rename(columns={"date":"Date"})
            d = d.set_index("Date").sort_index()
            for c in d.columns:
                out[c.upper()] = d[c].astype(float)
        except Exception as e:
            print("WARN: fred_oas.csv load failed:", e, file=sys.stderr)
    return out

def _try_read_series_csv(path: str, col: str="value") -> Optional[pd.Series]:
    if os.path.exists(path):
        try:
            df = pd.read_csv(path, parse_dates=["Date"])
            df = df.set_index("Date").sort_index()
            c = col if col in df.columns else df.columns[0]
            return df[c].astype(float)
        except Exception:
            return None
    return None

# ---------------------------- Build blocks ---------------------------

def build_blocks(pr: pd.DataFrame) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
    """
    Returns:
      last_scores: {block: score(0..100)}
      last_z:      {block: z/pct}
      last_vals:   {block: raw/latest value (unit varies)}
    """
    last_scores, last_z, last_vals = {}, {}, {}

    # --- Core series from prices (must exist in your prices universe) ---
    # Expected symbols (upper):
    # VIX, VIX3M, DXY, HYG, LQD, XLF, SPY, DGS30, DGS10, DGS2, DGS3MO
    # SOFR, RRPONTSYD, WRESBAL (weekly reserves), TGA (optional)
    # If some FRED series are not in prices parquet, you can add a tiny fetcher later;
    # Here we try to use what exists; missing ones will be skipped.

    def get(sym: str) -> pd.Series:
        try:
            return _series(pr, sym)
        except Exception:
            return pd.Series(dtype=float)

    dgs30 = get("DGS30")
    dgs10 = get("DGS10")
    dgs2  = get("DGS2")
    dgs3m = get("DGS3MO")
    sofr  = get("SOFR")
    rrp   = get("RRPONTSYD")
    wres  = get("WRESBAL")
    tga   = get("TGA")  # optional

    vix   = get("VIX")
    vix3  = get("VIX3M")
    dxy   = get("DXY")
    hyg   = get("HYG")
    lqd   = get("LQD")
    xlf   = get("XLF")
    spy   = get("SPY")
    usdjpy = get("USDJPY")  # spot FX, for vol proxy
    jp10  = get("JP10Y")    # optional (TVC:JP10Y proxy if present)

    # --- Derived transforms ---
    # Curves
    s2s30 = dgs30 - dgs2
    s10s2 = dgs10 - dgs2
    s10s3 = dgs10 - dgs3m

    # Δ30 blocks
    cr_ratio = _safe_ratio(hyg, lqd)
    cr_d30   = _delta_days(cr_ratio, DELTA_WIN_DAYS)
    relfin   = _safe_ratio(xlf, spy)
    relfin_d30 = _delta_days(relfin, DELTA_WIN_DAYS)
    sofr_d30 = _delta_days(sofr, DELTA_WIN_DAYS)
    vix_term = vix - vix3

    # USDJPY vol proxy (ann.)
    usdjpy_vol = _ann_sigma_from_series(usdjpy, base="logret", win=20) * 100.0

    # UST10y vol (ann.) proxy: std of 1d change of DGS10
    ust10v = _ann_sigma_from_series(dgs10.diff(), base="diff", win=20)

    # Net Liquidity Δ30 = Δ WRES - Δ TGA - Δ RRP
    netliq_d30 = None
    if not wres.empty and not rrp.empty:
        if tga.empty:
            netliq_d30 = _delta_days(wres, DELTA_WIN_DAYS)/1e3 - _delta_days(rrp, DELTA_WIN_DAYS)/1e3
        else:
            netliq_d30 = _delta_days(wres, DELTA_WIN_DAYS)/1e3 - _delta_days(tga, DELTA_WIN_DAYS)/1e3 - _delta_days(rrp, DELTA_WIN_DAYS)/1e3
        netliq_d30.name = "NET_LIQ_D30"

    # --- Z-scores / pct ---
    def last_zblock(name: str, series: pd.Series, zlen: int = ZLEN_DEFAULT, invert: bool = False, use_pct: bool=False):
        if series is None or series.empty:
            return
        z = _pct_rank(series, zlen) if use_pct else _zscore(series, zlen)
        sc = _score_from_z(z, invert=invert)
        last_scores[name] = _last_non_na(sc)
        last_z[name] = _last_non_na(z)
        last_vals[name] = _last_non_na(series)

    # Map core blocks
    last_zblock("DGS30", dgs30, zlen=CURVE_ZLEN, invert=False)
    last_zblock("SPREAD_2s30s", s2s30, zlen=CURVE_ZLEN, invert=False)
    last_zblock("SOFR_D30", sofr_d30, invert=False)
    # RRP as percentile (high RRP = tight liquidity => red -> invert via mapping below using 1-pct in your Pine. Here: treat high pct as RISK-OFF -> no invert)
    if not rrp.empty:
        p = _pct_rank(rrp, ZLEN_DEFAULT)
        last_scores["RRP_PCTL"] = _last_non_na((1 - p) * 100.0)  # 0..100 like Pine (1 - pct)
        last_z["RRP_PCTL"] = _last_non_na(1 - p)
        last_vals["RRP_PCTL"] = _last_non_na(rrp)
    last_zblock("STLFSI", get("STLFSI4"), invert=False)
    last_zblock("VIX",    vix, invert=False)
    last_zblock("USDJPY_VOL", usdjpy_vol, invert=False)
    last_zblock("DXY",    dxy, invert=False)
    last_zblock("HYG_LQD_D30", cr_d30, invert=False)
    last_zblock("VIX_TERM", vix_term, invert=False)
    last_zblock("10s2s", s10s2, zlen=CURVE_ZLEN, invert=True)  # invert like Pine
    last_zblock("10s3m", s10s3, zlen=CURVE_ZLEN, invert=True)  # invert like Pine
    last_zblock("XLF_SPY_D30", relfin_d30, invert=True)        # weak relative = risk-off → invert=True
    last_zblock("UST10Y_VOL", ust10v, invert=False)
    if netliq_d30 is not None:
        last_zblock("NET_LIQ_D30", netliq_d30, invert=True)    # falling net liq = risk-off → invert=True

    # Optional OAS from fred_oas.csv
    fred = _read_fred_oas()
    if "US_IG_OAS" in fred:
        last_zblock("IG_OAS", fred["US_IG_OAS"], invert=False)
    if "US_HY_OAS" in fred:
        last_zblock("HY_OAS", fred["US_HY_OAS"], invert=False)

    # Optional Japan 10y and spread
    if not jp10.empty and not dgs10.empty:
        last_zblock("JP10Y", jp10, invert=False)
        sp = dgs10 - jp10
        last_zblock("US_JP10Y_SPREAD", sp, invert=False)

    return last_scores, last_z, last_vals

def _composite(last_scores: Dict[str, float]) -> Tuple[float, int]:
    used = []
    wsum = 0.0
    csum = 0.0
    for k in CORE_BLOCKS:
        v = last_scores.get(k)
        if v is None or math.isnan(v): 
            continue
        w = WEIGHTS.get(k, 1.0)
        csum += v * w
        wsum += w
        used.append(k)
    comp = csum / wsum if wsum > 0 else float("nan")
    return comp, len(used)

def _regime(comp: float, gate_hits: int, fs_score: float = 0.0) -> str:
    d = comp - ((TH_G + TH_R) / 2.0)  # tip near 50
    if comp >= TH_R and (gate_hits >= 4 or fs_score >= 2.0):
        return "RISK-OFF"
    if comp >= TH_R:
        return "CAUTION"
    if comp <= TH_G and gate_hits <= 2 and fs_score <= 1.0 and (50 - comp) >= 10:
        return "RISK-ON"
    return "NEUTRAL"

def _gate_hits(last_scores: Dict[str, float]) -> int:
    def isRed(x): 
        return x is not None and not math.isnan(x) and x >= 70.0
    keys = ["HYG_LQD_D30","VIX","VIX_TERM","UST10Y_VOL","XLF_SPY_D30","10s2s","10s3m"]
    return sum(1 for k in keys if isRed(last_scores.get(k)))

def _one_liner(comp: float, netliq: Optional[float], dgs30: Optional[float], ust10v: Optional[float],
               dxy: Optional[float], vix: Optional[float], cr: Optional[float],
               fs_score: float, flow_sum: int) -> str:
    bias = "RISK-ON" if comp < 45 else "RISK-OFF" if comp > 55 else "NEUTRAL"
    size = "klein" if (fs_score >= 2 or (netliq and netliq > TH_R) or flow_sum >= 2) else ("moderat" if flow_sum > -2 else "moderat")
    dur  = "↓" if ((dgs30 and dgs30 > TH_R) or (ust10v and ust10v > TH_R)) else ("↑" if ((dgs30 and dgs30 < TH_G) and (ust10v and ust10v < TH_G)) else "≙")
    carry_hits = sum(1 for x in [vix, dxy, cr] if x is not None and x > TH_R)
    warn  = "  ⚠" if (carry_hits >= 2) else ""
    return f"Bias: {bias} | Größe: {size} | Dur {dur}{warn}"

# ---------------------------- Build time series -----------------------

def build_timeseries(pr: pd.DataFrame, days: int = 250) -> pd.DataFrame:
    """
    Compute timeseries for a subset of blocks + composite over the last N days available.
    This is light-weight (no reports per-day), just useful for plotting or debugging.
    """
    scores_hist = []
    # We will recompute blocks over time window (slow if huge). To keep it light,
    # sample every 'step' bars.
    # Here: compute for last 'days' trading days on SPY index as reference.
    try:
        ref = _series(pr, "SPY")
    except Exception:
        ref = pr.set_index("Date").groupby("symbol")["Close"].last()
        ref = ref.sort_index()
    if isinstance(ref, pd.Series):
        dates = ref.index[-days:]
    else:
        return pd.DataFrame()

    # To keep runtime reasonable, compute blocks on each date by slicing upto date.
    # This is O(N^2) if naive, so we do vectorized per-block transforms and then just take rolling last.
    # For simplicity, we only export a handful of series + composite.

    # Build once all primary series
    series_map = {}
    for sym in ["VIX","VIX3M","DXY","HYG","LQD","XLF","SPY","DGS30","DGS10","DGS2","DGS3MO","SOFR","RRPONTSYD","WRESBAL","TGA","USDJPY"]:
        s = _series(pr, sym)
        if not s.empty:
            series_map[sym] = s

    # Precompute transforms
    def has(keys): return all(k in series_map for k in keys)

    s_map = {}
    if has(["DGS30","DGS2"]):   s_map["SPREAD_2s30s"] = series_map["DGS30"] - series_map["DGS2"]
    if has(["DGS10","DGS2"]):   s_map["10s2s"]        = series_map["DGS10"] - series_map["DGS2"]
    if has(["DGS10","DGS3MO"]): s_map["10s3m"]        = series_map["DGS10"] - series_map["DGS3MO"]
    if has(["SOFR"]):           s_map["SOFR_D30"]     = _delta_days(series_map["SOFR"], DELTA_WIN_DAYS)
    if has(["HYG","LQD"]):      s_map["HYG_LQD_D30"]  = _delta_days(_safe_ratio(series_map["HYG"], series_map["LQD"]), DELTA_WIN_DAYS)
    if has(["XLF","SPY"]):      s_map["XLF_SPY_D30"]  = _delta_days(_safe_ratio(series_map["XLF"], series_map["SPY"]), DELTA_WIN_DAYS)
    if has(["VIX","VIX3M"]):    s_map["VIX_TERM"]     = series_map["VIX"] - series_map["VIX3M"]
    if has(["USDJPY"]):
        s_map["USDJPY_VOL"] = _ann_sigma_from_series(series_map["USDJPY"], win=20) * 100.0
    if has(["DGS10"]):
        s_map["UST10Y_VOL"] = _ann_sigma_from_series(series_map["DGS10"].diff(), base="diff", win=20)

    if has(["WRESBAL","RRPONTSYD"]):
        if "TGA" in series_map:
            net = _delta_days(series_map["WRESBAL"], DELTA_WIN_DAYS)/1e3 - _delta_days(series_map["TGA"], DELTA_WIN_DAYS)/1e3 - _delta_days(series_map["RRPONTSYD"], DELTA_WIN_DAYS)/1e3
        else:
            net = _delta_days(series_map["WRESBAL"], DELTA_WIN_DAYS)/1e3 - _delta_days(series_map["RRPONTSYD"], DELTA_WIN_DAYS)/1e3
        s_map["NET_LIQ_D30"] = net

    # Build rolling z-scores/scaled scores
    score_map = {}
    for k, s in s_map.items():
        inv = (k in ["10s2s","10s3m","XLF_SPY_D30","NET_LIQ_D30"])
        z   = _zscore(s, ZLEN_DEFAULT if k not in ["10s2s","10s3m"] else CURVE_ZLEN)
        sc  = _score_from_z(z, invert=inv)
        score_map[k] = sc

    # DGS30 itself:
    if "DGS30" in series_map:
        z = _zscore(series_map["DGS30"], CURVE_ZLEN)
        score_map["DGS30"] = _score_from_z(z, invert=False)

    # RRP percentile  (1 - pct)
    if "RRPONTSYD" in series_map:
        p = _pct_rank(series_map["RRPONTSYD"], ZLEN_DEFAULT)
        score_map["RRP_PCTL"] = (1 - p) * 100.0

    # Timeseries frame
    cols = ["Date"] + CORE_BLOCKS
    out = pd.DataFrame({"Date": dates})
    for k in CORE_BLOCKS:
        s = score_map.get(k)
        if s is not None:
            out[k] = s.reindex(dates).values
        else:
            out[k] = np.nan

    # Composite
    comp = []
    for i, dt in enumerate(dates):
        row = {k: out.loc[i, k] for k in CORE_BLOCKS}
        c, _ = _composite(row)
        comp.append(c)
    out["Composite"] = comp
    return out

# ---------------------------- Main -----------------------------------

def main() -> int:
    _ensure_dirs()
    pr = _pick_prices()

    # Build latest snapshot
    last_scores, last_z, last_vals = build_blocks(pr)
    comp, used = _composite(last_scores)
    gates = _gate_hits(last_scores)

    # Funding-Score & Flow-Pressure placeholders (0..3) & sum(-∞..+∞)
    # If you later add a builder, read from e.g. data/processed/funding_score.json / flow_pressure.json
    fs_score = 0.0
    flow_sum = 0

    regime = _regime(comp, gates, fs_score)
    oneliner = _one_liner(
        comp,
        last_scores.get("NET_LIQ_D30"),
        last_scores.get("DGS30"),
        last_scores.get("UST10Y_VOL"),
        last_scores.get("DXY"),
        last_scores.get("VIX"),
        last_scores.get("HYG_LQD_D30"),
        fs_score,
        flow_sum
    )

    snapshot = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "composite": None if (comp is None or math.isnan(comp)) else round(float(comp), 1),
        "used_blocks": used,
        "gate_hits": gates,
        "regime": regime,
        "one_liner": oneliner,
        "scores": last_scores,
        "z_or_pct": last_z,
        "raw_last": last_vals,
        "notes": {
            "zlen": ZLEN_DEFAULT,
            "curve_zlen": CURVE_ZLEN,
            "delta_window_days": DELTA_WIN_DAYS,
            "thresholds": {"green": TH_G, "red": TH_R},
            "weights": WEIGHTS,
        }
    }

    with open("data/processed/riskindex_snapshot.json","w",encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
    print("→ data/processed/riskindex_snapshot.json")

    # Build a short timeseries for plotting/debug
    try:
        ts = build_timeseries(pr, days=250)
        ts.to_csv("data/processed/riskindex_timeseries.csv", index=False)
        print(f"→ data/processed/riskindex_timeseries.csv ({len(ts)} rows)")
    except Exception as e:
        print("WARN: timeseries build failed:", e, file=sys.stderr)

    # Report
    report = {
        "ok_blocks": [k for k,v in last_scores.items() if v is not None],
        "missing_blocks": [k for k in CORE_BLOCKS if last_scores.get(k) is None],
        "composite": snapshot["composite"],
        "gate_hits": gates,
        "regime": regime,
        "generated": snapshot["ts"]
    }
    with open("data/reports/riskindex_report.json","w",encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print("→ data/reports/riskindex_report.json")

    return 0

if __name__ == "__main__":
    sys.exit(main())
