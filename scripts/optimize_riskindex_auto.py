#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Optimize RiskIndex – AUTO (keine Inputs)
- Nimmt riskindex_timeseries (sc_comp) + SPY-Preise
- Spielt automatisch ein sinnvolles Raster an Parametern durch:
    EMA-Spannen, On/Off-Schwellen, Modi (long_only/tri_state), Short-Gewichte
- Metriken: CAGR, Sharpe, MaxDD, WinRate, Trades
- Schreibt docs/opt_results_auto.csv (voll) + docs/opt_results_top10.csv (beste 10)
"""

from __future__ import annotations
import os, math, gzip, io
from pathlib import Path
import pandas as pd
import numpy as np

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

# ---- helpers ----
def _read_any_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return pd.read_csv(f)
    return pd.read_csv(path)

def _to_dt(df, col="date"):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        df = df.dropna(subset=[col]).sort_values(col)
        df = df.set_index(col)
    return df

def _dd(curve: pd.Series) -> float:
    # max drawdown in %
    x = curve.values
    peak = -np.inf
    mdd = 0.0
    for v in x:
        peak = max(peak, v)
        mdd = min(mdd, v/peak - 1.0)
    return float(mdd * 100.0)

def _cagr(curve: pd.Series) -> float:
    if curve.empty:
        return 0.0
    years = (curve.index[-1] - curve.index[0]).days / 365.25
    if years <= 0:
        return 0.0
    return float((curve.iloc[-1] ** (1/years) - 1.0) * 100.0)

def _sharpe(returns: pd.Series) -> float:
    r = returns.replace([np.inf, -np.inf], np.nan).dropna()
    if r.std() == 0 or r.empty:
        return 0.0
    return float(np.sqrt(252) * r.mean() / r.std())

def _hysteresis_signal(x: pd.Series, on: float, off: float,
                       mode: str="long_only", short_weight: float=-0.5) -> pd.Series:
    """
    x  = geglättete Composite (kleiner = risk-on)
    on = Schwelle um LONG einzugehen (<= on)
    off= Schwelle um LONG zu verlassen / SHORT zu werden (>= off)
    """
    pos = []
    last = 0.0
    for v in x.values:
        if np.isnan(v):
            pos.append(last)
            continue
        if v <= on:
            last = 1.0
        elif v >= off:
            last = 0.0 if mode == "long_only" else short_weight
        # sonst State halten
        pos.append(last)
    return pd.Series(pos, index=x.index, name="pos")

# ---- load data ----
ts = _read_any_csv(PROCESSED / "riskindex_timeseries.csv")
ts = _to_dt(ts, "date")
if ts.empty or "sc_comp" not in ts.columns:
    print("WARN: timeseries fehlt/leer – Optimizer übersprungen.")
    # dennoch leere Datei schreiben, damit Workflow nicht scheitert
    (DOCS / "opt_results_auto.csv").write_text("no_data\n", encoding="utf-8")
    raise SystemExit(0)

mkt = _read_any_csv(PROCESSED / "market_core.csv.gz")
mkt = _to_dt(mkt, "date")
if mkt.empty or "SPY" not in mkt.columns:
    print("WARN: market_core/ SPY fehlt – Optimizer übersprungen.")
    (DOCS / "opt_results_auto.csv").write_text("no_spy\n", encoding="utf-8")
    raise SystemExit(0)

px = pd.to_numeric(mkt["SPY"], errors="coerce").dropna()
px = px.reindex(pd.date_range(min(ts.index.min(), px.index.min()),
                              max(ts.index.max(), px.index.max()),
                              freq="D")).ffill().dropna()
ret = px.pct_change().fillna(0.0)

sc = pd.to_numeric(ts["sc_comp"], errors="coerce").reindex(px.index).ffill()

# ---- grid (auto, ohne Inputs) ----
ema_spans = [10, 14, 21, 34, 42, 63, 84, 126]
thresholds = [(40,60), (42,58), (45,55), (47,53)]
modes = ["long_only", "tri_state"]
short_ws = [-1.0, -0.5, -0.25]

rows = []
for span in ema_spans:
    ema = sc.ewm(span=span, adjust=False, min_periods=span).mean()
    for (on, off) in thresholds:
        for mode in modes:
            shorts = [0.0] if mode == "long_only" else short_ws
            for sw in shorts:
                pos = _hysteresis_signal(ema, on, off, mode=mode, short_weight=sw)
                strat_ret = ret * pos
                curve = (1.0 + strat_ret).cumprod()
                trades = int((pos.diff().abs() > 1e-9).sum())
                # pro-Trade Gewinn auf Wechsel zu 0/1/-:
                # einfacher Approx: summiere Returns pro "Phase"
                ph = pos.ne(pos.shift()).cumsum()
                phase_ret = (1.0 + strat_ret).groupby(ph).prod() - 1.0
                wins = int((phase_ret > 0).sum())
                total = int(phase_ret.shape[0])
                rows.append({
                    "ema_span": span,
                    "on": on, "off": off,
                    "mode": mode,
                    "short_w": sw if mode != "long_only" else 0.0,
                    "CAGR_%": _cagr(curve),
                    "Sharpe": _sharpe(strat_ret),
                    "MaxDD_%": _dd(curve),
                    "WinRate_%": (wins/total*100.0) if total else 0.0,
                    "Trades": trades,
                    "Years": (curve.index[-1]-curve.index[0]).days/365.25
                })

res = pd.DataFrame(rows)
if res.empty:
    (DOCS / "opt_results_auto.csv").write_text("no_results\n", encoding="utf-8")
    raise SystemExit(0)

res = res.sort_values(["Sharpe","CAGR_%"], ascending=[False, False])
res.to_csv(DOCS / "opt_results_auto.csv", index=False)
res.head(10).to_csv(DOCS / "opt_results_top10.csv", index=False)
print("Wrote:", DOCS / "opt_results_auto.csv")
print("Top row:\n", res.head(1))
