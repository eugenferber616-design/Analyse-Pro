#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train/Test (Walk-Forward) – AUTO
- Rolling 5y Train / 1y Test
- Nimmt riskindex_timeseries (sc_comp) + SPY aus market_core
- Schreibt:
    docs/train_test_results_auto.csv
    docs/train_test_summary_auto.csv
"""

from __future__ import annotations
import gzip
from pathlib import Path
import numpy as np
import pandas as pd

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

def _read_any_csv(p: Path) -> pd.DataFrame:
    if not p.exists(): return pd.DataFrame()
    if p.suffix == ".gz":
        with gzip.open(p, "rt", encoding="utf-8") as f:
            return pd.read_csv(f)
    return pd.read_csv(p)

def _to_dt_index(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    if df.empty: return df
    cols = {c.lower(): c for c in df.columns}
    dcol = cols.get(date_col.lower(), df.columns[0])
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.dropna(subset=[dcol]).sort_values(dcol).set_index(dcol)
    return df

def _maxdd(curve: pd.Series) -> float:
    arr, peak, mdd = curve.values, -np.inf, 0.0
    for v in arr:
        peak = max(peak, v); mdd = min(mdd, v/peak - 1.0)
    return float(mdd * 100.0)

def _sharpe(r: pd.Series) -> float:
    r = r.replace([np.inf, -np.inf], np.nan).dropna()
    if r.empty or r.std() == 0: return 0.0
    return float(np.sqrt(252) * r.mean() / r.std())

def _cagr(curve: pd.Series) -> float:
    if curve.empty: return 0.0
    years = (curve.index[-1] - curve.index[0]).days / 365.25
    if years <= 0: return 0.0
    return float((curve.iloc[-1] ** (1/years) - 1.0) * 100.0)

def _hyst(x: pd.Series, on: float, off: float,
          mode: str = "long_only", short_w: float = -0.5) -> pd.Series:
    pos, last = [], 0.0
    for v in x.values:
        if np.isnan(v): pos.append(last); continue
        if v <= on: last = 1.0
        elif v >= off: last = 0.0 if mode == "long_only" else short_w
        pos.append(last)
    return pd.Series(pos, index=x.index, name="pos")

# ---- load & align ----
ts = _to_dt_index(_read_any_csv(PROCESSED / "riskindex_timeseries.csv"), "date")
mkt = _to_dt_index(_read_any_csv(PROCESSED / "market_core.csv.gz"), "date")

if ts.empty or "sc_comp" not in ts.columns or mkt.empty or "SPY" not in mkt.columns:
    print("WARN: Basisdaten fehlen – Train/Test übersprungen.")
    (DOCS / "train_test_results_auto.csv").write_text("no_data\n", encoding="utf-8")
    raise SystemExit(0)

idx_union = ts.index.union(mkt.index)
full = pd.date_range(idx_union.min(), idx_union.max(), freq="D")

sc  = pd.to_numeric(ts["sc_comp"], errors="coerce").reindex(full).ffill()
px  = pd.to_numeric(mkt["SPY"], errors="coerce").reindex(full).ffill()
ret = px.pct_change().fillna(0.0)

ema_spans  = [10, 14, 21, 34, 42, 63, 84, 126]
thresholds = [(40,60), (42,58), (45,55), (47,53)]
modes      = ["long_only", "tri_state"]
short_ws   = [-1.0, -0.5, -0.25]

def eval_params(span, on, off, mode, sw, r, s):
    ema = s.ewm(span=span, adjust=False, min_periods=span).mean()
    pos = _hyst(ema, on, off, mode=mode, short_w=sw)
    strat_r = r * pos
    curve = (1.0 + strat_r).cumprod()
    return _sharpe(strat_r), _cagr(curve), _maxdd(curve)

years = sorted(set(pd.Index(full).to_period("Y").to_timestamp("Y")))
rows = []

for i in range(0, len(years) - 6):  # 5y train + 1y test
    train_start = years[i]
    train_end   = years[i+5]
    test_start  = years[i+5]
    test_end    = years[i+6]

    tr = (sc.index > train_start) & (sc.index <= train_end)
    te = (sc.index > test_start)  & (sc.index <= test_end)
    if tr.sum() < 250 or te.sum() < 200:
        continue

    best_key, best_params = None, None
    for span in ema_spans:
        for on, off in thresholds:
            for mode in modes:
                sw_list = [0.0] if mode == "long_only" else short_ws
                for sw in sw_list:
                    sh, cg, dd = eval_params(span, on, off, mode, sw, ret[tr], sc[tr])
                    key = (sh, cg, -abs(dd))
                    if (best_key is None) or (key > best_key):
                        best_key, best_params = key, dict(ema_span=span, on=on, off=off, mode=mode, short_w=sw)

    sh_te, cg_te, dd_te = eval_params(best_params["ema_span"], best_params["on"],
                                      best_params["off"], best_params["mode"], best_params["short_w"],
                                      ret[te], sc[te])

    rows.append({
        "train_start": train_start.date(), "train_end": train_end.date(),
        "test_start": test_start.date(),   "test_end": test_end.date(),
        **best_params,
        "Sharpe_test": sh_te, "CAGR%_test": cg_te, "MaxDD%_test": dd_te
    })

df = pd.DataFrame(rows)
if df.empty:
    (DOCS / "train_test_results_auto.csv").write_text("no_results\n", encoding="utf-8")
    raise SystemExit(0)

df.to_csv(DOCS / "train_test_results_auto.csv", index=False)
summary = {
    "windows": int(df.shape[0]),
    "Sharpe_test_mean":   float(df["Sharpe_test"].mean()),
    "CAGR%_test_mean":    float(df["CAGR%_test"].mean()),
    "MaxDD%_test_mean":   float(df["MaxDD%_test"].mean()),
    "Sharpe_test_median": float(df["Sharpe_test"].median()),
    "CAGR%_test_median":  float(df["CAGR%_test"].median()),
    "MaxDD%_test_median": float(df["MaxDD%_test"].median()),
}
pd.DataFrame([summary]).to_csv(DOCS / "train_test_summary_auto.csv", index=False)
print("Wrote:", DOCS / "train_test_results_auto.csv")
print("Summary:", summary)
