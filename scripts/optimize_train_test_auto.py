#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train/Test (Walk-Forward) – AUTO (keine Inputs)
- Rolling 5y Train / 1y Test
- Verwendet denselben Grid wie der Auto-Optimizer
- Wählt pro Train-Fenster das beste Set (nach Sharpe) und evaluiert auf dem nächsten Jahr
- Outputs:
    docs/train_test_results_auto.csv  (alle Fenster)
    docs/train_test_summary_auto.csv  (Aggregat)
"""

from __future__ import annotations
import gzip
from pathlib import Path
import numpy as np
import pandas as pd

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

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
    x = curve.values
    peak = -np.inf
    mdd = 0.0
    for v in x:
        peak = max(peak, v)
        mdd = min(mdd, v/peak - 1.0)
    return float(mdd * 100.0)

def _sharpe(r: pd.Series) -> float:
    r = r.replace([np.inf, -np.inf], np.nan).dropna()
    if r.std() == 0 or r.empty:
        return 0.0
    return float(np.sqrt(252) * r.mean() / r.std())

def _cagr(curve: pd.Series) -> float:
    if curve.empty:
        return 0.0
    years = (curve.index[-1] - curve.index[0]).days / 365.25
    if years <= 0:
        return 0.0
    return float((curve.iloc[-1] ** (1/years) - 1.0) * 100.0)

def _hysteresis_signal(x: pd.Series, on: float, off: float,
                       mode: str="long_only", short_weight: float=-0.5) -> pd.Series:
    pos = []
    last = 0.0
    for v in x.values:
        if np.isnan(v):
            pos.append(last); continue
        if v <= on:
            last = 1.0
        elif v >= off:
            last = 0.0 if mode == "long_only" else short_weight
        pos.append(last)
    return pd.Series(pos, index=x.index, name="pos")

# ---- load ----
ts = _to_dt(_read_any_csv(PROCESSED / "riskindex_timeseries.csv"), "date")
mkt = _to_dt(_read_any_csv(PROCESSED / "market_core.csv.gz"), "date")

if ts.empty or "sc_comp" not in ts.columns or mkt.empty or "SPY" not in mkt.columns:
    print("WARN: Basisdaten fehlen – Train/Test übersprungen.")
    Path(DOCS / "train_test_results_auto.csv").write_text("no_data\n", encoding="utf-8")
    raise SystemExit(0)

px = pd.to_numeric(mkt["SPY"], errors="coerce").dropna()
px = px.reindex(pd.date_range(min(ts.index.min(), px.index.min()),
                              max(ts.index.max(), px.index.max()),
                              freq="D")).ffill().dropna()
ret = px.pct_change().fillna(0.0)
sc  = pd.to_numeric(ts["sc_comp"], errors="coerce").reindex(px.index).ffill()

# grid
ema_spans = [10, 14, 21, 34, 42, 63, 84, 126]
thresholds = [(40,60), (42,58), (45,55), (47,53)]
modes = ["long_only", "tri_state"]
short_ws = [-1.0, -0.5, -0.25]

results = []

start = px.index.min()
end   = px.index.max()

# jährliche Kanten
years = sorted(set(pd.Index(px.index).to_period("Y").to_timestamp("Y")))

def evaluate(span, on, off, mode, sw, r, s):
    ema = s.ewm(span=span, adjust=False, min_periods=span).mean()
    pos = _hysteresis_signal(ema, on, off, mode=mode, short_weight=sw)
    strat_ret = r * pos
    curve = (1.0 + strat_ret).cumprod()
    return _sharpe(strat_ret), _cagr(curve), _dd(curve)

for i in range(0, len(years)-6):  # 5y train + 1y test
    train_start = years[i]
    train_end   = years[i+5]   # inkl. Jahr i+4 (5 Jahre)
    test_start  = years[i+5]
    test_end    = years[i+6]

    tr_mask = (sc.index > train_start) & (sc.index <= train_end)
    te_mask = (sc.index > test_start)  & (sc.index <= test_end)

    if tr_mask.sum() < 250 or te_mask.sum() < 200:
        continue

    sc_tr, sc_te = sc[tr_mask], sc[te_mask]
    r_tr,  r_te  = ret[tr_mask], ret[te_mask]

    # best set on train
    best = None
    for span in ema_spans:
        for (on, off) in thresholds:
            for mode in modes:
                shorts = [0.0] if mode == "long_only" else short_ws
                for sw in shorts:
                    sh, cg, dd = evaluate(span, on, off, mode, sw, r_tr, sc_tr)
                    key = (sh, cg, -abs(dd))
                    if (best is None) or (key > best[0]):
                        best = (key, dict(ema_span=span, on=on, off=off, mode=mode, short_w=sw))

    # test with best
    params = best[1]
    sh_te, cg_te, dd_te = evaluate(params["ema_span"], params["on"], params["off"],
                                   params["mode"], params["short_w"], r_te, sc_te)

    results.append({
        "train_start": train_start.date(),
        "train_end":   train_end.date(),
        "test_start":  test_start.date(),
        "test_end":    test_end.date(),
        **params,
        "Sharpe_test": sh_te,
        "CAGR%_test":  cg_te,
        "MaxDD%_test": dd_te
    })

df = pd.DataFrame(results)
if df.empty:
    Path(DOCS / "train_test_results_auto.csv").write_text("no_results\n", encoding="utf-8")
    raise SystemExit(0)

df.to_csv(DOCS / "train_test_results_auto.csv", index=False)

# Aggregat
summary = {
    "windows": int(df.shape[0]),
    "Sharpe_test_mean": float(df["Sharpe_test"].mean()),
    "CAGR%_test_mean":  float(df["CAGR%_test"].mean()),
    "MaxDD%_test_mean": float(df["MaxDD%_test"].mean()),
    "Sharpe_test_median": float(df["Sharpe_test"].median()),
    "CAGR%_test_median":  float(df["CAGR%_test"].median()),
    "MaxDD%_test_median": float(df["MaxDD%_test"].median()),
}
pd.DataFrame([summary]).to_csv(DOCS / "train_test_summary_auto.csv", index=False)

print("Wrote:", DOCS / "train_test_results_auto.csv")
print("Summary:", summary)
