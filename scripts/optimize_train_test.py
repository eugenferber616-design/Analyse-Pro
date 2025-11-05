#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
optimize_train_test_auto.py  —  Walk-Forward (3y Train → 1y Test)
- Signalquellen (aus riskindex_timeseries.csv):
    • bevorzugt: risk_index_bin (0..100)
    • fallback : sc_comp
- Benchmark: SPY Buy&Hold (Kennzahlen je Testfenster + Summary)
- Rolling-Fenster: 3 Jahre Train, 1 Jahr Test, Schritt ≈ 1 Jahr
- Grid-Search im Train, bestes Param-Set „stumpf“ im nächsten Test
Outputs:
  docs/train_test_results_auto.csv
  docs/train_test_summary_auto.csv
  docs/train_test_equity_auto.csv
  docs/train_test_summary_auto.json
"""

from __future__ import annotations
from pathlib import Path
import json, math
import numpy as np
import pandas as pd

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

# ---------- IO ----------
def _read_any_csv(p: Path) -> pd.DataFrame:
    if not p.exists(): return pd.DataFrame()
    if p.suffix == ".gz":
        return pd.read_csv(p, compression="gzip")
    return pd.read_csv(p)

def _to_dt_index(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    if df.empty: return df
    cols = {c.lower(): c for c in df.columns}
    dcol = cols.get(date_col.lower(), df.columns[0])
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce", utc=True).dt.tz_localize(None)
    df = df.dropna(subset=[dcol]).sort_values(dcol).set_index(dcol)
    return df

# ---------- Metrics ----------
def _maxdd(curve: pd.Series) -> float:
    if curve.empty: return 0.0
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

# ---------- Signal / Position ----------
def _ema_signal(sig: pd.Series, span: int) -> pd.Series:
    return sig.ewm(span=span, adjust=False, min_periods=max(5, span//4)).mean()

def _hyst(x: pd.Series, on: float, off: float, mode: str, short_w: float) -> pd.Series:
    """
    Hysterese auf einem 0..100-Signal:
      - <= on  → Long (1)
      - >= off → Flat (0) oder Short (short_w) je nach Modus
    """
    pos, last = [], 0.0
    for v in x.values:
        if np.isnan(v): pos.append(last); continue
        if v <= on: last = 1.0
        elif v >= off: last = 0.0 if mode == "long_only" else short_w
        pos.append(last)
    return pd.Series(pos, index=x.index, name="pos")

def _eval_params(span, on, off, mode, sw, ret, sig) -> tuple[float,float,float,pd.Series]:
    ema = _ema_signal(sig, span=span)
    pos = _hyst(ema, on, off, mode=mode, short_w=sw)
    strat_r = (ret * pos.shift(1).fillna(0.0)).fillna(0.0)  # 1-Tag Verzögerung
    curve = (1.0 + strat_r).cumprod()
    return _sharpe(strat_r), _cagr(curve), _maxdd(curve), curve

# ---------- Load & Align ----------
ts = _to_dt_index(_read_any_csv(PROCESSED / "riskindex_timeseries.csv"), "date")
mkt = _to_dt_index(_read_any_csv(PROCESSED / "market_core.csv.gz"), "date")
if mkt.empty or "SPY" not in mkt.columns:
    mkt_alt = _to_dt_index(_read_any_csv(PROCESSED / "market_core.csv"), "date")
    if not mkt_alt.empty: mkt = mkt_alt

if ts.empty or mkt.empty or "SPY" not in mkt.columns:
    (DOCS / "train_test_results_auto.csv").write_text("no_data\n", encoding="utf-8")
    raise SystemExit("Basisdaten fehlen (riskindex_timeseries / SPY).")

# Bevorzugtes Signal wählen
sig_cols = []
if "risk_index_bin" in ts.columns: sig_cols.append("risk_index_bin")
if "sc_comp"        in ts.columns: sig_cols.append("sc_comp")
if not sig_cols:
    (DOCS / "train_test_results_auto.csv").write_text("no_signal\n", encoding="utf-8")
    raise SystemExit("Keine Signalspalten (risk_index_bin / sc_comp).")

# Outer-Join & ffill, damit keine Historie „verloren“ geht
idx_union = ts.index.union(mkt.index)
full_idx = pd.date_range(idx_union.min(), idx_union.max(), freq="D")

spy_px  = pd.to_numeric(mkt["SPY"], errors="coerce").reindex(full_idx).ffill()
spy_ret = spy_px.pct_change().fillna(0.0)
bh_curve_full = (1.0 + spy_ret).cumprod()

# ---------- Grid ----------
# Fein wie dein Optimizer:
EMA_GRID  = list(range(10, 127, 1))
ON_GRID   = list(range(38, 51, 1))
OFF_GRID  = list(range(50, 63, 1))
MODES     = ["long_only", "tri_state"]
SHORT_WS  = [-1.0, -0.75, -0.5, -0.25]

# ---------- Walk-Forward ----------
years = sorted(set(pd.Index(full_idx).to_period("Y").to_timestamp("Y")))
rows = []
eq_rows = []

for sig_col in sig_cols:  # erst risk_index_bin, dann sc_comp
    sig_raw = pd.to_numeric(ts[sig_col], errors="coerce").reindex(full_idx).ffill()

    # 3y Train → 1y Test, Schritt ≈ 1 Jahr
    for i in range(0, len(years) - 4):
        train_start = years[i]
        train_end   = years[i+3]   # exklusive Grenze nutzen wir mit <= unten
        test_start  = years[i+3]
        test_end    = years[i+4]

        tr = (sig_raw.index > train_start) & (sig_raw.index <= train_end)
        te = (sig_raw.index > test_start)  & (sig_raw.index <= test_end)
        if tr.sum() < 250 or te.sum() < 200:
            continue

        # Benchmark im Testfenster
        bh_te_curve = bh_curve_full[te]
        bh_sh = _sharpe(spy_ret[te])
        bh_cg = _cagr(bh_te_curve)
        bh_dd = _maxdd(bh_te_curve)

        # --- Grid-Search im Train: max (Sharpe, CAGR, Calmar)
        best_key, best_params = None, None
        ema_tr_cache = {}  # Speed-Up
        for ema in EMA_GRID:
            if ema not in ema_tr_cache:
                ema_tr_cache[ema] = _ema_signal(sig_raw[tr], span=ema)
            ema_tr = ema_tr_cache[ema]
            for on in ON_GRID:
                for off in OFF_GRID:
                    if on >= off: 
                        continue
                    # long_only
                    pos_tr = _hyst(ema_tr, on, off, mode="long_only", short_w=0.0)
                    strat_r_tr = (spy_ret[tr] * pos_tr.shift(1).fillna(0.0)).fillna(0.0)
                    curve_tr = (1.0 + strat_r_tr).cumprod()
                    cand = (_sharpe(strat_r_tr), _cagr(curve_tr), -abs(_maxdd(curve_tr)))
                    key  = ("long_only", ema, on, off, 0.0)
                    if (best_key is None) or (cand > best_key):
                        best_key, best_params = cand, dict(mode="long_only", ema=ema, on=on, off=off, short_w=0.0)
                    # tri_state
                    for sw in SHORT_WS:
                        pos_tr2 = _hyst(ema_tr, on, off, mode="tri_state", short_w=sw)
                        strat_r_tr2 = (spy_ret[tr] * pos_tr2.shift(1).fillna(0.0)).fillna(0.0)
                        curve_tr2 = (1.0 + strat_r_tr2).cumprod()
                        cand2 = (_sharpe(strat_r_tr2), _cagr(curve_tr2), -abs(_maxdd(curve_tr2)))
                        if cand2 > best_key:
                            best_key, best_params = cand2, dict(mode="tri_state", ema=ema, on=on, off=off, short_w=sw)

        if best_params is None:
            continue

        # --- Test mit besten Parametern
        sh_te, cg_te, dd_te, curve_te = _eval_params(
            best_params["ema"], best_params["on"], best_params["off"],
            best_params["mode"], best_params["short_w"], spy_ret[te], sig_raw[te]
        )

        rows.append({
            "signal": sig_col,
            "train_start": train_start.date(), "train_end": train_end.date(),
            "test_start":  test_start.date(),  "test_end":  test_end.date(),
            **best_params,
            "Sharpe_test": sh_te, "CAGR%_test": cg_te, "MaxDD%_test": dd_te,
            "BH_Sharpe_test": bh_sh, "BH_CAGR%_test": bh_cg, "BH_MaxDD%_test": bh_dd
        })

        # Equity-Kurven speichern (optional)
        eq_rows.append(pd.DataFrame({
            "date": curve_te.index.date,
            "equity": curve_te.values,
            "benchmark": bh_te_curve.values,
            "signal": sig_col,
            "test_start": str(test_start.date()),
            "test_end": str(test_end.date())
        }))

# ---------- Outputs ----------
df = pd.DataFrame(rows)
if df.empty:
    (DOCS / "train_test_results_auto.csv").write_text("no_results\n", encoding="utf-8")
    (DOCS / "train_test_summary_auto.csv").write_text("no_results\n", encoding="utf-8")
    (DOCS / "train_test_summary_auto.json").write_text(json.dumps({"windows":0}), encoding="utf-8")
    raise SystemExit(0)

df.to_csv(DOCS / "train_test_results_auto.csv", index=False)

# Summary je Signal + Benchmark-Mittel aus denselben Fenstern
summary_rows = []
for sig_col, g in df.groupby("signal"):
    summary_rows.append({
        "signal": sig_col,
        "windows": int(g.shape[0]),
        "Sharpe_test_mean":   float(g["Sharpe_test"].mean()),
        "CAGR%_test_mean":    float(g["CAGR%_test"].mean()),
        "MaxDD%_test_mean":   float(g["MaxDD%_test"].mean()),
        "Sharpe_test_median": float(g["Sharpe_test"].median()),
        "CAGR%_test_median":  float(g["CAGR%_test"].median()),
        "MaxDD%_test_median": float(g["MaxDD%_test"].median()),
        "BH_Sharpe_test_mean": float(g["BH_Sharpe_test"].mean()),
        "BH_CAGR%_test_mean":  float(g["BH_CAGR%_test"].mean()),
        "BH_MaxDD%_test_mean": float(g["BH_MaxDD%_test"].mean()),
        "best_window": g.sort_values(["Sharpe_test","CAGR%_test","MaxDD%_test"], ascending=[False,False,True]).iloc[0].to_dict()
    })

sum_df = pd.DataFrame(summary_rows)
sum_df.to_csv(DOCS / "train_test_summary_auto.csv", index=False)
(DOCS / "train_test_summary_auto.json").write_text(sum_df.to_json(orient="records", indent=2), encoding="utf-8")

# Equity-Kurven zusammenführen
if eq_rows:
    eq_df = pd.concat(eq_rows, ignore_index=True)
    eq_df.to_csv(DOCS / "train_test_equity_auto.csv", index=False)

print("Wrote:", DOCS / "train_test_results_auto.csv")
print("Wrote:", DOCS / "train_test_summary_auto.csv")
print("Wrote:", DOCS / "train_test_equity_auto.csv")
print("Wrote:", DOCS / "train_test_summary_auto.json")
