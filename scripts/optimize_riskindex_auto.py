#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train/Test (Walk-Forward) – AUTO (V2)
- Nutzt riskindex_timeseries:
    • bevorzugt: risk_index_bin (0..100)
    • fallback : sc_comp        (0..100-ähnlich)
- Benchmark: SPY Buy&Hold (Kennzahlen je Testfenster + Summary)
- Rolling: 5y Train / 1y Test
- Schreibt:
    docs/train_test_results_auto.csv
    docs/train_test_summary_auto.csv
    docs/train_test_equity_auto.csv   (Equity-Kurven je Fenster & Signal)
"""

from __future__ import annotations
import gzip
from pathlib import Path
import numpy as np
import pandas as pd

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

# ---------- IO ----------
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
def _hyst(x: pd.Series, on: float, off: float,
          mode: str = "long_only", short_w: float = -0.5) -> pd.Series:
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

def _ema_signal(sig: pd.Series, span: int) -> pd.Series:
    return sig.ewm(span=span, adjust=False, min_periods=span).mean()

def _eval_params(span, on, off, mode, sw, ret, sig) -> tuple[float,float,float,pd.Series]:
    ema = _ema_signal(sig, span=span)
    pos = _hyst(ema, on, off, mode=mode, short_w=sw)
    strat_r = (ret * pos).fillna(0.0)
    curve = (1.0 + strat_r).cumprod()
    return _sharpe(strat_r), _cagr(curve), _maxdd(curve), curve

# ---------- Load ----------
ts = _to_dt_index(_read_any_csv(PROCESSED / "riskindex_timeseries.csv"), "date")
mkt = _to_dt_index(_read_any_csv(PROCESSED / "market_core.csv.gz"), "date")

if ts.empty or ("SPY" not in mkt.columns and not mkt.empty):
    # fallback: unkomprimiert
    if "SPY" not in mkt.columns:
        mkt2 = _to_dt_index(_read_any_csv(PROCESSED / "market_core.csv"), "date")
        if not mkt2.empty: mkt = mkt2

if ts.empty or mkt.empty or "SPY" not in mkt.columns:
    print("WARN: Basisdaten fehlen – Train/Test übersprungen.")
    (DOCS / "train_test_results_auto.csv").write_text("no_data\n", encoding="utf-8")
    raise SystemExit(0)

# wähle Signale: bevorzugt risk_index_bin (wenn vorhanden), sonst sc_comp
signal_cols = []
if "risk_index_bin" in ts.columns: signal_cols.append("risk_index_bin")
if "sc_comp" in ts.columns:        signal_cols.append("sc_comp")
if not signal_cols:
    print("WARN: Keine Signalspalten (risk_index_bin / sc_comp) – Abbruch.")
    (DOCS / "train_test_results_auto.csv").write_text("no_signal\n", encoding="utf-8")
    raise SystemExit(0)

# Align auf Daily
idx_union = ts.index.union(mkt.index)
full_idx = pd.date_range(idx_union.min(), idx_union.max(), freq="D")

# Basisreihen
spy_px = pd.to_numeric(mkt["SPY"], errors="coerce").reindex(full_idx).ffill()
spy_ret = spy_px.pct_change().fillna(0.0)
bh_curve_full = (1.0 + spy_ret).cumprod()

# Parameter-Grids
ema_spans  = [10, 14, 21, 34, 42, 63, 84, 126]
thresholds = [(40,60), (42,58), (45,55), (47,53)]
modes      = ["long_only", "tri_state"]
short_ws   = [-1.0, -0.5, -0.25]

years = sorted(set(pd.Index(full_idx).to_period("Y").to_timestamp("Y")))
rows = []
eq_rows = []  # Equity-Kurven (optional)

for sig_col in signal_cols:
    sig_raw = pd.to_numeric(ts[sig_col], errors="coerce").reindex(full_idx).ffill()

    for i in range(0, len(years) - 6):  # 5y train + 1y test
        train_start = years[i]
        train_end   = years[i+5]
        test_start  = years[i+5]
        test_end    = years[i+6]

        tr_mask = (sig_raw.index > train_start) & (sig_raw.index <= train_end)
        te_mask = (sig_raw.index > test_start)  & (sig_raw.index <= test_end)

        # ausreichende Länge?
        if tr_mask.sum() < 250 or te_mask.sum() < 200:
            continue

        # Benchmark Buy&Hold nur im Testfenster
        bh_te_curve = bh_curve_full[te_mask]
        bh_sh = _sharpe(spy_ret[te_mask])
        bh_cg = _cagr(bh_te_curve)
        bh_dd = _maxdd(bh_te_curve)

        # Grid-Suche auf Train
        best_key, best_params = None, None
        for span in ema_spans:
            ema_tr = _ema_signal(sig_raw[tr_mask], span=span)
            for on, off in thresholds:
                for mode in modes:
                    sw_list = [0.0] if mode == "long_only" else short_ws
                    for sw in sw_list:
                        # Kennzahlen Train
                        pos_tr = _hyst(ema_tr, on, off, mode=mode, short_w=sw)
                        strat_r_tr = (spy_ret[tr_mask] * pos_tr).fillna(0.0)
                        curve_tr = (1.0 + strat_r_tr).cumprod()
                        key = (_sharpe(strat_r_tr), _cagr(curve_tr), -abs(_maxdd(curve_tr)))
                        if (best_key is None) or (key > best_key):
                            best_key = key
                            best_params = dict(ema_span=span, on=on, off=off, mode=mode, short_w=sw)

        # Test mit besten Parametern
        sh_te, cg_te, dd_te, curve_te = _eval_params(
            best_params["ema_span"], best_params["on"], best_params["off"],
            best_params["mode"], best_params["short_w"], spy_ret[te_mask], sig_raw[te_mask]
        )

        rows.append({
            "signal": sig_col,
            "train_start": train_start.date(), "train_end": train_end.date(),
            "test_start":  test_start.date(),  "test_end":  test_end.date(),
            **best_params,
            "Sharpe_test": sh_te, "CAGR%_test": cg_te, "MaxDD%_test": dd_te,
            "BH_Sharpe_test": bh_sh, "BH_CAGR%_test": bh_cg, "BH_MaxDD%_test": bh_dd
        })

        # Equity-Kurven ablegen (optional, schlank)
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
    raise SystemExit(0)

df.to_csv(DOCS / "train_test_results_auto.csv", index=False)

# Summary je Signal + Benchmark-Durchschnitt aus denselben Fenstern
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
        "BH_Sharpe_test_mean":   float(g["BH_Sharpe_test"].mean()),
        "BH_CAGR%_test_mean":    float(g["BH_CAGR%_test"].mean()),
        "BH_MaxDD%_test_mean":   float(g["BH_MaxDD%_test"].mean()),
    })

pd.DataFrame(summary_rows).to_csv(DOCS / "train_test_summary_auto.csv", index=False)

# Equity-Kurven zusammenführen
if eq_rows:
    eq_df = pd.concat(eq_rows, ignore_index=True)
    eq_df.to_csv(DOCS / "train_test_equity_auto.csv", index=False)

print("Wrote:", DOCS / "train_test_results_auto.csv")
print("Wrote:", DOCS / "train_test_summary_auto.csv")
print("Wrote:", DOCS / "train_test_equity_auto.csv")
