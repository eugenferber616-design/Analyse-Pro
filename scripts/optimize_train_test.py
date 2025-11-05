#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import itertools
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

P = Path("data/processed")
F_COMP = P/"riskindex_components.csv.gz"   # Z-Score Komponenten
F_MKT  = P/"market_core.csv.gz"            # enthält SPY

# ========= Parameterräume =========
GROUPS = {
    "vol"   : ["vix","usdvol","vxterm","ust10v"],
    "curve" : ["10s2s","10s3m","2s30s"],
    "credit": ["cr","ig_oas","hy_oas"],
    "usd"   : ["dxy"],
    "liq"   : ["netliq"],
    "rates" : ["dgs30","sofr","stlfsi"],
    "equity": ["relfin"],
}

W_OPTS       = [0.5, 1.0, 2.0]            # Gruppen-Gewichte (relativ)
EMA_OPTS     = [42, 63, 84, 126]          # Glättung
THR_OPTS     = [(40,60), (45,55), (42,58)]
MODE_OPTS    = ["long_only", "tri_state"]
SHORT_OPTS   = [-0.5, -0.25]              # nur bei tri_state

# Walk-Forward Fenster
TRAIN_YEARS  = 8
TEST_YEARS   = 2
STEP_YEARS   = 2

# ========== Utils ==========
def load_inputs():
    comp = pd.read_csv(F_COMP, compression="infer", parse_dates=["date"]).set_index("date").sort_index()
    mkt  = pd.read_csv(F_MKT,  compression="infer", parse_dates=["date"]).set_index("date").sort_index()
    spy  = pd.to_numeric(mkt["SPY"], errors="coerce").dropna()
    comp = comp.reindex(spy.index).ffill()  # sicherstellen, dass Index deckungsgleich ist
    return comp, spy

def composite(df: pd.DataFrame, weights: dict[str,float]) -> pd.Series:
    cols = [c for c in weights if c in df.columns and weights[c] != 0]
    if not cols:
        return pd.Series(dtype=float)
    w = np.array([weights[c] for c in cols], dtype=float)
    w = w / np.sum(np.abs(w))  # normieren
    x = df[cols].copy()
    # robust gegen NAs (gewichtete Mittel nur über vorhandene)
    num = (x * w).sum(axis=1, skipna=True)
    den = (~x[cols].isna()).dot(np.abs(w))
    sc  = (num / den.replace(0, np.nan)).dropna()
    return sc

def strat_returns(spy: pd.Series, score: pd.Series, ema_len:int, on_thr:float, off_thr:float,
                  mode:str="long_only", short_weight:float=-0.5):
    sc = score.ewm(span=ema_len, adjust=False, min_periods=max(10, ema_len//3)).mean()
    sc = sc.reindex(spy.index).ffill().dropna()
    r  = spy.pct_change().fillna(0.0)

    sig = pd.Series(0.0, index=sc.index)
    if mode == "long_only":
        sig[sc < on_thr]  = 1.0
        sig[sc > off_thr] = 0.0
    else:
        sig[sc < on_thr]  = 1.0
        sig[(sc >= on_thr) & (sc <= off_thr)] = 0.0
        sig[sc > off_thr] = float(short_weight)

    sig = sig.ffill().reindex(r.index).fillna(0.0)
    eq = (1.0 + sig * r).cumprod()
    return eq, sig

def metrics(eq: pd.Series):
    r = eq.pct_change().dropna()
    if len(r) < 30:
        return {"ann": np.nan, "vol": np.nan, "sharpe": np.nan, "mdd": np.nan}
    ann = (eq.iloc[-1] / eq.iloc[0]) ** (252 / len(r)) - 1
    vol = r.std() * np.sqrt(252)
    sharpe = ann / vol if vol > 0 else np.nan
    mdd = (eq / eq.cummax() - 1).min()
    return {"ann": float(ann), "vol": float(vol), "sharpe": float(sharpe), "mdd": float(mdd)}

@dataclass
class ParamSet:
    weights: dict
    ema: int
    on_thr: float
    off_thr: float
    mode: str
    short_weight: float

def grid_iter(groups: dict[str, list[str]]):
    # Gruppen-Gewichte-Kombinationen
    for gw in itertools.product(*([W_OPTS] * len(groups))):
        group_w = dict(zip(groups.keys(), gw))
        yield group_w

def expand_component_weights(groups, group_w: dict[str,float], comp_cols: list[str]):
    weights = {}
    for g, comps in groups.items():
        present = [c for c in comps if c in comp_cols]
        if not present:
            continue
        share = group_w[g] / len(present)
        for c in present:
            weights[c] = share
    return weights

def select_best(train_spy, train_comp):
    best = None
    best_key = None
    comp_cols = list(train_comp.columns)

    for gw in grid_iter(GROUPS):
        w_map = expand_component_weights(GROUPS, gw, comp_cols)
        sc = composite(train_comp, w_map)
        if sc.empty:
            continue
        for ema in EMA_OPTS:
            for (on, off) in THR_OPTS:
                for mode in MODE_OPTS:
                    shorts = SHORT_OPTS if mode == "tri_state" else [0.0]
                    for sw in shorts:
                        eq, _ = strat_returns(train_spy, sc, ema, on, off, mode=mode, short_weight=sw)
                        m = metrics(eq)
                        key = (m["sharpe"], -m["mdd"])  # Maximieren Sharpe, dann geringerer Drawdown
                        if best is None or key > best_key:
                            best_key = key
                            best = ParamSet(weights=w_map, ema=ema, on_thr=on, off_thr=off, mode=mode, short_weight=sw)
    return best

def walk_forward(comp, spy, train_years=TRAIN_YEARS, test_years=TEST_YEARS, step_years=STEP_YEARS):
    start = spy.index.min()
    end   = spy.index.max()

    rows = []
    oos_equity_segments = []

    cur_start = pd.Timestamp(start)
    while True:
        train_end = cur_start + pd.DateOffset(years=train_years) - pd.Timedelta(days=1)
        test_end  = train_end + pd.DateOffset(years=test_years)

        if test_end > end - pd.Timedelta(days=5):
            break

        tr_mask = (spy.index >= cur_start) & (spy.index <= train_end)
        te_mask = (spy.index >  train_end) & (spy.index <= test_end)

        tr_spy = spy.loc[tr_mask]
        te_spy = spy.loc[te_mask]
        tr_comp = comp.loc[tr_mask]
        te_comp = comp.loc[te_mask]

        if len(tr_spy) < 200 or len(te_spy) < 60:
            cur_start = cur_start + pd.DateOffset(years=step_years)
            continue

        best = select_best(tr_spy, tr_comp)

        # Train Performance
        sc_tr = composite(tr_comp, best.weights)
        eq_tr, _ = strat_returns(tr_spy, sc_tr, best.ema, best.on_thr, best.off_thr,
                                 mode=best.mode, short_weight=best.short_weight)
        m_tr = metrics(eq_tr)

        # Test Performance (OOS)
        sc_te = composite(te_comp, best.weights)
        eq_te, _ = strat_returns(te_spy, sc_te, best.ema, best.on_thr, best.off_thr,
                                 mode=best.mode, short_weight=best.short_weight)
        m_te = metrics(eq_te)

        rows.append({
            "train_start": cur_start.date().isoformat(),
            "train_end": train_end.date().isoformat(),
            "test_end": test_end.date().isoformat(),
            "ema": best.ema, "on": best.on_thr, "off": best.off_thr,
            "mode": best.mode, "short_w": float(best.short_weight),
            **{f"gw_{k}": float(v) for k, v in best.weights.items()},
            "train_ann": m_tr["ann"], "train_sharpe": m_tr["sharpe"], "train_mdd": m_tr["mdd"],
            "test_ann":  m_te["ann"], "test_sharpe":  m_te["sharpe"],  "test_mdd":  m_te["mdd"],
        })

        oos_equity_segments.append(eq_te)

        cur_start = cur_start + pd.DateOffset(years=step_years)

    res = pd.DataFrame(rows)
    return res, oos_equity_segments

def main():
    comp, spy = load_inputs()
    res, segs = walk_forward(comp, spy)

    Path("docs").mkdir(parents=True, exist_ok=True)
    res.to_csv("docs/train_test_results.csv", index=False)
    print("Train/Test rows:", len(res))
    if not res.empty:
        agg = res[["test_ann","test_sharpe","test_mdd"]].mean().to_dict()
        pd.DataFrame([agg]).to_csv("docs/train_test_summary.csv", index=False)
        print("OOS mean:", agg)

    # Stitch OOS Segmente zu einer Equity-Kurve
    if segs:
        # Bring sie auf gemeinsamen Index (konkatten by index)
        eq_oos = pd.concat(segs).sort_index()
        eq_oos = eq_oos / eq_oos.iloc[0]
        eq_bh  = (1.0 + spy.pct_change().fillna(0.0)).cumprod()
        eq_bh  = eq_bh.reindex(eq_oos.index).ffill()
        plt.figure(figsize=(10,5))
        plt.plot(eq_bh.index, eq_bh.values, label="Buy&Hold SPY")
        plt.plot(eq_oos.index, eq_oos.values, label="OOS Equity (stitched)")
        plt.yscale("log"); plt.legend(); plt.title("Train/Test – Out-of-Sample Equity (log)")
        plt.tight_layout(); plt.savefig("docs/train_test_equity.png", dpi=160)
        print("Saved docs/train_test_results.csv, docs/train_test_summary.csv, docs/train_test_equity.png")

if __name__ == "__main__":
    main()
