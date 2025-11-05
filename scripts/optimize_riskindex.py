#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import itertools, json
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib.pyplot as plt

P = Path("data/processed")
F_COMP = P/"riskindex_components.csv.gz"
F_MKT  = P/"market_core.csv.gz"

def load():
    comp = pd.read_csv(F_COMP, compression="infer", parse_dates=["date"]).set_index("date").sort_index()
    mkt  = pd.read_csv(F_MKT,  compression="infer", parse_dates=["date"]).set_index("date").sort_index()
    spy  = pd.to_numeric(mkt["SPY"], errors="coerce").dropna()
    return comp, spy

def composite(df: pd.DataFrame, weights: dict[str,float]) -> pd.Series:
    cols = [c for c in weights if c in df.columns and weights[c]!=0]
    if not cols: return pd.Series(dtype=float)
    w = np.array([weights[c] for c in cols], dtype=float)
    w = w / np.sum(np.abs(w))
    x = df[cols].copy()
    num = (x * w).sum(axis=1, skipna=True)
    den = (~x[cols].isna()).dot(np.abs(w))
    sc  = (num / den.replace(0, np.nan)).dropna()
    return sc

def strat_returns(spy: pd.Series, score: pd.Series, ema_len:int, on_thr:float, off_thr:float, mode:str="long_only"):
    sc = score.ewm(span=ema_len, adjust=False, min_periods=max(10,ema_len//3)).mean()
    sc = sc.reindex(spy.index).ffill().dropna()
    r  = spy.pct_change().fillna(0.0)

    sig = pd.Series(0.0, index=sc.index)
    if mode=="long_only":
        sig[sc < on_thr]  = 1.0
        sig[sc > off_thr] = 0.0
    else:  # tri_state
        sig[sc < on_thr]  = 1.0
        sig[(sc >= on_thr) & (sc <= off_thr)] = 0.0
        sig[sc > off_thr] = -0.5
    sig = sig.ffill().reindex(r.index).fillna(0.0)

    eq = (1.0 + sig * r).cumprod()
    return eq, sig

def metrics(eq: pd.Series):
    r = eq.pct_change().dropna()
    ann = (eq.iloc[-1]/eq.iloc[0])**(252/len(r)) - 1
    vol = r.std()*np.sqrt(252)
    sharpe = ann/vol if vol>0 else np.nan
    mdd = (eq/eq.cummax()-1).min()
    return {"ann": float(ann), "vol": float(vol), "sharpe": float(sharpe), "mdd": float(mdd)}

def main():
    comp, spy = load()

    # Komponenten-Gruppen (verwende die vorhandenen Namen aus build_riskindex_components)
    groups = {
        "vol"   : ["vix","usdvol","vxterm","ust10v"],
        "curve" : ["10s2s","10s3m","2s30s"],
        "credit": ["cr","ig_oas","hy_oas"],
        "usd"   : ["dxy"],
        "liq"   : ["netliq"],
        "rates" : ["dgs30","sofr","stlfsi"],
        "equity": ["relfin"],
    }

    # Suchraster
    w_opts   = [0.5, 1.0, 2.0]           # relative Gruppen-Gewichte
    ema_opts = [42, 63, 84, 126]
    thr_opts = [(40,60), (45,55), (42,58)]
    mode_opts= ["long_only","tri_state"]

    rows=[]
    for wv in itertools.product(*([w_opts]*len(groups))):
        gw = dict(zip(groups.keys(), wv))
        # auf Komponenten verteilen
        weights = {}
        for g, comps in groups.items():
            present = [c for c in comps if c in comp.columns]
            if not present: continue
            for c in present:
                weights[c] = gw[g] / len(present)
        sc = composite(comp, weights)
        if sc.empty: continue

        for ema in ema_opts:
            for (on,off) in thr_opts:
                for mode in mode_opts:
                    eq, _ = strat_returns(spy, sc, ema, on, off, mode=mode)
                    m = metrics(eq.dropna())
                    rows.append({
                        "ema":ema,"on":on,"off":off,"mode":mode,
                        **{f"gw_{k}":float(v) for k,v in gw.items()},
                        **m
                    })

    out = pd.DataFrame(rows).sort_values("sharpe", ascending=False)
    Path("docs").mkdir(parents=True, exist_ok=True)
    out.to_csv("docs/opt_results.csv", index=False)
    print("Top 10:\n", out.head(10))

    # Plot bester Lauf
    best = out.iloc[0]
    best_weights={}
    for g, comps in groups.items():
        present = [c for c in comps if c in comp.columns]
        if not present: continue
        for c in present:
            best_weights[c]=best[f"gw_{g}"]/len(present)

    sc = composite(comp, best_weights)
    eq_best,_ = strat_returns(spy, sc, int(best.ema), float(best.on), float(best.off), best.mode)
    eq_bh = (1+spy.pct_change().fillna(0.0)).cumprod()
    plt.figure(figsize=(10,5))
    plt.plot(eq_bh.index, eq_bh.values, label="Buy&Hold SPY")
    plt.plot(eq_best.index, eq_best.values, label=f"Best {best.mode} (ema={int(best.ema)}, thr=({best.on},{best.off}))")
    plt.yscale("log"); plt.legend(); plt.title("Equity (log) – best run")
    plt.tight_layout(); plt.savefig("docs/opt_equity.png", dpi=160)
    print("Saved docs/opt_results.csv and docs/opt_equity.png")

if __name__=="__main__":
    main()
