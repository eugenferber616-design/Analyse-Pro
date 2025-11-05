#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
optimize_riskindex_auto.py
Brute-Force Optimizer ohne Eingaben:
 - Liest sc_comp (0-100) aus riskindex_timeseries.csv
 - Liest SPY aus market_core.csv.gz
 - Grid-Search für EMA/Thresholds/Modus/Short-Gewicht
 - Bewertet Out-of-Model-Backtest auf Daily-Basis (1d Delay)
Outputs:
 - docs/opt_results_auto.csv
 - docs/opt_best_params.json
"""
from __future__ import annotations
import json, math
from pathlib import Path
import pandas as pd
import numpy as np

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

def load_data() -> pd.DataFrame:
    ts = pd.read_csv(PROCESSED/"riskindex_timeseries.csv", parse_dates=["date"])
    ts = ts.rename(columns={"date":"date","sc_comp":"sc_comp"}).set_index("date").sort_index()
    mkt = pd.read_csv(PROCESSED/"market_core.csv.gz", compression="gzip")
    # flexible Date-Spalte erkennen
    dcol = None
    for c in ("date","Date","DATE"):
        if c in mkt.columns: dcol=c; break
    if dcol is None: dcol = mkt.columns[0]
    mkt[dcol] = pd.to_datetime(mkt[dcol], errors="coerce")
    mkt = mkt.set_index(dcol).sort_index()
    if "SPY" not in mkt.columns:
        raise SystemExit("SPY fehlt in market_core.csv.gz – Optimizer kann nicht laufen.")
    df = ts.join(mkt[["SPY"]], how="inner").dropna()
    df["ret"] = np.log(df["SPY"]/df["SPY"].shift(1)).fillna(0.0)  # log-returns
    return df

def kpis(eq: pd.Series) -> dict:
    if eq.empty: 
        return dict(CAGR=0, Sharpe=0, MaxDD=0, Calmar=0)
    rets = eq.pct_change().dropna()
    ann = 252
    cagr = float((eq.iloc[-1]/eq.iloc[0])**(ann/len(eq)) - 1.0) if len(eq)>0 and eq.iloc[0]>0 else 0.0
    vol  = float(rets.std() * math.sqrt(ann)) if rets.size>3 else 0.0
    sharpe = (float(rets.mean())*ann)/vol if vol>0 else 0.0
    runmax = eq.cummax()
    dd = (eq/runmax - 1.0).min() if (runmax>0).any() else 0.0
    calmar = (-cagr/dd) if dd<0 else 0.0
    return dict(CAGR=cagr, Sharpe=sharpe, MaxDD=float(dd), Calmar=calmar)

def make_signal(sc: pd.Series, ema:int, on:float, off:float, mode:str, short_w:float) -> pd.Series:
    # tiefer Score = risk-on (kaufen), hoher Score = risk-off (verkaufen)
    x = sc.ewm(span=ema, min_periods=max(5, ema//4)).mean()
    if mode == "long_only":
        sig = pd.Series(0.0, index=x.index)
        # entry long wenn unter on, exit wenn über off
        long = False
        for i, v in enumerate(x):
            if not long and v < on: long = True
            elif long and v > off: long = False
            sig.iat[i] = 1.0 if long else 0.0
        return sig
    elif mode == "tri_state":
        # +1 long unter on, -1 short über off, sonst flat
        sig = pd.Series(0.0, index=x.index)
        state = 0
        for i, v in enumerate(x):
            if v < on:   state = 1
            elif v > off: state = -1
            sig.iat[i] = 1.0 if state==1 else (short_w if state==-1 else 0.0)
        return sig
    else:
        raise ValueError("unknown mode")

def evaluate(df: pd.DataFrame, ema:int, on:int, off:int, mode:str, short_w:float) -> dict:
    sig = make_signal(df["sc_comp"], ema, on, off, mode, short_w)
    # 1-Tages Delay (um Lookahead zu vermeiden)
    strat_ret = sig.shift(1).fillna(0.0) * df["ret"]
    eq = (1.0 + strat_ret).cumprod()
    base = (1.0 + df["ret"]).cumprod()
    metrics = kpis(eq)
    # Trades zählen (State-Wechsel)
    flips = (sig != sig.shift(1)).fillna(False).sum()
    wins = (strat_ret > 0).sum()
    trades = int(flips)
    hitrate = float(wins/max(1, len(strat_ret[strat_ret!=0])))
    return dict(
        ema=ema, on=on, off=off, mode=mode, short_w=short_w,
        **metrics,
        Trades=trades, HitRate=hitrate,
        EqEnd=float(eq.iloc[-1]), EqBase=float(base.iloc[-1])
    )

def main():
    df = load_data()
    results = []

    ema_grid = list(range(10, 127, 1))
    on_grid  = list(range(38, 51, 1))
    off_grid = list(range(50, 63, 1))
    modes    = ["long_only", "tri_state"]
    short_ws = [-1.0, -0.75, -0.5, -0.25]

    for ema in ema_grid:
        for on in on_grid:
            for off in off_grid:
                if on >= off: 
                    continue
                # long_only
                results.append(evaluate(df, ema, on, off, "long_only", 0.0))
                # tri_state Varianten
                for sw in short_ws:
                    results.append(evaluate(df, ema, on, off, "tri_state", sw))

    res = pd.DataFrame(results)
    res["Sharpe"].replace([np.inf, -np.inf], np.nan, inplace=True)
    res = res.sort_values(["Sharpe","CAGR","Calmar"], ascending=False)
    out_csv = DOCS/"opt_results_auto.csv"
    res.to_csv(out_csv, index=False)
    best = res.iloc[0].to_dict() if not res.empty else {}
    (DOCS/"opt_best_params.json").write_text(json.dumps(best, indent=2, ensure_ascii=False), encoding="utf-8")
    print("✔ wrote", out_csv, "rows:", len(res))
    print("Best:", best)

if __name__ == "__main__":
    main()
