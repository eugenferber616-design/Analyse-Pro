#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
optimize_train_test_auto.py
Walk-Forward ohne Eingaben:
 - Train 3y → Test 1y, rollierend über die Historie
 - Pro Train-Fenster komplette Grid-Search (identisch zum Optimizer)
 - Bestes Param-Set dann stur im darauffolgenden Testfenster anwenden
Outputs:
 - docs/train_test_results_auto.csv
 - docs/train_test_summary.json
"""
from __future__ import annotations
import json, math, statistics
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta

PROCESSED = Path("data/processed")
DOCS = Path("docs")
DOCS.mkdir(parents=True, exist_ok=True)

def load_data() -> pd.DataFrame:
    ts = pd.read_csv(PROCESSED/"riskindex_timeseries.csv", parse_dates=["date"])
    ts = ts.rename(columns={"date":"date","sc_comp":"sc_comp"}).set_index("date").sort_index()
    mkt = pd.read_csv(PROCESSED/"market_core.csv.gz", compression="gzip")
    dcol = None
    for c in ("date","Date","DATE"):
        if c in mkt.columns: dcol=c; break
    if dcol is None: dcol = mkt.columns[0]
    mkt[dcol] = pd.to_datetime(mkt[dcol], errors="coerce")
    mkt = mkt.set_index(dcol).sort_index()
    if "SPY" not in mkt.columns:
        raise SystemExit("SPY fehlt in market_core.csv.gz – Train/Test kann nicht laufen.")
    df = ts.join(mkt[["SPY"]], how="inner").dropna()
    df["ret"] = np.log(df["SPY"]/df["SPY"].shift(1)).fillna(0.0)
    return df

def kpis(eq: pd.Series) -> dict:
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
    x = sc.ewm(span=ema, min_periods=max(5, ema//4)).mean()
    if mode == "long_only":
        sig = pd.Series(0.0, index=x.index)
        long = False
        for i, v in enumerate(x):
            if not long and v < on: long = True
            elif long and v > off: long = False
            sig.iat[i] = 1.0 if long else 0.0
        return sig
    elif mode == "tri_state":
        sig = pd.Series(0.0, index=x.index)
        state = 0
        for i, v in enumerate(x):
            if v < on: state = 1
            elif v > off: state = -1
            sig.iat[i] = 1.0 if state==1 else (short_w if state==-1 else 0.0)
        return sig
    else:
        raise ValueError("unknown mode")

def evaluate_one(df: pd.DataFrame, ema:int, on:int, off:int, mode:str, short_w:float, window: pd.Index) -> dict:
    sc  = df.loc[window, "sc_comp"]
    ret = df.loc[window, "ret"]
    sig = make_signal(sc, ema, on, off, mode, short_w)
    strat = sig.shift(1).fillna(0.0) * ret
    eq = (1.0 + strat).cumprod()
    metrics = kpis(eq)
    return dict(**metrics)

def grid() -> tuple[list[int], list[int], list[int], list[str], list[float]]:
    ema_grid = list(range(10,127,1))
    on_grid  = list(range(38,51,1))
    off_grid = list(range(50,63,1))
    modes    = ["long_only","tri_state"]
    short_ws = [-1.0, -0.75, -0.5, -0.25]
    return ema_grid, on_grid, off_grid, modes, short_ws

def main():
    df = load_data()
    dates = df.index

    train_years = 3
    test_years  = 1
    step_days   = 365  # roll um 1 Jahr
    results = []

    ema_grid, on_grid, off_grid, modes, short_ws = grid()

    start_ptr = df.index.min()
    end_all   = df.index.max()

    while True:
        train_end   = start_ptr + pd.DateOffset(years=train_years)
        test_end    = train_end + pd.DateOffset(years=test_years)
        if test_end > end_all - pd.DateOffset(days=5):
            break

        train_idx = df.loc[(df.index>=start_ptr) & (df.index<train_end)].index
        test_idx  = df.loc[(df.index>=train_end) & (df.index<test_end)].index
        if len(train_idx) < 250 or len(test_idx) < 100:
            start_ptr = start_ptr + pd.DateOffset(days=step_days)
            continue

        # --- Train: vollständige Grid-Search
        best = None
        best_key = None
        for ema in ema_grid:
            for on in on_grid:
                for off in off_grid:
                    if on >= off: 
                        continue
                    # long_only
                    m1 = evaluate_one(df, ema, on, off, "long_only", 0.0, train_idx)
                    key = ("long_only", ema, on, off, 0.0)
                    cand = (m1["Sharpe"], m1["CAGR"], m1["Calmar"])
                    if (best is None) or (cand > best):
                        best, best_key = cand, key
                    # tri_state
                    for sw in short_ws:
                        m2 = evaluate_one(df, ema, on, off, "tri_state", sw, train_idx)
                        key2 = ("tri_state", ema, on, off, sw)
                        cand2= (m2["Sharpe"], m2["CAGR"], m2["Calmar"])
                        if cand2 > best:
                            best, best_key = cand2, key2

        if best_key is None:
            start_ptr = start_ptr + pd.DateOffset(days=step_days)
            continue

        mode, ema, on, off, sw = best_key

        # --- Test mit den gefundenen Parametern
        sc  = df.loc[test_idx, "sc_comp"]
        ret = df.loc[test_idx, "ret"]
        sig = make_signal(sc, ema, on, off, mode, sw)
        strat = sig.shift(1).fillna(0.0) * ret
        eq = (1.0 + strat).cumprod()
        base = (1.0 + ret).cumprod()

        row = dict(
            train_start=str(train_idx[0].date()), train_end=str(train_idx[-1].date()),
            test_start=str(test_idx[0].date()),  test_end=str(test_idx[-1].date()),
            mode=mode, ema=ema, on=on, off=off, short_w=sw,
            EqEnd=float(eq.iloc[-1]), EqBase=float(base.iloc[-1]),
            **kpis(eq)
        )
        results.append(row)

        # nächstes Fenster
        start_ptr = start_ptr + pd.DateOffset(days=step_days)

    res = pd.DataFrame(results)
    out_csv = DOCS/"train_test_results_auto.csv"
    res.to_csv(out_csv, index=False)
    print("✔ wrote", out_csv, "rows:", len(res))

    # Zusammenfassung
    summary = {}
    if not res.empty:
        summary = {
            "windows": int(len(res)),
            "CAGR_mean": float(res["CAGR"].mean()),
            "Sharpe_mean": float(res["Sharpe"].mean()),
            "MaxDD_mean": float(res["MaxDD"].mean()),
            "Calmar_mean": float(res["Calmar"].mean()),
            "best_window": res.sort_values(["Sharpe","CAGR","Calmar"], ascending=False).iloc[0].to_dict(),
            "mode_most_common": res["mode"].mode().iat[0] if not res["mode"].empty else None,
            "ema_median": float(res["ema"].median()) if "ema" in res else None,
            "on_median":  float(res["on"].median()) if "on" in res else None,
            "off_median": float(res["off"].median()) if "off" in res else None,
            "short_w_median": float(res["short_w"].median()) if "short_w" in res else None,
        }
    (DOCS/"train_test_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Summary:", summary)

if __name__ == "__main__":
    main()
