#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ml_optimizer_v18_3_fast.py
--------------------------------
V18.3: FAST RESPONSE STRATEGY
Basis: V18.1 (Champion)

Ziel:
- "Late Entry" fixen (z.B. Feb 2020 Drop aus ATH).
- V18.1 wartet auf Price < SMA200. Das kostet oft die ersten -10%.

Änderung:
- Wenn "Stress Jump" (Acceleration) erkannt wird,
  DANN wird Trend Confirm (SMA200) IGNORIERT.
- Wir shorten sofort in die Panik, auch über SMA200.

Output:
  data/processed/ml_trade_signals_v18_3.csv
"""

import os
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf
import pandas_datareader.data as web

from sklearn.ensemble import GradientBoostingClassifier

warnings.filterwarnings("ignore")

# ---------------- CONFIG ----------------
START_DATE = "2000-01-01"
SPLIT_DATE = "2016-01-01"

OUTFILE = "data/processed/ml_trade_signals_v18_3.csv"

# VIX phase parameters
VIX_FAST = 5
VIX_SLOW = 20
VIX_MOM_WIN = 5

# Forward label horizons
LKA_RISKOFF = 20
LKA_SHORT = 10
LKA_REENTRY = 20

# Drawdown/Rally thresholds
DD_RISKOFF = -0.05
DD_SHORT = -0.03
RALLY_REENTRY = 0.05

# Risk-Score thresholds (V18.1 Standard)
SCORE_RISKOFF = 62
SCORE_SHORT = 72
SCORE_SAFE = 56
SCORE_RISKON = 46

# ML thresholds (V18.1 Standard)
TH_P_RISKOFF = 0.55
TH_P_SHORT = 0.50
TH_P_REENTRY = 0.52

# Acceleration Trigger
SCORE_JUMP_5D = 8.0
SCORE_JUMP_MIN_SCORE = 55.0  # Muss mind. erreicht sein für Jump

# ---------------- HELPERS ----------------
def ensure_outdir(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

def zscore(s, win=60):
    mu = s.rolling(win).mean()
    sd = s.rolling(win).std()
    return (s - mu) / sd

def clip01(x):
    return np.minimum(1.0, np.maximum(0.0, x))

def safe_div(a, b):
    return a / b.replace(0, np.nan)

def forward_min_max(series, lookahead):
    future = pd.concat([series.shift(-i) for i in range(1, lookahead + 1)], axis=1)
    return future.min(axis=1), future.max(axis=1)

def scale_linear(x, lo, hi):
    return clip01((x - lo) / (hi - lo))

# ---------------- DATA ----------------
def fetch_data():
    print("--- 1) Fetch Market + Macro ---")

    tickers = ["SPY", "^VIX", "^VIX3M", "HYG", "LQD"]
    px = yf.download(tickers, start=START_DATE, progress=False, ignore_tz=True)["Close"]
    px.rename(columns={"^VIX": "VIX", "^VIX3M": "VIX3M"}, inplace=True)

    fred = [
        "T10Y2Y",
        "BAMLH0A0HYM2",
        "WALCL",
        "STLFSI2",
        "RRPONTSYD"
    ]

    try:
        macro = web.DataReader(fred, "fred", START_DATE, datetime.now())
        macro = macro.resample("D").ffill()
    except Exception as e:
        print(f"[WARN] FRED partially unavailable: {e}")
        macro = pd.DataFrame(index=px.index)

    # Merge: Use SPY index as master to avoid Weekend/Holiday rows
    # reindex(px.index) discards non-trading days from macro
    df = pd.concat([px, macro], axis=1).ffill().bfill()
    df = df.reindex(px.index)

    if "HYG" in df.columns and "LQD" in df.columns:
        df["Credit_Ratio"] = safe_div(df["HYG"], df["LQD"])

    return df.dropna()

# ---------------- RISK SCORE ----------------
def compute_risk_score(df):
    spy = df["SPY"]
    ret = spy.pct_change()

    sma200 = spy.rolling(200).mean()

    dist_sma200 = safe_div(spy - sma200, sma200)
    sma200_slope = sma200.pct_change(20)
    mom20 = spy.pct_change(20)
    mom60 = spy.pct_change(60)

    # Trend-Risk
    t1 = scale_linear(-dist_sma200, 0.00, 0.10)
    t2 = scale_linear(-sma200_slope, 0.00, 0.05)
    t3 = scale_linear(-mom20, 0.00, 0.06)
    t4 = scale_linear(-mom60, 0.00, 0.10)

    trend_risk_01 = clip01(0.35*t1 + 0.30*t2 + 0.20*t3 + 0.15*t4)
    trend_score = 40 * trend_risk_01

    # Vol/Stress
    vix = df["VIX"]
    vix3m = df["VIX3M"] if "VIX3M" in df.columns else vix.rolling(20).mean()
    vix_slope = safe_div(vix, vix3m)
    vix_z = zscore(vix, 60)

    hv20 = ret.rolling(20).std() * np.sqrt(252)
    hv60 = ret.rolling(60).std() * np.sqrt(252)
    hv_ratio = safe_div(hv20, hv60)

    v1 = scale_linear(vix_z.fillna(0), 0.4, 1.8)
    v2 = scale_linear(vix_slope.fillna(1), 1.0, 1.18)
    v3 = scale_linear(hv_ratio.fillna(1), 1.0, 1.35)

    vol_risk_01 = clip01(0.45*v1 + 0.35*v2 + 0.20*v3)
    vol_score = 30 * vol_risk_01

    # Credit/Liquidity
    fin_stress = df["STLFSI2"] if "STLFSI2" in df.columns else pd.Series(0, index=df.index)
    hy_oas = df["BAMLH0A0HYM2"] if "BAMLH0A0HYM2" in df.columns else pd.Series(np.nan, index=df.index)
    hy_oas_z = zscore(hy_oas, 60)

    walcl = df["WALCL"] if "WALCL" in df.columns else pd.Series(np.nan, index=df.index)
    walcl_chg20 = walcl.diff(20)

    rrp = df["RRPONTSYD"] if "RRPONTSYD" in df.columns else pd.Series(np.nan, index=df.index)
    rrp_chg20 = rrp.diff(20)

    c1 = scale_linear(fin_stress.fillna(0), 0.4, 1.8)
    c2 = scale_linear(hy_oas_z.fillna(0), 0.4, 1.8)
    c3 = scale_linear(-walcl_chg20.fillna(0), 0.0, 50.0)
    c4 = scale_linear(rrp_chg20.fillna(0), 0.0, 200.0)

    credit_risk_01 = clip01(0.40*c1 + 0.35*c2 + 0.15*c3 + 0.10*c4)
    credit_score = 30 * credit_risk_01

    risk_score = trend_score + vol_score + credit_score

    out = pd.DataFrame(index=df.index)
    out["SPY"] = spy
    out["VIX"] = vix

    out["Dist_SMA200"] = dist_sma200
    out["SMA200_Slope"] = sma200_slope
    out["Mom20"] = mom20
    out["Mom60"] = mom60

    out["VIX_Slope"] = vix_slope
    out["VIX_Z60"] = vix_z
    out["HV20"] = hv20
    out["HV60"] = hv60
    out["HV_Ratio"] = hv_ratio

    out["HY_OAS"] = hy_oas
    out["HY_OAS_Z60"] = hy_oas_z
    out["Fin_Stress"] = fin_stress
    out["WALCL_20d"] = walcl_chg20
    out["RRP_20d"] = rrp_chg20

    out["Trend_Score"] = trend_score
    out["Vol_Score"] = vol_score
    out["Credit_Score"] = credit_score
    out["Risk_Score"] = risk_score

    return out.replace([np.inf, -np.inf], np.nan).dropna()

# ---------------- FEATURES & TARGETS ----------------
def engineer_ml_features(df, score_df):
    print("--- 2) Build ML Feature Table + Forward Targets ---")
    spy = df["SPY"]

    feat = score_df.copy()

    if "T10Y2Y" in df.columns:
        feat["Yield_Curve"] = df["T10Y2Y"]
    if "Credit_Ratio" in df.columns:
        feat["Credit_Ratio"] = df["Credit_Ratio"]
        feat["Credit_Z60"] = zscore(df["Credit_Ratio"], 60)

    feat["VIX_Level"] = df["VIX"]

    # Targets
    fmin20, _ = forward_min_max(spy, LKA_RISKOFF)
    dd20 = (fmin20 - spy) / spy

    fmin10, _ = forward_min_max(spy, LKA_SHORT)
    dd10 = (fmin10 - spy) / spy

    _, fmax20 = forward_min_max(spy, LKA_REENTRY)
    rl20 = (fmax20 - spy) / spy

    feat["Target_RiskOff20"] = (dd20 < DD_RISKOFF).astype(int)
    feat["Target_Short10"] = (dd10 < DD_SHORT).astype(int)
    feat["Target_Reentry20"] = (rl20 > RALLY_REENTRY).astype(int)

    return feat.dropna()

# ---------------- TRAINING ----------------
def train_window_models(feat):
    print("--- 3) Train Window Models ---")
    targets = ["Target_RiskOff20", "Target_Short10", "Target_Reentry20"]
    X = feat.drop(columns=targets)

    # Time-Split
    X_train = X[X.index < SPLIT_DATE]
    X_test  = X[X.index >= SPLIT_DATE]

    def train_one(yname, weight_pos=2.0, rs=42, n_est=100, lr=0.05):
        y = feat[yname]
        y_train = y[y.index < SPLIT_DATE]
        w = np.where(y_train==1, weight_pos, 1.0)

        clf = GradientBoostingClassifier(
            n_estimators=n_est,
            learning_rate=lr,
            max_depth=3,
            random_state=rs
        )
        clf.fit(X_train, y_train, sample_weight=w)
        
        acc = clf.score(X_train, y_train)
        print(f"Train Acc {yname}: {acc:.2%}")
        return clf

    mA = train_one("Target_RiskOff20", weight_pos=3.0, rs=1, n_est=160, lr=0.05)
    mB = train_one("Target_Short10",   weight_pos=2.5, rs=7, n_est=130, lr=0.06)
    mC = train_one("Target_Reentry20", weight_pos=1.8, rs=99, n_est=130, lr=0.05)

    return X, mA, mB, mC

# ---------------- STRATEGY V18.3 ----------------
def run_v18_3_strategy(df, score_df, feat, X, modelA, modelB, modelC):
    print("--- 4) Run V18.3 Fast Response Strategy ---")

    idx_all = X.index

    p_riskoff = pd.Series(modelA.predict_proba(X)[:, 1], index=idx_all)
    p_short = pd.Series(modelB.predict_proba(X)[:, 1], index=idx_all)
    p_reentry = pd.Series(modelC.predict_proba(X)[:, 1], index=idx_all)

    d = df.loc[idx_all].copy()
    s = score_df.loc[idx_all].copy()

    d_test = d[d.index >= SPLIT_DATE].copy()
    s_test = s[s.index >= SPLIT_DATE].copy()

    pA = p_riskoff[p_riskoff.index >= SPLIT_DATE]
    pB = p_short[p_short.index >= SPLIT_DATE]
    pC = p_reentry[p_reentry.index >= SPLIT_DATE]

    spy = d_test["SPY"]
    vix = d_test["VIX"]
    sma200 = spy.rolling(200).mean()

    # VIX phases
    vix_fast = vix.rolling(VIX_FAST).mean()
    vix_slow = vix.rolling(VIX_SLOW).mean()
    vix_mom = vix.pct_change(VIX_MOM_WIN)

    vix_p80 = vix.rolling(252).quantile(0.80)

    states = pd.Series(1, index=d_test.index)  # 1 long, 0 cash, -1 short
    trades = []

    current = 1
    entry_price = 0.0
    entry_date = None

    # Precompute score acceleration
    score_series = s_test["Risk_Score"]
    score_chg5 = score_series.diff(5)

    for i in range(200, len(d_test)):
        idx = d_test.index[i]
        price = spy.iloc[i]

        score = float(score_series.iloc[i])
        pr = float(pA.iloc[i])
        ps = float(pB.iloc[i])
        pre = float(pC.iloc[i])

        trend_broken = price < sma200.iloc[i]
        sma200_slope = float(s_test["SMA200_Slope"].iloc[i]) if "SMA200_Slope" in s_test.columns else 0.0

        vix_rising = (vix_fast.iloc[i] > vix_slow.iloc[i]) and (vix_mom.iloc[i] > 0)
        vix_peaking = (vix_fast.iloc[i] > vix_slow.iloc[i]) and (vix_mom.iloc[i] < 0)

        panic_here = vix.iloc[i] > max(24, vix_p80.iloc[i] if not np.isnan(vix_p80.iloc[i]) else 24)
        
        vol_score = float(s_test["Vol_Score"].iloc[i]) if "Vol_Score" in s_test.columns else 0.0
        stress_jump = float(score_chg5.iloc[i]) > SCORE_JUMP_5D

        # ----------------
        # 1) Risk Window
        # ----------------
        risk_window = (score >= SCORE_RISKOFF) or (pr > TH_P_RISKOFF) or stress_jump

        # ----------------
        # 2) Trend Confirm (OPTIONAL bei Stress Jump!)
        # ----------------
        is_downtrend = trend_broken or (sma200_slope < 0)
        
        # V18.3 Upgrade: Wenn Stress Jump, ignorieren wir Trend!
        trend_confirm = is_downtrend or stress_jump

        # ----------------
        # 3) Short Boost
        # ----------------
        short_boost = (
            (ps > TH_P_SHORT) or
            vix_rising or
            (vol_score > 18) or
            stress_jump
        )

        # ----------------
        # 4) Allow Short
        # ----------------
        allow_short = risk_window and trend_confirm and short_boost and (panic_here or stress_jump)

        # ----------------
        # 5) Cash-Entscheidung
        # ----------------
        prefer_cash = risk_window and not short_boost and (trend_broken or score >= SCORE_SHORT)

        # ----------------
        # 6) Reentry
        # ----------------
        allow_reentry = (
            (score < SCORE_SAFE) or
            (pr < 0.48) or
            vix_peaking or
            (pre > TH_P_REENTRY)
        )

        # ----------------
        # State machine
        # ----------------
        if current == 1:
            if allow_short:
                current = -1
                entry_price = price
                entry_date = idx
                trades.append({
                    "entry": idx.strftime("%Y-%m-%d"),
                    "type": "SHORT",
                    "p_riskoff": round(pr, 3),
                    "p_short": round(ps, 3),\
                    "score": round(score, 1),
                    "boost": "ACCEL" if stress_jump else "STD"
                })
            elif prefer_cash:
                current = 0

        elif current == -1:
            if allow_reentry:
                pnl = (entry_price - price) / entry_price * 100.0
                trades[-1]["exit"] = idx.strftime("%Y-%m-%d")
                trades[-1]["days"] = (idx - entry_date).days if entry_date else None
                trades[-1]["pnl"] = round(pnl, 2)
                trades[-1]["reason"] = (
                    "VIX_PEAK" if vix_peaking else 
                    "SCORE_COOL" if score < SCORE_SAFE else "REENTRY_ML"
                )
                current = 1
                entry_price = 0.0

        elif current == 0:
            if allow_reentry:
                current = 1
            elif allow_short:
                current = -1
                entry_price = price
                entry_date = idx
                trades.append({
                    "entry": idx.strftime("%Y-%m-%d"),
                    "type": "SHORT",
                    "p_riskoff": round(pr, 3),
                    "p_short": round(ps, 3),\
                    "score": round(score, 1),
                    "boost": "ACCEL" if stress_jump else "STD"
                })

        states.iloc[i] = current

    # Performance
    spy_ret = spy.pct_change().fillna(0.0)
    strat_ret = states.shift(1).fillna(1) * spy_ret

    cum_mkt = (1 + spy_ret).cumprod()
    cum_strat = (1 + strat_ret).cumprod()
    
    total_ret = (cum_strat.iloc[-1] - 1) * 100.0
    mkt_ret = (cum_mkt.iloc[-1] - 1) * 100.0
    
    dd_curve = (cum_strat - cum_strat.cummax()) / cum_strat.cummax()
    max_dd = dd_curve.min() * 100.0

    print(f"\nV18.3 Test Result (ab {SPLIT_DATE}):")
    print(f"Buy & Hold:     {mkt_ret:.1f}%")
    print(f"V18.3 Strategy: {total_ret:.1f}%")
    print(f"Max DD:         {max_dd:.1f}%")
    print(f"Short-Tage:     {(states == -1).sum()}")
    print(f"Cash-Tage:      {(states == 0).sum()}")
    print(f"Trades:         {len(trades)}")

    # CSV Export
    out_df = pd.DataFrame(index=d_test.index)
    out_df["State"] = states
    out_df["Score"] = s_test["Risk_Score"]
    out_df["P_RiskOff"] = pA
    out_df["P_Short"] = pB
    out_df["Entry_Signal"] = 0
    
    # Mark entries
    for t in trades:
        try:
            ent = t["entry"]
            if ent in out_df.index:
                out_df.loc[ent, "Entry_Signal"] = -1 if t["type"] == "SHORT" else 1
        except:
            pass

    ensure_outdir(OUTFILE)
    out_df.to_csv(OUTFILE)
    print(f"\n[OK] Exportiert: {OUTFILE}")

    # Print Trades
    print("\nTrades (first 30):")
    for t in trades[:30]:
        print(t)
        
    # Feature Importance (RiskOff)
    imp = pd.Series(modelA.feature_importances_, index=feat.drop(columns=["Target_RiskOff20", "Target_Short10", "Target_Reentry20"]).columns)
    print("\nTop RiskOff Features:")
    print(imp.sort_values(ascending=False).head(10))

# ---------------- MAIN ----------------
def run_main():
    df = fetch_data()
    score_df = compute_risk_score(df)
    feat = engineer_ml_features(df, score_df)
    
    # Train
    X, mA, mB, mC = train_window_models(feat)
    
    # Run V18.3
    run_v18_3_strategy(df, score_df, feat, X, mA, mB, mC)

if __name__ == "__main__":
    run_main()
