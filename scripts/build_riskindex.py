#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_riskindex.py  —  erweitert um:
  • Binären RiskIndex (8 Prädiktoren, Pine-Logik)
  • Funding-Stress (SOFR−IORB, WRESBAL, TGA, Bills<IORB)
  • Rebalance-Kanal / Bull-Steepener (Gates g1..g4)
  • Flow-Pressure (RV, CTA-Breaks, 21d SPY−TLT)

Inputs (so viele wie vorhanden):
  - data/processed/fred_core.csv.gz        (FRED-Makroserien)
  - data/processed/market_core.csv.gz      (VIX/DXY/USDJPY/HYG/LQD/XLF/SPY …)
  - data/processed/fred_oas.csv.gz         (optional; IG/HY OAS)

Outputs:
  - data/processed/riskindex_snapshot.json
  - data/processed/riskindex_timeseries.csv  (falls genügend Overlap)
"""

from __future__ import annotations
import os, math, json, sys
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np

F_FRED   = Path("data/processed/fred_core.csv.gz")
F_OAS    = Path("data/processed/fred_oas.csv.gz")
F_MARKET = Path("data/processed/market_core.csv.gz")
OUTDIR   = Path("data/processed")

# ----------------- Helpers -----------------
def _read_df(p: Path) -> pd.DataFrame:
    """CSV robust lesen: 'date' parse, Index=DatetimeIndex, Spaltennamen normalisieren."""
    if not p.exists():
        print(f"WARN: Datei fehlt → {p}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(p, compression="infer")
    except Exception as e:
        print(f"WARN: CSV-Read fehlgeschlagen ({p}): {e}")
        return pd.DataFrame()
    df.columns = [str(c).strip() for c in df.columns]
    date_col = None
    for cand in ("date", "Date", "DATE"):
        if cand in df.columns:
            date_col = cand
            break
    if date_col is None:
        date_col = df.columns[0]
    try:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_localize(None)
    except Exception:
        pass
    df = df.rename(columns={date_col: "date"}).set_index("date").sort_index()
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="ignore")
    return df

def _daily_ffill(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    idx = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.reindex(idx).ffill()

def _zscore(s: pd.Series, win: int = 252, minp: int | None = None) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if minp is None:
        minp = max(20, win//4)
    mu = s.rolling(win, min_periods=minp).mean()
    sd = s.rolling(win, min_periods=minp).std(ddof=0)
    z = (s - mu) / sd
    return z.replace([np.inf, -np.inf], np.nan)

def _score_from_z(z: float | None, invert: bool=False) -> float | None:
    if z is None or pd.isna(z):
        return None
    # 50 +/- 10 pro std — auf 0..100 clamp
    s = 50 + 10 * ((-z) if invert else z)
    return float(max(0, min(100, s)))

def _last(s: pd.Series | None) -> float | None:
    if s is None or s.empty:
        return None
    v = s.iloc[-1]
    return None if pd.isna(v) else float(v)

def _has(df: pd.DataFrame, *cols: str) -> bool:
    if df.empty: return False
    for c in cols:
        if c not in df.columns: return False
    return True

def _s(df: pd.DataFrame, col: str) -> pd.Series | None:
    return df[col] if _has(df, col) else None

# ----------------- Main -----------------
def main() -> int:
    OUTDIR.mkdir(parents=True, exist_ok=True)

    dfF = _read_df(F_FRED)
    dfM = _read_df(F_MARKET)
    dfO = _read_df(F_OAS) if F_OAS.exists() else pd.DataFrame()

    if dfF.empty and dfM.empty and dfO.empty:
        print("ERROR: Keine Eingabedateien gefunden – breche ohne Snapshot ab.")
        return 0  # weich raus, damit Workflow nicht scheitert

    # Tagesfrequenz & ffill
    if not dfF.empty: dfF = _daily_ffill(dfF)
    if not dfM.empty: dfM = _daily_ffill(dfM)
    if not dfO.empty: dfO = _daily_ffill(dfO)

    # Case-Normalisierung (Uppercase)
    if not dfM.empty: dfM.columns = [str(c).strip().upper() for c in dfM.columns]
    if not dfF.empty: dfF.columns = [str(c).strip().upper() for c in dfF.columns]
    if not dfO.empty: dfO.columns = [str(c).strip().upper() for c in dfO.columns]

    # Join
    dfs = [d for d in (dfF, dfM, dfO) if not d.empty]
    df  = pd.concat(dfs, axis=1).sort_index().ffill()
    if df.empty:
        print("WARN: Nach Join keine Daten – Snapshot übersprungen.")
        return 0

    cols_available = set(df.columns)

    # ========== Kontinuierliche Z-Scores / Composite (wie vorher) ==========
    W = 252
    S  = lambda c: _s(df, c)

    # FRED / Funding grob
    z_dgs30  = _zscore(S("DGS30"), W)                             if S("DGS30") is not None else None
    z_2s30s  = _zscore(S("DGS30") - S("DGS2"), W)                 if _has(df,"DGS30","DGS2") else None
    z_sofr30 = _zscore((S("SOFR") - S("SOFR").shift(30))*100, W)  if S("SOFR") is not None else None
    rrp_pct  = S("RRPONTSYD").rank(pct=True)                      if S("RRPONTSYD") is not None else None
    z_stlfsi = _zscore(S("STLFSI4"), W)                           if S("STLFSI4") is not None else None

    # Market
    z_vix    = _zscore(S("VIX"), W)                               if S("VIX") is not None else None
    z_vxterm = _zscore(S("VIX3M") - S("VIX"), W)                  if _has(df,"VIX3M","VIX") else None
    z_dxy    = _zscore(S("DXY"), W)                               if S("DXY") is not None else None

    if S("USDJPY") is not None:
        usdvol = S("USDJPY").pct_change().rolling(20).std() * math.sqrt(252) * 100
        z_usdvol = _zscore(usdvol, W)
    else:
        z_usdvol = None

    if _has(df,"HYG","LQD"):
        rel = (S("HYG")/S("LQD"))
        z_cr30 = _zscore((rel - rel.shift(30))*100, W)
    else:
        z_cr30 = None

    z_10s2   = _zscore(S("DGS10") - S("DGS2"),   W)               if _has(df,"DGS10","DGS2")   else None
    z_10s3m  = _zscore(S("DGS10") - S("DGS3MO"), W)               if _has(df,"DGS10","DGS3MO") else None

    if _has(df,"XLF","SPY"):
        relfs = (S("XLF")/S("SPY"))
        z_relfin30 = _zscore((relfs - relfs.shift(30))*100, W)
    else:
        z_relfin30 = None

    if S("DGS10") is not None:
        ust10v = S("DGS10").diff().rolling(20).std() * math.sqrt(252)
        z_ust10v = _zscore(ust10v, W)
    else:
        z_ust10v = None

    if _has(df,"WALCL","WTREGEN","RRPONTSYD","WRESBAL"):
        netliq = (S("WALCL") - S("WTREGEN") - S("RRPONTSYD") - S("WRESBAL"))/1e3
        z_netliq30 = _zscore(netliq - netliq.shift(30), W)
    else:
        z_netliq30 = None

    z_ig_oas = _zscore(S("IG_OAS"), W) if S("IG_OAS") is not None else None
    z_hy_oas = _zscore(S("HY_OAS"), W) if S("HY_OAS") is not None else None

    scores_cont = {
        "dgs30"  : _score_from_z(_last(z_dgs30)),
        "2s30s"  : _score_from_z(_last(z_2s30s)),
        "sofr"   : _score_from_z(_last(z_sofr30)),
        "rrp"    : (1.0 - float(_last(rrp_pct))) * 100.0 if rrp_pct is not None and _last(rrp_pct) is not None else None,
        "stlfsi" : _score_from_z(_last(z_stlfsi)),

        "vix"    : _score_from_z(_last(z_vix)),
        "usdvol" : _score_from_z(_last(z_usdvol)),
        "dxy"    : _score_from_z(_last(z_dxy)),
        "cr"     : _score_from_z(_last(z_cr30)),
        "vxterm" : _score_from_z(_last(z_vxterm)),

        "10s2s"  : _score_from_z(_last(z_10s2),  invert=True),
        "10s3m"  : _score_from_z(_last(z_10s3m), invert=True),
        "relfin" : _score_from_z(_last(z_relfin30), invert=True),
        "ust10v" : _score_from_z(_last(z_ust10v)),
        "netliq" : _score_from_z(_last(z_netliq30), invert=True),

        "ig_oas" : _score_from_z(_last(z_ig_oas)) if z_ig_oas is not None else None,
        "hy_oas" : _score_from_z(_last(z_hy_oas)) if z_hy_oas is not None else None,
    }
    sc_vals = [v for v in scores_cont.values() if v is not None]
    sc_comp = float(sum(sc_vals)/len(sc_vals)) if sc_vals else None

    # ========== Binärer RiskIndex (8 Prädiktoren wie Pine) ==========
    # Parameter (aus Pine)
    len_z              = 252
    roc_win            = 21
    smooth_len         = 5
    risk_lookback      = 252
    breadth_threshold  = 0.615

    def _zchg(series: pd.Series, win=roc_win, zlen=len_z):
        if series is None: return None
        chg = series - series.shift(win)
        return _zscore(chg, zlen)

    p_credit = None
    if _has(df,"HYG","LQD"):
        z_credit = _zchg(S("HYG")/S("LQD"))
        p_credit = 1 if (_last(-z_credit) is not None and _last(-z_credit) > 0.5) else 0

    p_vixterm = None
    if _has(df,"VIX","VIX3M"):
        p_vixterm = 1 if _last(S("VIX")) is not None and _last(S("VIX3M")) is not None and (_last(S("VIX")) >= _last(S("VIX3M"))) else 0

    p_curve = None
    if _has(df,"DGS10","DGS2") or _has(df,"DGS10","DGS3MO"):
        inv_10s2  = (_last(S("DGS10")) - _last(S("DGS2")))   if _has(df,"DGS10","DGS2")   else None
        inv_10s3m = (_last(S("DGS10")) - _last(S("DGS3MO"))) if _has(df,"DGS10","DGS3MO") else None
        p_curve   = 1 if ((inv_10s2 is not None and inv_10s2 < 0) or (inv_10s3m is not None and inv_10s3m < 0)) else 0

    p_fin = None
    if _has(df,"XLF","SPY"):
        rel_fin = (S("XLF")/S("SPY"))
        rel_fin_sma200 = rel_fin.rolling(200,min_periods=100).mean()
        rel_fin_low50  = rel_fin.rolling(50, min_periods=25).min()
        last_rel = _last(rel_fin)
        last_sma= _last(rel_fin_sma200)
        last_lo = _last(rel_fin_low50)
        p_fin   = 1 if (None not in (last_rel,last_sma,last_lo) and last_rel < last_sma and last_rel < last_lo) else 0

    p_tvol = None
    if S("MOVE") is not None:
        z_tvol = _zchg(S("MOVE"))
        p_tvol = 1 if (_last(z_tvol) is not None and _last(z_tvol) > 0.5) else 0

    p_gold = None
    if _has(df,"XAUUSD","SPY"):
        goldspy = S("XAUUSD")/S("SPY")
        z_g = _zchg(goldspy)
        p_gold = 1 if (_last(z_g) is not None and _last(z_g) > 0.5) else 0

    p_dxy = None
    if S("DXY") is not None:
        z_dx = _zchg(S("DXY"))
        p_dxy = 1 if (_last(z_dx) is not None and _last(z_dx) > 0.5) else 0

    p_breadth = None
    if S("S5TH") is not None:
        s5 = S("S5TH")
        s5_sma = s5.rolling(10, min_periods=5).mean()
        # Crossover (t heute > sma heute, gestern <=)
        cross = (s5 > s5_sma) & (s5.shift(1) <= s5_sma.shift(1))
        ratio = s5 / s5_sma
        ok    = (ratio > breadth_threshold) & cross
        p_breadth = 1 if (ok.iloc[-1] if len(ok) else False) else 0

    preds = [p_credit,p_vixterm,p_curve,p_fin,p_tvol,p_gold,p_dxy,p_breadth]
    preds_avail = [p for p in preds if p is not None]
    score_bin = sum(preds_avail) if preds_avail else None

    # Glätten + Min-Max-Skalierung wie Pine (über History):
    risk_index_bin = None
    if preds_avail:
        # Baue die Zeitreihe der binären Summe:
        def _series_from_preds_df(df: pd.DataFrame) -> pd.Series:
            # rekonstruiere jedes Prädikat als Serie (vectorisiert, wo möglich)
            # Für Einfachheit: benutze dieselben Regeln historisch (approx).
            out = pd.Series(0, index=df.index, dtype=float)

            # credit
            if _has(df,"HYG","LQD"):
                z_credit_all = _zscore((df["HYG"]/df["LQD"]).diff(roc_win), len_z)
                out += ((-z_credit_all) > 0.5).astype(float).fillna(0)

            # vixterm
            if _has(df,"VIX","VIX3M"):
                out += (df["VIX"] >= df["VIX3M"]).astype(float).fillna(0)

            # curve
            part = pd.Series(0, index=df.index, dtype=float)
            if _has(df,"DGS10","DGS2"):
                part = part | ((df["DGS10"] - df["DGS2"]) < 0)
            if _has(df,"DGS10","DGS3MO"):
                part = part | ((df["DGS10"] - df["DGS3MO"]) < 0)
            out += part.astype(float)

            # rel fin
            if _has(df,"XLF","SPY"):
                rel = (df["XLF"]/df["SPY"])
                sma200 = rel.rolling(200,min_periods=100).mean()
                low50  = rel.rolling(50,min_periods=25).min()
                out += ((rel < sma200) & (rel < low50)).astype(float).fillna(0)

            # tvol (MOVE)
            if _has(df,"MOVE"):
                zt = _zscore(df["MOVE"].diff(roc_win), len_z)
                out += (zt > 0.5).astype(float).fillna(0)

            # gold/spy
            if _has(df,"XAUUSD","SPY"):
                zgs = _zscore((df["XAUUSD"]/df["SPY"]).diff(roc_win), len_z)
                out += (zgs > 0.5).astype(float).fillna(0)

            # dxy
            if _has(df,"DXY"):
                zdx = _zscore(df["DXY"].diff(roc_win), len_z)
                out += (zdx > 0.5).astype(float).fillna(0)

            # breadth thrust (approx crossover)
            if _has(df,"S5TH"):
                s5 = df["S5TH"]
                s5s= s5.rolling(10,min_periods=5).mean()
                cross = (s5 > s5s) & (s5.shift(1) <= s5s.shift(1))
                ratio = s5 / s5s
                ok = (ratio > breadth_threshold) & cross
                out += ok.astype(float).fillna(0)

            return out

        sum_series = _series_from_preds_df(df)
        smooth = sum_series.rolling(smooth_len, min_periods=1).mean()
        lo = smooth.rolling(risk_lookback, min_periods=20).min()
        hi = smooth.rolling(risk_lookback, min_periods=20).max()
        rng = (hi - lo).replace(0, np.nan)
        risk_series = 100.0 * (smooth - lo) / rng
        risk_index_bin = float(risk_series.iloc[-1]) if pd.notna(risk_series.iloc[-1]) else None

    # ========== Funding-Stress (exakt nach Logik) ==========
    # Schwellen wie Pine
    thr_sofr_iorb = 0.05   # 5 bp
    thr_res_drop  = 70.0   # Mrd $
    thr_tga_rise  = 100.0  # Mrd $
    # Bills < IORB: wähle vorhandene Bill-Serie
    bill_series = S("DGS1MO") if S("DGS1MO") is not None else S("DTB4WK")

    # Weekly-Approx mit 7d-Diff (Original nutzt echte W)
    fs_rate_warn = fs_liq_warn = fs_bill_warn = False
    if _has(df,"SOFR","IORB"):
        fs_rate_warn = (_last(S("SOFR")) - _last(S("IORB"))) > thr_sofr_iorb
    # Reserves (WRESBAL) und TGA (WDTGAL/WTREGEN)
    tga = S("WDTGAL") if S("WDTGAL") is not None else S("WTREGEN")
    if S("WRESBAL") is not None or tga is not None:
        res_drop_w = None
        tga_rise_w = None
        if S("WRESBAL") is not None:
            res_drop_w = (_last(S("WRESBAL").shift(7)) - _last(S("WRESBAL"))) / 1000.0
        if tga is not None:
            tga_rise_w = (_last(tga) - _last(tga.shift(7))) / 1000.0
        fs_liq_warn = ((res_drop_w or 0) > thr_res_drop) or ((tga_rise_w or 0) > thr_tga_rise)
    if bill_series is not None and S("IORB") is not None:
        fs_bill_warn = _last(bill_series) is not None and _last(S("IORB")) is not None and (_last(bill_series) < (_last(S("IORB")) - 0.01))

    fs_score = (1 if fs_rate_warn else 0) + (1 if fs_liq_warn else 0) + (1 if fs_bill_warn else 0)

    # ========== Rebalance-Kanal / Bull-Steepener ==========
    # Gates: Steepen 10s2, Duration + (TLT), Credit-Entlastung (HYG/LQD), Front-End calm (SOFR-Δ)
    rb_curve_win  = 10
    rb_curve_bp   = 0.15    # pp ≈ 15bp
    rb_tlt_win    = 20
    rb_tlt_thr    = 0.02
    rb_cr_win     = 10
    rb_cr_thr     = -0.01
    rb_sofr_win   = 10
    rb_sofr_bp    = 0.05
    rb_min_gates  = 3
    rb_need_steep = True
    rb_need_any   = True

    g1_steepen = g2_duration = g3_credit = g4_frontcalm = None
    if _has(df,"DGS10","DGS2"):
        curve_10s2 = df["DGS10"] - df["DGS2"]
        g1_steepen = (_last(curve_10s2) - _last(curve_10s2.shift(rb_curve_win))) > rb_curve_bp
    if _has(df,"TLT"):
        tlt_ret = (_last(df["TLT"]) / _last(df["TLT"].shift(rb_tlt_win)) - 1.0) if _last(df["TLT"].shift(rb_tlt_win)) else None
        g2_duration = (tlt_ret is not None) and (tlt_ret > rb_tlt_thr)
    if _has(df,"HYG","LQD"):
        cr = df["HYG"]/df["LQD"]
        cr_move = (_last(cr) / _last(cr.shift(rb_cr_win)) - 1.0) if _last(cr.shift(rb_cr_win)) else None
        g3_credit = (cr_move is not None) and (cr_move < rb_cr_thr)
    if _has(df,"SOFR"):
        g4_frontcalm = (_last(df["SOFR"]) - _last(df["SOFR"].shift(rb_sofr_win))) < rb_sofr_bp

    gates = [g for g in (g1_steepen,g2_duration,g3_credit,g4_frontcalm) if g is not None]
    rb_gates_hit = sum(1 for g in gates if g)
    rebalance_on = (len(gates) >= rb_min_gates and
                    rb_gates_hit >= rb_min_gates and
                    ((g1_steepen if g1_steepen is not None else True) if rb_need_steep else True) and
                    ((g2_duration or g3_credit) if rb_need_any else True) and
                    (g4_frontcalm if g4_frontcalm is not None else True))

    # ========== Flow-Pressure ==========
    # (1) RealVol(SPY)
    flow_vol_len  = 20
    flow_vol_lo   = 0.14
    flow_vol_hi   = 0.24
    flow_adx_lo   = 10
    flow_adx_hi   = 15
    flow_reb_win  = 21
    flow_reb_thr  = 0.02

    flow_vol = flow_cta = flow_reb = 0
    if _has(df,"SPY"):
        log_ret = np.log(df["SPY"]).diff()
        rv = np.sqrt(252) * log_ret.rolling(flow_vol_len, min_periods=10).std()
        rv_now = _last(rv)
        if rv_now is not None:
            flow_vol = 1 if rv_now < flow_vol_lo else (-1 if rv_now > flow_vol_hi else 0)
    # CTA-Breaks: Preis vs. MAs + ADX-Näherung
    if _has(df,"SPY"):
        px = df["SPY"]
        ma20  = px.rolling(20, min_periods=10).mean()
        ma50  = px.rolling(50, min_periods=25).mean()
        ma100 = px.rolling(100,min_periods=50).mean()
        ma200 = px.rolling(200,min_periods=100).mean()
        last_px = _last(px); last_ma20=_last(ma20); last_ma50=_last(ma50); last_ma100=_last(ma100); last_ma200=_last(ma200)
        bearBrks = sum(1 for v in (last_ma20,last_ma50,last_ma100,last_ma200) if (last_px is not None and v is not None and last_px < v))
        bullBrks = sum(1 for v in (last_ma20,last_ma50,last_ma100,last_ma200) if (last_px is not None and v is not None and last_px > v))
        # ADX-Näherung: benutze Preis-Trend-Intensität via |SMA20−SMA50|/px als grobe Proxy
        adx_proxy = None
        if last_ma20 and last_ma50 and last_px:
            adx_proxy = 100 * abs(last_ma20 - last_ma50) / max(1e-9, last_px)
        if bearBrks >= 3 and (adx_proxy or 0) > flow_adx_hi:
            flow_cta = -1
        elif bullBrks >= 3 and (adx_proxy or 0) < flow_adx_lo:
            flow_cta = 1
        else:
            flow_cta = 0
    # Rebalance-Proxy: 21d SPY−TLT
    if _has(df,"SPY","TLT"):
        spy_ret = (_last(df["SPY"]) / _last(df["SPY"].shift(flow_reb_win)) - 1.0) if _last(df["SPY"].shift(flow_reb_win)) else None
        tlt_ret = (_last(df["TLT"]) / _last(df["TLT"].shift(flow_reb_win)) - 1.0) if _last(df["TLT"].shift(flow_reb_win)) else None
        if spy_ret is not None and tlt_ret is not None:
            diff = spy_ret - tlt_ret
            flow_reb = -1 if diff >  flow_reb_thr else (1 if diff < -flow_reb_thr else 0)
    flow_sum   = (flow_vol or 0) + (flow_cta or 0) + (flow_reb or 0)   # −3…+3
    flow_score = (flow_sum + 3) / 6.0 * 100.0                         # 0…100

    # ========== Regime/One-Liner wie zuvor (leicht angepasst) ==========
    def is_red(v): return v is not None and v >= 70
    gate_hits = sum(is_red(scores_cont.get(k)) for k in ["cr","vix","vxterm","ust10v","relfin","10s2s","10s3m"])
    # Funding-Stress fließt als fs_score (0..3) ein
    fs_pen = 10.0 if fs_score >= 2 else (4.0 if fs_score == 1 else 0.0)
    rg_tip = 70.0 - fs_pen - (3.0 if gate_hits >= 3 else 0.0) - (3.0 if gate_hits >= 5 else 0.0)
    rg_tip = max(50.0, min(90.0, rg_tip))
    rg_d   = (sc_comp - rg_tip) if sc_comp is not None else 0.0

    if sc_comp is None:
        regime = "NEUTRAL"
    else:
        regime = "RISK-OFF" if (rg_d >= 0 and (gate_hits >= 4 or fs_score >= 2)) else \
                 "CAUTION"  if (rg_d >= 0) else \
                 "RISK-ON"  if (gate_hits <= 2 and fs_score <= 1 and rg_d <= -10) else "NEUTRAL"

    bias = "RISK-ON" if (sc_comp is not None and sc_comp < 45) else ("RISK-OFF" if (sc_comp is not None and sc_comp > 55) else "NEUTRAL")
    size = "klein" if (fs_score >= 2 or (scores_cont.get("netliq") or 0) > 60 or flow_sum >= 2) else ("moderat" if flow_sum > -2 else "moderat+")
    dur  = "↓" if ((scores_cont.get("dgs30") or 0) > 60 or (scores_cont.get("ust10v") or 0) > 60) else ("↑" if ((scores_cont.get("dgs30") or 50) < 40 and (scores_cont.get("ust10v") or 50) < 40) else "≙")
    one_liner = f"Bias: {bias} | Größe: {size} | Dur {dur}"

    risks = []
    if (scores_cont.get("netliq") or 0) > 60: risks.append("Liquidität: knapp → Drawdowns können verstärkt werden.")
    if (scores_cont.get("vix") or 0)   > 60: risks.append("Volatilität erhöht → Risiko für High-Beta.")
    if (scores_cont.get("cr") or 0)    > 60: risks.append("Credit Spreads weit → HY/ZYK anfällig.")
    if (scores_cont.get("dxy") or 0)   > 60: risks.append("USD stark → Gegenwind für EM/Gold.")

    notes = [
        "Snapshot nutzt alle verfügbaren Reihen; fehlende Inputs werden ignoriert."
    ]
    if risk_index_bin is None: notes.append("Binärer RiskIndex nicht vollständig (fehlende Prädiktor-Reihen).")
    if fs_score == 0: notes.append("Funding-Stress unauffällig oder Daten unvollständig.")
    if not rebalance_on: notes.append(f"Rebalance-Kanal aus (Gates getroffen: {rb_gates_hit}/{max(3,len(gates))}).")

    # ========== Snapshot ==========
    snap = {
        "asof": datetime.now(timezone.utc).isoformat(),
        "composite": sc_comp,
        "risk_index_bin": risk_index_bin,
        "regime": regime,
        "fs_score": int(fs_score),
        "flow_sum": int(flow_sum),
        "flow_score": float(flow_score),
        "rebalance_on": bool(rebalance_on),
        "rb_gates_hit": int(rb_gates_hit),
        "scores": scores_cont,
        "preds_present": {
            "credit": p_credit is not None, "vixterm": p_vixterm is not None, "curve": p_curve is not None,
            "relfin": p_fin is not None, "tvol": p_tvol is not None, "gold": p_gold is not None,
            "dxy": p_dxy is not None, "breadth": p_breadth is not None
        },
        "one_liner": one_liner,
        "risks": risks,
        "available_columns": sorted(list(cols_available)),
        "notes": notes,
        "action_hints": [
            "Umschichten → Staples/Health/SPLV; ggf. TLT/GLD" if regime in ("CAUTION","RISK-OFF")
            else "Aufstocken → QQQ/IWM/XLF/SPHB; Defensives/Duration reduzieren." if regime == "RISK-ON"
            else "Neutral: Qual/Def mischen, Größe moderat"
        ],
    }

    OUTDIR.mkdir(parents=True, exist_ok=True)
    (OUTDIR / "riskindex_snapshot.json").write_text(json.dumps(snap, ensure_ascii=False, indent=2), encoding="utf-8")
    print("✔ wrote data/processed/riskindex_snapshot.json")

    # ========== Timeseries ==========
    # A) Composite (wie zuvor aus Z-Serien)
    zlist = [z for z in (z_dgs30, z_2s30s, z_sofr30, z_stlfsi, z_vix, z_usdvol, z_dxy, z_cr30,
                         z_vxterm, z_10s2, z_10s3m, z_relfin30, z_ust10v, z_netliq30) if z is not None]
    rows = []
    if zlist:
        idx = df.index
        for dt in idx:
            vals = []
            for z in zlist:
                v = z.get(dt, np.nan) if hasattr(z, "get") else (z.loc[dt] if dt in z.index else np.nan)
                if pd.notna(v):
                    vals.append(50 + 10 * v)
            if len(vals) >= max(6, len(zlist)//3):  # mind. 6 Reihen oder 1/3 der verfügbaren
                rows.append({"date": dt.date().isoformat(), "sc_comp": float(sum(vals)/len(vals))})

    # B) Binärer RiskIndex als Serie (optional)
    if preds_avail:
        # reuse sum_series from above builder
        # to avoid recomputation, regenerate quickly:
        # (Der Builder oben ist in Funktion gekapselt)
        def _bin_ts(df):
            # gleiche Funktion wie oben (kurz)
            out = pd.Series(0.0, index=df.index)
            if _has(df,"HYG","LQD"): out += ((-_zscore((df["HYG"]/df["LQD"]).diff(roc_win), len_z)) > 0.5).astype(float)
            if _has(df,"VIX","VIX3M"): out += (df["VIX"] >= df["VIX3M"]).astype(float)
            part = pd.Series(False, index=df.index)
            if _has(df,"DGS10","DGS2"):   part = part | ((df["DGS10"] - df["DGS2"]) < 0)
            if _has(df,"DGS10","DGS3MO"): part = part | ((df["DGS10"] - df["DGS3MO"]) < 0)
            out += part.astype(float)
            if _has(df,"XLF","SPY"):
                rel = (df["XLF"]/df["SPY"])
                sma200 = rel.rolling(200,min_periods=100).mean()
                low50  = rel.rolling(50,min_periods=25).min()
                out += ((rel < sma200) & (rel < low50)).astype(float).fillna(0)
            if _has(df,"MOVE"): out += (_zscore(df["MOVE"].diff(roc_win), len_z) > 0.5).astype(float).fillna(0)
            if _has(df,"XAUUSD","SPY"): out += (_zscore((df["XAUUSD"]/df["SPY"]).diff(roc_win), len_z) > 0.5).astype(float).fillna(0)
            if _has(df,"DXY"): out += (_zscore(df["DXY"].diff(roc_win), len_z) > 0.5).astype(float).fillna(0)
            if _has(df,"S5TH"):
                s5 = df["S5TH"]; s5s = s5.rolling(10,min_periods=5).mean()
                cross = (s5 > s5s) & (s5.shift(1) <= s5s.shift(1))
                ratio = s5 / s5s
                out += ((ratio > 0.615) & cross).astype(float).fillna(0)
            sm = out.rolling(smooth_len, min_periods=1).mean()
            lo = sm.rolling(risk_lookback, min_periods=20).min()
            hi = sm.rolling(risk_lookback, min_periods=20).max()
            rng = (hi - lo).replace(0, np.nan)
            return 100.0 * (sm - lo) / rng
        rb_ts = _bin_ts(df)
        for i, dt in enumerate(df.index):
            val = rb_ts.iloc[i]
            if pd.notna(val):
                # erweitere vorhandene Row falls vorhanden, sonst lege neue an
                if i < len(rows) and rows[i]["date"] == dt.date().isoformat():
                    rows[i]["risk_index_bin"] = float(val)
                else:
                    rows.append({"date": dt.date().isoformat(), "risk_index_bin": float(val)})

    if rows:
        # sortiere nach Datum, fülle fehlende Felder mit nur einer der beiden Spalten
        df_out = pd.DataFrame(rows).drop_duplicates(subset=["date"])
        df_out = df_out.sort_values("date")
        df_out.to_csv(OUTDIR / "riskindex_timeseries.csv", index=False)
        print("✔ wrote data/processed/riskindex_timeseries.csv rows:", len(df_out))
    else:
        print("timeseries skipped (insufficient overlap)")

    return 0

if __name__ == "__main__":
    sys.exit(main())
