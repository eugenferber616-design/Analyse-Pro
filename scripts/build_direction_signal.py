# scripts/build_direction_signal.py
# Erstellt: data/processed/direction_signal.csv.gz
# Quellen:  data/processed/options_oi_summary.csv(.gz)
#           data/processed/options_oi_by_strike.csv(.gz)

import os, io, csv, gzip, math
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE = Path("data/processed")
OUTP = BASE / "direction_signal.csv.gz"
BASE.mkdir(parents=True, exist_ok=True)

def read_csv_auto(path_no_gz: Path) -> pd.DataFrame:
    p = Path(path_no_gz)
    gz = Path(str(path_no_gz) + ".gz")
    if gz.exists():
        return pd.read_csv(gz, compression="gzip")
    if p.exists():
        return pd.read_csv(p)
    return pd.DataFrame()

def coerce_num(s):
    return pd.to_numeric(s, errors="coerce")

# ---- Laden
sumo = read_csv_auto(BASE / "options_oi_summary.csv")
bystr = read_csv_auto(BASE / "options_oi_by_strike.csv")

# defensive Defaults
for c in ["call_oi","put_oi","total_oi","call_iv_w","put_iv_w"]:
    if c in sumo.columns:
        sumo[c] = coerce_num(sumo[c])

if "expiry" in sumo.columns:
    sumo["expiry"] = pd.to_datetime(sumo["expiry"], errors="coerce")

need_cols = ["symbol","expiry","dte","strike","call_oi","put_oi","total_oi"]
for c in need_cols:
    if c not in bystr.columns:
        bystr[c] = np.nan

bystr["dte"]    = coerce_num(bystr["dte"]).astype("Int64")
bystr["strike"] = coerce_num(bystr["strike"])
for c in ["call_oi","put_oi","total_oi"]:
    bystr[c] = coerce_num(bystr[c])
if "expiry" in bystr.columns:
    bystr["expiry"] = pd.to_datetime(bystr["expiry"], errors="coerce")

# ---- einfache, robuste Richtungs-Heuristik (wie besprochen)
def compute_dir_strength(gsum: pd.DataFrame) -> tuple[int,int]:
    # Put/Call OI Ratio über alle Verfälle
    put_sum  = coerce_num(gsum.get("put_oi", pd.Series(dtype=float))).fillna(0).sum()
    call_sum = coerce_num(gsum.get("call_oi", pd.Series(dtype=float))).fillna(0).sum()
    pc = float(put_sum / max(1.0, call_sum))

    # IV-Skew (put - call), über Verfälle gemittelt
    iv_put  = coerce_num(gsum.get("put_iv_w", pd.Series(dtype=float))).dropna()
    iv_call = coerce_num(gsum.get("call_iv_w", pd.Series(dtype=float))).dropna()
    iv_skew = float((iv_put - iv_call).mean()) if len(iv_put) and len(iv_call) else np.nan

    # grober Trendfilter via nächster Preis-HV (wenn vorhanden in separatem File, hier 0)
    trend = 0.0

    # Gewichte (wie vorgeschlagen)
    W1, W2, W3 = 0.6, 0.3, 0.2
    pc_clip = min(3.0, max(0.3, pc))
    raw = W1*((1.0/pc_clip) - 1.0) + W2*(-(iv_skew if not np.isnan(iv_skew) else 0.0)) + W3*(trend)

    # Dead-zone
    DEAD = 0.15
    if abs(raw) < DEAD:
        d = 0
    else:
        d = 1 if raw > 0 else -1

    # Stärke 0..100 (sigmoid + lineare Skalierung)
    def sigmoid(x):
        return 1.0 / (1.0 + math.exp(-x))
    strength = int(round(100.0 * sigmoid(abs(raw))))
    return d, strength

# ---- Focus-Strike je Horizont
def pick_focus_for_horizon(df_sym: pd.DataFrame, horizon_days: int, dir_num: int) -> float | None:
    if df_sym.empty:
        return None
    # Wunschfenster:
    if horizon_days == 7:
        win = df_sym[(df_sym["dte"].notna()) & (df_sym["dte"] >= 1) & (df_sym["dte"] <= 7)]
        if win.empty:
            # fallback: nächster DTE
            k = df_sym["dte"].dropna()
            if k.empty: return None
            target = int(k[k>=0].min()) if (k>=0).any() else int(k.abs().min())
            win = df_sym[df_sym["dte"] == target]
    else:
        # nimm den Verfall mit minimalem |DTE - horizon|
        df_sym = df_sym[df_sym["dte"].notna()]
        if df_sym.empty: return None
        target = int((df_sym["dte"] - horizon_days).abs().idxmin())
        # idxmin gibt Index → einzelne Zeilen holen (gleicher expiry)
        exp = df_sym.loc[target, "expiry"] if "expiry" in df_sym.columns else None
        if pd.isna(exp):
            # fallback: genau diese Zeile als "Fenster"
            win = df_sym.loc[[target]]
        else:
            win = df_sym[df_sym["expiry"] == exp]

    if win.empty:
        return None

    # Auswahl je Richtung
    if dir_num > 0 and "call_oi" in win.columns:
        row = win.loc[win["call_oi"].idxmax()]
    elif dir_num < 0 and "put_oi" in win.columns:
        row = win.loc[win["put_oi"].idxmax()]
    else:
        row = win.loc[win["total_oi"].idxmax()]

    strike = float(row.get("strike", np.nan))
    return None if np.isnan(strike) else strike

# ---- Nächster Verfall (für allgemeines Focus-Strike)
def nearest_block(df_sym: pd.DataFrame):
    if df_sym.empty: return (None, None, None)
    k = df_sym["dte"].dropna()
    if k.empty: return (None, None, None)
    dte = int(k[k>=0].min()) if (k>=0).any() else int(k.abs().min())
    win = df_sym[df_sym["dte"] == dte]
    exp = pd.NaT
    if "expiry" in df_sym.columns:
        exp = win["expiry"].iloc[0]
    # Focus-Strike nahe Verfall (ohne Richtungsfilter → total_oi)
    row = win.loc[win["total_oi"].idxmax()]
    fs = float(row.get("strike", np.nan))
    return (exp if not pd.isna(exp) else None, dte, (None if np.isnan(fs) else fs))

rows = []
for sym, gsum in sumo.groupby("symbol", sort=False):
    d, s = compute_dir_strength(gsum)

    # passendes by-strike Subset
    gbs = bystr[bystr["symbol"] == sym].copy()
    exp, ndte, fs_general = nearest_block(gbs)

    fs_7  = pick_focus_for_horizon(gbs, 7,  d)
    fs_30 = pick_focus_for_horizon(gbs, 30, d)
    fs_60 = pick_focus_for_horizon(gbs, 60, d)

    rows.append({
        "symbol": sym,
        "dir": int(d),
        "strength": int(s),
        "nearest_expiry": (exp.date().isoformat() if isinstance(exp, pd.Timestamp) else ""),
        "nearest_dte": (int(ndte) if ndte is not None else ""),
        "focus_strike": (fs_general if fs_general is not None else ""),
        "focus_strike_7d":  (fs_7 if fs_7 is not None else ""),
        "focus_strike_30d": (fs_30 if fs_30 is not None else ""),
        "focus_strike_60d": (fs_60 if fs_60 is not None else "")
    })

out_df = pd.DataFrame(rows, columns=[
    "symbol","dir","strength","nearest_expiry","nearest_dte",
    "focus_strike","focus_strike_7d","focus_strike_30d","focus_strike_60d"
])

with gzip.open(str(OUTP), "wt", encoding="utf-8", newline="") as f:
    out_df.to_csv(f, index=False)

print("wrote", OUTP, "rows=", len(out_df))
