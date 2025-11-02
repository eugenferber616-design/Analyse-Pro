# scripts/build_direction_signal.py
# Erzeugt data/processed/direction_signal.csv mit Spalten:
# symbol, dir, strength, next_expiry, nearest_dte, focus_strike

import os, math, gzip
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path("data/processed")
OUTP = BASE / "direction_signal.csv"
OUTP.parent.mkdir(parents=True, exist_ok=True)

# ---------- Helper ----------
def nz(x, v=0.0):
    try:
        return v if pd.isna(x) else float(x)
    except Exception:
        return v

def sigmoid(x):
    # numerisch stabile Sigmoid
    x = max(-20.0, min(20.0, float(x)))
    return 1.0 / (1.0 + math.exp(-x))

def normalize_strength(raw_abs, conc=0.0):
    # 0..100
    base = sigmoid(raw_abs)  # 0..1
    # Konzentration (0..1) erhöht die Sicherheit leicht
    conf = base * (0.6 + 0.4 * max(0.0, min(1.0, conc)))
    return int(round(conf * 100))

def herfindahl(series):
    v = pd.to_numeric(series, errors="coerce").fillna(0.0).values
    s = float(v.sum())
    if s <= 0: return 0.0
    return float(((v / s) ** 2).sum())

def nearest_block(df_ex, today=None):
    if df_ex is None or df_ex.empty: return None
    g = df_ex.copy()
    if "expiry" not in g.columns: return None
    g["expiry"] = pd.to_datetime(g["expiry"], errors="coerce")
    g = g.dropna(subset=["expiry"])
    g["dte"] = (g["expiry"] - pd.Timestamp.today().normalize()).dt.days
    g = g[g["dte"] >= 0]
    if g.empty: return None
    row = g.sort_values("dte", ascending=True).iloc[0]
    fs = pd.to_numeric(row.get("focus_strike", np.nan), errors="coerce")
    if pd.isna(fs):
        # heuristisch: Strike mit max(total_oi) in diesem Verfall (falls vorhanden)
        one = g[g["expiry"] == row["expiry"]]
        if "total_oi" in one.columns and "strike" in one.columns:
            _r = one.sort_values("total_oi", ascending=False).iloc[0]
            fs = _r.get("strike", np.nan)
    return dict(next_expiry=row["expiry"].date().isoformat(),
                nearest_dte=int(row["dte"]),
                focus_strike=None if pd.isna(fs) else float(fs))

# ---------- 1) Wenn options_signals.csv existiert → direkt normieren ----------
sig_csv = BASE / "options_signals.csv"
if sig_csv.exists():
    df = pd.read_csv(sig_csv)
    # Erwartete Spalten: symbol, dir, strength, next_expiry, nearest_dte, focus_strike (oder Teilmenge)
    cols = {c.lower(): c for c in df.columns}
    out = []
    for _, r in df.iterrows():
        sym = str(r[cols.get("symbol")]).upper().strip()
        if not sym: 
            continue
        dirv = int(nz(r.get(cols.get("dir"), np.nan), 0.0))
        strength = int(round(nz(r.get(cols.get("strength"), np.nan), 0.0)))
        nexp = r.get(cols.get("next_expiry")) if cols.get("next_expiry") else ""
        ndte = r.get(cols.get("nearest_dte")) if cols.get("nearest_dte") else ""
        fstr = r.get(cols.get("focus_strike")) if cols.get("focus_strike") else ""
        out.append(dict(symbol=sym, dir=dirv, strength=int(max(0, min(100, strength))),
                        next_expiry=nexp, nearest_dte=ndte, focus_strike=fstr))
    pd.DataFrame(out).to_csv(OUTP, index=False)
    print("wrote", OUTP, "rows=", len(out))
    raise SystemExit(0)

# ---------- 2) Sonst aus summary + by_expiry bauen (robust & simpel) ----------
sumo = None
byex = None

try:
    sumo = pd.read_csv(BASE / "options_oi_summary.csv")
except Exception:
    pass
try:
    byex = pd.read_csv(BASE / "options_oi_by_expiry.csv")
except Exception:
    pass

if sumo is None or sumo.empty:
    pd.DataFrame([], columns=["symbol","dir","strength","next_expiry","nearest_dte","focus_strike"]).to_csv(OUTP, index=False)
    print("wrote", OUTP, "rows=0 (no inputs)")
    raise SystemExit(0)

for c in ["call_oi","put_oi","total_oi","call_iv_w","put_iv_w"]:
    if c in sumo.columns:
        sumo[c] = pd.to_numeric(sumo[c], errors="coerce")

if byex is not None and not byex.empty:
    for c in ["call_oi","put_oi","total_oi","strike"]:
        if c in byex.columns:
            byex[c] = pd.to_numeric(byex[c], errors="coerce")
    if "expiry" in byex.columns:
        byex["expiry"] = pd.to_datetime(byex["expiry"], errors="coerce")

W1 = float(os.getenv("DIR_W_PCR", 0.6))
W2 = float(os.getenv("DIR_W_IV", 0.3))
W3 = float(os.getenv("DIR_W_TR", 0.2))
DEAD = float(os.getenv("DIR_DEAD", 0.15))
WALL_DTE = int(os.getenv("DIR_WALL_DTE", 7))
WALL_DAMP = float(os.getenv("DIR_WALL_DAMP", 0.6))
PC_MIN = float(os.getenv("DIR_PC_MIN", 0.3))
PC_MAX = float(os.getenv("DIR_PC_MAX", 3.0))

out = []
for sym, g in sumo.groupby("symbol", sort=False):
    symU = str(sym).upper().strip()
    if not symU:
        continue

    put_sum  = nz(g.get("put_oi", np.nan).sum(), 0.0)
    call_sum = nz(g.get("call_oi", np.nan).sum(), 0.0)
    pc = put_sum / max(1.0, call_sum)
    pc = max(PC_MIN, min(PC_MAX, pc))

    iv_call = pd.to_numeric(g.get("call_iv_w", np.nan), errors="coerce")
    iv_put  = pd.to_numeric(g.get("put_iv_w",  np.nan), errors="coerce")
    iv_skew = float((iv_put - iv_call).dropna().mean()) if iv_put is not None else 0.0   # >0 = Down-Bias

    # einfacher Trend-Proxy: total_oi Slope über Expiries (wenn vorhanden)
    tr = 0.0
    try:
        x = pd.to_numeric(g.get("total_oi"), errors="coerce").fillna(0.0).values
        if x.size >= 3:
            # lineare Steigung
            xi = np.arange(x.size)
            coef = np.polyfit(xi, x, 1)[0]
            tr = np.sign(coef)
    except Exception:
        pass

    raw = (W1 * (1.0 / pc - 1.0)) + (W2 * (-iv_skew)) + (W3 * tr)
    dir_num = 0
    if raw > DEAD: dir_num = 1
    elif raw < -DEAD: dir_num = -1

    # Konzentration über Expiry (Herfindahl) – nur zur Confidence
    conc = 0.0
    if byex is not None and not byex.empty:
        ge = byex[byex["symbol"].str.upper() == symU] if "symbol" in byex.columns else None
        if ge is not None and not ge.empty and "total_oi" in ge.columns:
            conc = herfindahl(ge.groupby("expiry")["total_oi"].sum())

        # Wall-Bremse + Next-Block/Strike
        nb = nearest_block(ge)
    else:
        ge = None
        nb = None

    if nb:
        if nb["nearest_dte"] <= WALL_DTE and (ge is not None and not ge.empty):
            # „nahe Wall“ → Dämpfen
            raw *= WALL_DAMP
        strength = normalize_strength(abs(raw), conc=conc)
        out.append(dict(symbol=symU, dir=dir_num, strength=strength,
                        next_expiry=nb.get("next_expiry",""),
                        nearest_dte=nb.get("nearest_dte",""),
                        focus_strike=nb.get("focus_strike","")))
    else:
        strength = normalize_strength(abs(raw), conc=conc)
        out.append(dict(symbol=symU, dir=dir_num, strength=strength,
                        next_expiry="", nearest_dte="", focus_strike=""))

pd.DataFrame(out).to_csv(OUTP, index=False)
print("wrote", OUTP, "rows=", len(out))
