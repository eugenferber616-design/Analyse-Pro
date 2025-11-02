import os, math, gzip
import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path("data/processed")
OUTP = BASE / "direction_signal.csv.gz"
OUTP.parent.mkdir(parents=True, exist_ok=True)

# ---------------- CFG (via ENV überschreibbar) ----------------
HORIZONS = [7, 30, 60]
W1 = float(os.getenv("DIR_W_PCR", 0.6))    # Gewicht für Put/Call-Ratio (inverse)
W2 = float(os.getenv("DIR_W_IV",  0.3))    # Gewicht für IV-Skew (negatives Vorzeichen)
W3 = float(os.getenv("DIR_W_TR",  0.2))    # Gewicht für Trend (wenn verfügbar)
DEAD_ZONE = float(os.getenv("DIR_DEAD", 0.15))   # |raw| < DEAD → Neutral
WALL_DTE = int(os.getenv("DIR_WALL_DTE", 7))     # DTE-Schwelle für Wall-Bremse
WALL_DAMP = float(os.getenv("DIR_WALL_DAMP", 0.6))  # Dämpfung wenn Wall nah/groß
PC_MIN = float(os.getenv("DIR_PC_MIN", 0.3))
PC_MAX = float(os.getenv("DIR_PC_MAX", 3.0))

def read_csv_auto(path: Path) -> pd.DataFrame:
    if not path.exists():
        # Versuche unkomprimierte Variante
        alt = Path(str(path).replace(".csv.gz", ".csv"))
        if not alt.exists():
            return pd.DataFrame()
        return pd.read_csv(alt)
    if str(path).endswith(".gz"):
        return pd.read_csv(path, compression="gzip")
    return pd.read_csv(path)

def sigmoid(x: float) -> float:
    # numerisch stabile Sigmoid
    x = max(-20.0, min(20.0, x))
    return 1.0 / (1.0 + math.exp(-x))

def normalize_strength(raw_abs: float, conc: float) -> (float, int):
    # Confidence: Sigmoid(|raw|) * (0.6 + 0.4*conc)  (0..1)
    conf = sigmoid(raw_abs) * (0.6 + 0.4 * (0.0 if pd.isna(conc) else float(conc)))
    # Strength 0..100 (rundbar, „gefühlt linearer“)
    strength = int(round(100.0 * conf))
    return conf, strength

def choose_focus_strike(by_strike_df, symbol, expiry, dir_num):
    if by_strike_df.empty or pd.isna(expiry):
        return np.nan
    g = by_strike_df[(by_strike_df["symbol"].str.upper() == symbol) & (by_strike_df["expiry"] == expiry)]
    if g.empty:
        return np.nan
    # Für Up → Strike mit max Call-OI, für Down → max Put-OI
    if dir_num > 0 and "call_oi" in g.columns:
        row = g.sort_values("call_oi", ascending=False).head(1)
    elif dir_num < 0 and "put_oi" in g.columns:
        row = g.sort_values("put_oi", ascending=False).head(1)
    else:
        # Fallback: höchstes total OI (falls vorbereitet)
        if "total_oi" in g.columns:
            row = g.sort_values("total_oi", ascending=False).head(1)
        else:
            # letzter Fallback: größte Summe Call+Put
            g2 = g.copy()
            g2["tot"] = pd.to_numeric(g2.get("call_oi", 0), errors="coerce").fillna(0) + \
                        pd.to_numeric(g2.get("put_oi", 0), errors="coerce").fillna(0)
            row = g2.sort_values("tot", ascending=False).head(1)
    return float(row["strike"].iloc[0]) if not row.empty and "strike" in row.columns else np.nan

# ---------------- Load Inputs ----------------
sumo = read_csv_auto(BASE / "options_signals.csv.gz")
if sumo.empty:
    sumo = read_csv_auto(BASE / "options_signals.csv")  # Fallback

byexp = read_csv_auto(BASE / "options_oi_by_expiry.csv.gz")
if byexp.empty:
    byexp = read_csv_auto(BASE / "options_oi_by_expiry.csv")

bystr = read_csv_auto(BASE / "options_oi_by_strike.csv.gz")
if bystr.empty:
    bystr = read_csv_auto(BASE / "options_oi_by_strike.csv")

# optional: Volatilitätsregime (nur für Confidence/Trend-Bias)
hv = read_csv_auto(BASE / "hv_summary.csv.gz")
if hv.empty:
    hv = read_csv_auto(BASE / "hv_summary.csv")

# Typen & Spalten säubern
for df in (sumo, byexp, bystr):
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

if "expiry" in sumo.columns:
    sumo["expiry"] = pd.to_datetime(sumo["expiry"], errors="coerce")
if "expiry" in byexp.columns:
    byexp["expiry"] = pd.to_datetime(byexp["expiry"], errors="coerce")
if "expiry" in bystr.columns:
    bystr["expiry"] = pd.to_datetime(bystr["expiry"], errors="coerce")

# numerische Felder
for col in ["put_call_oi","iv_skew","oi_concentration","expiry_wall_7","nearest_dte","total_oi","call_oi","put_oi"]:
    if col in sumo.columns:
        sumo[col] = pd.to_numeric(sumo[col], errors="coerce")

for col in ["call_oi","put_oi","total_oi"]:
    if col in byexp.columns:
        byexp[col] = pd.to_numeric(byexp[col], errors="coerce")
if "strike" in bystr.columns:
    bystr["strike"] = pd.to_numeric(bystr["strike"], errors="coerce")
for col in ["call_oi","put_oi","total_oi"]:
    if col in bystr.columns:
        bystr[col] = pd.to_numeric(bystr[col], errors="coerce")

if not hv.empty and "symbol" in hv.columns:
    hv["symbol"] = hv["symbol"].astype(str).str.upper().str.strip()
    for c in ("hv20","hv60"):
        if c in hv.columns: hv[c] = pd.to_numeric(hv[c], errors="coerce")

# ---------------- Helper pro Symbol ----------------
def best_expiry_for_horizon(sym: str, horizon_days: int) -> pd.Timestamp:
    # wähle den Verfall im Fenster [0..horizon] mit höchstem total_oi
    g = byexp[byexp["symbol"] == sym]
    if g.empty or "expiry" not in g.columns:
        return pd.NaT
    today = pd.Timestamp.today().normalize()
    g = g.copy()
    g = g[(g["expiry"] >= today) & (g["expiry"] <= today + pd.Timedelta(days=horizon_days))]
    if g.empty:
        return pd.NaT
    # total_oi kann fehlen → ersatzweise call+put
    if "total_oi" not in g.columns:
        g["total_oi"] = pd.to_numeric(g.get("call_oi", 0), errors="coerce").fillna(0) + \
                        pd.to_numeric(g.get("put_oi", 0), errors="coerce").fillna(0)
    row = g.sort_values("total_oi", ascending=False).head(1)
    return row["expiry"].iloc[0] if not row.empty else pd.NaT

def estimate_trend_bias(sym: str) -> float:
    # Optionaler Trend-Bias (wenn HV da ist): niedrige kurzfristige HV relativ zu langfristig → stabilerer Trend
    if hv.empty or "hv20" not in hv.columns or "hv60" not in hv.columns:
        return 0.0
    r = hv[hv["symbol"] == sym]
    if r.empty:
        return 0.0
    hv20 = float(r["hv20"].iloc[0]) if not pd.isna(r["hv20"].iloc[0]) else np.nan
    hv60 = float(r["hv60"].iloc[0]) if not pd.isna(r["hv60"].iloc[0]) else np.nan
    if pd.isna(hv20) or pd.isna(hv60) or hv60 <= 0:
        return 0.0
    # einfacher Proxy: wenn HV20 <= HV60 → Trend stabiler → leichte positive Tendenz
    return +1.0 if hv20 <= hv60 else -0.5

def wall_is_large_near(sym: str) -> bool:
    # nutzt die bereits voraggregierte expiry_wall_7 + nearest_dte aus sumo
    r = sumo[sumo["symbol"] == sym]
    if r.empty:
        return False
    ew = pd.to_numeric(r["expiry_wall_7"], errors="coerce").fillna(0).max() if "expiry_wall_7" in r.columns else 0.0
    nd = pd.to_numeric(r["nearest_dte"], errors="coerce").fillna(9999).min() if "nearest_dte" in r.columns else 9999
    # heuristische Schwelle: "groß" relativ zu total_oi-Median, wenn verfügbar
    tot = pd.to_numeric(r["total_oi"], errors="coerce").fillna(0)
    med = float(np.nanmedian(tot)) if tot.size else 0.0
    large = ew >= max(1e5, 3.0 * med)  # konservativ
    return (nd <= WALL_DTE) and large

# ---------------- Score je Symbol × Horizont ----------------
rows = []
if sumo.empty:
    print("WARN: options_signals.csv(.gz) nicht gefunden/leer – breche ab.")
else:
    for sym, g in sumo.groupby("symbol", sort=False):
        # Feature-Bundle (Symbol-weit; summary über mehrere expiries)
        pc = float(pd.to_numeric(g.get("put_call_oi", pd.Series(dtype=float)), errors="coerce").median()) \
             if "put_call_oi" in g.columns else np.nan
        iv = float(pd.to_numeric(g.get("iv_skew", pd.Series(dtype=float)), errors="coerce").median()) \
             if "iv_skew" in g.columns else np.nan
        conc = float(pd.to_numeric(g.get("oi_concentration", pd.Series(dtype=float)), errors="coerce").median()) \
               if "oi_concentration" in g.columns else np.nan
        trend_bias = estimate_trend_bias(sym)

        # Clip PC-Ratio (robust)
        if not pd.isna(pc):
            pc = max(PC_MIN, min(PC_MAX, pc))

        # Roh-Score
        inv_pc_term = (1.0 / pc - 1.0) if not pd.isna(pc) else 0.0
        iv_term = (-iv) if not pd.isna(iv) else 0.0
        trend_term = trend_bias

        base_raw = W1 * inv_pc_term + W2 * iv_term + W3 * trend_term

        # Wall-Dämpfung (wenn nahe & groß)
        if wall_is_large_near(sym):
            base_raw *= WALL_DAMP

        # Für jeden Horizont: best_expiry, focus_strike, near_dte
        for hz in HORIZONS:
            bexp = best_expiry_for_horizon(sym, hz)
            near_dte = int((bexp - pd.Timestamp.today().normalize()).days) if pd.notna(bexp) else np.nan

            # Richtung & Dead-zone
            dir_num = 0
            if abs(base_raw) >= DEAD_ZONE:
                dir_num = 1 if base_raw > 0 else -1
            dir_text = "Up" if dir_num > 0 else ("Down" if dir_num < 0 else "Neutral")

            focus_strike = choose_focus_strike(bystr, sym, bexp, dir_num)

            conf, strength = normalize_strength(abs(base_raw), conc)

            # Data-Freshness (0..1): heuristisch aus nearest_dte (frischer, wenn nah)
            if pd.isna(near_dte):
                freshness = 0.5
            else:
                freshness = max(0.1, min(1.0, 1.0 - (near_dte / 60.0)))  # 0–60 Tage → 1..~0

            rows.append(dict(
                symbol=sym,
                horizon=int(hz),
                dir_num=int(dir_num),
                dir_text=dir_text,
                confidence=round(conf, 4),
                strength=int(strength),
                near_expiry=(bexp.date().isoformat() if pd.notna(bexp) else ""),
                focus_strike=(None if pd.isna(focus_strike) else float(focus_strike)),
                data_freshness=round(freshness, 3)
            ))

# ---------------- Write GZIP CSV ----------------
out_df = pd.DataFrame(rows, columns=[
    "symbol","horizon","dir_num","dir_text","confidence","strength","near_expiry","focus_strike","data_freshness"
])
with gzip.open(OUTP, "wt", encoding="utf-8", newline="") as f:
    out_df.to_csv(f, index=False)

print(f"wrote {OUTP} rows={len(out_df)} symbols={out_df['symbol'].nunique() if not out_df.empty else 0}")
