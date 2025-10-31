# scripts/build_options_signals.py
import os
import numpy as np
import pandas as pd

OUT = "data/processed/options_signals.csv"
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# Einlesen (failsafe)
sumo = pd.read_csv("data/processed/options_oi_summary.csv")
byex = pd.read_csv("data/processed/options_oi_by_expiry.csv")

# Datentypen säubern
for col in ["call_oi", "put_oi", "total_oi", "call_iv_w", "put_iv_w"]:
    if col in sumo.columns:
        sumo[col] = pd.to_numeric(sumo[col], errors="coerce")

if "expiry" in sumo.columns:
    # summary hat pro Symbol mehrere expiries → für IV-Statistik
    sumo["expiry"] = pd.to_datetime(sumo["expiry"], errors="coerce")

for col in ["call_oi", "put_oi", "total_oi"]:
    if col in byex.columns:
        byex[col] = pd.to_numeric(byex[col], errors="coerce")
if "expiry" in byex.columns:
    byex["expiry"] = pd.to_datetime(byex["expiry"], errors="coerce")

def herfindahl(df, weight_col: str) -> float:
    if df.empty or weight_col not in df.columns:
        return np.nan
    w = pd.to_numeric(df[weight_col], errors="coerce").fillna(0.0).values
    s = float(w.sum())
    return float(((w / s) ** 2).sum()) if s > 0 else np.nan

rows = []
for sym, g in sumo.groupby("symbol", sort=False):
    # Put/Call OI
    put_sum = pd.to_numeric(g.get("put_oi", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()
    call_sum = pd.to_numeric(g.get("call_oi", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()
    tot_sum  = pd.to_numeric(g.get("total_oi", pd.Series(dtype=float)), errors="coerce").fillna(0.0).sum()
    put_call_oi = float(put_sum / max(1.0, call_sum))

    # IV-Skew (gewichteter Durchschnitt je Seite bereits in call_iv_w/put_iv_w)
    iv_call = pd.to_numeric(g.get("call_iv_w", pd.Series(dtype=float)), errors="coerce")
    iv_put  = pd.to_numeric(g.get("put_iv_w",  pd.Series(dtype=float)), errors="coerce")
    # Differenz: Put minus Call (positiv = mehr Downside-Absicherung)
    iv_skew = float((iv_put - iv_call).dropna().mean()) if not iv_put.empty else np.nan

    # Konzentration je Verfall (Herfindahl aus by_expiry)
    ge = byex[byex["symbol"] == sym].copy()
    conc = herfindahl(ge, "total_oi")

    # „Expiry-Wall“ in den nächsten 7 Tagen
    expiry_wall_7 = np.nan
    near_dte = np.nan
    if not ge.empty and "expiry" in ge.columns:
        ge["days"] = (ge["expiry"] - pd.Timestamp.today(tz=None)).dt.days
        expiry_wall_7 = float(ge.loc[ge["days"].between(1, 7), "total_oi"].sum())
        pos = ge.loc[ge["days"] >= 0, "days"]
        near_dte = int(pos.min()) if not pos.empty else np.nan

    rows.append(dict(
        symbol=sym,
        put_call_oi=put_call_oi,
        iv_skew=iv_skew,                 # ~ annualisierte Differenz (z.B. 0.05 = 5 %-Pkt)
        oi_concentration=conc,           # 0..1 (höher = konzentrierter)
        expiry_wall_7=expiry_wall_7,     # Summe OI der nächsten 7 Tage
        nearest_dte=near_dte,            # Tage bis zum nächsten Verfall
        total_oi=tot_sum,
        call_oi=call_sum,
        put_oi=put_sum
    ))

pd.DataFrame(rows).to_csv(OUT, index=False)
print("wrote", OUT, "rows=", len(rows))
