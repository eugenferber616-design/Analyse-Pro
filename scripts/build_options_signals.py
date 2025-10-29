# scripts/build_options_signals.py
import pandas as pd, numpy as np, os
OUT="data/processed/options_signals.csv"; os.makedirs("data/processed", exist_ok=True)

sumo = pd.read_csv("data/processed/options_oi_summary.csv")
byex = pd.read_csv("data/processed/options_oi_by_expiry.csv")

def herfindahl(df, wcol):
    w = df[wcol].values
    s = w.sum()
    return np.nan if s<=0 else ((w/s)**2).sum()

rows=[]
for sym, g in sumo.groupby("symbol"):
    put_call = (g["put_oi"].sum() / max(1.0, g["call_oi"].sum()))
    skew_px  = (g["put_oi_otm"].sum() - g["call_oi_otm"].sum()) / max(1.0, g["total_oi"].sum())
    conc     = herfindahl(g, "total_oi")

    e = byex[byex["symbol"]==sym].copy()
    e["days"] = (pd.to_datetime(e["expiry"])-pd.Timestamp.today()).dt.days
    near = e[e["days"].between(1, 21)].sort_values("total_oi", ascending=False).head(3)
    expiry_wall_7 = near[near["days"]<=7]["total_oi"].sum()

    rows.append(dict(symbol=sym, put_call_oi=put_call, skew_proxy=skew_px,
                     oi_conc=conc, expiry_wall_7=expiry_wall_7))
pd.DataFrame(rows).to_csv(OUT, index=False)
print("wrote", OUT)
