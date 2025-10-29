# scripts/build_credit_proxy_v3.py
import pandas as pd, numpy as np, json, os
from sklearn.decomposition import PCA
OUT="data/processed/cds_proxy_v3.csv"; os.makedirs("data/processed", exist_ok=True)

oas = pd.read_csv("data/processed/fred_oas.csv", parse_dates=["date"])
# Pivot: date x (US_IG, US_HY, EU_IG?, EU_HY?) – falls EU leer, fülle mit US_IG shift/scale
wide = oas.pivot(index="date", columns="bucket", values="value").rename_axis(None,1)
for need in ["EU_IG","EU_HY"]:
    if need not in wide:
        wide[need] = wide["IG"].rolling(60,min_periods=20).mean() * (0.95 if need=="EU_IG" else 1.25)

# PCA auf normierten Spreads -> Composite
z = (wide - wide.rolling(60).mean())/wide.rolling(60).std()
comp = pd.Series(PCA(1).fit_transform(z.fillna(0)).ravel(), index=z.index, name="comp_oas")
comp = (comp - comp.rolling(60).mean())/comp.rolling(60).std()

# HYG/LQD Beta je Symbol (aus futures_quotes/fx_quotes oder Stooq OHLC -> hier Dummy: 0.5)
syms = [r.strip().split(",")[0] for r in open("watchlists/mylist.txt") if r.strip() and not r.startswith("#")]
rows=[]
for s in syms:
    beta_hyg = 0.5  # TODO: aus Rolling-Regression (20d) auf HYG returns berechnen
    beta_lqd = 0.2
    last = comp.dropna().iloc[-1]
    cds = max(0.0, (0.4*beta_hyg + 0.2*beta_lqd))*float(last)
    rows.append({"symbol":s, "proxy_spread": cds})

pd.DataFrame(rows).to_csv(OUT, index=False)
print("wrote", OUT)
