# scripts/build_factor_scores.py
import pandas as pd, numpy as np, os, json, statsmodels.api as sm
os.makedirs("data/processed", exist_ok=True)

fund = pd.read_csv("data/processed/fundamentals_pro.csv")
opt  = pd.read_csv("data/processed/options_signals.csv")
cds  = pd.read_csv("data/processed/cds_proxy_v3.csv")

df = fund.merge(opt, on="symbol", how="left").merge(cds, on="symbol", how="left")

def z(x):
    return (x - np.nanmean(x))/np.nanstd(x)

def sector_neutralize(frame, col, sector_col="sector"):
    out = []
    for sec, g in frame.groupby(sector_col):
        out.append(pd.Series(z(g[col]), index=g.index))
    s = pd.concat(out).sort_index()
    return s

# Value/Quality/Momentum/Risk/Credit Scores (sektor-neutral)
df["VAL"] = sector_neutralize(df, "fcf_yield") + sector_neutralize(df, "earnings_yield") \
          - sector_neutralize(df, "ev_ebitda") + sector_neutralize(df, "buyback_yield")

df["QLT"] = sector_neutralize(df, "roic_ttm") + sector_neutralize(df, "gross_margin") \
          - sector_neutralize(df, "accruals") + sector_neutralize(df, "piotroski_f")

# Momentum -> brauchst du aus Price-Renditen (hier Platzhalter)
df["MOM"] = np.nan

df["RSK"] = -sector_neutralize(df, "debt_to_equity")  # Proxy bis ATR/idioVol ergänzt
df["CRD"] = -z(df["proxy_spread"].values)

# Composite (Startgewichte)
w = dict(VAL=0.25, QLT=0.25, MOM=0.30, RSK=0.10, CRD=0.10)
df["AlphaCore"] = sum(w[k]*df[k] for k in w)

df[["symbol","sector","country","VAL","QLT","MOM","RSK","CRD","AlphaCore"]].to_csv(
    "data/processed/factor_scores.csv", index=False)
print("wrote factor_scores.csv")
