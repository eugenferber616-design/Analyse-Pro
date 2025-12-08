# scripts/build_regime_state.py
import os, json, pandas as pd, numpy as np

FRED = "data/processed/fred_oas.csv"
ECB  = "data/macro/ecb/exr_usd_eur.csv"  # optional
OUT  = "data/processed/regime_state.json"
os.makedirs("data/processed", exist_ok=True)

def z(s, w=60):
    s = pd.Series(s)
    return (s - s.rolling(w, min_periods=20).mean())/s.rolling(w, min_periods=20).std()

def main():
    o = pd.read_csv(FRED, parse_dates=["date"]) if os.path.exists(FRED) else pd.DataFrame()
    state = "NEUTRAL"; reason=[]
    if not o.empty:
        ig = o[o["bucket"]=="IG"].set_index("date")["value"]
        hy = o[o["bucket"]=="HY"].set_index("date")["value"]
        cz = 0.6*z(hy) + 0.4*z(ig)
        last = float(cz.dropna().iloc[-1]) if not cz.dropna().empty else 0.0
        if last >  0.7: state,reason=("RISK-OFF",["credit_widening"])
        if last < -0.7: state,reason=("RISK-ON", ["credit_easing"])
    json.dump({"state":state, "reason":reason}, open(OUT,"w"), indent=2)
    print("regime_state:", state)

if __name__ == "__main__":
    main()
