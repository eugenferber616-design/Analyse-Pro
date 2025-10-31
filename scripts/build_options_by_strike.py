# scripts/build_options_by_strike.py
import os, pandas as pd, numpy as np, sys, pathlib
IN = "data/processed/options_oi_summary.csv"
OUT = "data/processed/options_oi_by_strike.csv"

def parse_strikes(col):
    if pd.isna(col) or not str(col).strip():
        return []
    return [s.strip() for s in str(col).split(",") if s.strip()]

def main():
    if not os.path.exists(IN):
        print("missing", IN); sys.exit(0)
    df = pd.read_csv(IN)
    # wir nehmen die Top-Strikes je Verfall (proxy) und zählen über alle Verfälle
    rows = []
    for _, r in df.iterrows():
        sym = r.get("symbol","")
        for side in ("call_top_strikes","put_top_strikes"):
            for k in parse_strikes(r.get(side,"")):
                rows.append({"symbol": sym, "strike": k, "cnt": 1})
    if not rows:
        pathlib.Path(OUT).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["symbol","strike","cnt","rank_in_symbol"]).to_csv(OUT, index=False)
        print("wrote empty", OUT); return

    tmp = pd.DataFrame(rows).groupby(["symbol","strike"], as_index=False)["cnt"].sum()
    tmp["cnt"] = tmp["cnt"].astype(int)
    tmp["rank_in_symbol"] = tmp.groupby("symbol")["cnt"].rank(ascending=False, method="min").astype(int)
    tmp.sort_values(["symbol","rank_in_symbol","strike"], inplace=True)
    tmp.to_csv(OUT, index=False)
    print("wrote", OUT, "rows=", len(tmp))

if __name__ == "__main__":
    main()
