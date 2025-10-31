#!/usr/bin/env python3
import pandas as pd, os, sys

SRC = "data/processed/options_oi_by_strike.csv"
DST = "data/processed/options_oi_strike_max.csv"

def main():
    if not os.path.exists(SRC) or os.path.getsize(SRC) == 0:
        # leeres Ergebnis
        pd.DataFrame(columns=["symbol","max_strike","max_oi"]).to_csv(DST, index=False)
        print("no by_strike file -> wrote empty", DST)
        return 0

    df = pd.read_csv(SRC)
    if df.empty or "symbol" not in df.columns or "strike" not in df.columns:
        pd.DataFrame(columns=["symbol","max_strike","max_oi"]).to_csv(DST, index=False)
        print("invalid by_strike -> wrote empty", DST)
        return 0

    # Spalte für OI bestimmen (neu: total_oi, alt: cnt)
    oi_col = "total_oi" if "total_oi" in df.columns else ("cnt" if "cnt" in df.columns else None)
    if oi_col is None:
        pd.DataFrame(columns=["symbol","max_strike","max_oi"]).to_csv(DST, index=False)
        print("no oi column -> wrote empty", DST)
        return 0

    df["oi_val"] = pd.to_numeric(df[oi_col], errors="coerce").fillna(0).astype(float)
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")

    # pro Symbol die Zeile mit maximalem OI
    idx = df.groupby("symbol")["oi_val"].idxmax()
    out = df.loc[idx, ["symbol","strike","oi_val"]].rename(
        columns={"strike":"max_strike","oi_val":"max_oi"}
    ).sort_values("symbol")

    out.to_csv(DST, index=False)
    print("wrote", DST, "rows=", len(out))
    return 0

if __name__ == "__main__":
    sys.exit(main())
