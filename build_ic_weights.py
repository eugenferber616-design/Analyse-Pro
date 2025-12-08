# scripts/build_ic_weights.py
import os, json, pandas as pd, numpy as np
IN_FACT = "data/processed/factor_scores.csv"
IN_MOMO = "data/processed/momo_risk.csv.gz"
OUT = "data/processed/factor_weights.json"
os.makedirs("data/processed", exist_ok=True)

def main():
    w = {"VAL":0.25,"QLT":0.25,"MOM":0.30,"RSK":0.10,"CRD":0.10}
    if os.path.exists(IN_MOMO):  # Optional: könnte forward returns enthalten (nicht hier)
        pass
    json.dump({"weights":w, "method":"fallback"}, open(OUT,"w"), indent=2)
    print("wrote factor_weights.json (fallback)")

if __name__ == "__main__":
    main()
