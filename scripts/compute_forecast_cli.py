# scripts/compute_forecast_cli.py
# CLI: liest data/prices/{SYMBOL}.csv, berechnet Ensemble-Forecast und schreibt JSON nach data/processed/forecast_{SYMBOL}.json
import os, sys, json, csv, argparse
from typing import List
from forecast_ensemble import ensemble, IVBandCfg

def read_prices_csv(p):
    arr=[]
    with open(p, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            try:
                arr.append(float(row["close"]))
            except: pass
    return arr

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--prices", default=None, help="CSV mit Spalten date,close; default data/prices/{SYMBOL}.csv")
    ap.add_argument("--horizons", default="5,10", help="zB 5,10,20")
    ap.add_argument("--iv-annual", type=float, default=None, help="z.B. 0.22")
    ap.add_argument("--drift-bps-per-day", type=float, default=0.0)
    ap.add_argument("--outdir", default="data/processed")
    args = ap.parse_args()

    prices_path = args.prices or os.path.join("data","prices",f"{args.symbol}.csv")
    if not os.path.exists(prices_path):
        print(f"ERR: {prices_path} nicht gefunden.", file=sys.stderr); sys.exit(1)

    prices = read_prices_csv(prices_path)
    Hs = [int(x.strip()) for x in args.horizons.split(",") if x.strip()]

    out = ensemble(prices, Hs, IVBandCfg(iv_annual=args.iv_annual, drift_bps_per_day=args.drift_bps_per_day))
    os.makedirs(args.outdir, exist_ok=True)
    outp = os.path.join(args.outdir, f"forecast_{args.symbol}.json")
    with open(outp, "w", encoding="utf-8") as f:
        json.dump({"per_h": out.per_h, "meta": out.meta, "symbol": args.symbol}, f, indent=2)
    print(f"✔ forecast → {outp}")

if __name__ == "__main__":
    main()
