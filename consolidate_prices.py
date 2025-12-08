import os, glob
import pandas as pd

IN_DIR = "data/prices"
OUT_DIR = "data/prices"
SHARD = True   # auf False, wenn ein einziges File gewünscht ist

def load_one(p):
    sym = os.path.splitext(os.path.basename(p))[0]
    df = pd.read_csv(p)
    # toleriert “date,close” + evtl. BOM/Spaces
    df.columns = [c.strip().lower() for c in df.columns]
    df = df[['date','close']].copy()
    df['symbol'] = sym
    return df[['symbol','date','close']]

def main():
    files = sorted(glob.glob(os.path.join(IN_DIR, "*.csv")))
    if not files:
        print("no price csvs, skip"); return
    os.makedirs(OUT_DIR, exist_ok=True)

    if SHARD:
        # shard nach 1. Buchstabe
        buckets = {}
        for p in files:
            key = os.path.basename(p)[:1].upper()
            buckets.setdefault(key, []).append(p)
        for key, lst in buckets.items():
            parts = [load_one(p) for p in lst]
            df = pd.concat(parts, ignore_index=True)
            df['date'] = pd.to_datetime(df['date'])
            df.sort_values(['symbol','date'], inplace=True)
            outp = os.path.join(OUT_DIR, f"shard_{key}.parquet")
            df.to_parquet(outp, index=False)  # Snappy default
            print(f"→ {outp}: {len(df):,} rows")
    else:
        parts = [load_one(p) for p in files]
        df = pd.concat(parts, ignore_index=True)
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values(['symbol','date'], inplace=True)
        outp = os.path.join(OUT_DIR, "prices.parquet")
        df.to_parquet(outp, index=False)
        print(f"→ {outp}: {len(df):,} rows")

if __name__ == "__main__":
    main()
