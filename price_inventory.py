import pandas as pd, glob, os
from pathlib import Path

IN = sorted(glob.glob("data/prices/*.parquet"))
rows = []
for p in IN:
    df = pd.read_parquet(p, columns=['symbol','date'])
    grp = df.groupby('symbol')['date']
    tmp = grp.agg(rows='count', first='min', last='max').reset_index()
    tmp['source'] = os.path.basename(p)
    rows.append(tmp)
inv = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=['symbol','rows','first','last','source'])
Path("data/processed").mkdir(parents=True, exist_ok=True)
inv.to_csv("data/processed/price_inventory.csv", index=False)
print(f"inventory: {len(inv)} symbols -> data/processed/price_inventory.csv")
