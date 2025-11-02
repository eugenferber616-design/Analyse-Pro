#!/usr/bin/env python3
# Standardisiert/erzwingt das Ziel-Schema:
#   data/processed/options_oi_by_expiry.csv
#   Spalten: symbol, expiry, total_call_oi, total_put_oi

import os, gzip, io, sys
from pathlib import Path

try:
    import pandas as pd
except Exception as e:
    print("pandas wird benötigt:", e, file=sys.stderr); sys.exit(1)

OUT = Path("data/processed/options_oi_by_expiry.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

# mögliche Quellen (nimm die erste, die vorhanden ist)
CANDIDATES = [
    Path("data/processed/options_oi_by_expiry.csv"),
    Path("data/processed/options_oi_by_expiry.csv.gz"),
    Path("data/processed/options_by_expiry.csv"),
    Path("data/processed/options_by_expiry.csv.gz"),
    Path("data/processed/options_oi_summary.csv"),     # manchen Pipelines enthalten die Expiry-Aggregation dort
    Path("data/processed/options_oi_summary.csv.gz"),
]

def read_any(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(str(p))
    if p.suffix == ".gz":
        with gzip.open(p, "rb") as f:
            bio = io.BytesIO(f.read())
        # heuristik: CSV bevorzugt, ansonsten Parquet
        try:
            bio.seek(0); return pd.read_csv(bio)
        except Exception:
            bio.seek(0); return pd.read_parquet(bio)
    if p.suffix.lower() in (".parquet", ".pq"):
        return pd.read_parquet(p)
    return pd.read_csv(p)

src = None
for c in CANDIDATES:
    if c.exists():
        src = c; break

if src is None:
    print("Keine Quelle für BY_EXPIRY gefunden. Erwarte z. B. data/processed/options_oi_by_expiry.csv(.gz)", file=sys.stderr)
    sys.exit(0)  # sanft beenden, damit Workflow nicht bricht

df = read_any(src)

# Spalten normalisieren (breit akzeptieren, in Ziel-Schema mappen)
colmap = {}
for col in df.columns:
    cl = col.strip().lower()
    if cl in ("symbol",): colmap[col] = "symbol"
    elif cl in ("expiry","expiration","maturity","exp"): colmap[col] = "expiry"
    elif "total_call" in cl or cl == "call_oi" or cl == "calls_oi": colmap[col] = "total_call_oi"
    elif "total_put"  in cl or cl == "put_oi"  or cl == "puts_oi":  colmap[col] = "total_put_oi"

df = df.rename(columns=colmap)

# nur relevante Spalten
keep = ["symbol","expiry","total_call_oi","total_put_oi"]
for k in keep:
    if k not in df.columns:
        df[k] = 0

df = df[keep].copy()

# Typen & Formate
df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date
df = df[df["expiry"].notna()]
for k in ("total_call_oi","total_put_oi"):
    df[k] = pd.to_numeric(df[k], errors="coerce").fillna(0).astype("int64")

df = df.sort_values(["symbol","expiry"])
df.to_csv(OUT, index=False)
print(f"Wrote {OUT} ({len(df)} rows) from {src}")
