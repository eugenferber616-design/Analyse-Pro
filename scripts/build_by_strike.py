#!/usr/bin/env python3
# Standardisiert/erzwingt das Ziel-Schema:
#   data/processed/options_oi_by_strike.csv
#   Spalten: symbol, expiry, strike, call_oi, put_oi

import os, gzip, io, sys
from pathlib import Path

try:
    import pandas as pd
except Exception as e:
    print("pandas wird benötigt:", e, file=sys.stderr); sys.exit(1)

OUT = Path("data/processed/options_oi_by_strike.csv")
OUT.parent.mkdir(parents=True, exist_ok=True)

CANDIDATES = [
    Path("data/processed/options_oi_by_strike.csv"),
    Path("data/processed/options_oi_by_strike.csv.gz"),
    Path("data/processed/options_by_strike.csv"),
    Path("data/processed/options_by_strike.csv.gz"),
    Path("data/processed/options_strikes.csv"),
    Path("data/processed/options_strikes.csv.gz"),
]

def read_any(p: Path) -> pd.DataFrame:
    if not p.exists():
        raise FileNotFoundError(str(p))
    if p.suffix == ".gz":
        with gzip.open(p, "rb") as f:
            bio = io.BytesIO(f.read())
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
    print("Keine Quelle für BY_STRIKE gefunden. Erwarte z. B. data/processed/options_oi_by_strike.csv(.gz)", file=sys.stderr)
    sys.exit(0)

df = read_any(src)

# Spalten normalisieren
colmap = {}
for col in df.columns:
    cl = col.strip().lower()
    if cl in ("symbol",): colmap[col] = "symbol"
    elif cl in ("expiry","expiration","maturity","exp"): colmap[col] = "expiry"
    elif cl in ("strike","strike_price","k","x"): colmap[col] = "strike"
    elif cl in ("call_oi","calls_oi","total_call_oi"): colmap[col] = "call_oi"
    elif cl in ("put_oi","puts_oi","total_put_oi"):   colmap[col] = "put_oi"

df = df.rename(columns=colmap)

keep = ["symbol","expiry","strike","call_oi","put_oi"]
for k in keep:
    if k not in df.columns:
        df[k] = 0

df = df[keep].copy()

df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date
df = df[df["expiry"].notna()]
df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
df["call_oi"] = pd.to_numeric(df["call_oi"], errors="coerce").fillna(0).astype("int64")
df["put_oi"]  = pd.to_numeric(df["put_oi"],  errors="coerce").fillna(0).astype("int64")

df = df.sort_values(["symbol","expiry","strike"])
df.to_csv(OUT, index=False)
print(f"Wrote {OUT} ({len(df)} rows) from {src}")
