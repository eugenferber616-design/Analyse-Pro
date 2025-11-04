#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, csv, gzip, requests
from pathlib import Path
import pandas as pd

API = "https://api.stlouisfed.org/fred/series/observations"
KEY = os.getenv("FRED_API_KEY", "")
START = os.getenv("FRED_START","2003-01-01")

OUT_DIR = Path("data/processed"); OUT_DIR.mkdir(parents=True, exist_ok=True)

CORE = {
    "DGS30":"DGS30","DGS10":"DGS10","DGS2":"DGS2","DGS3MO":"DGS3MO",
    "SOFR":"SOFR","RRPONTSYD":"RRPONTSYD",
    # wöchentl./monatlich – ohne frequency-Param!
    "WALCL":"WALCL","WTREGEN":"WTREGEN","WRESBAL":"WRESBAL","STLFSI4":"STLFSI4",
}

OAS = {"IG_OAS":"BAMLC0A0CM","HY_OAS":"BAMLH0A0HYM2"}

def pull_series(series_id:str, retries:int=5, sleep:float=1.0)->pd.Series|None:
    params = {
        "series_id": series_id,
        "api_key": KEY, "file_type":"json",
        "observation_start": START,
        # KEIN 'frequency' mehr!
        "sort_order":"asc"
    }
    for i in range(1, retries+1):
        try:
            r = requests.get(API, params=params, timeout=60)
            if r.status_code!=200:
                print(f"WARN: fetch {series_id} failed (try {i}/{retries}): {r.status_code} {r.reason} url: {r.url}")
                time.sleep(sleep); continue
            js = r.json()
            vals = [(x["date"], x["value"]) for x in js.get("observations",[])]
            if not vals: return None
            s = pd.Series(
                pd.to_numeric([v for _,v in vals], errors="coerce"),
                index=pd.to_datetime([d for d,_ in vals])
            ).dropna()
            s.index = s.index.tz_localize(None)
            return s
        except Exception as e:
            print(f"WARN: fetch {series_id} failed (try {i}/{retries}): {e}")
            time.sleep(sleep)
    print(f"WARN: {series_id} endgültig fehlgeschlagen.")
    return None

def write_csv_gz(path:Path, df:pd.DataFrame):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["date"]+list(df.columns))
        for dt,row in df.iterrows():
            w.writerow([dt.date().isoformat()]+[row[c] for c in df.columns])

def main()->int:
    cols = {}
    for name,sid in CORE.items():
        s = pull_series(sid)
        if s is None: continue
        cols[name]=s

    if not cols:
        print("ERROR: keine FRED-Daten geladen."); return 1

    # tägliche Index-Achse, Vorwärtsauffüllung
    df = pd.concat(cols, axis=1).sort_index()
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    df = df.reindex(full).ffill()

    out_core = OUT_DIR/"fred_core.csv.gz"
    write_csv_gz(out_core, df)
    print(f"✔ wrote {out_core}  cols={list(df.columns)}  rows={len(df)}")

    # OAS (getrennt)
    ocols={}
    for name,sid in OAS.items():
        s = pull_series(sid)
        if s is None: continue
        ocols[name]=s
    if ocols:
        df2 = pd.concat(ocols, axis=1).sort_index()
        df2 = df2.reindex(full).ffill()
        out_oas = OUT_DIR/"fred_oas.csv.gz"
        write_csv_gz(out_oas, df2)
        print(f"✔ wrote {out_oas}  cols={list(df2.columns)}  rows={len(df2)}")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
