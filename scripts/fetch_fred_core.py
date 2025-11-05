#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, csv, gzip, requests
from pathlib import Path
import pandas as pd

API   = "https://api.stlouisfed.org/fred/series/observations"
KEY   = os.getenv("FRED_API_KEY", "").strip()
START = os.getenv("FRED_START", "2003-01-01")

OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -------- Kern-Serien mit Fallback-Kandidaten --------
CORE = {
    # Zinsen / Kurve
    "DGS30":   ["DGS30"],
    "DGS10":   ["DGS10"],
    "DGS2":    ["DGS2"],
    "DGS3MO":  ["DGS3MO", "TB3MS"],          # 3M Bill (Sekundärmarkt) als Fallback
    # Geldmarkt / Funding
    "SOFR":        ["SOFR"],
    "RRPONTSYD":   ["RRPONTSYD"],            # ON RRP: Total securities, daily
    # Fed-Balance / Treasury / Reserven
    "WALCL":   ["WALCL"],                    # Total Assets
    "WTREGEN": ["WTREGEN"],                  # Treasury General Account
    "WRESBAL": ["WRESBAL"],                  # Reserve Balances with Fed
    # Financial Stress (neu/alt)
    "STLFSI4": ["STLFSI4", "STLFSI"],
}

# -------- OAS (IG/HY) mit alternativen Kennungen --------
OAS = {
    "IG_OAS": ["BAMLC0A0CM", "BAMLC0A0CMTRIV"],
    "HY_OAS": ["BAMLH0A0HYM2", "BAMLH0A0HYM2TRIV"],
}

UA = {"User-Agent": "fred-core-fetch/1.0 (+github actions)"}

def _empty_gzip_csv(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        f.write("")  # gültige leere gzip-Datei

def pull_series(series_id: str, retries: int = 5, sleep: float = 0.7) -> pd.Series | None:
    """Ziehe genau EINE FRED-Serie; gib Series (float) oder None zurück."""
    if not KEY:
        return None
    params = {
        "series_id": series_id,
        "api_key": KEY,
        "file_type": "json",
        "observation_start": START,
        # KEIN 'frequency' → Original-Frequenz, wir reindexen später auf 'D'
        "sort_order": "asc",
    }
    for i in range(1, retries + 1):
        try:
            r = requests.get(API, params=params, headers=UA, timeout=60)
            if r.status_code != 200:
                print(f"WARN: fetch {series_id} failed ({i}/{retries}): {r.status_code} {r.reason}")
                time.sleep(sleep); continue
            js = r.json()
            obs = js.get("observations", [])
            if not obs:
                return None
            dates = [o.get("date") for o in obs]
            vals  = pd.to_numeric([o.get("value") for o in obs], errors="coerce")
            s = pd.Series(vals, index=pd.to_datetime(dates), name=series_id)
            s = s.dropna()
            if s.empty:
                return None
            s.index = s.index.tz_localize(None)
            # Deduplizieren & sortieren (sicherheitshalber)
            s = s[~s.index.duplicated(keep="last")].sort_index()
            return s
        except Exception as e:
            print(f"WARN: fetch {series_id} failed ({i}/{retries}): {e}")
            time.sleep(sleep)
    print(f"WARN: {series_id} endgültig fehlgeschlagen.")
    return None

def pull_first_available(name: str, candidates: list[str]) -> pd.Series | None:
    for sid in candidates:
        s = pull_series(sid)
        if s is not None and not s.empty:
            s = s.rename(name)
            print(f"OK: {name} ← {sid}  rows={int(s.size)}")
            return s
    print(f"WARN: Keine Serie verfügbar für {name} (Kandidaten: {candidates})")
    return None

def write_csv_gz(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Falls leer: leere (aber gültige) gzip-CSV schreiben
    if df is None or df.empty:
        _empty_gzip_csv(path)
        print(f"✔ wrote {path} rows=0 cols=[]")
        return
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date"] + list(df.columns))
        for dt, row in df.iterrows():
            w.writerow([dt.date().isoformat()] + [row[c] for c in df.columns])
    print(f"✔ wrote {path} rows={len(df)} cols={list(df.columns)}")

def to_daily_ffill(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.sort_index()
    full = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.reindex(full).ffill()

def main() -> int:
    # Kein API-Key → beide Artefakte leer (gültige .gz) und weich beenden
    if not KEY:
        print("WARN: FRED_API_KEY fehlt — schreibe leere Artefakte.")
        _empty_gzip_csv(OUT_DIR / "fred_core.csv.gz")
        _empty_gzip_csv(OUT_DIR / "fred_oas.csv.gz")
        return 0

    # ------- CORE ziehen -------
    core_cols: dict[str, pd.Series] = {}
    for name, cands in CORE.items():
        s = pull_first_available(name, cands)
        if s is not None and not s.empty:
            core_cols[name] = s

    if core_cols:
        core = pd.concat(core_cols, axis=1)
        # Spalten UPPERCASE für build_riskindex.py
        core.columns = [c.upper() for c in core.columns]
        # Tagesgitter + FFill
        core = to_daily_ffill(core)
        # Deduplizierter Index ist schon garantiert
    else:
        core = pd.DataFrame()

    write_csv_gz(OUT_DIR / "fred_core.csv.gz", core)

    # ------- OAS (separat) -------
    oas_cols: dict[str, pd.Series] = {}
    for name, cands in OAS.items():
        s = pull_first_available(name, cands)
        if s is not None and not s.empty:
            oas_cols[name] = s

    if oas_cols:
        oas = pd.concat(oas_cols, axis=1)
        oas.columns = [c.upper() for c in oas.columns]
        oas = to_daily_ffill(oas)
    else:
        oas = pd.DataFrame()

    write_csv_gz(OUT_DIR / "fred_oas.csv.gz", oas)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
