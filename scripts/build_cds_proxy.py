#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Erzeuge einfache CDS-Proxys je Aktie aus FRED OAS Indizes.
- Liest data/processed/fred_oas.csv (Serie, Datum, Wert)
- Mappt jeden Einzeltitel auf einen Proxy (US_IG / US_HY / EU_IG / EU_HY)
- Nimmt den letzten verfügbaren OAS-Wert je Proxy als 'proxy_spread' (in %)
- Schreibt:
    data/processed/cds_proxy.csv
    data/reports/cds_proxy_report.json
Optionale Mappings:
    config/mappings/proxy_map.yaml    # symbol → proxy
    config/mappings/default_proxy.yaml # default_proxy: US_IG
"""
import os, sys, json, datetime as dt
from typing import Dict
import pandas as pd

try:
    import yaml
except Exception:
    yaml = None  # yaml optional

OUT_CSV = "data/processed/cds_proxy.csv"
OUT_JSON = "data/reports/cds_proxy_report.json"
FRED_CSV = "data/processed/fred_oas.csv"

# Standard-Proxy-Familie → FRED-Serien-IDs
FRED_SERIES_BY_PROXY: Dict[str, str] = {
    "US_IG": "BAMLC0A0CM",     # ICE BofA US Corp IG OAS
    "US_HY": "BAMLH0A0HYM2",   # ICE BofA US HY OAS
    "EU_IG": "BEMLEIG",        # ICE BofA Euro Corp IG OAS
    "EU_HY": "BEMLEHY",        # ICE BofA Euro HY OAS
}

def _load_yaml(path, default):
    if not yaml or not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or default

def load_symbol_proxy_map() -> Dict[str, str]:
    # 1) symbol-genaues Mapping
    mp = _load_yaml("config/mappings/proxy_map.yaml", {})
    # 2) default proxy
    dflt_cfg = _load_yaml("config/mappings/default_proxy.yaml", {"default_proxy":"US_IG"})
    dflt = dflt_cfg.get("default_proxy", "US_IG")
    return mp, dflt

def latest_oas_values(df: pd.DataFrame) -> Dict[str, float]:
    """
    df: Spalten ['series_id','date','value'] (value in Prozentpunkten)
    Ergebnis: dict proxy -> letzter Wert (float)
    """
    # wir normalisieren Spaltennamen defensiv
    cols = {c.lower(): c for c in df.columns}
    s_col = cols.get("series_id", next((c for c in df.columns if c.lower()=="series_id"), None))
    d_col = cols.get("date", next((c for c in df.columns if c.lower()=="date"), None))
    v_col = cols.get("value", next((c for c in df.columns if c.lower()=="value"), None))
    if not all([s_col, d_col, v_col]):
        return {}

    # Datum parse & nach Serie den letzten Eintrag
    dfx = df[[s_col, d_col, v_col]].copy()
    dfx[d_col] = pd.to_datetime(dfx[d_col], errors="coerce")
    dfx = dfx.dropna(subset=[d_col, v_col])

    latest: Dict[str, float] = {}
    for proxy, series_id in FRED_SERIES_BY_PROXY.items():
        sub = dfx[dfx[s_col] == series_id]
        if sub.empty:
            continue
        last_row = sub.sort_values(d_col).iloc[-1]
        latest[proxy] = float(last_row[v_col])  # FRED OAS bereits in %-Punkten
    return latest

def main() -> int:
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    if not os.path.exists(FRED_CSV) or os.path.getsize(FRED_CSV) == 0:
        print("missing", FRED_CSV)
        # leere Ausgabe, aber kein harter Fehler
        pd.DataFrame(columns=["symbol","proxy","asof","proxy_spread"]).to_csv(OUT_CSV, index=False)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump({"ts": dt.datetime.utcnow().isoformat()+"Z",
                       "asof": None, "rows": 0, "missing": "fred_oas.csv"}, f, indent=2)
        return 0

    fred = pd.read_csv(FRED_CSV)
    latest_map = latest_oas_values(fred)
    if not latest_map:
        print("no latest OAS values found in fred_oas.csv")
        pd.DataFrame(columns=["symbol","proxy","asof","proxy_spread"]).to_csv(OUT_CSV, index=False)
        with open(OUT_JSON, "w", encoding="utf-8") as f:
            json.dump({"ts": dt.datetime.utcnow().isoformat()+"Z",
                       "asof": None, "rows": 0, "missing": "series"}, f, indent=2)
        return 0

    # asof = max Datum aus fred_oas
    asof = None
    if "date" in fred.columns:
        try:
            fred["date"] = pd.to_datetime(fred["date"], errors="coerce")
            asof = str(fred["date"].max().date())
        except Exception:
            asof = None

    # Watchlist laden
    wl = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
    symbols = []
    if os.path.exists(wl):
        if wl.lower().endswith(".csv"):
            try:
                wdf = pd.read_csv(wl)
                col = [c for c in wdf.columns if c.lower()=="symbol"]
                if col:
                    symbols = [str(x).strip() for x in wdf[col[0]].dropna().tolist()]
            except Exception:
                pass
        if not symbols:
            with open(wl, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and s.lower() != "symbol":
                        symbols.append(s)
    if not symbols:
        symbols = ["SPY"]

    # Mappings
    sym_map, default_proxy = load_symbol_proxy_map()
    if default_proxy not in FRED_SERIES_BY_PROXY:
        default_proxy = "US_IG"

    rows = []
    for sym in symbols:
        proxy = sym_map.get(sym, default_proxy)
        spread = latest_map.get(proxy)  # in %-Punkten
        rows.append({"symbol": sym, "proxy": proxy, "asof": asof, "proxy_spread": spread})

    df_out = pd.DataFrame(rows)
    df_out.to_csv(OUT_CSV, index=False)
    print("wrote", OUT_CSV, "rows=", len(df_out))
    sample = df_out.head(10).to_dict(orient="records")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "ts": dt.datetime.utcnow().isoformat()+"Z",
            "asof": asof,
            "rows": int(len(df_out)),
            "missing": int(df_out["proxy_spread"].isna().sum()),
            "sample": sample
        }, f, indent=2)
    return 0

if __name__ == "__main__":
    sys.exit(main())
