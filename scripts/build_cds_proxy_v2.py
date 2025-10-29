# scripts/build_cds_proxy_v2.py
import os, csv, json
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd

FRED_OAS_FILE = "data/processed/fred_oas.csv"
OUT_CSV       = "data/processed/cds_proxy.csv"
OUT_REPORT    = "data/reports/cds_proxy_report.json"
MAP_FILE      = "config/oas_proxy_map.csv"   # optional

# ────────────────────────────────────────────────────────────────────
# Hilfen
# ────────────────────────────────────────────────────────────────────

def ensure_dirs():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_REPORT), exist_ok=True)

def read_watchlist(path: str) -> pd.DataFrame:
    """
    Akzeptiert:
      - einfache Textliste (eine Spalte, optional header 'symbol')
      - CSV mit Spalte 'symbol'
    """
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f"watchlist not found: {path}")

    # Versuche CSV mit Kopf
    try:
        df = pd.read_csv(path)
        if "symbol" in df.columns:
            symbols = df["symbol"].astype(str).str.strip()
            symbols = symbols[symbols.ne("")].dropna().unique()
            return pd.DataFrame({"symbol": symbols})
    except Exception:
        pass

    # Fallback: simple Zeilenliste
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.lower() == "symbol":
                continue
            rows.append(s)
    return pd.DataFrame({"symbol": pd.Series(rows).dropna().unique()})

def read_oas_proxy_map(path: str) -> Tuple[Dict[str, Dict[str, Optional[str]]], Dict[str, str]]:
    """
    Erwartetes Format (siehe deine Vorlage):
      regions:
        US:
          IG: "<FRED_SERIES_ID_US_IG>"
          HY: "<FRED_SERIES_ID_US_HY>"
        EU:
          IG: "<FRED_SERIES_ID_EU_IG>"
          HY: "<FRED_SERIES_ID_EU_HY>"

      symbol,proxy
      <SYMBOL>,<REGION_TAG>   # z.B. SAP.DE,EU_IG (überschreibt Auto-Logik)
    Datei ist eine 'lockere' CSV/YAML-Mischung – wir parsen simple key:value Paare Zeile für Zeile.
    """
    region_series = {
        "US": {"IG": "BAMLC0A0CM",   "HY": "BAMLH0A0HYM2"},
        "EU": {"IG": None,           "HY": "BAMLHE00EHYIOAS"},
    }
    symbol_region_override: Dict[str, str] = {}

    if not os.path.exists(path):
        return region_series, symbol_region_override

    # Lockeres Parsen
    current = None
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            if line.lower().startswith("regions:"):
                current = "regions"
                continue

            if current == "regions":
                # Beispiele:
                # US:
                #   IG: "BAMLC0A0CM"
                #   HY: "BAMLH0A0HYM2"
                if line.endswith(":") and line[:-1] in ("US", "EU"):
                    block = line[:-1]
                    # read inner lines until next block/section
                    continue

            # minimalistische Key:Value-Erkennung
            if ":" in line:
                key, val = [x.strip() for x in line.split(":", 1)]
                val = val.strip().strip('"').strip("'")
                # Section-Style:
                if key in ("US", "EU") and val == "":
                    current = f"REGION_{key}"
                    region_block = key
                    continue
                if key in ("IG", "HY") and current and current.startswith("REGION_"):
                    region = current.split("_", 1)[1]
                    region_series[region][key] = (val or None) if val != '""' else None
                    continue

            # Symbol-Liste
            if "," in line and not line.lower().startswith("symbol"):
                sym, prox = [x.strip() for x in line.split(",", 1)]
                if prox:
                    symbol_region_override[sym] = prox

    return region_series, symbol_region_override

def load_fred_oas(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Erwartete Spalten: date, series_id, value, bucket, region
    needed = {"date", "series_id", "value"}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")

    # Datums-Typ
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["series_id", "date"])
    return df

def series_last(df: pd.DataFrame, series_id: str) -> Optional[float]:
    d = df[df["series_id"] == series_id]
    if d.empty:
        return None
    # Letzter nicht-NaN-Wert
    d = d.dropna(subset=["value"])
    if d.empty:
        return None
    return float(d.iloc[-1]["value"])

def infer_region(symbol: str) -> str:
    # sehr einfache Heuristik
    return "EU_IG" if symbol.upper().endswith(".DE") else "US_IG"

# ────────────────────────────────────────────────────────────────────
# Hauptlogik
# ────────────────────────────────────────────────────────────────────

def main(watchlist_path: Optional[str] = None):
    ensure_dirs()

    # Watchlist einlesen
    wl_path = os.environ.get("WATCHLIST_STOCKS") if not watchlist_path else watchlist_path
    if not wl_path:
        raise RuntimeError("WATCHLIST_STOCKS env not set and no --watchlist provided")
    watch = read_watchlist(wl_path)

    # Mapping/Defaults lesen
    region_series, overrides = read_oas_proxy_map(MAP_FILE)

    # FRED OAS laden
    fred = load_fred_oas(FRED_OAS_FILE)

    # Letzte Werte je Serie greifen
    us_ig_sid = region_series["US"]["IG"]
    us_hy_sid = region_series["US"]["HY"]
    eu_ig_sid = region_series["EU"]["IG"]
    eu_hy_sid = region_series["EU"]["HY"]

    us_ig = series_last(fred, us_ig_sid) if us_ig_sid else None
    us_hy = series_last(fred, us_hy_sid) if us_hy_sid else None
    eu_ig = series_last(fred, eu_ig_sid) if eu_ig_sid else None
    eu_hy = series_last(fred, eu_hy_sid) if eu_hy_sid else None

    # Fallback-Heuristik, wenn EU_IG fehlt:
    if eu_ig is None:
        # einfache, nachvollziehbare Regel: EU_IG ≈ US_IG − 0.08 (Floor 0.40)
        if us_ig is not None:
            eu_ig = max(0.40, round(us_ig - 0.08, 2))
        elif eu_hy is not None:
            eu_ig = max(0.40, round(eu_hy * 0.35, 2))
        else:
            eu_ig = None

    fred_used = {
        "US_IG": us_ig,
        "US_HY": us_hy,
        "EU_IG": eu_ig,
        "EU_HY": eu_hy,
    }

    rows = []
    for sym in watch["symbol"].tolist():
        prox_tag = overrides.get(sym)  # z.B. "EU_IG" oder "US_IG"
        if not prox_tag:
            prox_tag = infer_region(sym)

        base = None
        if prox_tag == "US_IG":
            base = us_ig
        elif prox_tag == "EU_IG":
            base = eu_ig
        elif prox_tag == "US_HY":
            base = us_hy
        elif prox_tag == "EU_HY":
            base = eu_hy

        # sehr defensiv: falls None, setze 0.70 als neutralen Fallback
        if base is None:
            base = 0.70

        rows.append({"symbol": sym, "region": prox_tag, "proxy_spread": round(float(base), 2)})

    out_df = pd.DataFrame(rows)
    out_df.to_csv(OUT_CSV, index=False)

    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "rows": len(out_df),
        "fred_oas_used": fred_used,
        "errors": [],
        "preview": OUT_CSV
    }
    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Konsole: kleine Vorschau
    print(json.dumps(report, indent=2))
    for _, r in out_df.head(10).iterrows():
        print(f'{r["symbol"]},{r["region"]},{r["proxy_spread"]}')

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", help="Pfad zu Watchlist (überschreibt env WATCHLIST_STOCKS)", default=None)
    args = ap.parse_args()
    main(args.watchlist)
