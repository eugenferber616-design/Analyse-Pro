# scripts/build_cds_proxy_v2.py
# -*- coding: utf-8 -*-
"""
CDS-Proxy je Symbol:
- Region-Erkennung (US/EU) aus Ticker-Suffix oder Fundamentals.country
- IG/HY-Heuristik aus Fundamentals (marktnahe, robuste Defaults)
- OAS-Zuordnung aus data/processed/fred_oas.csv
- EU_HY → Fallback auf US_HY (+ optionaler Aufschlag)
- Ausgabe: data/processed/cds_proxy.csv  (symbol,region,proxy_spread)

Aufruf:
  python scripts/build_cds_proxy_v2.py \
      --watchlist watchlists/mylist.txt \
      --eu-hy-premium 0.00

Optional:
  --fundamentals data/processed/fundamentals_core.csv
  --fred-oas      data/processed/fred_oas.csv
"""

import argparse
import csv
import json
import math
import os
from typing import Dict, Optional

import pandas as pd


DEF_WATCHLIST = "watchlists/mylist.txt"
DEF_FRED_OAS = "data/processed/fred_oas.csv"
DEF_FUNDS = "data/processed/fundamentals_core.csv"
OUT_CSV = "data/processed/cds_proxy.csv"
PREVIEW_TXT = "data/reports/eu_checks/cds_proxy_preview.txt"
REPORT_JSON = "data/reports/cds_proxy_report.json"


# --------- Helpers ---------
EU_SUFFIXES = {
    ".DE", ".PA", ".AS", ".MI", ".BR", ".MC", ".BE", ".SW", ".OL", ".ST",
    ".HE", ".CO", ".IR", ".LS", ".WA", ".VI", ".PR", ".PRG", ".VX", ".L"
}
# .L (London) ist nicht Eurozone; wir nutzen "EU" im Sinne "Nicht-US Europa" für den Proxy.
# Das genügt, da wir EU_IG aus Euro Indices ziehen und EU_HY via Fallback behandeln.


def read_watchlist(path: str) -> pd.Series:
    """Akzeptiert .txt (ein Ticker pro Zeile) oder .csv (Spalte 'symbol')."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Watchlist nicht gefunden: {path}")
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)
        col = "symbol" if "symbol" in df.columns else df.columns[0]
        syms = df[col].astype(str).str.strip()
    else:
        # .txt: pro Zeile ein Symbol; Kommentarzeilen/Leerzeilen ignorieren
        vals = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                t = line.strip()
                if not t or t.startswith("#") or t.lower().startswith("symbol"):
                    continue
                vals.append(t.split(",")[0].strip())
        syms = pd.Series(vals, name="symbol")
    syms = syms[syms != ""].drop_duplicates().reset_index(drop=True)
    return syms


def load_fred_oas(path: str) -> pd.DataFrame:
    """
    Erwartet Spalten: date, series_id, value, bucket, region
    Nimmt je (region,bucket) den letzten Wert.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"FRED-OAS fehlt: {path}")
    df = pd.read_csv(path)
    # Datentypen robust
    for c in ["region", "bucket", "series_id"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "date" in df.columns:
        # neueste zuerst
        df = df.sort_values("date")
    # letzter Wert pro (region,bucket)
    last = df.groupby(["region", "bucket"], as_index=False).tail(1)
    # in Dict: (region,bucket) -> value
    out = {}
    for _, r in last.iterrows():
        out[(r["region"], r["bucket"])] = float(r["value"]) if pd.notna(r["value"]) else math.nan
    return pd.DataFrame(
        [{"region": k[0], "bucket": k[1], "value": v} for k, v in out.items()]
    )


def load_fundamentals(path: str) -> pd.DataFrame:
    """
    Lädt Fundamentals; Spalten sind je nach Fetcher unterschiedlich.
    Wir nutzen defensiv: symbol, market_cap, debt_to_equity, net_margin, country
    """
    if not os.path.exists(path):
        # leeres DF -> Heuristik fällt deutlich konservativer aus
        return pd.DataFrame(columns=["symbol", "country", "market_cap", "debt_to_equity", "net_margin"])
    df = pd.read_csv(path)
    # Normalize columns
    cols_lower = {c: c.lower() for c in df.columns}
    df.columns = [cols_lower[c] for c in df.columns]
    # erwarte "symbol"; falls nicht, versuche aus "name" etc. – ansonsten fail-safe
    if "symbol" not in df.columns:
        # Manche Exporte haben in der ersten Spalte den Ticker
        df = df.rename(columns={df.columns[0]: "symbol"})
    # Datentypen
    for num in ["market_cap", "debt_to_equity", "net_margin"]:
        if num in df.columns:
            df[num] = pd.to_numeric(df[num], errors="coerce")
    if "country" not in df.columns:
        df["country"] = None
    # eindeutige letzte Zeile pro symbol
    df = df.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)
    return df[["symbol", "country", "market_cap", "debt_to_equity", "net_margin"]]


def infer_region(symbol: str, fund_country: Optional[str]) -> str:
    s = symbol.upper()
    # 1) Ticker-Suffix
    for suf in EU_SUFFIXES:
        if s.endswith(suf):
            return "EU"
    # 2) Country-Backup
    if fund_country:
        c = str(fund_country).strip().upper()
        if c and c not in ("US", "USA", "UNITED STATES", "UNITED-STATES"):
            return "EU"
    return "US"


def infer_bucket(row: pd.Series) -> str:
    """
    IG/HY-Heuristik (robust, konservativ):
      IG wenn
        - market_cap ≥ 5 Mrd  (5e9)  UND
        - debt_to_equity ≤ 150 (oder fehlt) UND
        - net_margin >= 0 (oder fehlt)
      sonst HY
    """
    mc = row.get("market_cap", float("nan"))
    dte = row.get("debt_to_equity", float("nan"))
    nm = row.get("net_margin", float("nan"))

    cond_mc = (pd.notna(mc) and mc >= 5e9)
    cond_dte = (pd.isna(dte) or dte <= 150.0)
    cond_nm = (pd.isna(nm) or nm >= 0.0)

    return "IG" if (cond_mc and cond_dte and cond_nm) else "HY"


def pick_oas(oas_df: pd.DataFrame, region: str, bucket: str,
             eu_hy_premium: float = 0.0) -> Optional[float]:
    """
    Holt (region,bucket) -> value; EU_HY-Fallback auf US_HY (+Premium).
    """
    # Direkt verfügbar?
    m = oas_df[(oas_df["region"] == region) & (oas_df["bucket"] == bucket)]
    if len(m):
        val = m["value"].iloc[-1]
        if pd.notna(val):
            return float(val)

    # EU_HY Fallback: US_HY + Premium
    if region == "EU" and bucket == "HY":
        mu = oas_df[(oas_df["region"] == "US") & (oas_df["bucket"] == "HY")]
        if len(mu):
            v = mu["value"].iloc[-1]
            if pd.notna(v):
                return float(v) + float(eu_hy_premium)

    # Letzte Reserve: US_IG
    r = oas_df[(oas_df["region"] == "US") & (oas_df["bucket"] == "IG")]
    if len(r):
        rv = r["value"].iloc[-1]
        if pd.notna(rv):
            return float(rv)
    return None


def ensure_dirs():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(PREVIEW_TXT), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)


# --------- Main ---------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default=DEF_WATCHLIST, help="Pfad zur Watchlist (.txt/.csv)")
    ap.add_argument("--fred-oas", default=DEF_FRED_OAS, help="Pfad zu data/processed/fred_oas.csv")
    ap.add_argument("--fundamentals", default=DEF_FUNDS, help="Pfad zu data/processed/fundamentals_core.csv")
    ap.add_argument("--eu-hy-premium", type=float, default=0.00,
                    help="Aufschlag (in %-Punkten) wenn EU_HY auf US_HY fällt, z.B. 0.20")
    args = ap.parse_args()

    ensure_dirs()

    # Eingaben laden
    syms = read_watchlist(args.watchlist)
    oas_df = load_fred_oas(args.fred_oas)
    funds = load_fundamentals(args.fundamentals)

    # Für schnelle Joins: Fundamentals per Symbol
    funds_idx = funds.set_index("symbol") if "symbol" in funds.columns else pd.DataFrame().set_index(pd.Index([]))

    out_rows = []
    preview_lines = ["symbol,region,proxy_spread"]
    errors = []

    for sym in syms:
        fs = funds_idx.loc[sym] if sym in funds_idx.index else None
        country = None
        if fs is not None and "country" in funds_idx.columns:
            country = fs.get("country", None)

        region = infer_region(sym, country)

        # Heuristik-Datenzeile bauen
        if fs is None:
            base = pd.Series({"market_cap": float("nan"),
                              "debt_to_equity": float("nan"),
                              "net_margin": float("nan")})
        else:
            base = fs

        bucket = infer_bucket(base)

        val = pick_oas(oas_df, region, bucket, eu_hy_premium=args.eu_hy_premium)

        if val is None or pd.isna(val):
            errors.append({"symbol": sym, "reason": "no_oas_value"})
            preview_lines.append(f"{sym},{region},nan")
        else:
            out_rows.append({"symbol": sym, "region": f"{region}_{bucket}", "proxy_spread": round(float(val), 2)})
            preview_lines.append(f"{sym},{region}_{bucket},{round(float(val), 2)}")

    # Schreiben
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "region", "proxy_spread"])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    with open(PREVIEW_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(preview_lines) + "\n")

    # Report
    fred_map = {(r["region"], r["bucket"]): r["value"] for _, r in oas_df.iterrows()}
    rep = {
        "ts": pd.Timestamp.utcnow().isoformat() + "Z",
        "rows": len(out_rows),
        "fred_oas_used": {
            "US_IG": fred_map.get(("US", "IG")),
            "US_HY": fred_map.get(("US", "HY")),
            "EU_IG": fred_map.get(("EU", "IG")),
            "EU_HY": fred_map.get(("EU", "HY")),  # bleibt i. d. R. None
        },
        "eu_hy_premium": args.eu_hy_premium,
        "errors": errors,
        "preview": OUT_CSV,
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

    # Konsolen-Ausgabe (kurz)
    print(json.dumps({k: v for k, v in rep.items() if k in ("ts", "rows", "fred_oas_used", "errors", "preview")}, indent=2))


if __name__ == "__main__":
    main()
