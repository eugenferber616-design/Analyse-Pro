#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_cot_energy_coverage.py

Liest data/processed/cot_disagg_energy_raw.csv.gz (CFTC Energy-Disagg-CSV),
normalisiert Spaltennamen und erstellt eine Coverage-Tabelle:

data/reports/cot_energy_coverage.csv mit Spalten:
  market_and_exchange_names, dataset, first_date, last_date,
  rows, watchlist_match, in_watchlist
"""

import os
import pandas as pd

PROC = "data/processed"
REPORTS = "data/reports"
ENERGY_FILE = os.path.join(PROC, "cot_disagg_energy_raw.csv.gz")
WATCHLIST_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")


def read_watchlist(path):
    items = []
    if not os.path.exists(path):
        return items
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith(("#", "//")):
                continue
            items.append(ln)
    return items


def find_col(df, candidates, label):
    """
    Sucht in df nach einer der Kandidaten (case-insensitive / trim).
    Gibt den echten Spaltennamen zurück oder None.
    """
    # Map: lower(strip(name)) -> original
    mapping = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in mapping:
            return mapping[key]
    return None


def main():
    if not os.path.exists(ENERGY_FILE):
        raise SystemExit("Energy-File fehlt: %s" % ENERGY_FILE)

    os.makedirs(REPORTS, exist_ok=True)

    df = pd.read_csv(ENERGY_FILE, compression="infer")

    if df.empty:
        raise SystemExit("Energy-File ist leer: %s" % ENERGY_FILE)

    # ---- Datumsspalte normalisieren ---------------------------------------
    date_candidates = [
        "report_date_as_yyyy_mm_dd",
        "Report_Date_as_YYYY_MM_DD",
        "As_of_Date_in_YYYY-MM-DD",
        "As of Date in YYYY-MM-DD",
        "As of Date in YYYY-MM-DD ",
    ]
    date_col = find_col(df, date_candidates, "report_date")
    if not date_col:
        raise SystemExit(
            "Spalte report_date_as_yyyy_mm_dd/As_of_Date_in_YYYY-MM-DD fehlt in %s. Vorhanden: %s"
            % (ENERGY_FILE, ", ".join(df.columns))
        )

    df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
        df[date_col], errors="coerce"
    ).dt.date

    # ---- Marktname normalisieren ------------------------------------------
    mkt_candidates = [
        "market_and_exchange_names",
        "Market_and_Exchange_Names",
        "Market_and_Exchange_Name",
        "Market and Exchange Names",
    ]
    mkt_col = find_col(df, mkt_candidates, "market_and_exchange_names")
    if not mkt_col:
        raise SystemExit(
            "Spalte market_and_exchange_names fehlt in %s. Vorhanden: %s"
            % (ENERGY_FILE, ", ".join(df.columns))
        )

    df["market_and_exchange_names"] = df[mkt_col].astype(str).str.strip()

    # ---- Coverage je Markt berechnen --------------------------------------
    grp = (
        df.groupby("market_and_exchange_names")["report_date_as_yyyy_mm_dd"]
        .agg(["min", "max", "count"])
        .reset_index()
        .rename(
            columns={
                "min": "first_date",
                "max": "last_date",
                "count": "rows",
            }
        )
    )
    grp.insert(1, "dataset", "energy_disagg")

    # ---- Watchlist-Matching -----------------------------------------------
    wl = read_watchlist(WATCHLIST_FILE)
    wl_set = set(wl)

    def match_wl(name):
        name = name.strip()
        return name if name in wl_set else ""

    grp["watchlist_match"] = grp["market_and_exchange_names"].map(match_wl)
    grp["in_watchlist"] = grp["watchlist_match"].ne("")

    out = os.path.join(REPORTS, "cot_energy_coverage.csv")
    grp.to_csv(out, index=False)
    print("✅ wrote", out, "rows:", len(grp))


if __name__ == "__main__":
    main()
