#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_cot_energy_coverage.py

Liest data/processed/cot_disagg_energy_raw.csv.gz und baut eine Coverage-Tabelle:
  market_and_exchange_names, dataset, first_date, last_date, rows,
  watchlist_match, in_watchlist

Output:
  data/reports/cot_energy_coverage.csv
"""

import os
import pandas as pd

PROC_DIR = "data/processed"
REPORTS_DIR = "data/reports"
ENERGY_FILE = os.getenv(
    "CFTC_DISAGG_ENERGY_OUT",
    os.path.join(PROC_DIR, "cot_disagg_energy_raw.csv.gz"),
)
COT_WATCHLIST_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")


def read_watchlist(path: str):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith(("#", "//")):
                out.append(ln)
    return out


def main():
    if not os.path.exists(ENERGY_FILE):
        raise SystemExit("Energy-Datei fehlt: %s" % ENERGY_FILE)

    df = pd.read_csv(ENERGY_FILE, compression="infer", low_memory=False)

    cols_lower = {c: c.strip().lower() for c in df.columns}
    df.rename(columns=cols_lower, inplace=True)

    if "market_and_exchange_names" not in df.columns:
        raise SystemExit("Spalte market_and_exchange_names fehlt in %s" % ENERGY_FILE)

    if "report_date_as_yyyy_mm_dd" not in df.columns:
        raise SystemExit("Spalte report_date_as_yyyy_mm_dd fehlt in %s" % ENERGY_FILE)

    df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
        df["report_date_as_yyyy_mm_dd"], errors="coerce"
    )

    wl = read_watchlist(COT_WATCHLIST_FILE)
    wl_upper = [w.upper() for w in wl]

    # Coverage je Markt
    grp = (
        df.groupby("market_and_exchange_names")["report_date_as_yyyy_mm_dd"]
        .agg(["min", "max", "count"])
        .reset_index()
    )

    grp.rename(
        columns={
            "min": "first_date",
            "max": "last_date",
            "count": "rows",
        },
        inplace=True,
    )

    # Watchlist-Match / in_watchlist
    def find_match(name: str):
        up = name.upper()
        if up in wl_upper:
            return name  # exakter Treffer
        # Fuzzy: Watchlist-Eintrag als Substring oder umgekehrt
        for w in wl:
            w_up = w.upper()
            if w_up in up or up in w_up:
                return w
        return ""

    grp["watchlist_match"] = grp["market_and_exchange_names"].apply(find_match)
    grp["in_watchlist"] = grp["watchlist_match"].apply(lambda x: bool(x))

    grp["dataset"] = "disagg_csv"

    # Spalten-Reihenfolge wie bei deinem Coverage-Snippet
    cols = [
        "market_and_exchange_names",
        "dataset",
        "first_date",
        "last_date",
        "rows",
        "watchlist_match",
        "in_watchlist",
    ]
    grp = grp[cols]

    os.makedirs(REPORTS_DIR, exist_ok=True)
    out_path = os.path.join(REPORTS_DIR, "cot_energy_coverage.csv")
    grp.to_csv(out_path, index=False)
    print("✅ wrote", out_path, "rows", len(grp))


if __name__ == "__main__":
    main()
