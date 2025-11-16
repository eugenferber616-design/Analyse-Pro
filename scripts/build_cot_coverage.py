#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_cot_coverage.py – Übersicht über COT-Märkte + Coverage für deine Watchlist.

- Liest:
    data/processed/cot_20y_disagg.csv[.gz]
    data/processed/cot_20y_tff.csv[.gz]
    watchlists/cot_markets.txt

- Schreibt:
    data/reports/cot_markets_coverage.csv
    data/reports/cot_markets_missing.txt
    data/reports/cot_market_names_all.txt   <-- NEU: alle Original-CFTC-Namen (TXT)
"""

import os
import pandas as pd

PROC = "data/processed"
REPORT_DIR = "data/reports"
WATCHLIST = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

os.makedirs(REPORT_DIR, exist_ok=True)


def rd_cot(name: str):
    """
    Lädt eine COT-Datei (csv oder csv.gz), wenn vorhanden.
    name z.B.: "cot_20y_disagg.csv" (wir probieren csv und csv.gz).
    """
    base = os.path.join(PROC, name)
    for p in (base, base + ".gz"):
        if os.path.exists(p):
            try:
                return pd.read_csv(p, compression="infer")
            except Exception as e:
                print("WARN: konnte", p, "nicht lesen:", e)
    return None


def read_watchlist(path: str):
    if not os.path.exists(path):
        print("INFO: Watchlist", path, "nicht gefunden.")
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln or ln.startswith("#") or ln.startswith("//"):
                continue
            out.append(ln)
    return out


def norm(s: str) -> str:
    """
    Grobe Normalisierung: Uppercase, mehrere Spaces entfernen.
    Genug für einfache Contains-Checks.
    """
    if not isinstance(s, str):
        s = str(s or "")
    s = s.upper().replace("’", "'")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def main():
    # 1) COT-Dateien laden
    dis = rd_cot("cot_20y_disagg.csv")
    tff = rd_cot("cot_20y_tff.csv")

    if dis is None and tff is None:
        raise SystemExit("Keine COT-Dateien in data/processed gefunden (cot_20y_disagg/tff).")

    dfs = []
    if dis is not None:
        dis = dis.copy()
        dis["dataset"] = "disagg"
        dfs.append(dis)
    if tff is not None:
        tff = tff.copy()
        tff["dataset"] = "tff"
        dfs.append(tff)

    df = pd.concat(dfs, ignore_index=True)

    # Spaltennamen robust finden
    cols = {c.lower(): c for c in df.columns}

    name_col = cols.get("market_and_exchange_names") or cols.get("market_and_exchange_name")
    if not name_col:
        raise SystemExit("Spalte market_and_exchange_names nicht gefunden.")

    # Datumsspalte
    date_col = None
    for cand in ["report_date_as_yyyy_mm_dd", "report_date", "as_of_date_in_form_yyyy_mm_dd"]:
        if cand in cols:
            date_col = cols[cand]
            break
    if not date_col:
        raise SystemExit("Konnte keine geeignete Datumsspalte finden.")

    # 2) NEU: vollständige Namensliste als TXT
    names_all = (
        df[name_col]
        .dropna()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    names_path = os.path.join(REPORT_DIR, "cot_market_names_all.txt")
    with open(names_path, "w", encoding="utf-8") as f:
        for n in names_all:
            f.write(str(n).strip() + "\n")
    print("wrote", names_path, "names:", len(names_all))

    # 3) Min/Max-Datum + Rowanzahl je Markt + Dataset
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    grp = (
        df.groupby([name_col, "dataset"], dropna=False)[date_col]
        .agg(["min", "max", "count"])
        .reset_index()
        .rename(
            columns={
                name_col: "market_and_exchange_names",
                "min": "first_date",
                "max": "last_date",
                "count": "rows",
            }
        )
    )

    # 4) Watchlist laden und simple Match-Flag erzeugen
    wl_raw = read_watchlist(WATCHLIST)
    wl_norm = [norm(x) for x in wl_raw]

    def match_watchlist(mkt: str) -> str:
        mm = norm(mkt)
        hits = []
        for w_raw, w_norm in zip(wl_raw, wl_norm):
            if mm == w_norm or mm.find(w_norm) >= 0 or w_norm.find(mm) >= 0:
                hits.append(w_raw)
        return "; ".join(hits) if hits else ""

    grp["watchlist_match"] = grp["market_and_exchange_names"].apply(match_watchlist)
    grp["in_watchlist"] = grp["watchlist_match"].apply(lambda x: bool(x))

    # 5) Coverage-CSV schreiben
    out_csv = os.path.join(REPORT_DIR, "cot_markets_coverage.csv")
    grp.sort_values(
        ["in_watchlist", "market_and_exchange_names", "dataset"],
        ascending=[False, True, True]
    ).to_csv(out_csv, index=False)
    print("wrote", out_csv, "rows:", len(grp))

    # 6) Welche Watchlist-Einträge wurden gar nicht gematcht?
    matched = set()
    for m in grp["watchlist_match"]:
        if not m:
            continue
        for part in m.split(";"):
            p = part.strip()
            if p:
                matched.add(p)

    missing = [x for x in wl_raw if x not in matched]
    miss_path = os.path.join(REPORT_DIR, "cot_markets_missing.txt")
    with open(miss_path, "w", encoding="utf-8") as f:
        for m in missing:
            f.write(m + "\n")
    print("wrote missing list:", miss_path, "entries:", len(missing))


if __name__ == "__main__":
    main()
