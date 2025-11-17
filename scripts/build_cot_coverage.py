#!/usr/bin/env python3
# build_cot_coverage.py
#
# Liest:
#   - data/processed/cot_20y_disagg.csv(.gz)
#   - data/processed/cot_20y_tff.csv(.gz)
#   - optional: data/processed/cot_disagg_energy_raw.csv(.gz)
# und baut:
#   - data/reports/cot_markets_coverage.csv
#   - data/reports/cot_markets_missing.txt
#   - data/reports/cot_market_names_all.txt

import os
import pandas as pd

BASE_PROC = "data/processed"
BASE_REP  = "data/reports"
WATCHLIST = "watchlists/cot_markets.txt"


def rd_csv(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, compression="infer")
    except Exception:
        return None


def normalise_dates(df: pd.DataFrame, dataset_label: str) -> pd.DataFrame:
    """
    Sorgt dafür, dass wir immer eine Spalte 'report_date' (datetime)
    + 'market_and_exchange_names' + 'dataset' haben.
    Behandelt Sonderfall: Energie-Rohdatei mit anderen Datumsnamen.
    """
    cols = {c.lower(): c for c in df.columns}

    # Marktname
    name_col = cols.get("market_and_exchange_names") or cols.get("market_and_exchange_name")
    if not name_col:
        raise SystemExit("Spalte 'market_and_exchange_names' fehlt in Dataset %s" % dataset_label)

    # Datumsspalten in bevorzugter Reihenfolge
    date_col = None
    if "report_date_as_yyyy_mm_dd" in cols:
        date_col = cols["report_date_as_yyyy_mm_dd"]
    elif "as_of_date_in_yyyy-mm-dd" in cols:
        date_col = cols["as_of_date_in_yyyy-mm-dd"]
    elif "as_of_date_in_form_yyyymmdd" in cols:
        date_col = cols["as_of_date_in_form_yyyymmdd"]
    elif "report_date_as_mm_dd_yyyy" in cols:
        date_col = cols["report_date_as_mm_dd_yyyy"]

    if date_col is None:
        raise SystemExit(
            "Spalte report_date_as_yyyy_mm_dd/As_of_Date_in_YYYY-MM-DD fehlt in Dataset %s. "
            "Vorhanden: %s" % (dataset_label, ", ".join(df.columns))
        )

    s = df[[name_col, date_col]].copy()
    s.rename(columns={name_col: "market_and_exchange_names"}, inplace=True)

    # Datumsparsing je nach Format
    if date_col.lower() in ("report_date_as_yyyy_mm_dd", "as_of_date_in_yyyy-mm-dd"):
        s["report_date"] = pd.to_datetime(s[date_col], errors="coerce")
    elif date_col.lower() == "as_of_date_in_form_yyyymmdd":
        s["report_date"] = pd.to_datetime(s[date_col].astype(str), format="%Y%m%d", errors="coerce")
    elif date_col.lower() == "report_date_as_mm_dd_yyyy":
        s["report_date"] = pd.to_datetime(s[date_col].astype(str), format="%m/%d/%Y", errors="coerce")
    else:
        s["report_date"] = pd.to_datetime(s[date_col], errors="coerce")

    s = s.dropna(subset=["report_date"])
    s["dataset"] = dataset_label
    return s[["market_and_exchange_names", "dataset", "report_date"]]


def load_watchlist(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    out: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            out.append(t)
    return out


def main() -> None:
    os.makedirs(BASE_REP, exist_ok=True)

    parts: list[pd.DataFrame] = []

    # 1) Disaggregated von Socrata
    dis = rd_csv(os.path.join(BASE_PROC, "cot_20y_disagg.csv")) or rd_csv(
        os.path.join(BASE_PROC, "cot_20y_disagg.csv.gz")
    )
    if dis is not None and not dis.empty:
        parts.append(normalise_dates(dis, "disagg"))

    # 2) TFF von Socrata
    tff = rd_csv(os.path.join(BASE_PROC, "cot_20y_tff.csv")) or rd_csv(
        os.path.join(BASE_PROC, "cot_20y_tff.csv.gz")
    )
    if tff is not None and not tff.empty:
        parts.append(normalise_dates(tff, "tff"))

    # 3) Optionale Energie-Rohdatei (Compressed History)
    energy = rd_csv(os.path.join(BASE_PROC, "cot_disagg_energy_raw.csv")) or rd_csv(
        os.path.join(BASE_PROC, "cot_disagg_energy_raw.csv.gz")
    )
    if energy is not None and not energy.empty:
        parts.append(normalise_dates(energy, "energy_raw"))

    if not parts:
        raise SystemExit("Keine COT-Daten gefunden (disagg/tff/energy_raw).")

    all_df = pd.concat(parts, ignore_index=True)
    all_df["market_and_exchange_names"] = all_df["market_and_exchange_names"].astype(str)

    # Coverage je Markt + Dataset
    cov = (
        all_df.groupby(["market_and_exchange_names", "dataset"])["report_date"]
        .agg(
            first_date=lambda s: s.min().strftime("%Y-%m-%d"),
            last_date=lambda s: s.max().strftime("%Y-%m-%d"),
            rows="size",
        )
        .reset_index()
    )

    # Watchlist-Mapping
    wl = load_watchlist(WATCHLIST)
    wl_set = set(wl)

    # direktes Matching: watchlist == market_and_exchange_names
    cov["watchlist_match"] = cov["market_and_exchange_names"].where(
        cov["market_and_exchange_names"].isin(wl_set),
        ""
    )
    cov["in_watchlist"] = cov["watchlist_match"].ne("")

    # fehlende Watchlist-Einträge
    covered_names = set(cov["market_and_exchange_names"].unique())
    missing = [w for w in wl if w not in covered_names]

    # Dateien schreiben
    cov_path = os.path.join(BASE_REP, "cot_markets_coverage.csv")
    cov.to_csv(cov_path, index=False)
    print("wrote", cov_path, "rows", len(cov))

    miss_path = os.path.join(BASE_REP, "cot_markets_missing.txt")
    with open(miss_path, "w", encoding="utf-8") as f:
        for m in missing:
            f.write(m + "\n")
    print("wrote", miss_path, "entries", len(missing))

    names_path = os.path.join(BASE_REP, "cot_market_names_all.txt")
    with open(names_path, "w", encoding="utf-8") as f:
        for n in sorted(covered_names):
            f.write(n + "\n")
    print("wrote", names_path, "unique markets", len(covered_names))


if __name__ == "__main__":
    main()
