#!/usr/bin/env python3
# merge_cot_energy_into_20y.py
#
# Ziel:
#   - Socrata Disagg (20y) + CFTC Energy-Compressed in EIN File mergen
#   - Ausgabe: data/processed/cot_20y_disagg_merged.csv.gz
#
# Eingaben:
#   data/processed/cot_20y_disagg.csv(.gz)
#   data/processed/cot_disagg_energy_raw.csv(.gz)

import os
import pandas as pd

BASE = "data/processed"


def rd_csv(*paths: str) -> pd.DataFrame | None:
    """Liest die erste existierende CSV/CSV.GZ-Datei."""
    for p in paths:
        if p and os.path.exists(p):
            try:
                return pd.read_csv(p, compression="infer")
            except Exception as e:
                print("WARN: Fehler beim Lesen", p, "->", e)
                return None
    return None


def ensure_report_date(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Stellt sicher, dass es eine Spalte 'report_date_as_yyyy_mm_dd' gibt
    (String 'YYYY-MM-DD'), egal wie sie ursprünglich hieß.
    """
    cols = {c.lower(): c for c in df.columns}

    # Wenn die Spalte schon da ist -> nur sauber casten
    if "report_date_as_yyyy_mm_dd" in cols:
        col = cols["report_date_as_yyyy_mm_dd"]
        df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
            df[col], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        return df

    # Energy-Compressed: andere Namensvarianten
    cand = None
    if "as_of_date_in_form_yyyymmdd" in cols:
        cand = cols["as_of_date_in_form_yyyymmdd"]
        dt = pd.to_datetime(df[cand].astype(str), format="%Y%m%d", errors="coerce")
    elif "as_of_date_in_yyyy-mm-dd" in cols:
        cand = cols["as_of_date_in_yyyy-mm-dd"]
        dt = pd.to_datetime(df[cand], errors="coerce")
    elif "report_date_as_mm_dd_yyyy" in cols:
        cand = cols["report_date_as_mm_dd_yyyy"]
        dt = pd.to_datetime(df[cand].astype(str), format="%m/%d/%Y", errors="coerce")
    else:
        raise SystemExit(
            "%s: keine geeignete Datumsspalte gefunden. Vorhanden: %s"
            % (label, ", ".join(df.columns))
        )

    df["report_date_as_yyyy_mm_dd"] = dt.dt.strftime("%Y-%m-%d")
    return df


def main() -> None:
    # 1) Socrata-Disagg laden
    disagg = rd_csv(
        os.path.join(BASE, "cot_20y_disagg.csv"),
        os.path.join(BASE, "cot_20y_disagg.csv.gz"),
    )
    if disagg is None or disagg.empty:
        raise SystemExit("cot_20y_disagg.csv(.gz) nicht gefunden oder leer.")

    # 2) Energy-Compressed laden (optional)
    energy = rd_csv(
        os.path.join(BASE, "cot_disagg_energy_raw.csv"),
        os.path.join(BASE, "cot_disagg_energy_raw.csv.gz"),
    )

    # Wenn keine Energy-Daten vorhanden sind -> einfach Originalfile kopieren
    if energy is None or energy.empty:
        out = os.path.join(BASE, "cot_20y_disagg_merged.csv.gz")
        print("Keine Energy-Datei gefunden – schreibe nur Disagg nach", out)
        disagg.to_csv(out, index=False, compression="gzip")
        return

    print("Disagg rows:", len(disagg), "Energy rows:", len(energy))

    # 3) gemeinsamen Datumsschlüssel herstellen
    disagg = ensure_report_date(disagg, "disagg")
    energy = ensure_report_date(energy, "energy")

    # 4) Market-Name normalisieren
    if "market_and_exchange_names" not in disagg.columns:
        raise SystemExit("disagg: Spalte 'market_and_exchange_names' fehlt.")
    if "market_and_exchange_names" not in energy.columns:
        raise SystemExit("energy: Spalte 'market_and_exchange_names' fehlt.")

    disagg["market_and_exchange_names"] = disagg["market_and_exchange_names"].astype(str)
    energy["market_and_exchange_names"] = energy["market_and_exchange_names"].astype(str)

    # 5) gemeinsame Spaltenmenge bilden
    all_cols = sorted(set(disagg.columns) | set(energy.columns))
    for c in all_cols:
        if c not in disagg.columns:
            disagg[c] = pd.NA
        if c not in energy.columns:
            energy[c] = pd.NA

    disagg = disagg[all_cols].copy()
    energy = energy[all_cols].copy()

    # 6) Source markieren, damit wir bei Duplikaten Socrata bevorzugen
    if "source" not in disagg.columns:
        disagg["source"] = "socrata"
    else:
        disagg["source"] = disagg["source"].fillna("socrata")

    if "source" not in energy.columns:
        energy["source"] = "energy_txt"
    else:
        energy["source"] = energy["source"].fillna("energy_txt")

    merged = pd.concat([disagg, energy], ignore_index=True)

    # 7) Doppelte Kombinationen (market, date) bereinigen:
    #    Socrata zuerst, Energy nur als Lückenfüller.
    merged = merged.sort_values(
        ["market_and_exchange_names", "report_date_as_yyyy_mm_dd", "source"],
        ascending=[True, True, True],
    )
    merged = merged.drop_duplicates(
        subset=["market_and_exchange_names", "report_date_as_yyyy_mm_dd"],
        keep="first",
    )

    out = os.path.join(BASE, "cot_20y_disagg_merged.csv.gz")
    merged.to_csv(out, index=False, compression="gzip")
    print("✅ wrote", out, "rows", len(merged), "cols", len(merged.columns))


if __name__ == "__main__":
    main()
