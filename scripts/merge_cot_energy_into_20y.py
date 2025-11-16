#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
merge_cot_energy_into_20y.py

Mergt die bestehenden 20y-Disaggregated-Daten (Socrata) mit den neuen
Energy-Daten aus den CFTC Historical Compressed Textfiles.

Input:
  - data/processed/cot_20y_disagg.csv.gz       (bisheriger Socrata-Pull)
  - data/processed/cot_disagg_energy_raw.csv.gz (neue Energy-Daten)

Output:
  - data/processed/cot_20y_disagg_merged.csv.gz

Regeln:
  - Schlüssel: (market_and_exchange_names, report_date_as_yyyy_mm_dd)
  - Wenn es für einen Schlüssel sowohl eine Socrata-Zeile als auch eine
    Energy-Zeile gibt → Energy gewinnt (ersetzt Socrata).
  - Neue Dates (z.B. > 2022) kommen komplett aus dem Energy-File dazu.
"""

import os
import gzip
import pandas as pd

BASE_PATH = os.getenv(
    "COT_20Y_DISAGG_IN",
    "data/processed/cot_20y_disagg.csv.gz",
)
ENERGY_PATH = os.getenv(
    "COT_ENERGY_IN",
    "data/processed/cot_disagg_energy_raw.csv.gz",
)
OUT_PATH = os.getenv(
    "COT_20Y_DISAGG_MERGED_OUT",
    "data/processed/cot_20y_disagg_merged.csv.gz",
)


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Spaltennamen vereinheitlichen."""
    df = df.copy()
    mapping = {}
    for c in df.columns:
        cl = c.strip()
        cl_low = cl.lower()

        # Datumsspalten-Varianten zur Standardspalte mappen
        if cl_low in ("report_date_as_yyyy_mm_dd", "report_date_as_yyyy_mm-dd".lower(),
                      "report_date_as_yyyy_mm_dd".lower()):
            mapping[c] = "report_date_as_yyyy_mm_dd"
        elif cl_low in ("report_date", "report date"):
            mapping[c] = "report_date_as_yyyy_mm_dd"

        # Market-Name-Varianten
        elif cl_low in ("market_and_exchange_names", "market_and_exchange_name",
                        "market and exchange names"):
            mapping[c] = "market_and_exchange_names"

        else:
            # alles andere klein schreiben, aber sonst lassen
            mapping[c] = cl_low

    df = df.rename(columns=mapping)
    return df


def _load_csv(path: str, name: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise SystemExit(f"{name} fehlt: {path}")
    try:
        df = pd.read_csv(path, compression="infer", low_memory=False)
    except Exception as e:
        raise SystemExit(f"Konnte {name} ({path}) nicht lesen: {e}")
    return df


def main():
    # ---------------- Dateien laden ----------------
    base = _load_csv(BASE_PATH, "Socrata-20y-disagg")
    energy = _load_csv(ENERGY_PATH, "CFTC-Energy-Disagg")

    base = _norm_cols(base)
    energy = _norm_cols(energy)

    # Schlüsselspalten prüfen
    for df, label in ((base, "base"), (energy, "energy")):
        if "market_and_exchange_names" not in df.columns:
            raise SystemExit(f"{label}: Spalte market_and_exchange_names fehlt")
        if "report_date_as_yyyy_mm_dd" not in df.columns:
            raise SystemExit(f"{label}: Spalte report_date_as_yyyy_mm_dd fehlt")

    # Datumstyp
    for df in (base, energy):
        df["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
            df["report_date_as_yyyy_mm_dd"], errors="coerce"
        )

    # Diagnose vor dem Merge
    print("=== Vor Merge ===")
    print("base rows:", len(base), "cols:", len(base.columns))
    print("energy rows:", len(energy), "cols:", len(energy.columns))

    # Schlüssel definieren
    key_cols = ["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]

    # Duplikate innerhalb der einzelnen Quellen entfernen (falls vorhanden)
    base = base.drop_duplicates(subset=key_cols, keep="last")
    energy = energy.drop_duplicates(subset=key_cols, keep="last")

    # Index setzen
    base_idx = base.set_index(key_cols)
    energy_idx = energy.set_index(key_cols)

    # Welche Schlüssel hat Energy?
    energy_keys = energy_idx.index

    # 1) base_only = alle base-Zeilen, deren Schlüssel NICHT in Energy vorkommen
    mask_base_keep = ~base_idx.index.isin(energy_keys)
    base_only = base_idx[mask_base_keep]

    print("base_only rows:", len(base_only))
    print("energy rows (übernehmen):", len(energy_idx))

    # 2) kombinieren: erst base_only, dann energy (Energy gewinnt bei Konflikt)
    merged_idx = pd.concat([base_only, energy_idx], axis=0)

    # zurück zu DataFrame mit Spalten
    merged = merged_idx.reset_index()

    # optional: nach Market + Datum sortieren
    merged = merged.sort_values(
        ["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]
    ).reset_index(drop=True)

    print("=== Nach Merge ===")
    print("merged rows:", len(merged), "cols:", len(merged.columns))

    # speichern als CSV.GZ
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with gzip.open(OUT_PATH, "wt", encoding="utf-8", newline="") as gz:
        merged.to_csv(gz, index=False)

    print(f"✅ wrote {OUT_PATH} rows={len(merged)} cols={len(merged.columns)}")

    # kleine Öl-Diagnose (nur Textausgabe)
    oil_mask = merged["market_and_exchange_names"].str.contains(
        "CRUDE OIL|BRENT|RBOB|HEATING OIL|ULSD|NATURAL GAS",
        case=False,
        na=False,
    )
    oil = merged[oil_mask]
    if not oil.empty:
        cov = (
            oil.groupby("market_and_exchange_names")["report_date_as_yyyy_mm_dd"]
            .agg(["min", "max", "count"])
            .reset_index()
        )
        print("=== Öl/Energy Coverage (merged) ===")
        print(cov.to_string(index=False))
    else:
        print("WARN: Keine Öl-/Energy-Märkte im merged-File gefunden (Filter leer).")


if __name__ == "__main__":
    main()
