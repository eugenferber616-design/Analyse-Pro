#!/usr/bin/env python3
# build_cot_energy_coverage.py
#
# Liest data/processed/cot_disagg_energy_raw.csv.gz (CFTC Energy Disagg),
# normalisiert das Datumsfeld auf report_date_as_yyyy_mm_dd
# und schreibt eine Coverage-Tabelle + komplette Marktnamenliste.

import os
import pandas as pd


RAW_PATH = "data/processed/cot_disagg_energy_raw.csv.gz"
REPORTS_DIR = "data/reports"


def ensure_date_column(df: pd.DataFrame, path: str) -> pd.DataFrame:
    """
    Stellt sicher, dass eine Spalte 'report_date_as_yyyy_mm_dd' existiert.
    Nutzt alternativ:
      - report_date_as_mm_dd_yyyy
      - as_of_date_in_form_yymmdd
    und schreibt die normalisierte Datei zurück.
    """
    lower_map = {c.lower(): c for c in df.columns}
    # gibt es schon eine passende Spalte?
    for key in ["report_date_as_yyyy_mm_dd", "report_date_as_yyyymmdd"]:
        if key in lower_map:
            # ggf. auf einheitlichen Namen umbenennen
            col = lower_map[key]
            if col != "report_date_as_yyyy_mm_dd":
                df = df.rename(columns={col: "report_date_as_yyyy_mm_dd"})
            return df

    # sonst: Alternativen suchen
    src_col = None
    fmt_hint = None

    # 1) MM/DD/YYYY oder ähnliches
    if "report_date_as_mm_dd_yyyy" in lower_map:
        src_col = lower_map["report_date_as_mm_dd_yyyy"]
        fmt_hint = None  # pandas rät das schon ganz gut

    # 2) YYMMDD-Formular-Datum
    elif "as_of_date_in_form_yymmdd" in lower_map:
        src_col = lower_map["as_of_date_in_form_yymmdd"]
        fmt_hint = "%y%m%d"

    if src_col is None:
        # Nichts brauchbares gefunden → klarer Fehler mit Spaltenliste
        cols = ", ".join(df.columns)
        raise SystemExit(
            "Spalte für Datum fehlt in {path}. Erwartet eine von: "
            "report_date_as_yyyy_mm_dd, report_date_as_mm_dd_yyyy, "
            "as_of_date_in_form_yymmdd.\nVorhandene Spalten: "
            f"{cols}"
        )

    # Datumswerte parsen und normalisieren
    if fmt_hint:
        dates = pd.to_datetime(df[src_col], format=fmt_hint, errors="coerce")
    else:
        dates = pd.to_datetime(df[src_col], errors="coerce")

    df["report_date_as_yyyy_mm_dd"] = dates.dt.strftime("%Y-%m-%d")

    # Datei mit neuer Spalte zurückschreiben, damit Folgeskripte sie ebenfalls sehen
    df.to_csv(path, index=False, compression="gzip")
    print(f"✅ report_date_as_yyyy_mm_dd aus {src_col} erzeugt und nach {path} geschrieben")
    return df


def read_watchlist(path: str):
    if not os.path.exists(path):
        return []
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or s.startswith("//"):
                continue
            items.append(s)
    return items


def main():
    if not os.path.exists(RAW_PATH):
        raise SystemExit(f"{RAW_PATH} fehlt (vorherigen Step 'Fetch CFTC Energy Disagg' prüfen)")

    df = pd.read_csv(RAW_PATH, compression="infer")
    if df.empty:
        raise SystemExit(f"{RAW_PATH} ist leer")

    # Spaltennamen-Map
    lower_map = {c.lower(): c for c in df.columns}

    # Marktspalte finden
    market_col = lower_map.get("market_and_exchange_names")
    if not market_col:
        raise SystemExit(
            "Spalte 'market_and_exchange_names' fehlt in "
            f"{RAW_PATH}. Spalten: {', '.join(df.columns)}"
        )

    # Datums-Spalte sicherstellen (legt ggf. report_date_as_yyyy_mm_dd neu an)
    df = ensure_date_column(df, RAW_PATH)

    # Jetzt sicher die Spalte holen (nach ensure_date_column existiert sie)
    date_col = "report_date_as_yyyy_mm_dd"
    dates = pd.to_datetime(df[date_col], errors="coerce")
    df = df.assign(_date=dates).dropna(subset=["_date"])

    if df.empty:
        raise SystemExit("Keine gültigen Datumswerte nach Normalisierung gefunden.")

    # Coverage aggregieren
    grp = df.groupby(market_col)["_date"]
    cov = grp.agg(first_date="min", last_date="max", rows="size").reset_index()
    cov["first_date"] = cov["first_date"].dt.date
    cov["last_date"] = cov["last_date"].dt.date

    # Watchlist-Mapping (gleiche Logik wie beim normalen COT-Coverage)
    wl_path = "watchlists/cot_markets.txt"
    wl = read_watchlist(wl_path)
    wl_set = set(wl)

    def wl_match(name: str) -> str:
        return name if name in wl_set else ""

    cov["watchlist_match"] = cov[market_col].apply(wl_match)
    cov["in_watchlist"] = cov["watchlist_match"] != ""

    cov = cov.sort_values(market_col)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    out_cov = os.path.join(REPORTS_DIR, "cot_energy_coverage.csv")
    cov.to_csv(out_cov, index=False)
    print(f"✅ wrote {out_cov} – rows: {len(cov)}")

    # Zusätzlich: komplette Marktnamenliste für Debug / Mapping
    names_all = sorted(df[market_col].dropna().unique())
    out_names = os.path.join(REPORTS_DIR, "cot_energy_market_names_all.txt")
    with open(out_names, "w", encoding="utf-8") as f:
        f.write("\n".join(names_all))
    print(f"✅ wrote {out_names} – unique markets: {len(names_all)}")


if __name__ == "__main__":
    main()
