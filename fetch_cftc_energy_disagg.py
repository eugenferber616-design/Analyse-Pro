#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetch_cftc_energy_disagg.py

Zieht die "Historical Compressed – Disaggregated Futures Only" Text-Zips von
cftc.gov, filtert auf Energy-Märkte (Öl / Gas / RBOB / Heating Oil / ULSD / Brent)
und speichert alles als kompaktes CSV.GZ.

Output:
    data/processed/cot_disagg_energy_raw.csv.gz

Benötigt:
    pip install requests pandas
"""

import os
import io
import re
import gzip
import zipfile
import logging
from urllib.parse import urljoin

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Konfiguration (per ENV overridebar)
# ---------------------------------------------------------------------------
INDEX_URL = os.getenv(
    "CFTC_HIST_INDEX",
    "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm",
)
OUT_PATH = os.getenv(
    "CFTC_DISAGG_ENERGY_OUT",
    "data/processed/cot_disagg_energy_raw.csv.gz",
)

# Welche Märkte wollen wir aus den Disaggregated-Files ziehen?
# → Fokus: Öl / Gas / RBOB / Heating / ULSD / Brent
ENERGY_KEYWORDS = [
    "CRUDE OIL",
    "WTI",
    "BRENT",
    "NATURAL GAS",
    "GASOLINE",
    "RBOB",
    "HEATING OIL",
    "ULSD",
]

# Optional: Watchlist für exakte Namen (deine cot_markets.txt)
COT_WATCHLIST_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")


# ---------------------------------------------------------------------------
# HTTP Session mit Retry
# ---------------------------------------------------------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        status=6,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8))
    s.headers.update({"User-Agent": "cot-energy-fetcher/1.0"})
    return s


SESSION = make_session()


# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------
def read_watchlist(path: str):
    """Liest watchlists/cot_markets.txt (eine Marktzeile pro Zeile)."""
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if ln and not ln.startswith(("#", "//")):
                out.append(ln)
    return out


def get_disagg_zip_urls(index_url: str):
    """
    Lädt die HistoricalCompressed-Indexseite und extrahiert alle
    fut_disagg_txt_YYYY.zip-Links.
    """
    logging.info("Lade Indexseite %s", index_url)
    r = SESSION.get(index_url, timeout=60)
    r.raise_for_status()
    html = r.text

    # Suche alle fut_disagg_txt_YYYY.zip
    hrefs = re.findall(
        r'href="([^"]*fut_disagg_txt_\d{4}\.zip)"',
        html,
        flags=re.IGNORECASE,
    )
    urls = []
    for h in hrefs:
        if h.startswith("http://") or h.startswith("https://"):
            urls.append(h)
        else:
            urls.append(urljoin(index_url, h))
    urls = sorted(set(urls))
    logging.info("Gefundene Disaggregated-ZIPs: %s", urls)
    return urls


def download_zip(url: str) -> zipfile.ZipFile:
    """Lädt ein .zip von CFTC und gibt ein ZipFile-Objekt zurück."""
    logging.info("Lade ZIP: %s", url)
    r = SESSION.get(url, timeout=120)
    r.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(r.content))


def extract_energy_from_zip(zf: zipfile.ZipFile, watchlist_upper=None) -> pd.DataFrame:
    """
    Liest alle .txt/.csv aus einem Zip, filtert auf Energy-Märkte
    (über ENERGY_KEYWORDS + optional exakte Watchlistnamen) und
    gibt einen DataFrame zurück.
    """
    frames = []
    names = [n for n in zf.namelist() if n.lower().endswith((".txt", ".csv"))]

    for name in names:
        logging.info("  lese Datei im ZIP: %s", name)
        with zf.open(name) as f:
            # CFTC-Dateien sind CSV mit Header
            try:
                df = pd.read_csv(f, dtype=str, low_memory=False)
            except Exception as e:
                logging.warning("  Konnte %s nicht als CSV lesen: %s", name, e)
                continue

        # Spaltennamen vereinheitlichen
        cols = {c: c.strip().lower() for c in df.columns}
        df.rename(columns=cols, inplace=True)

        if "market_and_exchange_names" not in df.columns:
            logging.warning("  Spalte market_and_exchange_names fehlt in %s – skip", name)
            continue

        me = df["market_and_exchange_names"].fillna("")
        me_upper = me.str.upper()

        # Keyword-Filter (Öl, Gas, RBOB, Heating, ULSD, Brent)
        mask_kw = pd.Series(False, index=df.index)
        for kw in ENERGY_KEYWORDS:
            mask_kw |= me_upper.str.contains(kw)

        # Optional: exakte Watchlist-Namen (falls du nur bestimmte Märkte willst)
        mask_wl = pd.Series(False, index=df.index)
        if watchlist_upper:
            mask_wl = me_upper.isin(watchlist_upper)

        mask = mask_kw | mask_wl
        df_energy = df[mask].copy()

        if not df_energy.empty:
            logging.info("  gefilterte Energy-Zeilen in %s: %d", name, len(df_energy))
            frames.append(df_energy)

    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    # Watchlist (für exakte Markt-Namen)
    wl = read_watchlist(COT_WATCHLIST_FILE)
    wl_upper = [w.upper() for w in wl]

    logging.info("Watchlist-Einträge (cot_markets.txt): %d", len(wl))

    urls = get_disagg_zip_urls(INDEX_URL)
    if not urls:
        raise SystemExit("Keine fut_disagg_txt_YYYY.zip-Links auf der CFTC-Seite gefunden")

    all_frames = []

    for url in urls:
        try:
            zf = download_zip(url)
        except Exception as e:
            logging.warning("Fehler beim Laden von %s: %s", url, e)
            continue

        df_energy = extract_energy_from_zip(zf, watchlist_upper=wl_upper)
        if not df_energy.empty:
            # Versuche, das Jahr aus dem Dateinamen zu extrahieren (für Diagnose)
            m = re.search(r"(\d{4})", url)
            year = m.group(1) if m else None
            if year:
                df_energy["source_year"] = year
            all_frames.append(df_energy)

    if not all_frames:
        raise SystemExit("Keine Energy-Daten in den CFTC-Disaggregated-ZIPs gefunden")

    df_all = pd.concat(all_frames, ignore_index=True)

    # Doppelungen aufräumen (falls gleiche Zeilen aus mehreren Quellen kommen)
    # Schlüssel: Markt + Datum
    if "report_date_as_yyyy_mm_dd" in df_all.columns:
        key_cols = ["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]
    elif "Report_Date_as_YYYY_MM_DD".lower() in df_all.columns:
        key_cols = ["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]
    else:
        key_cols = ["market_and_exchange_names"]

    before = len(df_all)
    df_all = df_all.drop_duplicates(subset=key_cols)
    logging.info("Zeilen vor/nach Drop-Duplicates: %d -> %d", before, len(df_all))

    # Sortierung für bessere Lesbarkeit
    if "report_date_as_yyyy_mm_dd" in df_all.columns:
        df_all["report_date_as_yyyy_mm_dd"] = pd.to_datetime(
            df_all["report_date_as_yyyy_mm_dd"], errors="coerce"
        )
        df_all = df_all.sort_values(
            ["market_and_exchange_names", "report_date_as_yyyy_mm_dd"]
        )

    # Schreiben als CSV.GZ
    with gzip.open(OUT_PATH, "wt", encoding="utf-8", newline="") as gz:
        df_all.to_csv(gz, index=False)

    logging.info("✅ wrote %s (rows=%d, cols=%d)", OUT_PATH, len(df_all), len(df_all.columns))


if __name__ == "__main__":
    main()
