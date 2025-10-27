#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robuster COT-Fetch:
- Parst die offiziellen CFTC-"Historical Compressed" Seiten (wie im AgenaTrader Addon).
- Lädt alle relevanten ZIPs (Disaggregated: Futures-Only & Futures+Options),
  extrahiert TXT/CSV, vereinheitlicht Spalten und schreibt eine schlanke Summary:
    data/processed/cot_summary.csv
- Optional: legt die großen, zusammengefügten Rohdaten NUR als .gz unter data/cache/ ab (nicht committen).

Speicher-schonend: Im Repo bleibt nur cot_summary.csv.
"""

import os, io, re, gzip, csv, json, time, zipfile, shutil
from datetime import datetime
from typing import List, Dict, Iterator, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ------------- Konfig -------------
OUT_DIR_PROC = "data/processed"
OUT_DIR_CACHE = "data/cache"
REPORTS_DIR = "data/reports"

# CFTC Einstiegsseiten (beide Varianten abscannen)
CFTC_INDEX_PAGES = [
    # Offizielles Portal – Historical Compressed
    "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm",
    # Fallback: älteres Listing im /dea/history/ Verzeichnis
    "https://www.cftc.gov/dea/history/index.htm",
]

# Wir suchen bevorzugt diese Muster (Disaggregated Reports)
ZIP_PATTERNS = [
    # Disaggregated Futures & Options Combined
    re.compile(r"deahistfo[_\-]?\d{4}\.zip$", re.IGNORECASE),
    # Disaggregated Futures Only
    re.compile(r"deacotdisagg[_\-]?txt[_\-]?\d{4}\.zip$", re.IGNORECASE),
    # gelegentliche alternative Benennungen
    re.compile(r"deahistfo\.zip$", re.IGNORECASE),            # „alle Jahre in einer Datei“
    re.compile(r"deacotdisagg[_\-]?txt\.zip$", re.IGNORECASE),
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "COT-Fetch (OSS, educational)"})
TIMEOUT = 30


def ensure_dirs():
    os.makedirs(OUT_DIR_PROC, exist_ok=True)
    os.makedirs(OUT_DIR_CACHE, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)


def absolute_url(base: str, href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        # gleiche Domain
        from urllib.parse import urlparse
        p = urlparse(base)
        return f"{p.scheme}://{p.netloc}{href}"
    # relativ
    from urllib.parse import urljoin
    return urljoin(base, href)


def find_zip_links() -> List[str]:
    """Parse beide Einstiegsseiten, sammle ZIP-Links, filtere auf Disagg-Dateien."""
    links = []
    for page in CFTC_INDEX_PAGES:
        try:
            r = SESSION.get(page, timeout=TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().endswith(".zip"):
                    continue
                url = absolute_url(page, href)
                name = url.split("/")[-1]
                if any(p.search(name) for p in ZIP_PATTERNS):
                    links.append(url)
        except Exception as e:
            print(f"warn: could not parse {page}: {e}")
    # Dedupe & sort
    links = sorted(set(links))
    return links


def stream_zip(url: str) -> Optional[bytes]:
    try:
        with SESSION.get(url, timeout=TIMEOUT, stream=True) as r:
            r.raise_for_status()
            return r.content
    except Exception as e:
        print(f"zip fail {url}: {e}")
        return None


def read_zip_members(zbytes: bytes) -> Iterator[tuple[str, bytes]]:
    with zipfile.ZipFile(io.BytesIO(zbytes)) as z:
        for n in z.namelist():
            if not n.lower().endswith((".txt", ".csv")):
                continue
            yield n, z.read(n)


def normalize_cot_frame(raw: bytes) -> pd.DataFrame:
    """
    Viele CFTC-TXT sind CSV-ähnlich (Komma-separiert). Wir lesen großzügig.
    Vereinheitlichen die wichtigsten Felder für eine Summary.
    """
    # Erst als Text
    txt = raw.decode("latin1", errors="ignore")
    # Manchmal ; als Separator → erst mal ersetzen
    # (wir lassen pandas das meiste erkennen)
    df = pd.read_csv(io.StringIO(txt))
    # Canonicalize Spaltennamen (lower, underscores)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Wichtige Standardfelder (nicht jede Datei hat alle):
    # "market_and_exchange_names", "market_code", "report_date_as_yyyy-mm-dd"
    # "open_interest_all", "noncomm_positions_long_all", "noncomm_positions_short_all", ...
    # Für die Summary bleiben wir schlank:
    want_cols = [
        "market_and_exchange_names",
        "market_code",
        "report_date_as_yyyy-mm-dd",
        "open_interest_all",
        "noncomm_positions_long_all",
        "noncomm_positions_short_all",
        "comm_positions_long_all",
        "comm_positions_short_all",
        "nonrept_positions_long_all",
        "nonrept_positions_short_all",
    ]
    for c in want_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Typkonvertierung – robust
    def to_num(s):
        try:
            return pd.to_numeric(s, errors="coerce")
        except Exception:
            return pd.Series([pd.NA]*len(s))

    numeric_cols = [c for c in want_cols if c not in ("market_and_exchange_names",
                                                      "market_code",
                                                      "report_date_as_yyyy-mm-dd")]
    for c in numeric_cols:
        df[c] = to_num(df[c])

    # Datum vereinheitlichen
    date_col = "report_date_as_yyyy-mm-dd"
    if df[date_col].notna().any():
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    else:
        # manche alten Dateien haben "report_date_as_yyyy_mm_dd"
        alt = "report_date_as_yyyy_mm_dd"
        if alt in df.columns:
            df[date_col] = pd.to_datetime(df[alt], errors="coerce").dt.date

    # Reduzieren auf want_cols
    df = df[want_cols]
    return df


def build_summary(big: pd.DataFrame) -> pd.DataFrame:
    """
    Kleine, nützliche Summary pro (market_code, report_date):
      - open_interest_all
      - noncomm_long/short
      - simple noncomm net
    """
    df = big.copy()
    df = df.rename(columns={
        "report_date_as_yyyy-mm-dd": "report_date",
        "market_and_exchange_names": "market",
        "open_interest_all": "oi",
        "noncomm_positions_long_all": "ncl",
        "noncomm_positions_short_all": "ncs",
    })
    df["noncomm_net"] = (df["ncl"].fillna(0) - df["ncs"].fillna(0))
    keep = ["market_code", "market", "report_date", "oi", "ncl", "ncs", "noncomm_net"]
    df = df[keep].dropna(subset=["market_code", "report_date"])
    df = df.sort_values(["market_code", "report_date"])
    return df


def main():
    ensure_dirs()

    links = find_zip_links()
    report = {"ts": datetime.utcnow().isoformat()+"Z", "links": links, "errors": [], "rows": 0}
    if not links:
        print("COT: no links found on CFTC pages.")
        json.dump(report, open(os.path.join(REPORTS_DIR, "cot_errors.json"), "w"), indent=2)
        # Leere Summary schreiben, damit der Validator grün/rot korrekt zeigt
        pd.DataFrame(columns=["market_code","market","report_date","oi","ncl","ncs","noncomm_net"])\
          .to_csv(os.path.join(OUT_DIR_PROC, "cot_summary.csv"), index=False)
        return 0

    frames: List[pd.DataFrame] = []
    for url in links:
        z = stream_zip(url)
        if not z:
            report["errors"].append({"url": url, "msg": "download_failed"})
            continue
        try:
            for name, raw in read_zip_members(z):
                try:
                    df = normalize_cot_frame(raw)
                    if not df.empty:
                        frames.append(df)
                except Exception as e:
                    report["errors"].append({"url": url, "member": name, "msg": str(e)})
        except Exception as e:
            report["errors"].append({"url": url, "msg": f"zip_read_failed: {e}"})

    if not frames:
        print("COT: parsed 0 frames.")
        json.dump(report, open(os.path.join(REPORTS_DIR, "cot_errors.json"), "w"), indent=2)
        pd.DataFrame(columns=["market_code","market","report_date","oi","ncl","ncs","noncomm_net"])\
          .to_csv(os.path.join(OUT_DIR_PROC, "cot_summary.csv"), index=False)
        return 0

    big = pd.concat(frames, ignore_index=True, sort=False)
    summary = build_summary(big)

    out_csv = os.path.join(OUT_DIR_PROC, "cot_summary.csv")
    summary.to_csv(out_csv, index=False)
    report["rows"] = int(len(summary))
    print(f"wrote {out_csv} rows={len(summary)}")

    # Optional: große Rohdaten als gz (nicht commiten)
    raw_path = os.path.join(OUT_DIR_CACHE, "cot_full.csv.gz")
    with gzip.open(raw_path, "wb") as gz:
        big.to_csv(io.TextIOWrapper(gz, encoding="utf-8"), index=False)
    print(f"cached raw (gz) → {raw_path}")

    json.dump(report, open(os.path.join(REPORTS_DIR, "cot_errors.json"), "w"), indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
