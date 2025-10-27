#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robustes COT-Fetch:
1) Bevorzugt die aktuellen Weekly-Feeds (newcot/*.txt)
2) Fallback: versucht mehrere History-ZIP-Muster pro Jahr
3) Aggregiert zu einer kleinen Summary (neueste Woche je Markt)

Outputs
- data/processed/cot_summary.csv
- data/reports/cot_errors.json
"""

import os, io, sys, json, zipfile, time, re
from datetime import datetime
import requests
import pandas as pd

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (COT lightweight fetch; github actions)"
})
BASE = "https://www.cftc.gov"

# Weekly feeds (relativ stabil, CSV-kompatibel)
WEEKLY_ENDPOINTS = [
    "/dea/newcot/FinFutWk.txt",     # Financial Futures (CSV-like)
    "/dea/newcot/FutWk.txt",        # Legacy Futures (CSV-like)
    "/dea/newcot/deacotdisagg.txt", # Disaggregated Futures (CSV-like)
]

# Yearly ZIP candidates – wir testen mehrere Muster je Jahr
ZIP_PATTERNS = [
    "/dea/history/deacotdisagg_txt_{YYYY}.zip",
    "/dea/history/deahistfo_{YYYY}.zip",
    "/dea/history/deahist_{YYYY}.zip",
    "/dea/history/deacot_{YYYY}.zip",
    "/dea/newcot/deacotdisagg_txt_{YYYY}.zip",  # manche Jahre liegen (noch) unter newcot
]

OUT_SUMMARY = "data/processed/cot_summary.csv"
OUT_ERRORS  = "data/reports/cot_errors.json"

def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

def fetch_text(url):
    r = SESSION.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} for {url}")
    return r.text

def fetch_bytes(url):
    r = SESSION.get(url, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} for {url}")
    return r.content

def try_weekly_frames(errors):
    """Versucht die drei Weekly-Feeds. Rückgabe: Liste DataFrames (kann leer sein)."""
    frames = []
    for rel in WEEKLY_ENDPOINTS:
        url = BASE + rel
        try:
            txt = fetch_text(url)
            # Weekly-Dateien sind i.d.R. CSV-kompatibel (quoted)
            df = pd.read_csv(io.StringIO(txt))
            if not df.empty:
                df["__source"] = rel.split("/")[-1]
                frames.append(df)
        except Exception as e:
            errors.append({"stage": "weekly", "url": url, "msg": str(e)})
        time.sleep(0.3)
    return frames

def try_zip_year(year, errors):
    """Versucht mehrere ZIP-Namen pro Jahr, liefert Liste DataFrames aus gefundenen CSV/TXT Dateien."""
    for pat in ZIP_PATTERNS:
        url = BASE + pat.format(YYYY=year)
        try:
            content = fetch_bytes(url)
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                dfs = []
                for name in zf.namelist():
                    if not name.lower().endswith((".csv", ".txt")):
                        continue
                    raw = zf.read(name)
                    # Viele CFTC-Textdateien sind CSV-ähnlich
                    try:
                        df = pd.read_csv(io.BytesIO(raw))
                    except Exception:
                        # Fallback: Semikolon / Tab / Pipe testen
                        for sep in [";", "\t", "|"]:
                            try:
                                df = pd.read_csv(io.BytesIO(raw), sep=sep)
                                break
                            except Exception:
                                df = None
                    if df is not None and not df.empty:
                        df["__source"] = f"{os.path.basename(url)}:{name}"
                        dfs.append(df)
                if dfs:
                    return dfs  # beim ersten erfolgreichen ZIP aufhören
        except Exception as e:
            errors.append({"stage": "zip", "year": year, "url": url, "msg": str(e)})
        time.sleep(0.3)
    return []

def normalize_columns(df):
    """
    Vereinheitlicht die wichtigsten Spaltennamen, soweit möglich.
    CFTC liefert je Datei leicht unterschiedliche Labels.
    Wir mappen häufige Varianten auf ein gemeinsames Set.
    """
    colmap = {c.lower(): c for c in df.columns}
    def has(name):
        return name in colmap

    # häufige Namensvarianten
    rename = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("market_and_exchange_names", "market_and_exchange_name"):
            rename[c] = "market"
        elif lc in ("as_of_date_in_form_mm/dd/yyyy","report_date_as_yyyy-mm-dd","report_date_as_yyyy_mm_dd",
                    "report_date_as_yyyymmdd","report_date"):
            rename[c] = "report_date"
        elif lc in ("open_interest_all","open_interest"):
            rename[c] = "oi"
        elif "noncomm" in lc and "long" in lc:
            rename[c] = "noncomm_long"
        elif "noncomm" in lc and "short" in lc:
            rename[c] = "noncomm_short"
        elif ("noncommercial" in lc and "long" in lc):
            rename[c] = "noncomm_long"
        elif ("noncommercial" in lc and "short" in lc):
            rename[c] = "noncomm_short"
        elif ("commercial" in lc and "long" in lc):
            rename[c] = "comm_long"
        elif ("commercial" in lc and "short" in lc):
            rename[c] = "comm_short"
        elif ("nonreportable" in lc and "long" in lc) or ("nonrep" in lc and "long" in lc):
            rename[c] = "nonrep_long"
        elif ("nonreportable" in lc and "short" in lc) or ("nonrep" in lc and "short" in lc):
            rename[c] = "nonrep_short"
        elif ("spread" in lc) and ("noncomm" in lc or "noncommercial" in lc):
            rename[c] = "noncomm_spread"

    df = df.rename(columns=rename)

    # report_date in ein Datumsformat bringen
    if "report_date" in df.columns:
        try:
            df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
        except Exception:
            pass
    else:
        # manche Dateien haben "As of" etc. – ignorieren wir dann
        df["report_date"] = pd.NaT

    # Marktname
    if "market" not in df.columns:
        # manche Dateien splitten Name/Exchange getrennt:
        cand = [c for c in df.columns if "market" in c.lower()]
        if cand:
            df["market"] = df[cand[0]]
        else:
            df["market"] = None

    pick = ["market","report_date","oi","noncomm_long","noncomm_short","noncomm_spread",
            "comm_long","comm_short","nonrep_long","nonrep_short","__source"]
    present = [c for c in pick if c in df.columns]
    return df[present].copy()

def aggregate_latest(frames):
    """Wählt pro Markt die neueste Woche und gibt eine kompakte Tabelle zurück."""
    if not frames:
        return pd.DataFrame(columns=[
            "market","report_date","oi","noncomm_long","noncomm_short","noncomm_spread",
            "comm_long","comm_short","nonrep_long","nonrep_short","source"
        ])
    df = pd.concat(frames, ignore_index=True)
    df = normalize_columns(df)
    # neueste Woche je Markt
    df = df.sort_values(["market","report_date"])
    last = df.groupby("market").tail(1).reset_index(drop=True)
    last = last.rename(columns={"__source":"source"})
    return last

def main():
    ensure_dirs()
    errors = []
    frames = []

    # 1) Weekly first
    weekly = try_weekly_frames(errors)
    frames.extend(weekly)

    # 2) If nothing, try some years (neueste -> zurück)
    if not frames:
        current_year = datetime.utcnow().year
        for y in range(current_year, current_year-5, -1):
            dfs = try_zip_year(y, errors)
            if dfs:
                frames.extend(dfs)
                break

    # 3) Aggregate
    summary = aggregate_latest(frames)

    # 4) Write outputs
    summary.to_csv(OUT_SUMMARY, index=False)
    print(f"wrote {OUT_SUMMARY} rows={len(summary)}")

    with open(OUT_ERRORS, "w", encoding="utf-8") as f:
        json.dump({"errors": errors[-100:]}, f, indent=2)

    if not len(summary):
        print("COT note: no rows (see cot_errors.json)")

    return 0

if __name__ == "__main__":
    sys.exit(main())
