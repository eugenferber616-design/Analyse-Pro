#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_cot_20y.py  (Disaggregated + TFF, unified)
Zieht 20 Jahre COT von CFTC-Socrata für mehrere Datasets (ENV COT_DATASET_IDS)
und mapped alle Felder auf ein gemeinsames Schema, das vom Agena-Indicator
COT_PositionViewer erwartet.

Schreibt:
  - data/processed/cot_20y.csv.gz
  - data/reports/cot_20y_report.json
"""

import os, json, time, datetime as dt
import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Netz / Socrata Settings (ENV overridebar) ─────────────────────
SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 25000))

API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
# Einzelwert (Legacy) oder Kommaliste:
DATASET_IDS  = os.getenv("COT_DATASET_IDS", os.getenv("COT_DATASET_ID", "kh3c-gbw2,6dca-eg5q"))
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")
YEARS        = int(os.getenv("COT_YEARS", "20"))
MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()   # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

# ── HTTP Session mit Retry ─────────────────────────────────────────
def make_session():
    s = requests.Session()
    retry = Retry(
        total=SOC_RETRIES,
        connect=SOC_RETRIES,
        read=SOC_RETRIES,
        status=SOC_RETRIES,
        backoff_factor=SOC_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10))
    headers = {"Accept": "application/json"}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    s.headers.update(headers)
    return s

SESSION = make_session()

def sget(path, params):
    url = f"{API_BASE}/{path}"
    r = SESSION.get(url, params=params, timeout=SOC_TIMEOUT)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP {r.status_code} for {url} ; body={r.text[:500]}") from e
    return r.json()

# ── Helfer ─────────────────────────────────────────────────────────
def read_markets(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith(("#","//")):
                out.append(s)
    return out

def soql_quote(s: str) -> str:
    """SOQL-sicheres String-Literal (einzelne Quotes verdoppeln)."""
    return "'" + s.replace("'", "''") + "'"

def fetch_range(date_from, date_to, markets=None, dataset_id=None):
    rows_all, offset = [], 0
    base_where = f"report_date_as_yyyy_mm_dd between '{date_from}' and '{date_to}'"
    where = base_where
    if markets:
        # Disagg + TFF nutzen beide market_and_exchange_names
        quoted = ",".join(soql_quote(m) for m in markets)
        where = f"{base_where} AND market_and_exchange_names in ({quoted})"

    params_base = {
        "$where": where,
        "$order": "report_date_as_yyyy_mm_dd ASC",
        "$limit": SOC_LIMIT,
    }

    while True:
        params = dict(params_base)
        params["$offset"] = offset
        chunk = sget(f"{dataset_id}.json", params)
        if not chunk:
            break
        rows_all.extend(chunk)
        if len(chunk) < SOC_LIMIT:
            break
        offset += SOC_LIMIT
        time.sleep(0.15)
    df = pd.DataFrame(rows_all)
    df["__dataset_id"] = dataset_id
    return df

# ── Schema-Mapper: TFF + Disagg → Unified ─────────────────────────
UNIFIED_COLS = [
    "report_date_as_yyyy_mm_dd",
    "market_and_exchange_names",
    "open_interest_all",
    # Dealers / Commercial-Proxy
    "dealer_positions_long_all",
    "dealer_positions_short_all",
    # Asset Manager
    "asset_mgr_positions_long",
    "asset_mgr_positions_short",
    # Leveraged Funds / Managed Money
    "lev_money_positions_long",
    "lev_money_positions_short",
    # Other Reportables
    "other_rept_positions_long",
    "other_rept_positions_short",
    # (optionale Legacy-Fallbacks – bleiben leer, wenn nicht vorhanden)
    "commercial_long","commercial_short",
    "noncomm_long","noncomm_short",
    "nonrept_long","nonreportable_long",
    "nonrept_short","nonreportable_short",
]

def coerce_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def map_to_unified(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # 1) Standardspalten angleichen (Unterschiede Bindestrich/Underscore etc.)
    #    In der Praxis heißen sie identisch; Sicherheitshalber normalisieren wir alternative Keys.
    alt_map = {
        "report_date": "report_date_as_yyyy_mm_dd",
        "as_of_date_in_form_yyyy_mm_dd": "report_date_as_yyyy_mm_dd",
        "contract_market_name": "market_and_exchange_names",
        "commodity_name": "market_and_exchange_names",
        "commodity": "market_and_exchange_names",
        # Open Interest ggf. anders benannt (sehr selten)
        "open_interest": "open_interest_all",
        "open_interest_all": "open_interest_all",
    }
    for a,b in alt_map.items():
        if a in df.columns and b not in df.columns:
            df[b] = df[a]

    # 2) Datensatz-spezifische Mappings
    ds = str(df["__dataset_id"].iloc[0]).lower()

    # Disaggregated Combined (kh3c-gbw2): Spalten existieren bereits im Zielschema
    if "kh3c-gbw2" in ds:
        # Nichts weiter nötig – behalten nur die relevanten Spalten
        pass

    # TFF Combined (6dca-eg5q): auf unified Spalten umbiegen
    elif "6dca-eg5q" in ds:
        tff_map = {
            # Dealers / Intermediaries → dealer_positions_*
            "dealer_intermed_positions_long_all":  "dealer_positions_long_all",
            "dealer_intermed_positions_short_all": "dealer_positions_short_all",
            # Asset Manager → asset_mgr_*
            "asset_manager_long_all":  "asset_mgr_positions_long",
            "asset_manager_short_all": "asset_mgr_positions_short",
            # Leveraged Funds → lev_money_*
            "leveraged_funds_long_all":  "lev_money_positions_long",
            "leveraged_funds_short_all": "lev_money_positions_short",
            # Other Reportables → other_rept_*
            "other_reportables_long_all":  "other_rept_positions_long",
            "other_reportables_short_all": "other_rept_positions_short",
            # Open Interest (falls anders benannt)
            "open_interest_all": "open_interest_all",
        }
        for src, dst in tff_map.items():
            if src in df.columns and dst not in df.columns:
                df[dst] = df[src]

    # 3) Fehlende Zielspalten anlegen
    for c in UNIFIED_COLS:
        if c not in df.columns:
            df[c] = np.nan

    # 4) Typkonvertierung auf numerisch
    num_cols = [
        "open_interest_all",
        "dealer_positions_long_all","dealer_positions_short_all",
        "asset_mgr_positions_long","asset_mgr_positions_short",
        "lev_money_positions_long","lev_money_positions_short",
        "other_rept_positions_long","other_rept_positions_short",
        "commercial_long","commercial_short",
        "noncomm_long","noncomm_short",
        "nonrept_long","nonreportable_long",
        "nonrept_short","nonreportable_short",
    ]
    df = coerce_num(df, num_cols)

    # 5) Datum auf YYYY-MM-DD trimmen
    if "report_date_as_yyyy_mm_dd" in df.columns:
        df["report_date_as_yyyy_mm_dd"] = (
            df["report_date_as_yyyy_mm_dd"]
            .astype(str).str.replace("_","-", regex=False).str.slice(0,10)
        )

    # 6) nur unified Spalten + dataset id behalten
    keep = UNIFIED_COLS + ["__dataset_id"]
    df = df[[c for c in keep if c in df.columns]].copy()
    return df

# ── Main ───────────────────────────────────────────────────────────
def main():
    today = dt.date.today()
    date_to = today.strftime("%Y-%m-%d")
    # +10 Tage Puffer gegen Zeitzonen/Kalender-Kanten
    date_from = (today - dt.timedelta(days=365 * YEARS + 10)).strftime("%Y-%m-%d")

    report = {
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "datasets": [d.strip() for d in DATASET_IDS.split(",") if d.strip()],
        "years": YEARS,
        "mode": MODE,
        "rows": 0,
        "date_from": date_from,
        "date_to": date_to,
        "errors": [],
        "files": {},
        "by_dataset_rows": {}
    }

    markets = None
    if MODE != "ALL":
        markets = read_markets(MARKETS_FILE)

    frames = []
    for dsid in report["datasets"]:
        try:
            df_raw = fetch_range(date_from, date_to, markets=markets, dataset_id=dsid)
            report["by_dataset_rows"][dsid] = int(len(df_raw))
            if not df_raw.empty:
                frames.append(map_to_unified(df_raw))
        except Exception as e:
            report["errors"].append({"dataset": dsid, "stage": "fetch", "msg": str(e)})

    if frames:
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(columns=UNIFIED_COLS)

    # Deduplikation: gleicher Markt/Datum → letzter gewinnt (i.d.R. identisch)
    if not df.empty:
        df.sort_values(["market_and_exchange_names","report_date_as_yyyy_mm_dd","__dataset_id"],
                       inplace=True)
        df = df.drop_duplicates(subset=["market_and_exchange_names","report_date_as_yyyy_mm_dd"],
                                keep="last")

    # Ausgabe (nur .gz)
    out_gz = os.path.join(OUT_DIR, "cot_20y.csv.gz")
    try:
        df.to_csv(out_gz, index=False, compression="gzip")
        report["files"]["cot_20y_csv_gz"] = out_gz
        report["rows"] = int(len(df))
    except Exception as e:
        report["errors"].append({"stage": "compress", "msg": str(e)})

    # Abschlussreport
    json.dump(report, open(os.path.join(REP_DIR, "cot_20y_report.json"), "w"), indent=2)
    print(f"wrote {out_gz} rows={report['rows']} from datasets={report['datasets']}")

if __name__ == "__main__":
    main()
