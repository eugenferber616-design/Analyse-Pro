#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch latest COT (Futures+Options Combined) from CFTC Socrata and write:
- data/processed/cot_latest_raw.csv  (full latest report, all markets or watchlist)
- data/processed/cot.csv             (alias for compatibility)
- data/processed/cot_summary.csv     (small summary)
- data/reports/cot_errors.json       (run report)

Robust: Retries, Timeout, Backoff, kleineren Page-LIMIT.
"""

import os
import json
import time
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── ENV / Konfiguration ────────────────────────────────────────────────────────
API_BASE     = os.getenv("CFTC_API_BASE", "https://publicreporting.cftc.gov/resource")
DATASET_ID   = os.getenv("COT_DATASET_ID", "gpe5-46if")
APP_TOKEN    = os.getenv("CFTC_APP_TOKEN", "")

MODE         = os.getenv("COT_MARKETS_MODE", "ALL").upper()  # "ALL" | "FILE" | "LIST"
MARKETS_FILE = os.getenv("COT_MARKETS_FILE", "watchlists/cot_markets.txt")

OUT_DIR = "data/processed"
REP_DIR = "data/reports"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(REP_DIR, exist_ok=True)

# robuste Netz-Settings (per ENV überschreibbar)
SOC_TIMEOUT  = int(os.getenv("SOC_TIMEOUT", 120))    # Sekunden
SOC_RETRIES  = int(os.getenv("SOC_RETRIES", 6))
SOC_BACKOFF  = float(os.getenv("SOC_BACKOFF", 1.6))
SOC_LIMIT    = int(os.getenv("SOC_LIMIT", 25000))    # kleinere Page-Größe als 50k

# ── Requests-Session mit Retries ───────────────────────────────────────────────
def make_session():
    s = requests.Session()
    retry = Retry(
        total=SOC_RETRIES,
        connect=SOC_RETRIES,
        read=SOC_RETRIES,
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

def sget(path: str, params: dict):
    url = f"{API_BASE}/{path}"
    r = SESSION.get(url, params=params, timeout=SOC_TIMEOUT)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP {r.status_code} for {url} ; body={r.text[:500]}") from e
    return r.json()

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_latest_date() -> str | None:
    """max(report_date_as_yyyy_mm_dd) robust via ORDER DESC LIMIT 1"""
    rows = sget(f"{DATASET_ID}.json", {
        "$select": "report_date_as_yyyy_mm_dd",
        "$order":  "report_date_as_yyyy_mm_dd DESC",
        "$limit":  1
    })
    if not rows:
        return None
    return rows[0]["report_date_as_yyyy_mm_dd"]

def read_markets(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                out.append(s)
    return out

def _soql_quote(s: str) -> str:
    """SOQL-safe quoting: einfache Anführungszeichen doppeln & umschließen."""
    return "'" + s.replace("'", "''") + "'"

def fetch_latest_all(latest_date: str, markets: list[str] | None) -> pd.DataFrame:
    """
    Pull ALL rows for that latest date, optional: filter by market_and_exchange_names IN (...)
    Pagination mit SOC_LIMIT, kleine Pause dazwischen.
    """
    base_where = "report_date_as_yyyy_mm_dd = @d"
    where = base_where
    params_base = {"@d": latest_date, "$order": "id ASC", "$limit": SOC_LIMIT}

    if markets:
        quoted = ",".join(_soql_quote(m) for m in markets)
        where = f"{base_where} AND market_and_exchange_names in ({quoted})"

    rows = []
    offset = 0
    while True:
        params = dict(params_base)
        params["$where"]  = where
        params["$offset"] = offset

        chunk = sget(f"{DATASET_ID}.json", params)
        if not chunk:
            break

        rows.extend(chunk)
        if len(chunk) < SOC_LIMIT:
            break
        offset += SOC_LIMIT
        time.sleep(0.2)  # sanfter Takt
    return pd.DataFrame(rows)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    report = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": DATASET_ID,
        "latest": None,
        "rows": 0,
        "filtered_mode": MODE,
        "errors": []
    }

    try:
        latest = get_latest_date()
        report["latest"] = latest
    except Exception as e:
        report["errors"].append({"stage": "latest", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    if not latest:
        report["errors"].append({"stage": "latest", "msg": "no latest date"})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    markets = None
    if MODE != "ALL":
        wl = read_markets(MARKETS_FILE)
        markets = wl if wl else None

    try:
        df = fetch_latest_all(latest, markets)
    except Exception as e:
        report["errors"].append({"stage": "fetch", "msg": str(e)})
        json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)
        raise SystemExit(1)

    # Schreiben
    latest_raw = os.path.join(OUT_DIR, "cot_latest_raw.csv")
    df.to_csv(latest_raw, index=False)
    df.to_csv(os.path.join(OUT_DIR, "cot.csv"), index=False)

    # kleine Summary
    def _num(series):
        return pd.to_numeric(series, errors="coerce")

    if not df.empty:
        summary = pd.DataFrame({
            "market":      df.get("market_and_exchange_names"),
            "report_date": df.get("report_date_as_yyyy_mm_dd"),
            "oi":          _num(df.get("open_interest_all")),
            "ncl":         _num(df.get("noncomm_positions_long_all")),
            "ncs":         _num(df.get("noncomm_positions_short_all")),
        })
        summary["noncomm_net"] = summary["ncl"] - summary["ncs"]
    else:
        summary = pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"])

    summary.to_csv(os.path.join(OUT_DIR, "cot_summary.csv"), index=False)

    report["rows"] = int(len(df))
    json.dump(report, open(os.path.join(REP_DIR, "cot_errors.json"), "w"), indent=2)

if __name__ == "__main__":
    main()
