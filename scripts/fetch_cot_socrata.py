#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pull COT (Commitments of Traders) via CFTC Socrata open data.

Outputs
-------
- data/processed/cot_summary.csv     (latest week per selected market)
- data/processed/cot_latest_raw.csv  (raw rows of latest week per selected market)
- data/reports/cot_errors.json       (diagnostics)
"""

import os
import io
import json
import time
import math
import datetime as dt
from typing import List, Dict

import requests
import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# Config (via ENV)
# ──────────────────────────────────────────────────────────────────────────────
BASE = "https://publicreporting.cftc.gov/resource"

DATASET_ID = os.getenv("COT_DATASET_ID", "gpe5-46if").strip()  # TFF / disaggregated futures+opts (Socrata)
APP_TOKEN   = os.getenv("CFTC_APP_TOKEN", "")

# Zeitraum: entweder SINCE_DATE=YYYY-MM-DD oder WEEKS rückwärts (Default 156 ≈ 3 Jahre)
SINCE_DATE  = os.getenv("COT_SINCE_DATE", "").strip()
WEEKS_BACK  = int(os.getenv("COT_WEEKS", "156"))

# Märkte: Komma-separierte Schlüsselworte (case-insensitive, enthält-Filter)
MARKETS_ENV = os.getenv("COT_MARKETS", "E-MINI S&P 500, EURO FX, WTI, 10-YEAR NOTE")
MARKET_PATTERNS: List[str] = [m.strip() for m in MARKETS_ENV.split(",") if m.strip()]

# Output-Pfade
OUT_DIR      = "data/processed"
REPORTS_DIR  = "data/reports"
OUT_SUMMARY  = os.path.join(OUT_DIR, "cot_summary.csv")
OUT_LATEST   = os.path.join(OUT_DIR, "cot_latest_raw.csv")
OUT_REPORT   = os.path.join(REPORTS_DIR, "cot_errors.json")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)


def clean_token(s: str) -> str:
    return (s or "").strip().replace("\r", "").replace("\n", "")


def _today_utc_date() -> dt.date:
    return dt.datetime.utcnow().date()


def _since_date() -> str:
    if SINCE_DATE:
        return SINCE_DATE
    # weeks back
    days = 7 * max(1, WEEKS_BACK)
    return (_today_utc_date() - dt.timedelta(days=days)).isoformat()


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lower-case columns and provide aliases for common CFTC fields."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Provide soft aliases (many datasets share these names)
    aliases = {
        "market_and_exchange_names": ["market_and_exchange_names", "market_and_exchange_name", "mkt_and_exch"],
        "report_date":               ["report_date_as_yyyy_mm_dd", "report_date", "report_date_as_yyyy_mm_dd_"],
        "open_interest_all":         ["open_interest_all", "open_interest_all_", "open_interest_all__"],
        "noncomm_positions_long_all":  ["noncomm_positions_long_all", "noncomm_long_all", "noncomm_long_all_"],
        "noncomm_positions_short_all": ["noncomm_positions_short_all", "noncomm_short_all", "noncomm_short_all_"],
        "noncomm_positions_net_all":   ["noncomm_positions_net_all", "noncomm_net_all", "noncomm_net_all_"],
    }

    # find first existing column for each canonical name
    for canon, candidates in aliases.items():
        for cand in candidates:
            if cand in df.columns:
                if canon != cand:
                    df[canon] = df[cand]
                break
        # if none found, leave missing -> handled downstream
    return df


def socrata_fetch_all(dataset_id: str,
                      app_token: str,
                      where: str,
                      select: str,
                      order: str,
                      page_size: int = 50000) -> pd.DataFrame:
    """Fetch all rows from Socrata dataset with paging. Uses both header and
    $$app_token param; on 401/403 retries without token (lower rate limit)."""
    rows = []
    offset = 0
    token = clean_token(app_token)

    while True:
        params = {
            "$select": select,
            "$where":  where,
            "$order":  order,
            "$limit":  page_size,
            "$offset": offset
        }
        headers = {}
        if token:
            headers["X-App-Token"] = token
            params["$$app_token"]   = token

        url = f"{BASE}/{dataset_id}.json"
        r = requests.get(url, params=params, headers=headers, timeout=60)

        if r.status_code in (401, 403):
            # fallback: try without token (sometimes header is rejected)
            params.pop("$$app_token", None)
            r = requests.get(url, params=params, timeout=60)

        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")

        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
        time.sleep(0.2)

    return pd.DataFrame(rows)


def percent_share(series: pd.Series) -> float:
    if series is None or len(series) == 0:
        return None
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    total = s.sum()
    if total <= 0:
        return None
    return float(100.0 * s.max() / total)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main() -> int:
    ensure_dirs()
    errors: List[Dict] = []

    since = _since_date()
    select = ",".join([
        "market_and_exchange_names",
        "report_date_as_yyyy_mm_dd",
        "open_interest_all",
        "noncomm_positions_long_all",
        "noncomm_positions_short_all",
        "noncomm_positions_net_all"
    ])
    where  = f"report_date_as_yyyy_mm_dd >= '{since}'"
    order  = "report_date_as_yyyy_mm_dd ASC"

    try:
        df = socrata_fetch_all(
            dataset_id=DATASET_ID,
            app_token=APP_TOKEN,
            where=where,
            select=select,
            order=order,
            page_size=50000
        )
    except Exception as e:
        # Hard failure: write empty outputs + report error
        pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"]).to_csv(OUT_SUMMARY, index=False)
        pd.DataFrame().to_csv(OUT_LATEST, index=False)
        report = {
            "ts": dt.datetime.utcnow().isoformat()+"Z",
            "dataset": DATASET_ID,
            "since": since,
            "rows": 0,
            "latest": None,
            "filtered_markets": MARKET_PATTERNS,
            "errors": [{"stage": "fetch", "msg": str(e)}],
            "token_len": len(clean_token(APP_TOKEN))
        }
        with open(OUT_REPORT, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print("COT fetch failed:", e)
        return 0

    df = _normalize_cols(df)

    # early exit if nothing
    if df is None or df.empty or "market_and_exchange_names" not in df.columns:
        pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"]).to_csv(OUT_SUMMARY, index=False)
        pd.DataFrame().to_csv(OUT_LATEST, index=False)
        report = {
            "ts": dt.datetime.utcnow().isoformat()+"Z",
            "dataset": DATASET_ID,
            "since": since,
            "rows": 0,
            "latest": None,
            "filtered_markets": MARKET_PATTERNS,
            "errors": [{"stage": "normalize", "msg": "no rows or missing columns"}],
            "token_len": len(clean_token(APP_TOKEN))
        }
        with open(OUT_REPORT, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print("COT note: no rows")
        return 0

    # Coerce date + numbers
    if "report_date" not in df.columns and "report_date_as_yyyy_mm_dd" in df.columns:
        df["report_date"] = df["report_date_as_yyyy_mm_dd"]
    if "report_date" in df.columns:
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date

    for col in ["open_interest_all", "noncomm_positions_long_all", "noncomm_positions_short_all", "noncomm_positions_net_all"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter by patterns, then pick latest week per pattern
    summary_rows = []
    latest_rows = []

    for pat in MARKET_PATTERNS:
        try:
            sub = df[df["market_and_exchange_names"].str.contains(pat, case=False, na=False)].copy()
            if sub.empty:
                errors.append({"market": pat, "stage": "filter", "msg": "no_match"})
                continue

            latest_date = sub["report_date"].max() if "report_date" in sub.columns else None
            if latest_date is None:
                errors.append({"market": pat, "stage": "latest", "msg": "no_report_date"})
                continue

            latest = sub[sub["report_date"] == latest_date].copy()
            if latest.empty:
                errors.append({"market": pat, "stage": "latest", "msg": "no_latest_rows"})
                continue

            # Summaries (aggregate if multiple rows per market)
            oi  = float(pd.to_numeric(latest.get("open_interest_all", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
            ncl = float(pd.to_numeric(latest.get("noncomm_positions_long_all", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
            ncs = float(pd.to_numeric(latest.get("noncomm_positions_short_all", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
            nnet = float(pd.to_numeric(latest.get("noncomm_positions_net_all", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())

            summary_rows.append({
                "market": pat,
                "report_date": latest_date.isoformat(),
                "oi": int(oi),
                "ncl": int(ncl),
                "ncs": int(ncs),
                "noncomm_net": int(nnet)
            })

            latest["filter_key"] = pat
            latest_rows.append(latest)
        except Exception as e:
            errors.append({"market": pat, "stage": "process", "msg": str(e)})

    # Write outputs
    pd.DataFrame(summary_rows, columns=["market","report_date","oi","ncl","ncs","noncomm_net"]).to_csv(OUT_SUMMARY, index=False)

    if latest_rows:
        raw = pd.concat(latest_rows, ignore_index=True)
        raw.to_csv(OUT_LATEST, index=False)
    else:
        pd.DataFrame().to_csv(OUT_LATEST, index=False)

    report = {
        "ts": dt.datetime.utcnow().isoformat()+"Z",
        "dataset": DATASET_ID,
        "since": since,
        "rows": int(len(df)),
        "latest": None if df.get("report_date") is None or df["report_date"].isna().all()
                  else str(pd.to_datetime(df["report_date"]).max().date()),
        "filtered_markets": MARKET_PATTERNS,
        "errors": errors,
        "token_len": len(clean_token(APP_TOKEN))
    }
    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"wrote {OUT_SUMMARY} rows={len(summary_rows)} ; latest_raw={OUT_LATEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
