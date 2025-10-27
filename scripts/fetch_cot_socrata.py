#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CFTC COT via Socrata API (publicreporting.cftc.gov)
- zieht TFF (Traders in Financial Futures) "Futures Only" (Dataset-ID default: gpe5-46if)
- speichert kleine, nützliche CSVs für dein Repo

Outputs
- data/processed/cot_summary.csv      (pro Markt: letzte Woche, OI, NonComm Long/Short, Net)
- data/processed/cot_latest_raw.csv   (letzte Woche, Rohfelder)
- data/reports/cot_errors.json        (Fehlerliste)

Optionale Filter
- watchlists/cot_markets.txt  (Zeilen mit Teilstrings/Regex für market_and_exchange_names)
Env
- CFTC_APP_TOKEN (GitHub Secret)
- COT_DATASET_ID (optional, default gpe5-46if  = TFF Futures Only)
- COT_WEEKS      (optional, wie viele Wochen Historie abziehen; default 52)
"""

import os, json, time, math, re
from datetime import datetime, timedelta
from typing import List, Dict
import requests
import pandas as pd

BASE = "https://publicreporting.cftc.gov/resource"

# ------------ helpers ------------
def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)
    os.makedirs("watchlists", exist_ok=True)

def load_market_filters(path="watchlists/cot_markets.txt") -> List[str]:
    if not os.path.exists(path):
        return []
    lines = [ln.strip() for ln in open(path, encoding="utf-8").read().splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]

def socrata_fetch_all(dataset_id: str, app_token: str, where: str, select: str, order: str,
                      page_size: int = 50000) -> pd.DataFrame:
    headers = {"X-App-Token": app_token} if app_token else {}
    rows = []
    offset = 0
    while True:
        params = {
            "$select": select,
            "$where": where,
            "$order": order,
            "$limit": page_size,
            "$offset": offset,
        }
        r = requests.get(f"{BASE}/{dataset_id}.json", params=params, headers=headers, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"http {r.status_code}: {r.text[:250]}")
        chunk = r.json()
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size
        time.sleep(0.2)  # freundlich bleiben
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df

def to_int(s):
    try:
        return int(float(s))
    except Exception:
        return None

# ------------ main ------------
def main():
    ensure_dirs()

    token = os.getenv("CFTC_APP_TOKEN", "")
    dataset_id = os.getenv("COT_DATASET_ID", "gpe5-46if")  # TFF Futures Only
    weeks = int(os.getenv("COT_WEEKS", "52"))

    errors: List[Dict] = []

    # Zeitraum: letzte N Wochen
    since = (datetime.utcnow() - timedelta(days=7*weeks)).date().isoformat()

    # Minimalfelder (TFF Futures Only hat diese Spaltennamen)
    select = ",".join([
        "report_date",
        "market_and_exchange_names",
        "open_interest_all",
        "noncomm_positions_long_all",
        "noncomm_positions_short_all",
        "noncomm_positions_spread_all",
        "change_in_open_interest_all"
    ])

    where = f"report_date >= '{since}'"
    order = "report_date desc"

    try:
        df = socrata_fetch_all(dataset_id, token, where, select, order)
    except Exception as e:
        errors.append({"stage": "fetch", "msg": str(e)})
        df = pd.DataFrame()

    # Typen sauber machen
    if not df.empty:
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.date
        for c in ["open_interest_all",
                  "noncomm_positions_long_all",
                  "noncomm_positions_short_all",
                  "noncomm_positions_spread_all",
                  "change_in_open_interest_all"]:
            if c in df.columns:
                df[c] = df[c].map(to_int)

    # optional: Markt-Filter anwenden (Teilstring- oder einfache Regex-Matches)
    patterns = load_market_filters()
    if patterns and not df.empty:
        mask = False
        for p in patterns:
            # akzeptiere Regex /plain text
            try:
                rx = re.compile(p, re.IGNORECASE)
                m = df["market_and_exchange_names"].str.contains(rx)
            except re.error:
                m = df["market_and_exchange_names"].str.contains(p, case=False, na=False)
            mask = m if mask is False else (mask | m)
        df = df[mask].copy()

    # letzte Woche bestimmen
    if df.empty:
        # leere Artefakte trotzdem schreiben
        pd.DataFrame(columns=["market","report_date","oi","ncl","ncs","noncomm_net"]
                     ).to_csv("data/processed/cot_summary.csv", index=False)
        pd.DataFrame().to_csv("data/processed/cot_latest_raw.csv", index=False)
    else:
        latest_date = df["report_date"].max()
        dfl = df[df["report_date"] == latest_date].copy()

        # Summary-Felder
        dfl["noncomm_net"] = (dfl["noncomm_positions_long_all"].fillna(0)
                              - dfl["noncomm_positions_short_all"].fillna(0))
        out = dfl.rename(columns={
            "market_and_exchange_names": "market",
            "open_interest_all": "oi",
            "noncomm_positions_long_all": "ncl",
            "noncomm_positions_short_all": "ncs"
        })[["market", "report_date", "oi", "ncl", "ncs", "noncomm_net"]].sort_values(
            ["market"]
        )

        out.to_csv("data/processed/cot_summary.csv", index=False)
        dfl.to_csv("data/processed/cot_latest_raw.csv", index=False)

    # Fehlerbericht
    report = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "dataset": dataset_id,
        "since": since,
        "rows": 0 if df.empty else int(len(df)),
        "latest": None if df.empty else str(df["report_date"].max()),
        "filtered_markets": patterns,
        "errors": errors,
    }
    with open("data/reports/cot_errors.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"wrote data/processed/cot_summary.csv rows={0 if df.empty else len(out)}")
    if errors:
        print(json.dumps(report, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
