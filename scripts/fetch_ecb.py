#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ECB series fetcher (new data-api.ecb.europa.eu, with legacy fallback)
- Saves each series to data/macro/ecb/<alias>.csv as date,value
Usage:
  python scripts/fetch_ecb.py
  python scripts/fetch_ecb.py --only exr_usd_eur
  python scripts/fetch_ecb.py --since 2010-01-01
"""

import os, sys, argparse, time, json
from datetime import datetime
import requests
import pandas as pd

OUT_DIR = "data/macro/ecb"
os.makedirs(OUT_DIR, exist_ok=True)

# --- Define the series you want (extend as you like) --------------------------
# Flow/key follow SDMX pattern. See ECB Data API browser for exact codes.
SERIES = {
    # Spot USD/EUR (daily close). Alias -> (flowRef, key, params)
    "exr_usd_eur": ("EXR", "D.USD.EUR.SP00.A", {"lastNObservations": "0"}),

    # Systemic Stress Indicator (CISS) – weekly, Euro area total
    # Reference: "CISS/M.U2.Z0Z.F.W0.SS_CI.4F.B.F" (example)
    "ciss_ea": ("CISS", "M.U2.Z0Z.F.W0.SS_CI.4F.B.F", {"lastNObservations": "0"}),

    # Gov yield curve 10Y (Par yield) – monthly (example)
    # Flow YC, key may vary by segment/maturity; adjust to your needs.
    # "yc_10y_ea": ("YC", "M.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y.P", {"lastNObservations": "0"}),
}

NEW_BASE = "https://data-api.ecb.europa.eu/service/data"
LEGACY_BASE = "https://sdw-wsrest.ecb.europa.eu/service/data"

def ecb_get(flow, key, params, retries=3, timeout=45):
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0.0"}
    url_new = f"{NEW_BASE}/{flow}/{key}"
    url_old = f"{LEGACY_BASE}/{flow}/{key}"

    for attempt in range(1, retries+1):
        for base_url in (url_new, url_old):
            try:
                r = requests.get(base_url, params=params, headers=headers, timeout=timeout)
                if r.status_code == 200:
                    return r.json()
                # some servers answer 406 for wrong Accept -> loosen
                r = requests.get(base_url, params=params, headers={"Accept":"application/json"}, timeout=timeout)
                if r.status_code == 200:
                    return r.json()
            except requests.RequestException as e:
                err = str(e)
        # backoff
        time.sleep(1.5 * attempt)
    raise RuntimeError(f"ECB API failed for {flow}/{key} ; last_error={err}")

def parse_sdmx_json(json_obj):
    # Minimal SDMX-JSON reader: extract (time, value)
    # Works with ECB SDMX-JSON "dataSets"/"structure"/"series"
    try:
        series = json_obj["data"]["series"]
        # SDMX 3
    except KeyError:
        # SDMX 2 style
        data_sets = json_obj.get("dataSets") or json_obj.get("dataset") or []
        if not data_sets:
            return pd.DataFrame(columns=["date","value"])
        ds = data_sets[0]
        # series indexed by "series 0:0:0" etc.
        series = ds.get("series", {})
    out = []
    # Find observations for first series found
    for _, ser in series.items():
        obs = ser.get("observations") or ser.get("obs", {})
        # observations: {"0":[value], "1":[value], ...}, time is in structure/observation
        # Try modern SDMX path
        if isinstance(obs, dict) and obs:
            # Need time periods list
            try:
                time_list = json_obj["structure"]["dimensions"]["observation"][0]["values"]
                # Build mapping index->time
                idx_to_time = {i: v["id"] for i, v in enumerate(time_list)}
                for k, v in obs.items():
                    i = int(k)
                    val = v[0] if isinstance(v, list) else v
                    t = idx_to_time.get(i)
                    if t is not None and val is not None:
                        out.append((t, float(val)))
            except Exception:
                # Fallback – try "observations" with explicit time key
                pass
        # If SDMX 3 compact format wasn't recognized, try "data/observations"
        break  # only first series
    if not out:
        # Newer API sometimes provides data/observations directly
        try:
            observations = json_obj["data"]["observations"]
            for k, arr in observations.items():
                # k like "0:0:0:2024-09-30"
                parts = k.split(":")
                t = parts[-1]
                val = arr[0] if isinstance(arr, list) else arr
                if val is not None:
                    out.append((t, float(val)))
        except Exception:
            pass
    df = pd.DataFrame(out, columns=["date","value"]).dropna()
    # normalize date
    def norm_date(s):
        s = str(s)
        if len(s) == 7 and s[4]=="-":  # YYYY-MM
            return f"{s}-01"
        return s
    if not df.empty:
        df["date"] = df["date"].map(norm_date)
        df = df.sort_values("date")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="only fetch this alias (key of SERIES)")
    ap.add_argument("--since", help="filter to start date YYYY-MM-DD")
    args = ap.parse_args()

    since = args.since

    to_fetch = {args.only: SERIES[args.only]} if args.only else SERIES
    report = {"ts": datetime.utcnow().isoformat()+"Z", "files": {}, "errors": []}

    for alias, (flow, key, params) in to_fetch.items():
        try:
            js = ecb_get(flow, key, params)
            df = parse_sdmx_json(js)
            if since and not df.empty:
                df = df[df["date"] >= since]
            out = os.path.join(OUT_DIR, f"{alias}.csv")
            df.to_csv(out, index=False)
            report["files"][alias] = out
        except Exception as e:
            report["errors"].append({"alias": alias, "msg": str(e)})

    os.makedirs("data/reports", exist_ok=True)
    with open("data/reports/ecb_errors.json","w",encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
