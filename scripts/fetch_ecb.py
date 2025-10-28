#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, time, argparse, requests, pandas as pd
from datetime import datetime

OUT_DIR = "data/macro/ecb"; os.makedirs(OUT_DIR, exist_ok=True)
REP = "data/reports/ecb_errors.json"; os.makedirs("data/reports", exist_ok=True)

# Aliase -> (flow, key, params)  (SDMX)
SERIES = {
    # USD/EUR Spot (daily close)
    "exr_usd_eur": ("EXR", "D.USD.EUR.SP00.A", {"lastNObservations": "0"}),
    # Composite Indicator of Systemic Stress – Euro Area
    "ciss_ea":     ("CISS", "M.U2.Z0Z.F.W0.SS_CI.4F.B.F", {"lastNObservations": "0"}),
}

NEW = "https://data-api.ecb.europa.eu/service/data"
OLD = "https://sdw-wsrest.ecb.europa.eu/service/data"

def ecb_get(flow, key, params, retries=3):
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0.0"}
    for i in range(retries):
        for base in (NEW, OLD):
            try:
                r = requests.get(f"{base}/{flow}/{key}", params=params, headers=headers, timeout=45)
                if r.status_code==200: return r.json()
                r = requests.get(f"{base}/{flow}/{key}", params=params, headers={"Accept":"application/json"}, timeout=45)
                if r.status_code==200: return r.json()
            except requests.RequestException: pass
        time.sleep(1.4*(i+1))
    raise RuntimeError(f"ECB fetch failed for {flow}/{key}")

def parse_sdmx_json(j):
    out=[]
    # Try SDMX 3 observations
    try:
        for k, arr in j["data"]["observations"].items():
            t = k.split(":")[-1]
            v = arr[0] if isinstance(arr,list) else arr
            if v is not None: out.append((t,float(v)))
    except Exception:
        pass
    df = pd.DataFrame(out, columns=["date","value"])
    if not df.empty:
        df["date"] = df["date"].astype(str)
        df.loc[df["date"].str.len()==7, "date"] += "-01"
        df = df.sort_values("date")
    return df

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--only"); ap.add_argument("--since")
    args=ap.parse_args()
    use = {args.only: SERIES[args.only]} if args.only else SERIES
    report={"ts": datetime.utcnow().isoformat()+"Z", "files":{}, "errors":[]}

    for alias,(flow,key,params) in use.items():
        try:
            js = ecb_get(flow,key,params)
            df = parse_sdmx_json(js)
            if args.since: df = df[df["date"]>=args.since]
            out = os.path.join(OUT_DIR,f"{alias}.csv"); df.to_csv(out,index=False)
            report["files"][alias]=out
        except Exception as e:
            report["errors"].append({"alias":alias,"msg":str(e)})

    with open(REP,"w",encoding="utf-8") as f: json.dump(report,f,indent=2)
    print(json.dumps(report, indent=2))

if __name__=="__main__": main()
