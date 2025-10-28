# scripts/fetch_ecb.py (Ausschnitt)

import os, sys, time, json, csv, pathlib, urllib.parse
import requests

BASE = "https://data-api.ecb.europa.eu/service/data"
OUTDIR = pathlib.Path("data/macro/ecb"); OUTDIR.mkdir(parents=True, exist_ok=True)
REPORT = pathlib.Path("data/reports/ecb_errors.json")

SERIES = {
    # Wechselkurs (läuft schon bei dir):
    "exr_usd_eur": ("EXR", "EXR.D.USD.EUR.SP00.A", {"lastNObservations":"720", "format":"csvdata"}),

    # CISS – Composite Indicator of Systemic Stress (neue Keys):
    "ciss_ea_d":  ("CISS", "CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX", {"lastNObservations":"720", "format":"csvdata"}),
    "ciss_us_d":  ("CISS", "CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX", {"lastNObservations":"720", "format":"csvdata"}),
    # Optional auch monthly:
    # "ciss_ea_m": ("CISS", "CISS.M.U2.Z0Z.4F.EC.SS_CIN.IDX", {"lastNObservations":"240","format":"csvdata"}),
}

def fetch(dataflow, key, params):
    url = f"{BASE}/{dataflow}/{key}"
    r = requests.get(url, params=params, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {r.url}\n{r.text[:400]}")
    return r.text

def write_csv(alias, csv_text):
    out = OUTDIR / f"{alias}.csv"
    out.write_text(csv_text, encoding="utf-8")
    return out

def main():
    errors = []
    files  = {}
    for alias, (dataflow, key, params) in SERIES.items():
        try:
            csv_text = fetch(dataflow, key, params)
            path = write_csv(alias, csv_text)
            files[alias] = str(path)
            print(f"✅ ECB {alias}: {sum(1 for _ in csv_text.splitlines())-1} rows  -> {path}")
        except Exception as e:
            msg = str(e)
            print(f"❌ ECB {alias} failed: {msg}")
            errors.append({"alias": alias, "err": msg})
            continue

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps({"files": files, "errors": errors}, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
