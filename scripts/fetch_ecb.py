#!/usr/bin/env python3
import csv, io, json, os, sys, time
from urllib.parse import urlencode
import requests

BASE = "https://data-api.ecb.europa.eu/service/data"

def fetch_csv(dataset: str, series_key: str, *, last_n=720, timeout=30):
    params = {"lastNObservations": str(last_n), "format": "csvdata"}
    url = f"{BASE}/{dataset}/{series_key}?{urlencode(params)}"
    r = requests.get(url, timeout=timeout)
    if r.status_code != 200 or not r.text.strip():
        raise RuntimeError(f"HTTP {r.status_code} for {url}\n{r.text[:300]}")
    # ECB liefert CSV mit Header; wir geben (header, rows) zurück
    buf = io.StringIO(r.text)
    reader = csv.reader(buf)
    rows = list(reader)
    return rows[0], rows[1:], url

def write_csv(path: str, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)

def main():
    outdir = "data/macro/ecb"
    repdir = "data/reports"
    os.makedirs(outdir, exist_ok=True)
    os.makedirs(repdir, exist_ok=True)

    report = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "files": {}, "errors": []}

    # 1) EXR USD/EUR (Daily spot)
    try:
        h, rows, url = fetch_csv("EXR", "D.USD.EUR.SP00.A", last_n=720)
        path = os.path.join(outdir, "exr_usd_eur.csv")
        write_csv(path, h, rows)
        report["files"]["exr_usd_eur"] = path
    except Exception as e:
        report["errors"].append({"alias": "exr_usd_eur", "err": str(e)})

    # 2) CISS – bevorzugt NEW CISS; bei Fehler fallback auf legacy
    ciss_targets = [
        ("ciss_ea_new", "CISS", "CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX"),
        ("ciss_us_new", "CISS", "CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX"),
        ("ciss_ea",     "CISS", "CISS.D.U2.Z0Z.4F.EC.SS_CI.IDX"),
        ("ciss_us",     "CISS", "CISS.D.US.Z0Z.4F.EC.SS_CI.IDX"),
    ]
    for alias, ds, key in ciss_targets:
        try:
            h, rows, url = fetch_csv(ds, key, last_n=720)
            fname = f"{alias}.csv"
            path = os.path.join(outdir, fname)
            write_csv(path, h, rows)
            report["files"][alias] = path
        except Exception as e:
            report["errors"].append({"alias": alias, "err": str(e)})

    # Bericht ablegen
    with open(os.path.join(repdir, "ecb_errors.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Summarische Ausgabe für CI-Logs
    ok = [k for k in report["files"]]
    print("ECB OK:", ok)
    if report["errors"]:
        print("ECB ERRORS:", json.dumps(report["errors"][:3], indent=2))

if __name__ == "__main__":
    sys.exit(main())
