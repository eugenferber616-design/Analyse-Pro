# scripts/fetch_ecb.py  (v2 – SDMX JSON → CSV)
import os, json, csv, requests

OUTDIR = "data/macro/ecb"; os.makedirs(OUTDIR, exist_ok=True)
BASE   = "https://data-api.ecb.europa.eu/service/data"

SERIES = {
    # dataset -> series key (SDMX) -> output file
    ("CISS","D.U2.Z0Z.4F.EC.SS_CIN.IDX"): ("ciss_ea.csv", "EA"),
    ("CISS","D.US.Z0Z.4F.EC.SS_CIN.IDX"): ("ciss_us.csv", "US"),
    ("EXR", "D.USD.EUR.SP00.A")          : ("exr_usd_eur.csv", "EXR"),
}

def fetch_json(dataset, key):
    url = f"{BASE}/{dataset}/{key}"
    # SDMX-JSON liefert strukturierte Daten
    params = {"format":"sdmx-json", "compress":"true", "startPeriod":"1999-01-01"}
    r = requests.get(url, params=params, timeout=40,
                     headers={"Accept":"application/vnd.sdmx.data+json"})
    if r.status_code != 200 or not r.text or r.text.lstrip().startswith("<"):
        return None, {"dataset":dataset, "key":key, "status":r.status_code,
                      "snippet": r.text[:280].replace("\n"," ")}
    try:
        return r.json(), None
    except Exception as e:
        return None, {"dataset":dataset, "key":key, "status":"json_err", "err":str(e)}

def sdmx_to_rows(obj):
    # Minimal-Parser für ECB SDMX-JSON
    ds = obj.get("data",{}).get("dataSets",[{}])[0]
    ts = obj.get("data",{}).get("structure",{}).get("dimensions",{}).get("observation",[])
    # erste Dimension ist Zeit
    time_vals = [v["id"] for v in ts[0]["values"]]
    out = []
    for idx_str, val in ds.get("observations", {}).items():
        # idx_str z.B. "0:1234" -> zweites Feld: Zeitindex
        parts = idx_str.split(":")
        t_idx = int(parts[-1])
        date  = time_vals[t_idx]
        value = val[0]
        out.append((date, value))
    out.sort()
    return out

def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","value"])
        w.writerows(rows)

def main():
    errs = []
    ok   = []
    for (dataset, key), (fname, _tag) in SERIES.items():
        obj, err = fetch_json(dataset, key)
        if err:
            errs.append(err); continue
        rows = sdmx_to_rows(obj)
        if not rows:
            errs.append({"dataset":dataset,"key":key,"status":"empty"})
            continue
        write_csv(os.path.join(OUTDIR, fname), rows)
        ok.append(fname)

    # kleines Preview/Report
    os.makedirs("data/reports/eu_checks", exist_ok=True)
    with open("data/reports/eu_checks/ecb_preview.txt","w",encoding="utf-8") as f:
        for (dataset,key),(fname,_) in SERIES.items():
            p = os.path.join(OUTDIR,fname)
            f.write(f"{p},{'OK' if os.path.exists(p) and os.path.getsize(p)>0 else 'MISSING'}\n")
    with open("data/reports/ecb_errors.json","w",encoding="utf-8") as f:
        json.dump({"ok":ok,"errors":errs}, f, indent=2)

if __name__ == "__main__":
    main()
