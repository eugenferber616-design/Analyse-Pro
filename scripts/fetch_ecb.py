# scripts/fetch_ecb.py
import os, json, requests

BASE = "https://data-api.ecb.europa.eu/service/data"
OUTDIR = "data/macro/ecb"
ERR = "data/reports/ecb_errors.json"
os.makedirs(OUTDIR, exist_ok=True)
os.makedirs(os.path.dirname(ERR), exist_ok=True)

errors = []

def fetch_to_csv(url: str, out_path: str, alias: str):
    r = requests.get(url, timeout=60)
    if r.status_code == 200:
        open(out_path, "w", encoding="utf-8").write(r.text)
        print(f"ECB {alias}: {out_path}")
    else:
        errors.append({"alias": alias, "status": r.status_code, "err": r.text[:400]})
        print(f"ECB {alias} failed: {r.status_code}")

def main():
    # USD/EUR Spot, daily, 3 Jahre (~720 Beobachtungen)
    exr_url = f"{BASE}/EXR/D.USD.EUR.SP00.A?lastNObservations=720&format=csvdata"
    fetch_to_csv(exr_url, os.path.join(OUTDIR, "exr_usd_eur.csv"), "exr_usd_eur")

    # CISS (Euro area), weekly → optional; wenn 400 kommt, einfach im Workflow erstmal ignorieren
    ciss_key = "CISS/M.U2.Z0Z.F.W0.SS_CI.4F.B.B"  # laut ECB Data Explorer
    ciss_url = f"{BASE}/{ciss_key}?lastNObservations=520&format=csvdata"
    fetch_to_csv(ciss_url, os.path.join(OUTDIR, "ciss_ea.csv"), "ciss_ea")

    open(ERR, "w").write(json.dumps({"errors": errors}, indent=2))

if __name__ == "__main__":
    main()
