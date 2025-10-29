# scripts/fetch_ecb.py
import csv, os, sys, time, json, requests
OUTDIR = "data/macro/ecb"; os.makedirs(OUTDIR, exist_ok=True)

SE = {
    "ciss_ea": ("CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX", f"{OUTDIR}/ciss_ea.csv"),
    "ciss_us": ("CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX", f"{OUTDIR}/ciss_us.csv"),
    "exr_usd_eur": ("EXR.D.USD.EUR.SP00.A",       f"{OUTDIR}/exr_usd_eur.csv"),  # lassen wir wie gehabt
}
BASE = "https://data-api.ecb.europa.eu/service/data"

def fetch(alias, key, out):
    params = {"startPeriod":"1999-01-01", "format":"csvdata"}
    url = f"{BASE}/{alias.split('_')[0].upper()}/{key}"
    # Sonderfall EXR → dataset "EXR"
    if alias == "exr_usd_eur":
        url = f"{BASE}/EXR/{key}"
    r = requests.get(url, params=params, headers={"Accept":"text/csv"}, timeout=30)
    if r.status_code != 200 or not r.text or r.text.lstrip().startswith("<"):
        return {"alias": alias, "err": f"HTTP {r.status_code}: {r.text[:240].replace('\\n',' ')}"}
    with open(out, "w", encoding="utf-8", newline="") as f: f.write(r.text)
    return None

def main():
    errs = []
    for a,(key,out) in SE.items():
        err = fetch(a, key, out)
        if err: errs.append(err)
    print("ECB OK:", [k for k,(s,_) in SE.items() if not any(e["alias"]==k for e in errs)])
    print("ECB ERRORS:", json.dumps(errs, indent=2, ensure_ascii=False))

if __name__ == "__main__": main()
