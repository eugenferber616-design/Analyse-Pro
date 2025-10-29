# scripts/fetch_ecb.py
import os, json, time, requests

OUTDIR = "data/macro/ecb"
REPORTS_DIR = "data/reports"
EU_CHECKS_DIR = os.path.join(REPORTS_DIR, "eu_checks")
os.makedirs(OUTDIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)
os.makedirs(EU_CHECKS_DIR, exist_ok=True)

BASE = "https://data-api.ecb.europa.eu/service/data"

# Map: alias -> (dataset, series_id, outfile)
SERIES = {
    # CISS Euro Area & US (neue ECB API)
    "ciss_ea":    ("CISS", "CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX", os.path.join(OUTDIR, "ciss_ea.csv")),
    "ciss_us":    ("CISS", "CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX", os.path.join(OUTDIR, "ciss_us.csv")),
    # USD/EUR Spot 2:15pm C.E.T. (Offizieller ECB Referenzkurs)
    "exr_usd_eur":("EXR",  "EXR.D.USD.EUR.SP00.A",           os.path.join(OUTDIR, "exr_usd_eur.csv")),
}

HEADERS = {
    "Accept": "text/csv; charset=utf-8",
    "User-Agent": "Analyse-Pro/1.0 (ECB fetch; github actions)",
}

def is_probably_csv(text: str, ct: str | None) -> bool:
    if not text:
        return False
    if ct and "text/csv" in ct.lower():
        return True
    # Fallback-Heuristik: CSV hat typischerweise Kommas + Zeilenumbrüche und KEIN '<html'
    t = text.lstrip().lower()
    if t.startswith("<html") or t.startswith("<!doctype html") or t.startswith("<?xml"):
        return False
    return ("," in text) and ("\n" in text)

def fetch_one(alias: str, dataset: str, series_id: str, outfile: str, retries: int = 3, backoff: float = 0.8):
    """
    Holt eine einzelne Serie als CSV von der ECB-API (neues Data Portal).
    """
    params = {
        "format": "csvdata",
        "startPeriod": "1999-01-01",
    }
    url = f"{BASE}/{dataset}/{series_id}"

    last_err = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=40)
            ct = r.headers.get("Content-Type", "")
            if r.status_code == 200 and is_probably_csv(r.text, ct):
                with open(outfile, "w", encoding="utf-8", newline="") as f:
                    f.write(r.text)
                return None  # success
            else:
                # typischer Fehler: HTML/Error-Seite als Text zurück
                snippet = (r.text or "")[:240].replace("\n", " ").replace("\r", " ")
                last_err = {
                    "alias": alias,
                    "status": r.status_code,
                    "content_type": ct,
                    "err": f"Unexpected response (not CSV). Snippet: {snippet}",
                    "url": r.url,
                }
        except requests.RequestException as e:
            last_err = {"alias": alias, "err": f"RequestException: {e}", "url": url}

        # Backoff und nächster Versuch
        time.sleep(backoff * (2 ** i))

    return last_err or {"alias": alias, "err": "Unknown error", "url": url}

def main():
    errors = []
    ok_aliases = []

    for alias, (dataset, series_id, outfile) in SERIES.items():
        err = fetch_one(alias, dataset, series_id, outfile)
        if err:
            errors.append(err)
        else:
            ok_aliases.append(alias)

    # Log-Ausgabe
    print("ECB OK:", ok_aliases)
    print("ECB ERRORS:", json.dumps(errors, indent=2, ensure_ascii=False))

    # Reports schreiben
    with open(os.path.join(REPORTS_DIR, "ecb_errors.json"), "w", encoding="utf-8") as f:
        json.dump({"errors": errors}, f, indent=2, ensure_ascii=False)

    # kleiner Preview-Report für Ampel
    preview_lines = []
    for alias, (_, _, outfile) in SERIES.items():
        try:
            if os.path.exists(outfile) and os.path.getsize(outfile) > 0:
                # nur Kopfzeile + erste Datenzeile zeigen
                with open(outfile, "r", encoding="utf-8") as f:
                    head = f.readline().strip()
                    first = f.readline().strip()
                preview_lines.append(f"{outfile},{('' if not first else first.split(',')[0])},{'OK' if alias in ok_aliases else 'ERR'}")
            else:
                preview_lines.append(f"{outfile},,MISSING")
        except Exception as e:
            preview_lines.append(f"{outfile},,READ_ERR:{e}")

    with open(os.path.join(EU_CHECKS_DIR, "ecb_preview.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(preview_lines) + "\n")

if __name__ == "__main__":
    main()
