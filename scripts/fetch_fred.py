# scripts/fetch_fred.py
import os, json, requests, sys
from pathlib import Path
import yaml

FRED = "https://api.stlouisfed.org/fred/series/observations"

def fred_series(series_id: str, api_key: str, start="1990-01-01"):
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    r = requests.get(FRED, params=params, timeout=30)
    # Wenn 400/404 etc., Exception werfen -> wird oben abgefangen
    r.raise_for_status()
    j = r.json()
    return j.get("observations", [])

def main():
    out_dir = Path("data/macro/fred")
    out_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        print("No FRED_API_KEY", file=sys.stderr)
        sys.exit(0)  # sauber beenden, nicht als Fehler zählen

    cfg = yaml.safe_load(open("config/config.yaml", "r", encoding="utf-8"))
    series = cfg.get("fred", {}).get("series", [])

    errors = []
    combined = {}

    for item in series:
        sid = item["id"]
        label = item.get("label", sid)
        try:
            obs = fred_series(sid, api_key)
        except requests.HTTPError as e:
            print(f"[FRED] Skip {sid}: {e}", file=sys.stderr)
            errors.append({"id": sid, "error": str(e)})
            continue

        # Letzte 120 Punkte
        obs = obs[-120:]
        combined[sid] = {"label": label, "observations": obs}

        # Einzeln ablegen
        (out_dir / f"{sid}.json").write_text(
            json.dumps({"id": sid, "label": label, "observations": obs},
                       ensure_ascii=False),
            encoding="utf-8"
        )

    # Gesamtdatei + kleiner Report
    (out_dir / "fred_all.json").write_text(
        json.dumps(combined, ensure_ascii=False), encoding="utf-8"
    )
    Path("data/reports").mkdir(parents=True, exist_ok=True)
    (Path("data/reports") / "fred_errors.json").write_text(
        json.dumps(errors, ensure_ascii=False), encoding="utf-8"
    )

if __name__ == "__main__":
    os.makedirs("data/processed", exist_ok=True)  # harmless
    os.makedirs("docs", exist_ok=True)            # harmless
    sys.exit(main())
