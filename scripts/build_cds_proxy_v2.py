# scripts/build_cds_proxy_v2.py
import os, csv, json, argparse
from datetime import datetime

FRED_OAS_CSV = "data/processed/fred_oas.csv"      # erzeugt von fetch_fred_oas.py
OUT_CSV      = "data/processed/cds_proxy.csv"
REPORT_JSON  = "data/reports/cds_proxy_report.json"
MAP_CSV      = "config/oas_proxy_map.csv"         # enthält Series-IDs unter regions

parser = argparse.ArgumentParser()
parser.add_argument("--watchlist", required=True)
args = parser.parse_args()

os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)

def read_watchlist(p):
    syms = []
    with open(p, encoding="utf-8") as f:
        for ln in f:
            s = ln.strip()
            if not s or s.startswith("#"): continue
            syms.append(s)
    return syms

def read_map_and_series(p):
    # sehr einfache YAML/CSV-Mischung lesen (wie in deinem Repo)
    # Wir parsen nur die regions-Keys + manuelle symbol-Zuordnungen.
    per_symbol = {}
    regions = {"US":{"IG":None,"HY":None},"EU":{"IG":None,"HY":None}}
    if not os.path.exists(p): 
        return per_symbol, regions
    with open(p, encoding="utf-8") as f:
        mode = "top"
        current = None
        for ln in f:
            line = ln.strip()
            if not line or line.startswith("#"): 
                continue
            if "," in line and mode == "top" and not line.lower().startswith("symbol"):
                # symbol,proxy Zeilen
                parts = [x.strip() for x in line.split(",")]
                if len(parts) >= 2 and parts[0]:
                    per_symbol[parts[0]] = parts[1] or None
                continue
            if line.lower().startswith("regions:"):
                mode = "regions"
                continue
            if mode == "regions":
                if line.endswith(":"):
                    current = line[:-1].strip()
                    continue
                if ":" in line and current in regions:
                    k,v = [x.strip() for x in line.split(":",1)]
                    v = v.strip().strip('"').strip("'")
                    v = v if v else None
                    if k in ("IG","HY"):
                        regions[current][k] = v
    return per_symbol, regions

def latest_oas_values(csv_path):
    # csv columns: date,series_id,value,bucket,region  (aus fetch_fred_oas.py)
    latest = {}
    if not os.path.exists(csv_path):
        return latest
    with open(csv_path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            sid = row["series_id"]
            val = row.get("value")
            dt  = row.get("date")
            if val in (None,""): 
                continue
            try:
                valf = float(val)
            except:
                continue
            # überschreiben → am Ende bleibt letzte Zeile bestehen (CSV ist asc sortiert)
            latest[sid] = valf
    return latest

def compute_eu_ig_proxy(us_ig, us_hy, eu_hy):
    # Hauptformel: EU_IG ≈ US_IG * (EU_HY / US_HY)
    if us_ig is None:
        return None
    if eu_hy is not None and us_hy not in (None,0):
        return round(us_ig * (eu_hy / us_hy), 2)
    # Fallback 1: wenn EU_HY fehlt → nimm US_IG
    if eu_hy is None and us_hy is not None:
        return round(us_ig, 2)
    # Fallback 2: wenn US_HY fehlt → nimm EU_HY * (US_IG / US_HY_median?) -> einfach US_IG
    return round(us_ig, 2)

def region_of(sym):
    return "EU" if sym.endswith(".DE") else "US"

def proxy_for_symbol(sym, per_symbol_override, latest_vals, regions_series):
    # Symbol-Override?
    override = per_symbol_override.get(sym)
    reg = region_of(sym)

    us_ig_id = regions_series["US"]["IG"]
    us_hy_id = regions_series["US"]["HY"]
    eu_hy_id = regions_series["EU"]["HY"]
    eu_ig_id = regions_series["EU"]["IG"]  # None

    us_ig = latest_vals.get(us_ig_id) if us_ig_id else None
    us_hy = latest_vals.get(us_hy_id) if us_hy_id else None
    eu_hy = latest_vals.get(eu_hy_id) if eu_hy_id else None

    # Falls explizit "US_IG/US_HY/EU_HY/EU_IG" als override drinsteht:
    if override:
        key = override.upper()
        if key == "US_IG" and us_ig is not None: return round(us_ig,2), "US", "US_IG"
        if key == "US_HY" and us_hy is not None: return round(us_hy,2), "US", "US_HY"
        if key == "EU_HY" and eu_hy is not None: return round(eu_hy,2), "EU", "EU_HY"
        if key == "EU_IG" and eu_ig_id:
            eu_ig = latest_vals.get(eu_ig_id)
            if eu_ig is not None: return round(eu_ig,2), "EU", "EU_IG"

    if reg == "US":
        # US: direkt US_IG (sauberste Näherung für CDS IG)
        return (round(us_ig,2) if us_ig is not None else None), "US", "US_IG"
    else:
        # EU: unsere Proxy-Formel
        val = compute_eu_ig_proxy(us_ig, us_hy, eu_hy)
        return val, "EU", "EU_IG_proxy"

symbols = read_watchlist(args.watchlist)
per_symbol_override, regions_series = read_map_and_series(MAP_CSV)
latest_vals = latest_oas_values(FRED_OAS_CSV)

rows_out = []
for s in symbols:
    val, reg, src = proxy_for_symbol(s, per_symbol_override, latest_vals, regions_series)
    rows_out.append([s.replace(",",""), reg, val])

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["symbol","region","proxy_spread"])
    w.writerows(rows_out)

report = {
    "ts": datetime.utcnow().isoformat()+"Z",
    "rows": len(rows_out),
    "fred_oas_used": {
        "US_IG": latest_vals.get(regions_series["US"]["IG"]),
        "US_HY": latest_vals.get(regions_series["US"]["HY"]),
        "EU_IG": latest_vals.get(regions_series["EU"]["IG"]) if regions_series["EU"]["IG"] else None,
        "EU_HY": latest_vals.get(regions_series["EU"]["HY"]),
    },
    "errors": [],
    "preview": OUT_CSV
}
with open(REPORT_JSON, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

print(json.dumps(report, indent=2))
