# scripts/fetch_fred_oas.py
import os, time, requests, pandas as pd

API = os.getenv("FRED_API_KEY") or ""
OBS = "https://api.stlouisfed.org/fred/series/observations"
SRCH = "https://api.stlouisfed.org/fred/series/search"

OUT = "data/processed/fred_oas.csv"

# Zielbegriffe (wir suchen dynamisch die korrekte Serien-ID)
QUERIES = {
    "US_IG_OAS": "ICE BofA US Corporate Option-Adjusted Spread",
    "US_HY_OAS": "ICE BofA US High Yield Option-Adjusted Spread",
    "EU_IG_OAS": "ICE BofA Euro Corporate Option-Adjusted Spread",
    "EU_HY_OAS": "ICE BofA Euro High Yield Option-Adjusted Spread",
}

def search_series(query):
    p = {
        "search_text": query,
        "api_key": API,
        "file_type": "json",
        # bessere Treffer: sortiere nach Popularität und letzer Beobachtung
        "order_by": "popularity",
        "sort_order": "desc",
        "limit": 10,
    }
    r = requests.get(SRCH, params=p, timeout=30)
    r.raise_for_status()
    items = r.json().get("seriess", [])
    # nimm die erste Serie, deren Titel OAS enthält (robust)
    for s in items:
        if "OAS" in s.get("title","").upper():
            return s["id"]
    return items[0]["id"] if items else None

def pull_observations(series_id):
    p = {
        "series_id": series_id,
        "api_key": API,
        "file_type": "json",
        "observation_start": "1999-01-01",
    }
    r = requests.get(OBS, params=p, timeout=30)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    rows = []
    for o in obs:
        try:
            v = float(o["value"])
        except Exception:
            v = None
        rows.append({"date": o["date"], "value": v})
    return pd.DataFrame(rows)

def main():
    if not API:
        print("No FRED_API_KEY"); return 0
    os.makedirs(os.path.dirname(OUT), exist_ok=True)

    cols = []
    df = None
    for col, q in QUERIES.items():
        try:
            sid = search_series(q)
            if not sid:
                print("no sid for", col, q); continue
            time.sleep(0.2)
            d = pull_observations(sid)
            if d.empty: 
                print("empty", col, sid); 
                continue
            d.rename(columns={"value": col}, inplace=True)
            cols.append(col)
            df = d if df is None else df.merge(d, on="date", how="outer")
            time.sleep(0.2)
        except Exception as e:
            print("FRED fail", col, e)

    if df is None or not cols:
        print("no OAS data"); return 0

    df.sort_values("date", inplace=True)
    df.to_csv(OUT, index=False)
    print("wrote", OUT, len(df), "rows,", len(cols), "cols")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
