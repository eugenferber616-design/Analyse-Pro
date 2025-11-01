# scripts/fetch_cot.py
import argparse, io, gzip, sys, time
from datetime import datetime, timedelta
import pandas as pd
import requests

CFTC_BASE = "https://www.cftc.gov/dea/newcot"
# Wir verwenden die disaggregated + legacy weekly files und mergen robust.

FILES = [
    # Disaggregated (Futures Only)
    "disagg_fut_txt_2025.csv", "disagg_fut_txt_2024.csv", "disagg_fut_txt_2023.csv",
    # … bei Bedarf weitere Jahre, wir holen dynamisch unten via helper
]
LEGACY = [
    "fut_fin_txt_2025.csv", "fut_fin_txt_2024.csv", "fut_fin_txt_2023.csv",
    # … ebenfalls dynamisch ergänzt
]

def _year_files(prefix, y0, y1):
    return [f"{prefix}_{y}.csv" for y in range(y1, y0-1, -1)]

def _dl(url, retry=3, timeout=20):
    for i in range(retry):
        r = requests.get(url, timeout=timeout)
        if r.ok:
            return r.content
        time.sleep(1.5)
    raise RuntimeError(f"HTTP {r.status_code} for {url}")

def fetch_year_range(years: int) -> pd.DataFrame:
    year1 = datetime.utcnow().year
    year0 = year1 - (years - 1)

    dis = _year_files("disagg_fut_txt", year0, year1)
    leg = _year_files("fut_fin_txt",    year0, year1)

    frames = []
    for fname in dis + leg:
        url = f"{CFTC_BASE}/{fname}"
        try:
            raw = _dl(url)
            df = pd.read_csv(io.BytesIO(raw))
            df["source_file"] = fname
            frames.append(df)
        except Exception as e:
            # still proceed (CFTC lässt ältere Files manchmal weg)
            print(f"[WARN] skip {fname}: {e}", file=sys.stderr)

    if not frames:
        raise RuntimeError("No CFTC files fetched.")

    df = pd.concat(frames, ignore_index=True)

    # Normalize date column
    date_cols = [c for c in df.columns if "report_date" in c.lower() or "as_of_date" in c.lower()]
    if date_cols:
        col = date_cols[0]
        df["report_date"] = pd.to_datetime(df[col], errors="coerce").dt.date
    else:
        raise RuntimeError("No report date column found.")

    # Einheitliche Namensmap (nur die wichtigsten Felder)
    rename = {
        "Market_and_Exchange_Names":"market_and_exchange_names",
        "Contract_Market_Name":"contract_market_name",
        "Open_Interest_All":"open_interest_all",
        "Open_Interest":"open_interest",
        "Dealer_Positions_Long_All":"dealer_positions_long_all",
        "Dealer_Positions_Short_All":"dealer_positions_short_all",
        "Asset_Mgr_Positions_Long_All":"asset_mgr_positions_long",
        "Asset_Mgr_Positions_Short_All":"asset_mgr_positions_short",
        "Lev_Money_Positions_Long_All":"lev_money_positions_long",
        "Lev_Money_Positions_Short_All":"lev_money_positions_short",
        "Other_Rept_Positions_Long_All":"other_rept_positions_long",
        "Other_Rept_Positions_Short_All":"other_rept_positions_short",
        "Noncommercial_Long_All":"noncomm_long",
        "Noncommercial_Short_All":"noncomm_short",
        "Commercial_Long_All":"commercial_long",
        "Commercial_Short_All":"commercial_short",
        "Nonreportable_Positions_Long_All":"nonreportable_long",
        "Nonreportable_Positions_Short_All":"nonreportable_short",
    }
    # soft rename
    for k,v in list(rename.items()):
        if k in df.columns and v not in df.columns:
            df.rename(columns={k:v}, inplace=True)

    # Sort & Keep only needed columns (breit genug für deinen Viewer)
    keep = [
        "report_date",
        "market_and_exchange_names","contract_market_name","commodity_name","commodity",
        "open_interest_all","open_interest",
        "dealer_positions_long_all","dealer_positions_short_all",
        "asset_mgr_positions_long","asset_mgr_positions_short",
        "lev_money_positions_long","lev_money_positions_short",
        "other_rept_positions_long","other_rept_positions_short",
        "noncomm_long","noncomm_short",
        "commercial_long","commercial_short",
        "nonreportable_long","nonreportable_short",
    ]
    have = [c for c in keep if c in df.columns]
    df = df[have].copy()
    df.sort_values("report_date", inplace=True)
    df.dropna(subset=["report_date"], inplace=True)

    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=20)
    ap.add_argument("--out",   type=str, required=True)  # expect .csv or .csv.gz
    args = ap.parse_args()

    df = fetch_year_range(args.years)

    if args.out.endswith(".gz"):
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        with gzip.open(args.out, "wb", compresslevel=9) as gz:
            gz.write(buf.getvalue())
    else:
        df.to_csv(args.out, index=False)

    print(f"wrote {args.out} rows={len(df)}")

if __name__ == "__main__":
    main()
