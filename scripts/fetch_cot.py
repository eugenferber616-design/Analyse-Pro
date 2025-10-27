# scripts/fetch_cot.py
import os, io, zipfile, requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

OUT_GZ   = "data/processed/cot.csv.gz"        # große Vollhistorie (komprimiert)
OUT_SUM  = "data/processed/cot_summary.csv"   # kleines Commit-File

INDEX = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm"
BASE  = "https://www.cftc.gov"
KEYWORDS = ("deacotdisagg", "deahistfo")

def discover_zip_urls():
    r = requests.get(INDEX, timeout=60); r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.endswith(".zip") and any(k in href for k in KEYWORDS):
            urls.append(urljoin(BASE, a["href"]))
    return sorted(set(urls))

def read_zip_to_frames(url):
    r = requests.get(url, timeout=120); r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    frames = []
    for name in z.namelist():
        if not name.lower().endswith((".txt",".csv")): 
            continue
        try:
            df = pd.read_csv(z.open(name), sep=",", engine="python")
        except Exception:
            df = pd.read_csv(z.open(name), sep="|", engine="python")
        frames.append(df)
    return frames

def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    # Spaltennamen variieren je Set – wir greifen defensiv
    cols = {c.lower(): c for c in df.columns}
    date   = cols.get("report_date_as_yyyy-mm-dd") or cols.get("report_date_as_yyyy_mm_dd") or cols.get("report_date_as_yyyy-mm-dd")
    market = cols.get("market_and_exchange_names") or cols.get("market_and_exchange_name")
    # typische Netto-Positionen (Disaggregated, Futures-only)
    noncomm_long  = cols.get("noncomm_positions_long_all")
    noncomm_short = cols.get("noncomm_positions_short_all")
    comm_long     = cols.get("comm_positions_long_all")
    comm_short    = cols.get("comm_positions_short_all")

    keep = [k for k in [date, market, noncomm_long, noncomm_short, comm_long, comm_short] if k]
    if not keep: 
        return pd.DataFrame()

    out = df[keep].copy()
    if date: out.rename(columns={date: "date"}, inplace=True)
    if market: out.rename(columns={market: "market"}, inplace=True)
    if noncomm_long:  out.rename(columns={noncomm_long: "noncomm_long"}, inplace=True)
    if noncomm_short: out.rename(columns={noncomm_short: "noncomm_short"}, inplace=True)
    if comm_long:     out.rename(columns={comm_long: "comm_long"}, inplace=True)
    if comm_short:    out.rename(columns={comm_short: "comm_short"}, inplace=True)

    # leichte Verdichtung: wöchentlich pro Markt die Netto-Werte
    for c in ["noncomm_long","noncomm_short","comm_long","comm_short"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "noncomm_long" in out.columns and "noncomm_short" in out.columns:
        out["noncomm_net"] = out["noncomm_long"] - out["noncomm_short"]
    if "comm_long" in out.columns and "comm_short" in out.columns:
        out["comm_net"] = out["comm_long"] - out["comm_short"]

    return out

def main():
    os.makedirs("data/processed", exist_ok=True)
    urls = discover_zip_urls()
    frames = []
    for u in urls:
        try:
            frames.extend(read_zip_to_frames(u))
        except Exception as e:
            print("COT zip fail", u, e)

    if not frames:
        print("no COT data"); return 0

    full = pd.concat(frames, ignore_index=True)

    # 1) Große Vollhistorie komprimiert (unter 100 MB)
    full.to_csv(OUT_GZ, index=False, compression="gzip")

    # 2) Schlankes Summary (commit-freundlich)
    summ_frames = []
    for f in frames:
        s = build_summary(f)
        if not s.empty: summ_frames.append(s)
    if summ_frames:
        summary = pd.concat(summ_frames, ignore_index=True)
        # bisschen klein halten
        summary.dropna(how="all", axis=1, inplace=True)
        summary.to_csv(OUT_SUM, index=False)

    print(f"wrote {OUT_GZ} ({os.path.getsize(OUT_GZ)//1024//1024} MB gz) and {OUT_SUM if os.path.exists(OUT_SUM) else 'no summary'}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
