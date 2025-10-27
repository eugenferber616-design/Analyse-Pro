# scripts/fetch_cot.py
import os, io, zipfile, requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

OUT = "data/processed/cot.csv"

INDEX = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm"
BASE  = "https://www.cftc.gov"

KEYWORDS = (
    "deacotdisagg",   # disaggregated (futures only)
    "deahistfo",      # futures+options compressed history
)

def discover_zip_urls():
    r = requests.get(INDEX, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".zip") and any(k in href.lower() for k in KEYWORDS):
            urls.append(urljoin(BASE, href))
    return sorted(set(urls))

def read_zip_to_frames(url):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    frames = []
    for name in z.namelist():
        if not name.lower().endswith((".txt",".csv")): 
            continue
        try:
            df = pd.read_csv(z.open(name), sep=",", engine="python")
        except Exception:
            # manche Dateien sind pipe-delimited
            df = pd.read_csv(z.open(name), sep="|", engine="python")
        df["source_file"] = name
        frames.append(df)
    return frames

def main():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    urls = discover_zip_urls()
    if not urls:
        print("no COT zip urls found"); return 0
    frames = []
    for u in urls:
        try:
            frames.extend(read_zip_to_frames(u))
        except Exception as e:
            print("COT zip fail", u, e)
    if not frames:
        print("no COT data"); return 0
    df = pd.concat(frames, ignore_index=True)
    df.to_csv(OUT, index=False)
    print("wrote", OUT, len(df), "rows from", len(urls), "zip(s)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
