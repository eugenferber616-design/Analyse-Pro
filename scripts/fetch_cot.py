# scripts/fetch_cot.py
import os, io, zipfile, requests, pandas as pd

OUT = "data/processed/cot.csv"

URLS = {
  # Disaggregated Reports – History (jährliche ZIPs zusammengepackt von CFTC)
  "fut_only": "https://www.cftc.gov/files/dea/history/deacotdisagg_txt_2006_2024.zip",
  "fut_opt":  "https://www.cftc.gov/files/dea/history/deahistfo_2006_2024.zip",
}

def load_zip(url):
    r = requests.get(url, timeout=60); r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    frames = []
    for name in z.namelist():
        if not name.lower().endswith(".txt"): continue
        df = pd.read_csv(z.open(name), sep=",", engine="python")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def main():
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    frames = []
    for k,u in URLS.items():
        try:
            df = load_zip(u)
            if not df.empty:
                df["report_type"] = k
                frames.append(df)
        except Exception as e:
            print("COT fail", k, e)
    if not frames:
        print("no COT data"); return 0
    out = pd.concat(frames, ignore_index=True)
    out.to_csv(OUT, index=False)
    print("wrote", OUT, len(out))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
