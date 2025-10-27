# scripts/fetch_ice_cds_snapshot.py
import os, sys, json, time, re
from urllib.parse import urljoin
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup

ICE_SOURCES = {
    # Public report pages (die enthalten Links auf CSV-Downloads)
    "single_names": "https://www.theice.com/marketdata/reports/180",
    "indices":      "https://www.theice.com/marketdata/reports/181",
}

OUT_FIELDS = ["date","type","entity","ticker","currency","tenor","doc_clause","spread_bps","price"]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

def sess():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "en-US,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    # Manche ICE-Seiten zeigen Disclaimer – Cookie vorab akzeptieren.
    s.cookies.set("ice_disclaimer_accepted", "true", domain=".theice.com")
    s.cookies.set("PRIVACY_ACKNOWLEDGED", "true", domain=".theice.com")
    return s

def find_csv_links(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if any(ext in href.lower() for ext in [".csv", "download", "export"]):
            links.append(urljoin(base_url, href))
    # Reserve: manchmal stehen CSV-URLs in Skripten
    for m in re.finditer(r'href="([^"]+\.csv[^"]*)"', html, re.I):
        links.append(urljoin(base_url, m.group(1)))
    # de-dupe
    out = []
    seen = set()
    for u in links:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def load_first_csv(s: requests.Session, page_url: str) -> pd.DataFrame | None:
    r = s.get(page_url, timeout=30)
    r.raise_for_status()
    csv_links = find_csv_links(page_url, r.text)
    for link in csv_links:
        try:
            rr = s.get(link, timeout=30)
            if "text/csv" in rr.headers.get("Content-Type","").lower() or link.lower().endswith(".csv"):
                df = pd.read_csv(pd.compat.StringIO(rr.text)) if hasattr(pd.compat, "StringIO") else pd.read_csv(
                    pd.io.common.StringIO(rr.text)
                )
                if not df.empty:
                    return df
        except Exception:
            continue
    return None

def normalize_single_names(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in cols: return cols[c]
        return None

    name_col   = pick("reference entity","name","entity")
    ticker_col = pick("ticker","short name","ric","bbg","isin")
    ccy_col    = pick("ccy","currency")
    tenor_col  = pick("tenor")
    doc_col    = pick("doc clause","doc", "docclause")
    spr_col    = pick("par spread (bps)","spread (bps)","spread","par spread")
    price_col  = pick("price","clean price","price (%)")

    if not name_col or (not spr_col and not price_col):
        return pd.DataFrame(columns=OUT_FIELDS)

    out = pd.DataFrame({
        "entity":     df.get(name_col),
        "ticker":     df.get(ticker_col),
        "currency":   df.get(ccy_col),
        "tenor":      (df.get(tenor_col) if tenor_col else "5Y"),
        "doc_clause": df.get(doc_col),
        "spread_bps": pd.to_numeric(df.get(spr_col), errors="coerce") if spr_col else None,
        "price":      pd.to_numeric(df.get(price_col), errors="coerce") if price_col else None,
    })
    out["type"] = "single_name"
    return out

def normalize_indices(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in cols: return cols[c]
        return None

    name_col  = pick("index","name","series")
    ccy_col   = pick("ccy","currency")
    tenor_col = pick("tenor")
    spr_col   = pick("par spread (bps)","spread (bps)","spread")
    price_col = pick("price","clean price","price (%)")

    if not name_col or (not spr_col and not price_col):
        return pd.DataFrame(columns=OUT_FIELDS)

    out = pd.DataFrame({
        "entity":     df.get(name_col),
        "ticker":     df.get(name_col),
        "currency":   df.get(ccy_col),
        "tenor":      (df.get(tenor_col) if tenor_col else "5Y"),
        "doc_clause": None,
        "spread_bps": pd.to_numeric(df.get(spr_col), errors="coerce") if spr_col else None,
        "price":      pd.to_numeric(df.get(price_col), errors="coerce") if price_col else None,
    })
    out["type"] = "index"
    return out

def main(out_csv: str, raw_dir: str):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)

    today = datetime.utcnow().date().isoformat()
    s = sess()
    frames, errors = [], []

    for key, url in ICE_SOURCES.items():
        try:
            # 1) Versuche CSV direkt
            df = load_first_csv(s, url)
            # 2) Falls kein CSV gefunden, fallback: Tabellen aus HTML (wenn doch serverseitig gerendert)
            if df is None:
                html = s.get(url, timeout=30).text
                tables = pd.read_html(html) if html else []
                df = max(tables, key=lambda t: t.shape[0]) if tables else None

            if df is None or df.empty:
                errors.append({"source": key, "reason": "no_data"})
                continue

            # Raw dump
            raw_path = os.path.join(raw_dir, f"{today}_{key}.csv")
            df.to_csv(raw_path, index=False)

            nf = normalize_single_names(df) if key == "single_names" else normalize_indices(df)
            if nf.empty:
                errors.append({"source": key, "reason": "normalize_empty"})
            else:
                frames.append(nf)
        except Exception as e:
            errors.append({"source": key, "reason": "exception", "msg": str(e)})

    if not frames:
        # schreibe minimalen Report und exit ohne Fehlercode
        with open("data/reports/ice_cds_errors.json","w",encoding="utf-8") as f:
            json.dump({"date": today, "rows": 0, "errors": errors}, f, indent=2)
        print("No frames parsed from ICE.")
        return 0

    data = pd.concat(frames, ignore_index=True)
    data["date"] = today
    data = data[OUT_FIELDS]

    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        old = pd.read_csv(out_csv)
        data = pd.concat([old, data], ignore_index=True)
        data.drop_duplicates(subset=["date","type","entity"], keep="last", inplace=True)

    data.to_csv(out_csv, index=False)
    with open("data/reports/ice_cds_errors.json","w",encoding="utf-8") as f:
        json.dump({"date": today, "rows": int(data.shape[0]), "errors": errors}, f, indent=2)

    print(f"ICE CDS snapshot written: {out_csv}, rows={data.shape[0]}")
    return 0

if __name__ == "__main__":
    out = sys.argv[sys.argv.index("--out")+1]    if "--out" in sys.argv else "data/processed/cds_eod.csv"
    raw = sys.argv[sys.argv.index("--rawdir")+1] if "--rawdir" in sys.argv else "data/cds"
    raise SystemExit(main(out, raw))
