# scripts/fetch_ice_cds_snapshot.py
import os, sys, json, re
from urllib.parse import urljoin
from datetime import datetime
import pandas as pd, requests
from bs4 import BeautifulSoup

BASES = [
    "https://www.theice.com/marketdata/reports/",
    "https://theice.com/marketdata/reports/",
]
REPORTS = {"single_names": "180", "indices": "181"}
OUT_FIELDS = ["date","type","entity","ticker","currency","tenor","doc_clause","spread_bps","price"]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

def new_session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.8"})
    for d in [".theice.com", "theice.com", "www.theice.com"]:
        s.cookies.set("ice_disclaimer_accepted", "true", domain=d)
        s.cookies.set("PRIVACY_ACKNOWLEDGED", "true", domain=d)
    return s

def csv_from_page(sess, page_url):
    r = sess.get(page_url, timeout=30, allow_redirects=True)
    r.raise_for_status()
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        if any(x in a["href"].lower() for x in [".csv", "download", "export"]):
            links.append(urljoin(page_url, a["href"]))
    for m in re.finditer(r'href="([^"]+\.csv[^"]*)"', html, re.I):
        links.append(urljoin(page_url, m.group(1)))
    for u in dict.fromkeys(links):
        try:
            rr = sess.get(u, timeout=30)
            ct = rr.headers.get("Content-Type","").lower()
            if "csv" in ct or u.lower().endswith(".csv"):
                # pandas StringIO compat
                try:
                    sio = pd.compat.StringIO(rr.text)  # older pandas compat
                except Exception:
                    from io import StringIO
                    sio = StringIO(rr.text)
                df = pd.read_csv(sio)
                if not df.empty:
                    return df
        except Exception:
            continue
    return None

def _cols(df): return {c.lower().strip(): c for c in df.columns}
def _pick(cols, *cands):
    for c in cands:
        if c in cols: return cols[c]
    return None

def normalize_single(df):
    cols = _cols(df)
    name   = _pick(cols,"reference entity","name","entity")
    ticker = _pick(cols,"ticker","short name","ric","bbg","isin")
    ccy    = _pick(cols,"ccy","currency")
    tenor  = _pick(cols,"tenor")
    doc    = _pick(cols,"doc clause","doc","docclause")
    spr    = _pick(cols,"par spread (bps)","spread (bps)","spread","par spread")
    price  = _pick(cols,"price","clean price","price (%)")
    if not name or (not spr and not price):
        return pd.DataFrame(columns=OUT_FIELDS)
    out = pd.DataFrame({
        "entity": df.get(name),
        "ticker": df.get(ticker),
        "currency": df.get(ccy),
        "tenor": df.get(tenor) if tenor else "5Y",
        "doc_clause": df.get(doc),
        "spread_bps": pd.to_numeric(df.get(spr), errors="coerce") if spr else None,
        "price": pd.to_numeric(df.get(price), errors="coerce") if price else None,
    })
    out["type"] = "single_name"
    return out

def normalize_index(df):
    cols = _cols(df)
    name  = _pick(cols,"index","name","series")
    ccy   = _pick(cols,"ccy","currency")
    tenor = _pick(cols,"tenor")
    spr   = _pick(cols,"par spread (bps)","spread (bps)","spread")
    price = _pick(cols,"price","clean price","price (%)")
    if not name or (not spr and not price):
        return pd.DataFrame(columns=OUT_FIELDS)
    out = pd.DataFrame({
        "entity": df.get(name),
        "ticker": df.get(name),
        "currency": df.get(ccy),
        "tenor": df.get(tenor) if tenor else "5Y",
        "doc_clause": None,
        "spread_bps": pd.to_numeric(df.get(spr), errors="coerce") if spr else None,
        "price": pd.to_numeric(df.get(price), errors="coerce") if price else None,
    })
    out["type"] = "index"
    return out

def main(out_csv, rawdir):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    os.makedirs(rawdir, exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)
    today = datetime.utcnow().date().isoformat()
    s = new_session()
    frames, errs = [], []

    for key, rid in REPORTS.items():
        df = None
        # probiere mehrere Basen, vermeide die falsche /report/-URL
        for base in BASES:
            try:
                page = base + rid
                tmp = csv_from_page(s, page)
                if tmp is not None and not tmp.empty:
                    df = tmp; break
            except requests.HTTPError as e:
                errs.append({"source": key, "reason": "http", "msg": str(e)})
            except Exception as e:
                errs.append({"source": key, "reason": "exception", "msg": str(e)})
        if df is None or df.empty:
            errs.append({"source": key, "reason": "no_data"})
            continue

        raw_path = os.path.join(rawdir, f"{today}_{key}.csv")
        df.to_csv(raw_path, index=False)

        nf = normalize_single(df) if key == "single_names" else normalize_index(df)
        if nf.empty:
            errs.append({"source": key, "reason": "normalize_empty"})
        else:
            frames.append(nf)

    if not frames:
        rep = {"date": today, "rows": 0, "errors": errs}
        with open("data/reports/ice_cds_errors.json","w",encoding="utf-8") as f: json.dump(rep, f, indent=2)
        print(json.dumps(rep, indent=2)); return 0

    data = pd.concat(frames, ignore_index=True)
    data["date"] = today
    data = data[OUT_FIELDS]
    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        old = pd.read_csv(out_csv)
        data = pd.concat([old, data], ignore_index=True).drop_duplicates(subset=["date","type","entity"], keep="last")

    data.to_csv(out_csv, index=False)
    rep = {"date": today, "rows": int(data.shape[0]), "errors": errs}
    with open("data/reports/ice_cds_errors.json","w",encoding="utf-8") as f: json.dump(rep, f, indent=2)
    print(f"ICE CDS snapshot written: {out_csv}, rows={data.shape[0]}")
    return 0

if __name__ == "__main__":
    out = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/cds_eod.csv"
    raw = sys.argv[sys.argv.index("--rawdir")+1] if "--rawdir" in sys.argv else "data/cds"
    raise SystemExit(main(out, raw))
