import os, sys, time, json, pandas as pd
from datetime import datetime
import requests

ICE_SOURCES = {
    # öffentlich sichtbare Tabellen; falls ICE die Pfade ändert, hier anpassen
    "single_names": "https://www.theice.com/marketdata/reports/180",  # Single-Name 5Y Tabelle
    "indices":      "https://www.theice.com/marketdata/reports/181",  # CDX / iTraxx Indizes
}

OUT_FIELDS = ["date","type","entity","ticker","currency","tenor","doc_clause","spread_bps","price"]

def read_html_table(url: str) -> pd.DataFrame:
    # robust: erst requests, dann pandas.read_html auf Content
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    tables = pd.read_html(r.text, flavor="bs4")
    if not tables:
        return pd.DataFrame()
    # nimm die größte Tabelle
    df = max(tables, key=lambda t: t.shape[0] * max(1, t.shape[1]))
    return df

def normalize_single_names(df: pd.DataFrame) -> pd.DataFrame:
    # ICE benennt Spalten gern um; wir versuchen generisch zu mappen
    cols = {c.lower().strip(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in cols: return cols[c]
        return None

    name_col   = pick("reference entity","name","entity")
    ticker_col = pick("ticker","short name","ric")
    ccy_col    = pick("ccy","currency")
    tenor_col  = pick("tenor")
    doc_col    = pick("doc clause","doc", "docclause")
    spr_col    = pick("par spread (bps)","spread (bps)","spread","par spread")
    price_col  = pick("price","clean price","price (%)")

    if not name_col or (not spr_col and not price_col):
        return pd.DataFrame(columns=OUT_FIELDS)

    out = pd.DataFrame({
        "entity":   df.get(name_col),
        "ticker":   df.get(ticker_col),
        "currency": df.get(ccy_col),
        "tenor":    (df.get(tenor_col) or "5Y"),
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
        "entity":   df.get(name_col),
        "ticker":   df.get(name_col),   # bei Indizes gleichsetzen
        "currency": df.get(ccy_col),
        "tenor":    (df.get(tenor_col) or "5Y"),
        "doc_clause": None,
        "spread_bps": pd.to_numeric(df.get(spr_col), errors="coerce") if spr_col else None,
        "price":      pd.to_numeric(df.get(price_col), errors="coerce") if price_col else None,
    })
    out["type"] = "index"
    return out

def main(out_csv: str, raw_dir: str):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)

    today = datetime.utcnow().date().isoformat()
    frames = []
    errors = []

    for key, url in ICE_SOURCES.items():
        try:
            df = read_html_table(url)
            if df.empty:
                errors.append({"source": key, "reason": "no_table"})
                continue
            # raw dump der Tabelle
            raw_path = os.path.join(raw_dir, f"{today}_{key}.csv")
            df.to_csv(raw_path, index=False)

            if key == "single_names":
                nf = normalize_single_names(df)
            else:
                nf = normalize_indices(df)
            if nf.empty:
                errors.append({"source": key, "reason": "normalize_empty"})
                continue
            frames.append(nf)
        except Exception as e:
            errors.append({"source": key, "reason": "exception", "msg": str(e)})

    if not frames:
        print("No frames parsed from ICE.")
        return 0

    data = pd.concat(frames, ignore_index=True)
    data["date"] = today
    data = data[OUT_FIELDS].copy()

    # Append-Modus: bestehende Datei einlesen und anfügen, ohne Dubletten pro (date,type,entity)
    if os.path.exists(out_csv) and os.path.getsize(out_csv) > 0:
        old = pd.read_csv(out_csv)
        data = pd.concat([old, data], ignore_index=True)
        data.drop_duplicates(subset=["date","type","entity"], keep="last", inplace=True)

    data.to_csv(out_csv, index=False)

    # Error-Report
    rep = {"date": today, "rows": int(data.shape[0]), "errors": errors}
    with open("data/reports/ice_cds_errors.json","w",encoding="utf-8") as f:
        json.dump(rep, f, ensure_ascii=False, indent=2)

    print(f"ICE CDS snapshot written: {out_csv}, rows={data.shape[0]}")
    if errors:
        print("Notes:", errors)
    return 0

if __name__ == "__main__":
    out = sys.argv[sys.argv.index("--out")+1]    if "--out" in sys.argv else "data/processed/cds_eod.csv"
    raw = sys.argv[sys.argv.index("--rawdir")+1] if "--rawdir" in sys.argv else "data/cds"
    raise SystemExit(main(out, raw))
