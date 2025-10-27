#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch COT history from CFTC (robust, multiple fallbacks) and build a compact summary.

Outputs
- data/processed/cot_summary.csv    (tiny, kept in git)
- data/processed/cot.csv.gz         (compressed full table; excluded from git)
- data/reports/cot_report.json      (counts + errors)
Raw cache (optional):
- data/cot/raw/*.zip
"""

import os, io, json, gzip, csv, re, sys, time
from datetime import datetime
from typing import List, Tuple
import pandas as pd
import requests
from zipfile import ZipFile

# ---------------- utils ----------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "cot-fetch/1.0"})

def ensure_dirs():
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports", exist_ok=True)
    os.makedirs("data/cot/raw", exist_ok=True)

def ylist(start=2006) -> List[int]:
    y0 = int(os.getenv("COT_START_YEAR", start))
    y1 = datetime.utcnow().year
    return list(range(y0, y1 + 1))

def try_download(url: str) -> bytes:
    r = SESSION.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} for {url}")
    return r.content

def first_member(z: ZipFile, patterns: Tuple[str, ...]) -> str:
    names = z.namelist()
    for p in patterns:
        for n in names:
            if re.search(p, n, re.IGNORECASE):
                return n
    # fallback: erste Textdatei
    for n in names:
        if n.lower().endswith(".txt"):
            return n
    raise RuntimeError("no text file in zip")

def parse_legacy_txt(text: str) -> pd.DataFrame:
    """
    CFTC text files are pipe '|' or comma separated, with header lines.
    We normalize to CSV by replacing multiple spaces and splitting on comma or pipe.
    """
    lines = []
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        # skip obvious header separators
        if ln.lower().startswith("as of") or ln.lower().startswith("legacy") or ln.lower().startswith("disaggregated"):
            continue
        lines.append(ln)

    # detect delimiter
    sample = "|".join(lines[:5])
    delim = "|" if "|" in sample else ","

    # read with pandas
    df = pd.read_csv(io.StringIO("\n".join(lines)), sep=delim, engine="python", dtype=str)
    # unify column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Common columns across legacy/disaggregated often include:
    # market_and_exchange_names, as_of_date_in_form_yyyymmdd, open_interest_all,
    # noncomm_positions_long_all, noncomm_positions_short_all,
    # comm_positions_long_all, comm_positions_short_all, 
    # (or disaggregated equivalents: prod_merchant_long, money_mgr_long, etc.)
    cols = {c: c for c in df.columns}

    # Map potential variants
    date_col = next((c for c in df.columns if "as_of_date" in c or c == "report_date_as_yyyymmdd"), None)
    name_col = next((c for c in df.columns if "market_and_exchange" in c), None)
    if not date_col or not name_col:
        # sometimes 'market_and_exchange_names' is 'market_and_exchange_name'
        name_col = name_col or next((c for c in df.columns if "market_and_exchange_name" in c), None)
    if not date_col or not name_col:
        raise RuntimeError("cannot find date/name columns")

    out = pd.DataFrame()
    out["date"]  = pd.to_datetime(df[date_col].astype(str), errors="coerce")
    out["market"] = df[name_col].astype(str)

    def num(col_candidates: List[str]) -> pd.Series:
        for cc in col_candidates:
            if cc in df.columns:
                return pd.to_numeric(df[cc].astype(str).str.replace("[^0-9\\-\\.]", "", regex=True), errors="coerce")
        return pd.Series([pd.NA]*len(df))

    # Non-commercial long/short (legacy) or use disaggregated approximations (managed money)
    out["noncomm_long"]  = num([
        "noncomm_positions_long_all",
        "m_money_mgr_long_all", "money_manager_long_all"
    ])
    out["noncomm_short"] = num([
        "noncomm_positions_short_all",
        "m_money_mgr_short_all", "money_manager_short_all"
    ])
    out["comm_long"]     = num([
        "comm_positions_long_all",
        "prod_merc_long_all","producer_merchant_long_all"
    ])
    out["comm_short"]    = num([
        "comm_positions_short_all",
        "prod_merc_short_all","producer_merchant_short_all"
    ])
    return out

# -------------- main fetch --------------
def fetch_all() -> Tuple[pd.DataFrame, list]:
    errs = []
    frames = []

    years = ylist(2006)

    # Modern disaggregated annual zips (pattern tried first)
    # Examples historically used by CFTC; we try multiple patterns.
    patt_disagg = [
        "https://www.cftc.gov/files/dea/history/deacotdisagg_txt_{Y}.zip",
        "https://www.cftc.gov/files/dea/history/deacotdisagg_{Y}.zip",
        "https://www.cftc.gov/dea/newcot/deacotdisagg_txt_{Y}.zip",
    ]
    # Legacy futures+options history (pre-2006 exist as single-year zips)
    patt_legacy = [
        "https://www.cftc.gov/files/dea/history/deahistfo_{Y}.zip",
        "https://www.cftc.gov/dea/history/deahistfo_{Y}.zip",
    ]

    for Y in years:
        got = False
        for patt in patt_disagg + patt_legacy:
            url = patt.format(Y=Y)
            try:
                raw = try_download(url)
                raw_path = f"data/cot/raw/{os.path.basename(url)}"
                with open(raw_path, "wb") as f:
                    f.write(raw)
                with ZipFile(io.BytesIO(raw)) as z:
                    member = first_member(z, patterns=(r"txt$", r".*\\.txt$"))
                    text = z.read(member).decode("latin-1", errors="ignore")
                    df0 = parse_legacy_txt(text)
                    dfN = normalize(df0)
                    dfN["year_src"] = Y
                    frames.append(dfN)
                    got = True
                break
            except Exception as e:
                errs.append({"year": Y, "url": url, "msg": str(e)})
        if not got:
            # do not abort whole run—continue
            continue

    if not frames:
        return pd.DataFrame(), errs

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["date", "market"])
    df = df.sort_values(["market", "date"])
    return df, errs

def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    # letzte Woche pro Markt + Kernzahlen
    ix = df.groupby("market")["date"].idxmax()
    last = df.loc[ix, ["market", "date", "noncomm_long","noncomm_short","comm_long","comm_short"]].copy()
    last["noncomm_net"] = last["noncomm_long"].fillna(0) - last["noncomm_short"].fillna(0)
    last["comm_net"]    = last["comm_long"].fillna(0)    - last["comm_short"].fillna(0)
    last = last.sort_values("market").reset_index(drop=True)
    return last

def main():
    ensure_dirs()

    df, errs = fetch_all()

    if df.empty:
        # still write empty summary & report
        pd.DataFrame(columns=["market","date","noncomm_long","noncomm_short","comm_long","comm_short","noncomm_net","comm_net"])\
          .to_csv("data/processed/cot_summary.csv", index=False)
        report = {"ts": datetime.utcnow().isoformat()+"Z", "rows": 0, "errors": errs}
        with open("data/reports/cot_report.json","w") as f: json.dump(report, f, indent=2)
        print("no COT data fetched.")
        return 0

    # write full compressed
    full_out = "data/processed/cot.csv.gz"
    with gzip.open(full_out, "wt", encoding="utf-8") as gz:
        df.to_csv(gz, index=False)
    print("wrote", full_out, "rows=", len(df))

    # summary
    sm = build_summary(df)
    sm.to_csv("data/processed/cot_summary.csv", index=False)
    print("wrote data/processed/cot_summary.csv rows=", len(sm))

    # report
    report = {
        "ts": datetime.utcnow().isoformat()+"Z",
        "rows_full": int(len(df)),
        "rows_summary": int(len(sm)),
        "years": list(sorted(df["date"].dt.year.unique().tolist())),
        "errors": errs[:50],
    }
    with open("data/reports/cot_report.json","w") as f:
        json.dump(report, f, indent=2)

    return 0

if __name__ == "__main__":
    sys.exit(main())
