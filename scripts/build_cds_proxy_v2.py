# scripts/build_cds_proxy_v2.py
# -*- coding: utf-8 -*-
"""
CDS-Proxy je Symbol mit IG/HY-Scoring & dynamischem EU_HY-Fallback.

Inputs:
  - --watchlist watchlists/mylist.txt  (oder .csv mit Spalte 'symbol')
  - --fred-oas  data/processed/fred_oas.csv  (date,series_id,value,bucket,region)
  - --fundamentals data/processed/fundamentals_core.csv
      benötigte Felder (falls vorhanden):
        symbol, country, market_cap, debt_to_equity, net_margin, oper_margin, beta

Heuristischer IG/HY-Score (0..5):
  size      : market_cap >= size_min
  leverage  : debt_to_equity <= dte_max
  profit    : net_margin >= nm_min
  margin    : oper_margin >= om_min
  beta      : beta <= beta_max
IG wenn score >= score_cut.

EU_HY:
  - wenn EU_HY in fred_oas fehlt:
      skaliert = US_HY * (EU_IG/US_IG)^alpha          (wenn EU_IG & US_IG da sind)
      sonst     = US_HY + eu_hy_premium

Output:
  data/processed/cds_proxy.csv (symbol,region,proxy_spread)
  data/reports/eu_checks/cds_proxy_preview.txt
  data/reports/cds_proxy_report.json
"""

import argparse, csv, json, math, os
from typing import Optional, Dict
import pandas as pd

DEF_WATCHLIST = "watchlists/mylist.txt"
DEF_FRED_OAS = "data/processed/fred_oas.csv"
DEF_FUNDS = "data/processed/fundamentals_core.csv"

OUT_CSV = "data/processed/cds_proxy.csv"
PREVIEW_TXT = "data/reports/eu_checks/cds_proxy_preview.txt"
REPORT_JSON = "data/reports/cds_proxy_report.json"

EU_SUFFIXES = {
    ".DE",".PA",".AS",".MI",".BR",".MC",".BE",".SW",".OL",".ST",".HE",".CO",
    ".IR",".LS",".WA",".VI",".PR",".VX",".L"
}

def ensure_dirs():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(PREVIEW_TXT), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)

def read_watchlist(path: str) -> pd.Series:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Watchlist nicht gefunden: {path}")
    if path.lower().endswith(".csv"):
        df = pd.read_csv(path)
        col = "symbol" if "symbol" in df.columns else df.columns[0]
        s = df[col].astype(str).str.strip()
    else:
        vals = []
        for ln in open(path, "r", encoding="utf-8"):
            t = ln.strip()
            if not t or t.startswith("#") or t.lower().startswith("symbol"):
                continue
            vals.append(t.split(",")[0].strip())
        s = pd.Series(vals, name="symbol")
    return s[s!=""].drop_duplicates().reset_index(drop=True)

def load_fred_oas(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"FRED-OAS fehlt: {path}")
    df = pd.read_csv(path)
    for c in ("region","bucket","series_id"):
        if c in df.columns: df[c] = df[c].astype(str)
    if "value" in df.columns: df["value"] = pd.to_numeric(df["value"], errors="coerce")
    if "date" in df.columns:  df = df.sort_values("date")
    last = df.groupby(["region","bucket"], as_index=False).tail(1)
    return last.reset_index(drop=True)

def load_fundamentals(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        # leeres Schema – Classifier fällt auf defaults zurück
        return pd.DataFrame(columns=["symbol","country","market_cap","debt_to_equity","net_margin","oper_margin","beta"])
    df = pd.read_csv(path)
    # Normalize colnames
    df.columns = [c.lower() for c in df.columns]
    if "symbol" not in df.columns:
        df = df.rename(columns={df.columns[0]:"symbol"})
    for num in ("market_cap","debt_to_equity","net_margin","oper_margin","beta"):
        if num in df.columns:
            df[num] = pd.to_numeric(df[num], errors="coerce")
    if "country" not in df.columns:
        df["country"] = None
    return df.drop_duplicates(subset=["symbol"], keep="last")

def infer_region(symbol: str, country: Optional[str]) -> str:
    s = symbol.upper()
    if any(s.endswith(suf) for suf in EU_SUFFIXES): return "EU"
    if country:
        c = str(country).strip().upper()
        if c and c not in ("US","USA","UNITED STATES","UNITED-STATES"): return "EU"
    return "US"

def score_ig(row: pd.Series, p: argparse.Namespace) -> int:
    """Gibt Punkte 0..5 zurück."""
    pts = 0
    mc  = row.get("market_cap", float("nan"))
    dte = row.get("debt_to_equity", float("nan"))
    nm  = row.get("net_margin", float("nan"))
    om  = row.get("oper_margin", float("nan"))
    bt  = row.get("beta", float("nan"))

    if pd.notna(mc)  and mc  >= p.size_min:     pts += 1
    if pd.isna(dte)  or dte <= p.dte_max:       pts += 1
    if pd.isna(nm)   or nm  >= p.nm_min:        pts += 1
    if pd.notna(om)  and om  >= p.om_min:       pts += 1
    if pd.isna(bt)   or bt  <= p.beta_max:      pts += 1
    return pts

def pick_eu_hy_from_scaling(oas: pd.DataFrame, alpha: float, premium: float) -> Optional[float]:
    def _get(reg, buck):
        m = oas[(oas["region"]==reg)&(oas["bucket"]==buck)]
        return float(m["value"].iloc[-1]) if len(m) and pd.notna(m["value"].iloc[-1]) else None
    us_hy = _get("US","HY")
    eu_ig = _get("EU","IG")
    us_ig = _get("US","IG")
    if us_hy is not None and eu_ig is not None and us_ig is not None and us_ig>0:
        scale = (eu_ig / us_ig) ** float(alpha)
        return us_hy * scale
    return None if us_hy is None else us_hy + premium

def pick_oas(oas: pd.DataFrame, region: str, bucket: str, eu_hy_value: Optional[float]) -> Optional[float]:
    m = oas[(oas["region"]==region) & (oas["bucket"]==bucket)]
    if len(m):
        v = m["value"].iloc[-1]
        if pd.notna(v): return float(v)
    if region=="EU" and bucket=="HY" and eu_hy_value is not None:
        return float(eu_hy_value)
    # letzte Reserve: US_IG
    r = oas[(oas["region"]=="US")&(oas["bucket"]=="IG")]
    return float(r["value"].iloc[-1]) if len(r) and pd.notna(r["value"].iloc[-1]) else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default=DEF_WATCHLIST)
    ap.add_argument("--fred-oas", default=DEF_FRED_OAS)
    ap.add_argument("--fundamentals", default=DEF_FUNDS)

    # IG/HY-Scoring Parameter
    ap.add_argument("--size-min", type=float, default=5e9, help="Marktkap, IG-Punkt (Default 5 Mrd)")
    ap.add_argument("--dte-max", type=float, default=150.0, help="Debt/Equity, IG-Punkt")
    ap.add_argument("--nm-min",  type=float, default=0.00, help="Net Margin, IG-Punkt")
    ap.add_argument("--om-min",  type=float, default=0.08, help="Operative Marge, IG-Punkt")
    ap.add_argument("--beta-max",type=float, default=1.30, help="Beta, IG-Punkt")
    ap.add_argument("--score-cut",type=int,   default=3,    help=">= Punkte ⇒ IG (0..5)")

    # EU_HY Ermittlung
    ap.add_argument("--eu-hy-alpha", type=float, default=1.0, help="Skalierungsexponent (EU_IG/US_IG)^alpha")
    ap.add_argument("--eu-hy-premium", type=float, default=0.20, help="Fallback-Premium auf US_HY (pp)")

    args = ap.parse_args()
    ensure_dirs()

    syms = read_watchlist(args.watchlist)
    oas  = load_fred_oas(args.fred_oas)
    fdf  = load_fundamentals(args.fundamentals).set_index("symbol", drop=False)

    # dynamischer EU_HY-Wert (falls in OAS fehlt)
    eu_hy_value = None
    if not len(oas[(oas["region"]=="EU")&(oas["bucket"]=="HY")]):
        eu_hy_value = pick_eu_hy_from_scaling(oas, args.eu_hy_alpha, args.eu_hy_premium)

    out_rows, preview, errors = [], ["symbol,region,proxy_spread"], []
    for sym in syms:
        row = fdf.loc[sym] if sym in fdf.index else pd.Series(dtype="float64")
        region = infer_region(sym, row.get("country", None) if len(row) else None)

        # Score & Bucket
        pts = score_ig(row, args)
        bucket = "IG" if pts >= args.score_cut else "HY"

        val = pick_oas(oas, region, bucket, eu_hy_value)
        if val is None or pd.isna(val):
            errors.append({"symbol": sym, "reason": "no_oas_value"})
            preview.append(f"{sym},{region}_{bucket},nan")
            continue

        val = round(float(val), 2)
        out_rows.append({"symbol": sym, "region": f"{region}_{bucket}", "proxy_spread": val})
        preview.append(f"{sym},{region}_{bucket},{val}")

    # Outputs
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol","region","proxy_spread"])
        w.writeheader()
        for r in out_rows: w.writerow(r)

    with open(PREVIEW_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(preview) + "\n")

    fred_map: Dict[str, Optional[float]] = {}
    for reg in ("US","EU"):
        for b in ("IG","HY"):
            m = oas[(oas["region"]==reg)&(oas["bucket"]==b)]
            fred_map[f"{reg}_{b}"] = float(m["value"].iloc[-1]) if len(m) and pd.notna(m["value"].iloc[-1]) else None

    rep = {
        "ts": pd.Timestamp.utcnow().isoformat()+"Z",
        "rows": len(out_rows),
        "fred_oas_used": fred_map,
        "eu_hy_value": eu_hy_value,
        "params": {
            "size_min": args.size_min, "dte_max": args.dte_max, "nm_min": args.nm_min,
            "om_min": args.om_min, "beta_max": args.beta_max, "score_cut": args.score_cut,
            "eu_hy_alpha": args.eu_hy_alpha, "eu_hy_premium": args.eu_hy_premium
        },
        "errors": errors,
        "preview": OUT_CSV
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)

    # Konsolen-Short
    short = {k: rep[k] for k in ("ts","rows","fred_oas_used","eu_hy_value","preview")}
    print(json.dumps(short, indent=2))

if __name__ == "__main__":
    main()
