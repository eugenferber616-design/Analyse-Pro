# scripts/build_cds_proxy_v2.py
# -*- coding: utf-8 -*-
"""
CDS-Proxy V2.1 (Robust Interpolation Model)
-------------------------------------------
Berechnet einen Risiko-Proxy basierend auf:
1. Markt-Level (FRED OAS Indizes für IG/HY)
2. Firmen-Score (Fundamentals + Volatilität)

Vorteil: Extrem stabil, keine Ausreißer, keine komplexen Mathe-Abstürze.
Ideal für RiskIndex Heatmaps.

Outputs:
  - data/processed/cds_proxy.csv
"""

from __future__ import annotations
import argparse, csv, json, os, gzip, math
from typing import Optional, Dict, Tuple
import pandas as pd

# Defaults
DEF_WATCHLIST = "watchlists/mylist.txt"
DEF_FRED_OAS  = "data/processed/fred_oas.csv"
DEF_FUNDS     = "data/processed/fundamentals_core.csv"
DEF_HV        = "data/processed/hv_summary.csv" # csv oder csv.gz

OUT_CSV      = "data/processed/cds_proxy.csv"
REPORT_JSON  = "data/reports/cds_proxy_report.json"

EU_SUFFIXES = {
    ".DE",".PA",".AS",".MI",".BR",".MC",".BE",".SW",".OL",".ST",".HE",".CO",
    ".IR",".LS",".WA",".VI",".PR",".VX",".L",".PL",".NL",".ES"
}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def ensure_dirs():
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)

def read_watchlist(path: str) -> list:
    if not os.path.exists(path): return []
    vals = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            t = ln.strip()
            # Ignoriere Kommentare und Header, splitte bei Komma (für Proxy-Liste)
            if not t or t.startswith("#") or t.lower().startswith("symbol"): continue
            vals.append(t.split(",")[0].strip().upper())
    return sorted(list(set(vals)))

def load_fred_oas(path: str) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    df = pd.read_csv(path)
    # Letzten Wert pro Region/Bucket holen
    if "date" in df.columns: df = df.sort_values("date")
    last = df.groupby(["region", "bucket"], as_index=False).tail(1)
    return last

def load_fundamentals(path: str) -> pd.DataFrame:
    if not os.path.exists(path): return pd.DataFrame()
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    cols = ["market_cap","debt_to_equity","net_margin","oper_margin","beta"]
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.drop_duplicates(subset=["symbol"], keep="last").set_index("symbol")

def load_hv_summary(path: str) -> Dict[str, float]:
    out = {}
    # Check for .csv or .csv.gz
    real_path = path
    if not os.path.exists(real_path) and os.path.exists(path + ".gz"): real_path += ".gz"
    
    if not os.path.exists(real_path): return out
    
    opener = gzip.open if real_path.endswith(".gz") else open
    try:
        # Text-Mode erzwingen für gzip
        mode = "rt" if real_path.endswith(".gz") else "r"
        with opener(real_path, mode, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sym = row.get("symbol", "").upper()
                hv60 = row.get("hv60", row.get("hv20", "0.25"))
                try: out[sym] = float(hv60)
                except: out[sym] = 0.25
    except: pass
    return out

def infer_region(symbol: str) -> str:
    if any(symbol.endswith(suf) for suf in EU_SUFFIXES): return "EU"
    return "US"

def get_oas_value(oas: pd.DataFrame, region: str, bucket: str) -> float:
    # Holt den aktuellen Spread aus FRED Daten
    row = oas[(oas["region"]==region) & (oas["bucket"]==bucket)]
    if not row.empty:
        val = row["value"].iloc[-1]
        if pd.notna(val): return float(val)
    
    # Fallbacks falls FRED Daten fehlen
    if region == "EU":
        # EU Fallback auf US Werte + Premium
        us_val = get_oas_value(oas, "US", bucket)
        return us_val * 1.1 if us_val else (1.5 if bucket=="IG" else 4.5)
    
    return 1.20 if bucket == "IG" else 4.00

def clamp(n, minn, maxn):
    return max(min(maxn, n), minn)

# ──────────────────────────────────────────────────────────────────────────────
# Main Logic (Interpolation)
# ──────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--watchlist", default=DEF_WATCHLIST)
    parser.add_argument("--fred-oas", default=DEF_FRED_OAS)
    parser.add_argument("--fundamentals", default=DEF_FUNDS)
    parser.add_argument("--hv", default=DEF_HV)
    
    # Dummy args für Kompatibilität mit Batch-Datei (falls dort Flags gesetzt sind)
    parser.add_argument("--eu-hy-alpha", type=float, default=1.0)
    parser.add_argument("--eu-hy-premium", type=float, default=0.20)
    parser.add_argument("--hv-anchor", type=float, default=0.25)
    parser.add_argument("--hv-min", type=float, default=0.85)
    parser.add_argument("--hv-max", type=float, default=1.15)
    
    args, unknown = parser.parse_known_args()
    
    ensure_dirs()
    print("Building CDS Proxy V2.1 (Robust Interpolation)...")

    # Load Data
    syms = read_watchlist(args.watchlist)
    oas_df = load_fred_oas(args.fred_oas)
    fund_df = load_fundamentals(args.fundamentals)
    hv_map = load_hv_summary(args.hv)

    out_rows = []
    
    for sym in syms:
        region = infer_region(sym)
        
        # 1. Fundamental Score (0 bis 5)
        # -----------------------------
        score = 0
        has_funda = False
        if sym in fund_df.index:
            row = fund_df.loc[sym]
            has_funda = True
            
            # Scoring Logik (Groß, Profitable, Wenig Schulden = Gut)
            if row.get("market_cap", 0) > 10e9: score += 1      # Size
            if row.get("net_margin", 0) > 0: score += 1         # Profit
            if row.get("oper_margin", 0) > 0.10: score += 1     # Quality
            if row.get("debt_to_equity", 200) < 150: score += 1 # Leverage
            if row.get("beta", 1.5) < 1.2: score += 1           # Volatility
        else:
            # Ohne Daten gehen wir vom Mittelmaß (High Yield Tendenz) aus
            score = 1 

        # 2. Risk Ratio (0.0 = Top IG, 1.0 = Junk HY)
        # -------------------------------------------
        # Wir invertieren den Score: 5 Punkte -> 0 Risk, 0 Punkte -> 1 Risk
        funda_risk = 1.0 - (score / 5.0)
        
        # HV Risk (Volatilität)
        # BUGFIX: HV kommt als Prozent (z.B. 22.13), nicht als Dezimal (0.2213)
        # Wir normieren auf 25% = neutral (1.0)
        hv_raw = hv_map.get(sym, 25.0)  # Default 25% Vol
        # Falls als Dezimal: umrechnen
        if hv_raw < 1.0:
            hv_raw = hv_raw * 100.0
        hv_risk_factor = clamp(hv_raw / 25.0, 0.7, 1.5)  # 25% = neutral
        
        # NEUE FORMEL: Mehr Gewicht auf Fundamentals, weniger auf Volatilität
        # Score 5/5 (funda_risk=0) -> base_risk = 0.0 -> Premium IG
        # Score 0/5 (funda_risk=1) -> base_risk = 1.0 -> Junk HY
        base_risk = funda_risk
        
        # HV als Multiplikator nur für mittlere Risiken
        # Top-Firmen (Score 5) bleiben bei ~0, schlechte Firmen werden volatiler
        total_risk = base_risk * hv_risk_factor
        
        # Begrenzen auf 0.0 bis 1.5 (über 1.0 = distressed)
        t = clamp(total_risk, 0.0, 1.5) 

        # 3. Interpolation mit Premium-Zone
        # ---------------------------------
        spread_ig = get_oas_value(oas_df, region, "IG")
        spread_hy = get_oas_value(oas_df, region, "HY")
        
        # NEUE LOGIK: Top-Firmen können UNTER dem IG-Durchschnitt liegen
        # t=0.0 -> 50% des IG-Spreads (Premium Investment Grade)
        # t=0.5 -> 100% IG-Spread
        # t=1.0 -> HY-Spread
        # t>1.0 -> Über HY (Distressed)
        if t <= 0.5:
            # Premium Zone: zwischen 50% IG und 100% IG
            proxy_spread = spread_ig * (0.5 + t)  # t=0 -> 0.5*IG, t=0.5 -> 1.0*IG
        else:
            # Normal Zone: zwischen IG und HY (und darüber)
            t_norm = (t - 0.5) / 0.5  # Normiere 0.5-1.0 auf 0-1
            if t <= 1.0:
                proxy_spread = spread_ig + t_norm * (spread_hy - spread_ig)
            else:
                # Distressed Zone: über HY
                t_dist = t - 1.0  # 0.0 - 0.5
                proxy_spread = spread_hy + t_dist * spread_hy  # +50% für t=1.5
        
        out_rows.append({
            "symbol": sym,
            "region": region,
            "proxy_spread": round(proxy_spread, 2)
        })

    # Save CSV
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "region", "proxy_spread"])
        w.writeheader()
        w.writerows(out_rows)

    print(f"[OK] CDS Proxy calculated for {len(out_rows)} symbols.")
    print(f"  Output: {OUT_CSV}")

    # Report
    rep = {
        "timestamp": pd.Timestamp.utcnow().isoformat(),
        "rows": len(out_rows),
        "model": "Robust Interpolation V2.1",
        "spreads_used": {
            "US_IG": get_oas_value(oas_df, "US", "IG"),
            "US_HY": get_oas_value(oas_df, "US", "HY"),
            "EU_IG": get_oas_value(oas_df, "EU", "IG")
        }
    }
    with open(REPORT_JSON, "w") as f:
        json.dump(rep, f, indent=2)

if __name__ == "__main__":
    main()
