#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CDS-Proxy v2 (US + EU)
- Liest:
  • data/processed/fundamentals_core.csv     (Leverage, Größe, Beta, Margen)
  • data/processed/fred_oas.csv              (US OAS: IG/HY; EU OAS: ICE BofA Euro IG/HY falls vorhanden)
  • data/processed/fx_quotes.csv             (optional: HV20 von DE/Xetra via stooq, wenn vorhanden)
  • data/macro/ecb/ciss_ea.csv (optional), ciss_us.csv (optional) — nur als Zusatz-Risiko
- Liefert: data/processed/cds_proxy.csv + data/reports/cds_proxy_report.json

Formel (heuristisch, robust bei Datenlücken):
  base_oas = 
      if country in EU → euro_ig_oas (oder ersatz: us_ig_oas * 0.9)
      else              → us_ig_oas
  lev_adj  = f(debt_to_equity, net_debt, ev_ebitda) → 0 … +350 bp
  size_adj = f(market_cap via fundamentals?) not available → fallback shares_out → proxy_size
  vol_adj  = f(beta, HV20?) → 0 … +150 bp
  ciss_adj = small addon (0 … +50 bp), wenn CISS stark erhöht

  proxy_spread_bps = clamp(base_oas + lev_adj + size_adj + vol_adj + ciss_adj, 30, 1500)

Hinweis: Das ist kein echtes CDS, sondern ein Spread-Proxy für relative Risiko-Einschätzung.
"""

import os, json, math, pandas as pd

OUT = "data/processed/cds_proxy.csv"
REP = "data/reports/cds_proxy_report.json"
os.makedirs(os.path.dirname(OUT), exist_ok=True)
os.makedirs(os.path.dirname(REP), exist_ok=True)

def read_csv(path):
    if not os.path.exists(path) or os.path.getsize(path)==0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def latest_value(df, col_val="value"):
    if df.empty or col_val not in df.columns:
        return None
    # versuche Spalte date oder PERIOD
    if "date" in df.columns:
        df = df.sort_values("date")
    elif "DATE" in df.columns:
        df = df.sort_values("DATE")
    return pd.to_numeric(df[col_val], errors="coerce").dropna().tail(1).values[0] if col_val in df else None

def clip(v, lo, hi):
    if v is None or math.isnan(v): return None
    return max(lo, min(hi, v))

def nz(v, d=0.0):
    return d if v is None or (isinstance(v, float) and math.isnan(v)) else v

def main():
    report = {"inputs":{}, "notes":[], "rows":0}
    f_fund = "data/processed/fundamentals_core.csv"
    f_oas  = "data/processed/fred_oas.csv"
    f_fx   = "data/processed/fx_quotes.csv"
    f_ciss_ea = "data/macro/ecb/ciss_ea.csv"
    f_ciss_us = "data/macro/ecb/ciss_us.csv"

    dfF = read_csv(f_fund)
    dfO = read_csv(f_oas)
    dfX = read_csv(f_fx)
    dfC_ea = read_csv(f_ciss_ea)
    dfC_us = read_csv(f_ciss_us)

    report["inputs"]["fundamentals_core"] = len(dfF)
    report["inputs"]["fred_oas"]          = len(dfO)
    report["inputs"]["fx_quotes"]         = len(dfX)
    report["inputs"]["ciss_ea"]           = len(dfC_ea)
    report["inputs"]["ciss_us"]           = len(dfC_us)

    # OAS-Level ziehen
    # Erwartete Felder in fred_oas.csv: series, date, value
    def series_last(series_id):
        if dfO.empty: return None
        d = dfO[dfO["series"]==series_id]
        if d.empty: return None
        d = d.sort_values("date")
        return pd.to_numeric(d["value"], errors="coerce").dropna().tail(1).values[0] if "value" in d else None

    us_ig = series_last("US_IG_OAS")     # deine fetch_fred_oas.py sollte diese Alias vergeben
    us_hy = series_last("US_HY_OAS")
    eu_ig = series_last("EU_IG_OAS")     # Euro IG (falls vorhanden)
    eu_hy = series_last("EU_HY_OAS")

    if eu_ig is None:
        report["notes"].append("EU_IG_OAS fehlt – fallback 0.9 * US_IG_OAS")
        eu_ig = nz(us_ig, 150) * 0.9
    if us_ig is None:
        report["notes"].append("US_IG_OAS fehlt – setze 150bp als Fallback")
        us_ig = 150.0

    # CISS leichte Addons
    ciss_ea_last = latest_value(dfC_ea, "OBS_VALUE") if not dfC_ea.empty else None
    ciss_us_last = latest_value(dfC_us, "OBS_VALUE") if not dfC_us.empty else None
    # Scale: 0..1 → 0..50bp
    def ciss_to_bps(x):
        if x is None: return 0.0
        return clip(float(x), 0.0, 1.0) * 50.0

    rows = []
    if not dfF.empty:
        for _, r in dfF.iterrows():
            sym = str(r.get("symbol",""))
            cn  = str(r.get("country","")).upper()
            beta = r.get("beta")
            dte  = r.get("debt_to_equity")
            ndebt = r.get("net_debt")
            ev_ebitda = r.get("ev_ebitda")

            # Base OAS
            if cn in ("DE","FR","NL","IT","ES","SE","FI","DK","IE","AT","BE","PT","PL","CZ","HU","NO","CH","GB"):
                base = eu_ig
                ciss_add = ciss_to_bps(ciss_ea_last)
            else:
                base = us_ig
                ciss_add = ciss_to_bps(ciss_us_last)

            # Leverage-Adjust (sehr grob/heuristisch)
            lev_add = 0.0
            if pd.notna(dte):
                if dte <= 50:    lev_add += 0
                elif dte <= 100: lev_add += 25
                elif dte <= 150: lev_add += 50
                elif dte <= 250: lev_add += 100
                else:            lev_add += 180
            if pd.notna(ev_ebitda):
                # EV/EBITDA hoch → eher teuer/gehebelt; invertiert adjusten
                if ev_ebitda >= 20: lev_add += 100
                elif ev_ebitda >= 12: lev_add += 50

            if pd.notna(ndebt) and ndebt > 0:
                # absolutes NetDebt leicht berücksichtigen (Skalierung niedrig halten)
                lev_add += min(100.0, math.log10(max(1.0, float(ndebt))) * 5.0)

            # Vol-Adjust
            vol_add = 0.0
            if pd.notna(beta):
                if beta >= 1.6: vol_add += 120
                elif beta >= 1.3: vol_add += 70
                elif beta >= 1.1: vol_add += 40
                elif beta <= 0.7: vol_add -= 20

            # Falls HV20 aus fx_quotes.csv (Close-Vol) vorhanden → Bonus/Addon
            hv_add = 0.0
            if not dfX.empty and "symbol" in dfX.columns and "last_close" in dfX.columns:
                # (wir haben hier keine HV20-Spalte; wenn du später HV20 berechnest, nutze sie hier)
                pass

            proxy = base + lev_add + vol_add + hv_add + ciss_add
            proxy = clip(proxy, 30.0, 1500.0)

            rows.append({
                "symbol": sym,
                "country": cn,
                "base_oas_bps": round(base, 2) if base is not None else None,
                "lev_adj_bps": round(lev_add, 2),
                "vol_adj_bps": round(vol_add, 2),
                "ciss_adj_bps": round(ciss_add, 2),
                "proxy_spread_bps": round(proxy, 2)
            })

    dfOut = pd.DataFrame(rows, columns=[
        "symbol","country","base_oas_bps","lev_adj_bps","vol_adj_bps","ciss_adj_bps","proxy_spread_bps"
    ])
    dfOut.to_csv(OUT, index=False, encoding="utf-8")
    report["rows"] = int(len(dfOut))
    with open(REP, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    main()
