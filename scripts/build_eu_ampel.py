#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, pandas as pd
from datetime import datetime

BASE="data/reports/eu_checks"; os.makedirs(BASE, exist_ok=True)

def write_txt(path, lines):
    with open(path,"w",encoding="utf-8") as f:
        for ln in lines: f.write(ln+"\n")

def main():
    summary = {"ts": datetime.utcnow().isoformat()+"Z"}

    # --- ECB preview ---
    ecb_dir="data/macro/ecb"
    ecb_files=[f for f in os.listdir(ecb_dir)] if os.path.isdir(ecb_dir) else []
    ecb_preview=[]
    for fn in sorted(ecb_files)[:10]:
        df=pd.read_csv(os.path.join(ecb_dir,fn))
        last = "" if df.empty else f"{df.iloc[-1]['date']},{df.iloc[-1]['value']}"
        ecb_preview.append(f"{fn},{last}")
    write_txt(os.path.join(BASE,"ecb_preview.txt"), ["file,last_date,last_value"]+ecb_preview)
    summary["ecb_files"]=len(ecb_files)

    # --- Stooq preview (EU Symbole aus Watchlist) ---
    wl = []
    wlf = "watchlists/mylist.txt"
    if os.path.exists(wlf):
        wl=[l.strip() for l in open(wlf,encoding="utf-8") if l.strip() and not l.startswith("#")]
    EU_SUFFIX = (".DE",".PA",".AS",".MC",".MI",".BR",".WA",".PR",".VI",".LS",".BE",".SW",".HE",".CO",".OL",".ST",".FR",".IR",".NL",".PT",".PL",".CZ",".HU",".AT",".FI",".DK",".IE",".NO",".SE",".CH",".GB")
    eu_syms=[s for s in wl if s.upper().endswith(EU_SUFFIX)]
    stq_lines=["symbol,last_date,rows"]
    for s in eu_syms[:30]:
        p=os.path.join("data/market/stooq", f"{s.lower().replace('^','idx_')}.csv")
        if os.path.exists(p):
            df=pd.read_csv(p); last=df.iloc[-1]["date"] if not df.empty else ""
            stq_lines.append(f"{s},{last},{len(df)}")
        else:
            stq_lines.append(f"{s},,0")
    write_txt(os.path.join(BASE,"stooq_preview.txt"), stq_lines)
    summary["stooq_eu_symbols"]=len(eu_syms)

    # --- CDS proxy preview (EU only) ---
    cds = "data/processed/cds_proxy.csv"
    cds_lines=["symbol,region,proxy_spread"]
    if os.path.exists(cds):
        df=pd.read_csv(cds)
        df=df[df["region"]=="EU"].head(30)
        for _,r in df.iterrows():
            cds_lines.append(f"{r['symbol']},{r['region']},{r['proxy_spread']}")
    write_txt(os.path.join(BASE,"cds_proxy_preview.txt"), cds_lines)

    json.dump(summary, open(os.path.join(BASE,"summary.json"),"w"), indent=2)
    print(json.dumps(summary, indent=2))

if __name__=="__main__": main()
