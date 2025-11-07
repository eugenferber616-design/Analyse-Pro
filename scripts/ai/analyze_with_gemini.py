#!/usr/bin/env python3
import os, json, gzip, pandas as pd
from pathlib import Path
from ai_client import get_gemini
from prompt_templates import SYSTEM, make_user_prompt

def load_csv(path):
    if path.endswith(".gz"): return pd.read_csv(path, compression="gzip")
    return pd.read_csv(path)

def short_snip(s, n=280): return (s or "")[:n]

def build_snippets(sym):
    out = {"funds":"","earn":"","opt":"","crd":"","hv":"","risk":""}
    try:
        f = load_csv("data/processed/fundamentals_core.csv")
        row = f[f.symbol==sym].iloc[0].to_dict() if (f.symbol==sym).any() else {}
        out["funds"] = short_snip(f"mcap={row.get('market_cap')}, beta={row.get('beta')}, "
                                  f"margins gross/oper/net={row.get('gross_margin')}/{row.get('oper_margin')}/{row.get('net_margin')}, "
                                  f"roe={row.get('roe_ttm')}")
    except: pass
    try:
        e = load_csv("data/processed/earnings_results.csv.gz")
        ee = e[e.symbol==sym].sort_values("period").tail(4)[["period","eps_actual","eps_estimate","surprise_pct"]]
        out["earn"] = short_snip(ee.to_csv(index=False))
    except: pass
    try:
        o = load_csv("data/processed/options_oi_by_strike.csv.gz") if Path("data/processed/options_oi_by_strike.csv.gz").exists() \
            else load_csv("data/processed/options_oi_by_strike.csv")
        oo = o[o.symbol==sym].groupby("expiry").agg({"call_oi":"sum","put_oi":"sum"}).tail(3)
        out["opt"] = short_snip(oo.to_csv())
    except: pass
    try:
        c = load_csv("data/processed/cds_proxy.csv")
        cc = c[c.symbol==sym].tail(1).to_dict("records")
        out["crd"] = short_snip(json.dumps(cc))
    except: pass
    try:
        h = load_csv("data/processed/hv_summary.csv.gz")
        hh = h[h.symbol==sym][["hv20","hv60"]].tail(1).to_dict("records")
        out["hv"] = short_snip(json.dumps(hh))
    except: pass
    try:
        rj = json.load(open("data/processed/riskindex_snapshot.json"))
        out["risk"] = short_snip(json.dumps(rj))
    except: pass
    return out

def main(watchlist="watchlists/mylist.txt", limit=0, model="gemini-1.5-flash"):
    syms = []
    with open(watchlist,"r",encoding="utf-8") as f:
        lines = [x.strip() for x in f if x.strip()]
        syms = lines[1:] if lines[0].lower().startswith("symbol") else lines
    if limit: syms = syms[:int(limit)]

    model = get_gemini(model)
    out_path = Path("data/processed/ai_analysis.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as w:
        for sym in syms:
            snips = build_snippets(sym)
            prompt = make_user_prompt(sym, snips)
            resp = model.generate_content([{"role":"user","parts":[SYSTEM]}, prompt])
            txt = resp.text.strip()
            # defensive: JSON extrahieren
            try:
                payload = json.loads(txt)
            except:
                start = txt.find("{"); end = txt.rfind("}")
                payload = json.loads(txt[start:end+1]) if start!=-1 else {"symbol":sym,"error":"no_json","raw":txt}
            payload["symbol"] = payload.get("symbol", sym)
            w.write(json.dumps(payload, ensure_ascii=False) + "\n")
            print("ok:", sym)

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="watchlists/mylist.txt")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--model", default="gemini-1.5-flash")
    args = ap.parse_args()
    main(args.watchlist, args.limit, args.model)
