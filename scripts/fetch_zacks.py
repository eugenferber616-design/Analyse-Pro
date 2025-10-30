#!/usr/bin/env python3
import os, re, time, json, csv, hashlib, argparse
import requests
from bs4 import BeautifulSoup

def read_symbols(path: str) -> list[str]:
    syms = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.split("#", 1)[0].strip()
            if not ln: continue
            # CSV erste Spalte erlauben
            if "," in ln: ln = ln.split(",", 1)[0].strip()
            # Nur "Wort" bis Whitespace
            ln = ln.split()[0]
            syms.append(ln.upper())
    # Duplikate raus
    return sorted(set(syms))

def load_yaml(p: str) -> dict:
    import yaml
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_text_from_url(url: str, session: requests.Session) -> tuple[str, str]:
    r = session.get(url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    # sichtbarer Text
    txt = soup.get_text(" ", strip=True)
    return txt, r.url

def clean_text(txt: str, remove_patterns: list[str]) -> str:
    for pat in remove_patterns or []:
        txt = re.sub(pat, "", txt, flags=re.I | re.M)
    # Mehrfach-Spaces normieren
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    return txt

def extract_fields(txt: str, capture_cfg: dict) -> dict:
    out = {}
    for field, spec in (capture_cfg or {}).items():
        pat = spec.get("regex")
        if not pat: 
            continue
        m = re.search(pat, txt, flags=re.I | re.S)
        if m:
            # nimm 1. Gruppe wenn vorhanden, sonst Match
            out[field] = (m.group(1) if m.lastindex else m.group(0)).strip()
    return out

def maybe_translate_en_to_de(text: str) -> str | None:
    # aktiviere Übersetzung nur, wenn ENV TRANSLATE=1 gesetzt ist
    if not text or os.getenv("TRANSLATE", "0") != "1":
        return None
    try:
        from transformers import MarianMTModel, MarianTokenizer
        model_name = os.getenv("MARIAN_MODEL", "Helsinki-NLP/opus-mt-en-de")
        tok = MarianTokenizer.from_pretrained(model_name)
        mdl = MarianMTModel.from_pretrained(model_name)
        chunks = [text[i:i+800] for i in range(0, len(text), 800)]
        outs = []
        for ch in chunks:
            enc = tok([ch], return_tensors="pt", truncation=True, max_length=900)
            gen = mdl.generate(**enc)
            outs.append(tok.decode(gen[0], skip_special_tokens=True))
        return " ".join(outs)
    except Exception as e:
        print("[translate] skip:", e)
        return None

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="watchlists/mylist.txt")
    ap.add_argument("--config", default="config/zacks_scraper.yaml")
    ap.add_argument("--out", default="data/processed/zacks_summaries.jsonl")
    ap.add_argument("--csv", default="data/processed/zacks_summaries.csv")
    ap.add_argument("--log", default="data/reports/zacks_log.json")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    os.makedirs(os.path.dirname(args.csv), exist_ok=True)
    os.makedirs(os.path.dirname(args.log), exist_ok=True)

    cfg = load_yaml(args.config)
    targets = cfg.get("targets", [])
    remove_patterns = cfg.get("remove_patterns", [])
    rate_ms = int(cfg.get("rate_ms", 900))

    syms = read_symbols(args.watchlist)
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; AnalyseProBot/1.0)"
    })

    rows_jsonl = []
    rows_csv = []
    errs = []

    for i, sym in enumerate(syms, 1):
        row_acc = {"symbol": sym}
        primary_url = None
        try:
            for t in targets:
                url = t["url"].format(symbol=sym)
                if primary_url is None:
                    primary_url = url
                txt, final_url = get_text_from_url(url, session)
                fields = extract_fields(txt, t.get("capture", {}))
                # Earnings Summary – vorher säubern
                if "earnings_summary" in fields:
                    fields["earnings_summary"] = clean_text(fields["earnings_summary"], remove_patterns)
                row_acc.update(fields)
                time.sleep(rate_ms/1000.0)
            en = row_acc.get("earnings_summary", "")
            de = maybe_translate_en_to_de(en)
            h  = sha1(en or "")
            out_obj = {
                "symbol": sym,
                "source_url": primary_url,
                "opinion_en": en or None,
                "opinion_de": de,
                "hash": h,
                "asof": time.strftime("%Y-%m-%d"),
                # ein paar strukturierte Felder mitgeben (wenn gefunden)
                "zacks_rank": row_acc.get("zacks_rank"),
                "value_score": row_acc.get("value_score"),
                "growth_score": row_acc.get("growth_score"),
                "momentum_score": row_acc.get("momentum_score"),
                "earnings_date_hint": row_acc.get("earnings_date_hint"),
            }
            rows_jsonl.append(out_obj)
            rows_csv.append({
                "symbol": sym,
                "opinion_en": (en[:400] + "…") if en and len(en) > 400 else en,
                "opinion_de": (de[:400] + "…") if de and len(de) > 400 else de,
                "zacks_rank": row_acc.get("zacks_rank"),
                "value_score": row_acc.get("value_score"),
                "growth_score": row_acc.get("growth_score"),
                "momentum_score": row_acc.get("momentum_score"),
                "source_url": primary_url
            })
        except Exception as e:
            errs.append({"symbol": sym, "error": str(e)})

        if i % 25 == 0:
            print(f"[zacks] {i}/{len(syms)}")

    # JSONL schreiben
    with open(args.out, "w", encoding="utf-8") as f:
        for obj in rows_jsonl:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # CSV Vorschau
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows_csv[0].keys()) if rows_csv else 
                           ["symbol","opinion_en","opinion_de","zacks_rank","value_score","growth_score","momentum_score","source_url"])
        w.writeheader(); w.writerows(rows_csv)

    # Log
    with open(args.log, "w", encoding="utf-8") as f:
        json.dump({"total": len(syms), "ok": len(rows_jsonl), "errors": errs}, f, indent=2)
    print(f"[zacks] done: OK={len(rows_jsonl)} / {len(syms)}  → {args.out}, {args.csv}")

if __name__ == "__main__":
    main()
