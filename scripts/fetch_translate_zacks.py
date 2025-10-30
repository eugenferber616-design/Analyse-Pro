#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zacks Web-Fetcher + MarianMT Übersetzer (EN->DE) mit Diff-Filter.

Output (JSONL pro Zeile):
{ "symbol": "...", "source_url": "...", "opinion_en": "...", "opinion_de": "...", "asof": "2025-10-30T00:00:00Z" }

Konservativ:
- robots.txt respektieren
- Rate-Limit (schlaf ms), Backoff, ETag/Last-Modified Cache
- Hash-basierter Diff-Filter (nur Neue/Geänderte übersetzen)
"""
from __future__ import annotations
import os, re, csv, json, time, hashlib, argparse, typing as T
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import lxml
from dateutil import tz
from tqdm import tqdm
import syntok.segmenter as syntok

# --------------------- Config ---------------------
BASE = "https://www.zacks.com/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AnalyseProBot/0.1; +https://example.org/bot)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
OUT_JSONL = "data/processed/zacks_opinions.jsonl"
OUT_ERRORS = "data/reports/zacks_fetch_errors.json"
CACHE_DIR  = "data/cache/zacks"
RATE_MS    = int(os.getenv("ZACKS_SLEEP_MS", "1500"))  # 1.5 s default
RETRIES    = 2
TIMEOUT    = 25

# --------------------- Utils ---------------------
def ensure_dir(p: str | Path):
    Path(p).parent.mkdir(parents=True, exist_ok=True)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00","Z")

def read_watchlist(path: str) -> list[str]:
    p = Path(path)
    if not p.exists(): return []
    if p.suffix.lower() == ".csv":
        with p.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            col = None
            if rdr.fieldnames:
                for c in rdr.fieldnames:
                    if c.lower() in ("symbol","ticker"): col = c; break
                if col is None: col = rdr.fieldnames[0]
            out = []
            for row in rdr:
                t = (row.get(col,"") or "").split("#",1)[0].strip()
                if t and t.lower() not in ("symbol","ticker"): out.append(t.upper())
            return sorted(set(out))
    # txt
    out = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        t = ln.split("#",1)[0].split("//",1)[0].strip().split(",")[0].split()[0]
        if t and t.lower() not in ("symbol","ticker"): out.append(t.upper())
    return sorted(set(out))

def hash_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

# JSONL state helpers (für Diff-Filter)
def load_existing_jsonl(path: str) -> dict[str, dict]:
    d = {}
    p = Path(path)
    if not p.exists(): return d
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                j = json.loads(line)
                d[j["symbol"]] = j
            except Exception:
                continue
    return d

def append_jsonl(path: str, rows: list[dict]):
    ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

# --------------------- Robots + Session + Cache ---------------------
@dataclass
class CacheEntry:
    etag: str | None = None
    lastmod: str | None = None
    body_hash: str | None = None

def cache_path_for(url: str) -> Path:
    Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
    key = hashlib.md5(url.encode("utf-8")).hexdigest()
    return Path(CACHE_DIR) / f"{key}.json"

def read_cache(url: str) -> CacheEntry:
    p = cache_path_for(url)
    if not p.exists(): return CacheEntry()
    try:
        j = json.loads(p.read_text("utf-8"))
        return CacheEntry(j.get("etag"), j.get("lastmod"), j.get("body_hash"))
    except Exception:
        return CacheEntry()

def write_cache(url: str, ce: CacheEntry):
    p = cache_path_for(url)
    p.write_text(json.dumps(ce.__dict__, indent=2), encoding="utf-8")

def can_fetch_robots(base: str, path: str) -> bool:
    # Minimal robots-Prüfung
    try:
        import urllib.robotparser as rp
        rob = rp.RobotFileParser()
        rob.set_url(urljoin(base, "/robots.txt"))
        rob.read()
        return rob.can_fetch(HEADERS["User-Agent"], urljoin(base, path))
    except Exception:
        return True  # im Zweifel nicht blockieren (wir crawlen eh sehr langsam)
    
def rate_sleep():
    time.sleep(RATE_MS/1000.0)

def http_get_with_cache(sess: requests.Session, url: str) -> str | None:
    ce = read_cache(url)
    hdrs = dict(HEADERS)
    if ce.etag: hdrs["If-None-Match"] = ce.etag
    if ce.lastmod: hdrs["If-Modified-Since"] = ce.lastmod

    for k in range(RETRIES+1):
        try:
            r = sess.get(url, headers=hdrs, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 304:
                # nicht geändert -> wir würden den Body neu fetchen müssen;
                # hier lassen wir 304 als "kein Update" gelten (Parser nutzt alten Hash-Vergleich auf Text)
                return None
            if r.status_code >= 400:
                if 500 <= r.status_code < 600 and k < RETRIES:
                    rate_sleep(); continue
                return None
            text = r.text or ""
            etag = r.headers.get("ETag")
            lastmod = r.headers.get("Last-Modified")
            body_hash = hash_text(text)
            write_cache(url, CacheEntry(etag, lastmod, body_hash))
            return text
        except requests.RequestException:
            rate_sleep()
            continue
    return None

# --------------------- Parsing (Zacks) ---------------------
def build_urls_for(symbol: str) -> list[str]:
    # ein paar “stabile” Seiten mit sinnvollen Texten
    return [
        f"https://www.zacks.com/stock/quote/{symbol}",
        f"https://www.zacks.com/stock/research/{symbol}/stock-style-scores",
        f"https://www.zacks.com/stock/research/{symbol}/brokerage-recommendations",
    ]

def extract_text_blocks(html: str) -> list[str]:
    """Konservativer Text-Extractor: Meta description + Überschriften-nahe Absätze + 'rank'/'score' Kontexte."""
    out = []
    soup = BeautifulSoup(html, "lxml")
    # Meta description
    meta = soup.find("meta", {"name":"description"})
    if meta and meta.get("content"): out.append(meta["content"])
    # Hauptcontent
    for sel in ["article", "div#premium_research", "section", "div#quote_ribbon"]:
        box = soup.select_one(sel)
        if not box: continue
        # nimm Absätze mit brauchbarer Länge
        for p in box.find_all(["p","li"]):
            t = p.get_text(" ", strip=True)
            if t and len(t) >= 80:
                out.append(t)
    # Fallback: Schlüsselwörter sammeln
    key_re = re.compile(r"(Zacks Rank|Style Score|Value Score|Momentum Score|Earnings ESP|estimate revision|surprise)", re.I)
    for p in soup.find_all(["p","li","div","span"]):
        t = p.get_text(" ", strip=True)
        if t and key_re.search(t) and t not in out and len(t) > 40:
            out.append(t)
    # dedupe, säubern
    clean = []
    seen = set()
    for t in out:
        t = re.sub(r"\s+", " ", t).strip()
        if t and t not in seen:
            seen.add(t); clean.append(t)
    return clean[:6]  # maximal 6 Snippets/Seite, sonst zu viel Text

def join_best_snippet(snips: list[str]) -> str:
    # Priorisiere Snippets, die Rank/Score/Revision/Surprise enthalten
    pri = []
    sc = re.compile(r"(rank|score|revision|surprise|estimate|momentum|value|growth)", re.I)
    for s in snips:
        w = 2 if sc.search(s) else 1
        pri.append((w, len(s), s))
    pri.sort(reverse=True)
    parts = [x[2] for x in pri[:3]]  # Top 3
    return " ".join(parts)

# --------------------- Cleaning / Rules ---------------------
REMOVE_PATTERNS = [
    r"See\s+the\s+full\s+Zacks\s+Rank\s+definition.*?$",
    r"Visit\s+Zacks\.com.*?$",
    r"Disclosure:\s+.*?$",
    r"Zacks Investment Research.*?$",
]
def clean_english_text(txt: str) -> str:
    for pat in REMOVE_PATTERNS:
        txt = re.sub(pat, "", txt, flags=re.I)
    return re.sub(r"\s+", " ", txt).strip()

# Schutz für Zahlen/Prozente
def protect_numbers(txt: str) -> str:
    return re.sub(r"([\+\-]?\d+(?:\.\d+)?%?)", r"<KEEP>\1</KEEP>", txt)

def unprotect_numbers(txt: str) -> str:
    return txt.replace("<KEEP>","").replace("</KEEP>","")

# Satz-Splitting (syntok) -> reduziert Halluzinationen
def split_sentences(txt: str) -> list[str]:
    sents = []
    for paragraph in syntok.process(txt):
        for sent in paragraph:
            sents.append("".join([t.spacing + t.value for t in sent]).strip())
    return [s for s in sents if s]

# --------------------- MarianMT Übersetzung ---------------------
_translator = None
def get_translator():
    global _translator
    if _translator is None:
        from transformers import MarianMTModel, MarianTokenizer, pipeline
        model_name = "Helsinki-NLP/opus-mt-en-de"
        _translator = pipeline("translation", model=model_name, tokenizer=model_name)
    return _translator

def translate_en2de(txt: str) -> str:
    if not txt: return ""
    prot = protect_numbers(txt)
    sents = split_sentences(prot)
    tr = get_translator()
    out = []
    for s in sents:
        out.append(tr(s, max_length=512)[0]["translation_text"])
    de = " ".join(out)
    return unprotect_numbers(de)

# --------------------- Orchestrierung ---------------------
def process_symbol(sym: str, sess: requests.Session, state: dict[str,dict]) -> tuple[dict|None, dict|None]:
    # Aus bestehenden JSONL: Hash der alten EN-Quelle (für Diff)
    old = state.get(sym)
    old_hash = hash_text(old.get("opinion_en","")) if old else None

    urls = build_urls_for(sym)
    texts = []
    for u in urls:
        # robots-check für jeweilige Seite
        path = "/" + "/".join(u.split("/")[3:])
        if not can_fetch_robots(BASE, path):
            continue
        rate_sleep()
        html = http_get_with_cache(sess, u)
        if html is None:
            # 304 oder Fehler → versuchen wir keine weiteren Parses hier
            continue
        snips = extract_text_blocks(html)
        if snips:
            texts.append(join_best_snippet(snips))

    en = clean_english_text(" ".join([t for t in texts if t]))
    if not en:
        return None, {"symbol": sym, "reason": "no_text"}

    new_hash = hash_text(en)
    if old_hash == new_hash:
        # Keine Übersetzung nötig, aber ggf. URL aktualisieren
        return {"symbol": sym, "source_url": urls[0], "opinion_en": old["opinion_en"], "opinion_de": old.get("opinion_de",""), "asof": now_iso()}, None

    # Translate
    de = translate_en2de(en)
    row = {
        "symbol": sym,
        "source_url": urls[0],
        "opinion_en": en,
        "opinion_de": de,
        "asof": now_iso()
    }
    return row, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", default="watchlists/mylist.txt")
    ap.add_argument("--out", default=OUT_JSONL)
    ap.add_argument("--errors", default=OUT_ERRORS)
    ap.add_argument("--max", type=int, default=100000)
    ap.add_argument("--sleep-ms", type=int, default=RATE_MS)
    args = ap.parse_args()

    global RATE_MS
    RATE_MS = args.sleep_ms

    syms = read_watchlist(args.watchlist)[:args.max]
    state = load_existing_jsonl(args.out)

    sess = requests.Session()
    ok_rows, errs = [], []

    for s in tqdm(syms, desc="Zacks fetch/translate"):
        try:
            row, err = process_symbol(s, sess, state)
            if row: ok_rows.append(row)
            if err: errs.append(err)
        except Exception as e:
            errs.append({"symbol": s, "reason": str(e)})

    if ok_rows:
        append_jsonl(args.out, ok_rows)
        print(f"Wrote {len(ok_rows)} rows → {args.out}")
    ensure_dir(args.errors)
    with open(args.errors,"w",encoding="utf-8") as f:
        json.dump({"ts": now_iso(), "watchlist": args.watchlist, "ok": len(ok_rows), "failed": len(errs), "errors": errs}, f, indent=2)
    print(f"Errors: {len(errs)} → {args.errors}")

if __name__ == "__main__":
    main()
