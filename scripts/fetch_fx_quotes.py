import os, csv, requests, sys, time, re, json
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"
ENV = load_env()
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

PROVIDER_TARGET = "OANDA"
PROVIDER_PAT = r"^(?P<pfx>[A-Z]+):(?P<base>[A-Z]{3})[ _/]?(?P<quote>[A-Z]{3})$"
PLAIN_PAT     = r"^(?P<base>[A-Z]{3})[ _/]?(?P<quote>[A-Z]{3})$"

def read_list(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return [l.strip() for l in f if l.strip() and not l.lower().startswith("symbol")]

def normalize_fx(sym: str):
    """liefert beide Varianten als Kandidaten: OANDA:EUR_USD und FOREX:EURUSD"""
    x = sym.strip().upper()
    if not x:
        return []
    # 1) Provider:BASE[sep]QUOTE
    m = re.fullmatch(PROVIDER_PAT, x)
    if m:
        b, q = m.group("base"), m.group("quote")
        return [f"OANDA:{b}_{q}", f"FOREX:{b}{q}"]
    # 2) BASE[sep]QUOTE
    m = re.fullmatch(PLAIN_PAT, x)
    if m:
        b, q = m.group("base"), m.group("quote")
        return [f"OANDA:{b}_{q}", f"FOREX:{b}{q}"]
    # 3) Bereits korrekt?
    if re.fullmatch(r"^OANDA:[A-Z]{3}_[A-Z]{3}$", x):
        b, q = x.split(":")[1].split("_")
        return [x, f"FOREX:{b}{q}"]
    if re.fullmatch(r"^FOREX:[A-Z]{6}$", x):
        bq = x.split(":")[1]
        return [f"OANDA:{bq[:3]}_{bq[3:]}",
                x]
    return []

def get_quote(session, symbol):
    """Versucht /forex/quote → wenn leer, fällt auf /forex/candle (letzter Close) zurück."""
    params = {"symbol": symbol, "token": TOKEN}
    r = session.get(f"{API}/forex/quote", params=params, timeout=20)
    if r.status_code == 429:
        time.sleep(2.5); r = session.get(f"{API}/forex/quote", params=params, timeout=20)
    r.raise_for_status()
    j = r.json() or {}
    # /forex/quote liefert i.d.R. c(last), t(ts); oft KEIN a/b im Free-Tier
    if (j.get("c") not in (None, 0)) and (j.get("t") not in (None, 0)):
        return {"last": j.get("c"), "bid": j.get("b"), "ask": j.get("a"), "timestamp": j.get("t"), "source": "quote"}

    # Fallback: letzte Kerze (1-Min / 5-Min)
    import time as _t
    now = int(_t.time())
    for res in ("1", "5", "15"):
        p2 = {"symbol": symbol, "resolution": res, "from": now - 48*3600, "to": now, "token": TOKEN}
        rc = session.get(f"{API}/forex/candle", params=p2, timeout=20)
        if rc.status_code == 429:
            _t.sleep(2.5); rc = session.get(f"{API}/forex/candle", params=p2, timeout=20)
        rc.raise_for_status()
        cj = rc.json() or {}
        if cj.get("s") == "ok" and cj.get("c"):
            # nimm letzte nicht-Null-Close
            closes, times = cj.get("c", []), cj.get("t", [])
            for i in range(len(closes)-1, -1, -1):
                if closes[i] not in (None, 0):
                    return {"last": closes[i], "bid": None, "ask": None, "timestamp": times[i], "source": f"candle_{res}"}
    return None

def main(wl: str, outcsv: str, errors_path: str = "data/reports/fx_errors.json"):
    if not TOKEN:
        print("No FINNHUB token"); return 0  # nicht failen, sauber loggen

    raw = read_list(wl)
    # Aus allen Einträgen Kandidaten erzeugen (beide Provider-Schemata)
    candidates = []
    for s in raw:
        cands = normalize_fx(s)
        if cands:
            candidates.append(tuple(cands))  # (OANDA:..., FOREX:...)
        else:
            print("fx skip (unparsable):", s)

    # dedupe auf Basis des OANDA-Ziels
    seen = set()
    pairs = []
    for oanda, forex in candidates:
        if oanda not in seen:
            seen.add(oanda)
            pairs.append((oanda, forex))

    os.makedirs(os.path.dirname(outcsv), exist_ok=True)
    os.makedirs(os.path.dirname(errors_path), exist_ok=True)

    fieldnames = ["pair","bid","ask","last","timestamp"]
    rl = RateLimiter(50, 1300)
    rows, errs = [], {"total": len(pairs), "ok": 0, "failed": 0, "errors": []}

    session = requests.Session()
    for oanda_sym, forex_sym in pairs:
        rl.wait()
        ok = None
        tried = []
        for sym in (oanda_sym, forex_sym):
            tried.append(sym)
            try:
                q = get_quote(session, sym)
                if q:
                    rows.append({"pair": sym, "bid": q["bid"], "ask": q["ask"], "last": q["last"], "timestamp": q["timestamp"]})
                    errs["ok"] += 1
                    ok = True
                    break
            except Exception as e:
                pass
        if not ok:
            errs["failed"] += 1
            errs["errors"].append({"pairs_tried": tried, "reason": "empty_or_denied"})

    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames); w.writeheader(); w.writerows(rows)

    with open(errors_path, "w", encoding="utf-8") as f:
        json.dump(errs, f, ensure_ascii=False, indent=2)

    print(f"wrote {outcsv} rows={len(rows)} / total={len(pairs)}  (errors: {errs['failed']})")
    # ⬇️ Pipeline nicht hart failen; Logs zeigen Details
    return 0

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/fx_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/fx_quotes.csv"
    sys.exit(main(wl, outcsv))
