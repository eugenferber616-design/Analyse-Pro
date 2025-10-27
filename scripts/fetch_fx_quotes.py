import os, csv, requests, sys, time, re, json
from util import load_env
from cache import RateLimiter

API = "https://finnhub.io/api/v1"
ENV = load_env()
TOKEN = ENV.get("FINNHUB_TOKEN") or ENV.get("FINNHUB_API_KEY")

# -------- Helpers -----------------------------------------------------

def read_list(path: str):
    with open(path, "r", encoding="utf-8") as f:
        rows = [l.strip() for l in f if l.strip() and not l.lower().startswith("symbol")]
    return rows

# Akzeptiert:
#  - EURUSD / eurusd
#  - EUR/USD, EUR_USD
#  - FOREX:EURUSD, FX:EURUSD, FXCM:EUR/USD, OANDA:EUR_USD, IDC:EURUSD
# Normalisiert auf: OANDA:EUR_USD
PROVIDER_TARGET = "OANDA"
PROVIDER_PAT = r"^(?P<pfx>[A-Z]+):(?P<base>[A-Z]{3})[ _/]?(?P<quote>[A-Z]{3})$"
PLAIN_PAT     = r"^(?P<base>[A-Z]{3})[ _/]?(?P<quote>[A-Z]{3})$"

def normalize_fx(sym: str, provider: str = PROVIDER_TARGET) -> str | None:
    x = sym.strip().upper()
    if not x:
        return None

    # 1) Provider:BASE[sep]QUOTE
    m = re.fullmatch(PROVIDER_PAT, x)
    if m:
        base, quote = m.group("base"), m.group("quote")
        return f"{provider}:{base}_{quote}"

    # 2) BASE[sep]QUOTE (EURUSD, EUR/USD, EUR_USD)
    m = re.fullmatch(PLAIN_PAT, x)
    if m:
        base, quote = m.group("base"), m.group("quote")
        return f"{provider}:{base}_{quote}"

    # 3) Bereits korrekt: OANDA:EUR_USD
    if re.fullmatch(rf"^{provider}:[A-Z]{{3}}_[A-Z]{{3}}$", x):
        return x

    return None

def valid_row(j: dict) -> bool:
    # Finnhub-Quote keys: a(ask), b(bid), c(last), t(timestamp)
    # Manche Provider liefern 0/None wenn Paar nicht unterstützt
    return any([
        j.get("a") not in (None, 0),
        j.get("b") not in (None, 0),
        j.get("c") not in (None, 0),
    ]) and j.get("t") not in (None, 0)

# -------- Main --------------------------------------------------------

def main(wl: str, outcsv: str, errors_path: str = "data/reports/fx_errors.json"):
    if not TOKEN:
        print("No FINNHUB token"); return 2

    raw = read_list(wl)
    normed = []
    for s in raw:
        n = normalize_fx(s)
        if n:
            normed.append(n)
        else:
            print("fx skip (unparsable):", s)

    # Dedupe + Sort
    pairs = sorted(set(normed))
    os.makedirs(os.path.dirname(outcsv), exist_ok=True)
    os.makedirs(os.path.dirname(errors_path), exist_ok=True)

    fieldnames = ["pair", "bid", "ask", "last", "timestamp"]
    rl = RateLimiter(50, 1300)

    rows = []
    errs = {"total": len(pairs), "ok": 0, "failed": 0, "errors": []}

    session = requests.Session()
    for p in pairs:
        rl.wait()
        try:
            params = {"symbol": p, "token": TOKEN}
            r = session.get(f"{API}/forex/quote", params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(2.5)
                r = session.get(f"{API}/forex/quote", params=params, timeout=20)
            r.raise_for_status()
            j = r.json() or {}

            if not valid_row(j):
                errs["failed"] += 1
                errs["errors"].append({"pair": p, "reason": "empty_or_invalid", "raw": j})
                continue

            rows.append({
                "pair": p,
                "bid": j.get("b"),
                "ask": j.get("a"),
                "last": j.get("c"),
                "timestamp": j.get("t"),
            })
            errs["ok"] += 1

        except Exception as e:
            errs["failed"] += 1
            errs["errors"].append({"pair": p, "reason": "exception", "msg": str(e)})

    # Schreiben
    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    with open(errors_path, "w", encoding="utf-8") as f:
        json.dump(errs, f, ensure_ascii=False, indent=2)

    print(f"wrote {outcsv} rows={len(rows)} / total={len(pairs)}  (errors: {errs['failed']})")
    return 0 if rows else 1

if __name__ == "__main__":
    wl = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/fx_sample.txt"
    outcsv = sys.argv[sys.argv.index("--out")+1] if "--out" in sys.argv else "data/processed/fx_quotes.csv"
    sys.exit(main(wl, outcsv))
