import os, sys, csv, json, time
from typing import Dict, Any, List

FIELDS = ["symbol","last","change","pct","open","high","low","prev_close",
          "volume","exchange","currency","timestamp","source"]

def read_list(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        out = []
        for ln in f:
            s = ln.split("#",1)[0].strip()
            if s: out.append(s)
    return sorted(set(out))

def nz(x):
    return None if x in ("", None) else x

def get_quote(sym: str) -> Dict[str, Any] | None:
    import yfinance as yf
    t = yf.Ticker(sym)

    # bevorzugt fast_info (schnell)
    fi = getattr(t, "fast_info", None)
    last = getattr(fi, "last_price", None) if fi else None
    open_ = getattr(fi, "open", None) if fi else None
    high  = getattr(fi, "day_high", None) if fi else None
    low   = getattr(fi, "day_low", None) if fi else None
    prev  = getattr(fi, "previous_close", None) if fi else None
    vol   = getattr(fi, "last_volume", None) if fi else None
    cur   = getattr(fi, "currency", None) if fi else None
    exch  = getattr(fi, "exchange", None) if fi else None

    if last is None:
        # Fallback: letzte 5 Tage / 30min
        hist = t.history(period="5d", interval="30m")
        if not hist.empty:
            last = float(hist["Close"].dropna().iloc[-1])
            ts   = int(hist.index[-1].timestamp())
        else:
            return None
    else:
        import time as _t
        ts = int(_t.time())

    # prev_close ggf. ergänzen
    if prev is None:
        d = t.history(period="5d")
        if not d.empty:
            prev = float(d["Close"].dropna().iloc[-2]) if len(d) >= 2 else float(d["Close"].dropna().iloc[-1])

    # change & pct
    chg = (None if (last is None or prev is None) else float(last) - float(prev))
    pct = (None if (chg is None or prev in (0, None)) else 100.0 * chg / float(prev))

    return {
        "symbol": sym,
        "last": nz(last),
        "change": nz(chg),
        "pct": nz(pct),
        "open": nz(open_),
        "high": nz(high),
        "low": nz(low),
        "prev_close": nz(prev),
        "volume": nz(vol),
        "exchange": exch or getattr(t, "info", {}).get("exchange"),
        "currency": cur or getattr(t, "info", {}).get("currency"),
        "timestamp": ts,
        "source": "yfinance",
    }

def main(watchlist: str, outcsv: str, errpath: str = "data/reports/fut_errors.json") -> int:
    os.makedirs(os.path.dirname(outcsv), exist_ok=True)
    os.makedirs(os.path.dirname(errpath), exist_ok=True)

    syms = read_list(watchlist)
    rows, errs = [], {"total": len(syms), "ok": 0, "failed": 0, "errors": []}

    for s in syms:
        try:
            q = get_quote(s)
            if q:
                rows.append(q); errs["ok"] += 1
            else:
                errs["failed"] += 1; errs["errors"].append({"symbol": s, "reason": "no_data"})
        except Exception as e:
            errs["failed"] += 1; errs["errors"].append({"symbol": s, "reason": "exception", "msg": str(e)})
        time.sleep(0.2)  # höflich

    with open(outcsv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS); w.writeheader(); w.writerows(rows)
    with open(errpath, "w", encoding="utf-8") as f:
        json.dump(errs, f, ensure_ascii=False, indent=2)

    print(f"wrote {outcsv} rows={len(rows)} / total={len(syms)} (errors: {errs['failed']})")
    return 0

if __name__ == "__main__":
    wl  = sys.argv[sys.argv.index("--watchlist")+1] if "--watchlist" in sys.argv else "watchlists/fut_sample.txt"
    out = sys.argv[sys.argv.index("--out")+1]       if "--out" in sys.argv       else "data/processed/futures_quotes.csv"
    err = sys.argv[sys.argv.index("--errors")+1]    if "--errors" in sys.argv    else "data/reports/fut_errors.json"
    raise SystemExit(main(wl, out, err))
