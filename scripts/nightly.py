# scripts/nightly.py
import argparse, sys, time, pathlib, json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--window-days", type=int, default=7)
    ap.add_argument("--cheap-mode", action="store_true")
    ap.add_argument("--watchlist", type=str, default="")
    args = ap.parse_args()

    root = pathlib.Path(".")
    cache = root / "data" / "cache"
    reports = root / "data" / "reports"
    cache.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    # Nur als Platzhalter: wir protokollieren die Parameter in eine JSON
    run_info = {
        "batch": args.batch,
        "window_days": args.window_days,
        "cheap_mode": args.cheap_mode,
        "watchlist": args.watchlist,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    (reports / "last_run.json").write_text(json.dumps(run_info, indent=2), encoding="utf-8")

    # Wenn Watchlist angegeben, prüfen wir nur, ob die Datei existiert
    if args.watchlist:
        wl = pathlib.Path(args.watchlist)
        if wl.exists():
            print(f"[nightly] Watchlist gefunden: {wl} ({len(wl.read_text().splitlines())} Zeilen)")
        else:
            print(f"[nightly] Warnung: Watchlist nicht gefunden: {wl}")

    print("[nightly] OK – Stub ausgeführt. (Später kommen hier die echten Datenpulls.)")

if __name__ == "__main__":
    sys.exit(main())
