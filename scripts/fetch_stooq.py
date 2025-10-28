# scripts/fetch_stooq.py
# Zweck: XETRA (".DE") Tagesdaten aus Yahoo (stabil) holen,
#        optionales Stooq-Mapping einlesen (für spätere Nutzung / Preview),
#        Ergebnisse als Einzel-CSV + kleine Zusammenfassung/QA schreiben.

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import yfinance as yf


def read_watchlist(path: str) -> list[str]:
    """Liest Watchlist ein und filtert NUR Xetra (.DE)."""
    out: list[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.endswith(".DE"):
                out.append(s)
    # eindeutige, sortierte Symbole
    return sorted(set(out))


def load_stooq_map(map_csv: str | None) -> dict[str, str]:
    """Optionales Mapping symbol -> stooq (sap.de, dte.de, …) einlesen."""
    if not map_csv:
        return {}
    p = Path(map_csv)
    if not p.exists():
        return {}
    mp: dict[str, str] = {}
    with p.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            src = (row.get("symbol") or "").strip()
            dst = (row.get("stooq") or "").strip()
            if src and dst:
                mp[src] = dst
    return mp


def ensure_dirs():
    os.makedirs("data/market/stooq", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/reports/eu_checks", exist_ok=True)


def dl_yahoo_daily(ticker: str, days: int) -> pd.DataFrame:
    """Lädt Tagesdaten via yfinance. Gibt leeres DF zurück, wenn nichts gefunden."""
    df = yf.download(
        ticker,
        period=f"{days}d",
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    # yfinance liefert Index als DatetimeIndex, für CSV Vorschau brauchen wir Spaltennamen
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()  # Date wird Spalte
    # Vereinheitliche Spaltennamen
    ren = {c: c.capitalize() for c in df.columns}
    df = df.rename(columns=ren)
    return df


def write_preview(rows: list[list[str | float]], preview_csv: str):
    """Kleine QA-Tabelle (symbol, last_date, last_close)."""
    with open(preview_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "last_date", "last_close"])
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watchlist", required=True, help="Pfad zur Watchlist (Textdatei)")
    ap.add_argument("--days", type=int, default=365, help="Zeitraum in Tagen")
    ap.add_argument("--sleep-ms", type=int, default=200, help="Pause zwischen Requests")
    ap.add_argument(
        "--map",
        default="config/symbol_map_stooq.csv",
        help="Optionales Mapping symbol->stooq (für Preview/Logging)",
    )
    ap.add_argument(
        "--outdir",
        default="data/market/stooq",
        help="Ausgabeordner für Einzel-CSV je Symbol",
    )
    ap.add_argument(
        "--preview",
        default="data/processed/fx_quotes.csv",
        help="Kleine Vorschau/QA CSV",
    )
    args = ap.parse_args()

    ensure_dirs()

    symbols = read_watchlist(args.watchlist)
    stooq_map = load_stooq_map(args.map)

    print(f"🔎 Watchlist: {args.watchlist}")
    print(f"   Gefiltert (Xetra .DE): {len(symbols)} Symbole")
    if stooq_map:
        print(f"   Mapping geladen: {len(stooq_map)} Einträge aus {args.map}")

    ok = 0
    fail = []
    rows_preview: list[list[str | float]] = []

    for s in symbols:
        try:
            df = dl_yahoo_daily(s, args.days)
            if df.empty:
                print(f"ERR {s}: no data")
                fail.append({"symbol": s, "reason": "no_data"})
                time.sleep(args.sleep_ms / 1000.0)
                continue

            out_p = os.path.join(args.outdir, f"{s.replace('.','_')}.csv")
            df.to_csv(out_p, index=False)
            ok += 1

            # Preview-Zeile
            last_date = str(df["Date"].iloc[-1])
            last_close = float(df["Close"].iloc[-1])
            rows_preview.append([s, last_date, last_close])

        except Exception as e:
            print(f"ERR {s}: {e}")
            fail.append({"symbol": s, "reason": str(e)})

        time.sleep(args.sleep_ms / 1000.0)

    write_preview(rows_preview, args.preview)

    # zusätzlich kurze Textvorschau für den Workflow
    preview_txt = "data/reports/eu_checks/stooq_preview.txt"
    with open(preview_txt, "w", encoding="utf-8") as f:
        f.write("symbol,last_date,last_close\n")
        for r in rows_preview[:40]:
            f.write(",".join(map(str, r)) + "\n")

    # und ein kleines JSON-Report
    report = {
        "ts": pd.Timestamp.utcnow().isoformat(),
        "watchlist": args.watchlist,
        "days": args.days,
        "symbols": len(symbols),
        "ok": ok,
        "failed": len(fail),
        "files_dir": args.outdir,
        "preview_csv": args.preview,
        "preview_txt": preview_txt,
        "note": "Nur Xetra (.DE) via yfinance. Mapping-Datei wird nur für Logging/Vorschau mitgeführt.",
        "fail": fail,
    }
    with open("data/reports/stooq_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"✅ Downloaded .DE files: {ok} / {len(symbols)}")
    print(f"↳ Preview: {args.preview}")
    if fail:
        print(f"⚠️  Fehlgeschlagen: {len(fail)} (siehe data/reports/stooq_report.json)")


if __name__ == "__main__":
    sys.exit(main())
