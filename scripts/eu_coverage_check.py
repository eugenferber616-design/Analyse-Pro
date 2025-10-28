#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EU Coverage Check – schreibt Ampel & Missing-Listen nach data/reports/eu_checks/

- Liest Watchlists (Stocks/ETF) aus Umgebungsvariablen WATCHLIST_STOCKS / WATCHLIST_ETF
- Filtert EU-Symbole (Suffixe wie .DE/.FR/… oder ISIN-Prefixe wie DE/FR/IE/…)
- Prüft Abdeckung in:
    - data/processed/fundamentals_core.csv
    - data/processed/earnings_results.csv
    - data/processed/etf_basics.csv
    - data/processed/options_oi_totals.csv   (häufig US-lastig → optional)
    - data/processed/fred_oas.csv            (seriell, nicht symbolbasiert → nur Vorhandensein)
- Schreibt:
    - data/reports/eu_checks/summary.txt
    - data/reports/eu_checks/summary.json
    - data/reports/eu_checks/<dataset>_preview.txt
    - data/reports/eu_checks/<dataset>_missing.txt
"""

from __future__ import annotations
import csv, json, os, re
from pathlib import Path
from typing import List, Dict, Tuple

ROOT = Path(".")
PROCESSED = ROOT / "data" / "processed"
REPORTDIR = ROOT / "data" / "reports" / "eu_checks"
REPORTDIR.mkdir(parents=True, exist_ok=True)

WATCHLIST_STOCKS = os.getenv("WATCHLIST_STOCKS", "watchlists/mylist.txt")
WATCHLIST_ETF    = os.getenv("WATCHLIST_ETF", "watchlists/etf_sample.txt")

# EU-Erkennung (Ticker-Suffix & ISIN-Prefixe)
EU_TICKER = re.compile(r"\.(DE|FR|NL|IT|ES|SE|FI|DK|IE|AT|BE|PT|PL|CZ|HU|NO|CH|GB)$", re.IGNORECASE)
EU_ISIN   = re.compile(r"^(DE|FR|NL|IT|ES|SE|FI|DK|IE|AT|BE|PT|PL|CZ|HU|NO|CH|GB)", re.IGNORECASE)

def read_watchlist_symbols(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        return []
    out: List[str] = []
    with p.open(encoding="utf-8") as f:
        # erlaubt CSV oder einfache Listen
        first = f.readline()
        if "," in first:
            # CSV mit Header "symbol"?
            f.seek(0)
            rdr = csv.DictReader(f)
            cand_cols = [c for c in rdr.fieldnames or [] if c.lower() in ("symbol","ticker","ric")]
            for row in rdr:
                if not cand_cols: break
                sym = (row.get(cand_cols[0]) or "").strip()
                if sym: out.append(sym)
        else:
            # einfache .txt-Liste
            s = first.strip()
            if s and s.lower() != "symbol":
                out.append(s)
            for line in f:
                s = line.strip()
                if s and s.lower() != "symbol":
                    out.append(s)
    return sorted(set(out))

def filter_eu_symbols(symbols: List[str]) -> List[str]:
    return sorted({s for s in symbols if EU_TICKER.search(s)})

def load_csv_index_by_symbol(path: Path) -> Tuple[List[str], Dict[str, List[str]]]:
    """
    liest CSV und baut ein Index dict[symbol] -> rows (als Liste Spalten-Strings)
    symbol-Spalte wird heuristisch gesucht: 'symbol'/'ticker' oder erste Spalte
    """
    if not path.exists() or path.stat().st_size == 0:
        return [], {}
    with path.open(encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        try:
            header = next(rdr)
        except StopIteration:
            return [], {}
        # symbol column guess
        sym_idx = 0
        for i, col in enumerate(header):
            if col.lower() in ("symbol","ticker","ric"):
                sym_idx = i
                break
        idx: Dict[str, List[str]] = {}
        for row in rdr:
            if not row: continue
            sym = row[sym_idx].strip()
            if sym:
                idx.setdefault(sym, []).append(row)
        return header, idx

def preview_rows(path: Path, limit: int = 20) -> List[str]:
    if not path.exists(): return ["<missing file>"]
    out: List[str] = []
    with path.open(encoding="utf-8", newline="") as f:
        for i, line in enumerate(f):
            out.append(line.rstrip("\n"))
            if i >= limit: break
    return out

def write_txt(relname: str, lines: List[str]) -> None:
    (REPORTDIR / relname).write_text("\n".join(lines) + "\n", encoding="utf-8")

def traffic_light(present: int, expected: int) -> str:
    if expected <= 0:
        return "🟦"  # not applicable
    ratio = present / expected
    if present == expected and expected > 0:
        return "🟢"
    if ratio >= 0.5:
        return "🟡"
    if present == 0:
        return "🔴"
    return "🟠"

def main():
    # Watchlists lesen und EU filtern
    wl_stocks = read_watchlist_symbols(WATCHLIST_STOCKS)
    wl_etf    = read_watchlist_symbols(WATCHLIST_ETF)

    eu_stocks = filter_eu_symbols(wl_stocks)
    eu_etf    = filter_eu_symbols(wl_etf)

    summary = {
        "watchlists": {
            "stocks_all": len(wl_stocks), "stocks_eu": len(eu_stocks),
            "etf_all": len(wl_etf),       "etf_eu": len(eu_etf),
        },
        "datasets": {}
    }

    # --- FUNDAMENTALS ---
    fundamentals = PROCESSED / "fundamentals_core.csv"
    hdr, idx = load_csv_index_by_symbol(fundamentals)
    present_stocks = [s for s in eu_stocks if s in idx]
    missing_stocks = [s for s in eu_stocks if s not in idx]
    write_txt("fundamentals_preview.txt", preview_rows(fundamentals, 30))
    write_txt("fundamentals_missing.txt", [*missing_stocks] or ["<none missing>"])
    summary["datasets"]["fundamentals_core.csv"] = {
        "present": len(present_stocks),
        "expected": len(eu_stocks),
        "missing": missing_stocks,
        "ampel": traffic_light(len(present_stocks), len(eu_stocks)),
    }

    # --- EARNINGS RESULTS ---
    earn = PROCESSED / "earnings_results.csv"
    hdr, idx = load_csv_index_by_symbol(earn)
    present_stocks = [s for s in eu_stocks if s in idx]
    missing_stocks = [s for s in eu_stocks if s not in idx]
    write_txt("earnings_results_preview.txt", preview_rows(earn, 30))
    write_txt("earnings_results_missing.txt", [*missing_stocks] or ["<none missing>"])
    summary["datasets"]["earnings_results.csv"] = {
        "present": len(present_stocks),
        "expected": len(eu_stocks),
        "missing": missing_stocks,
        "ampel": traffic_light(len(present_stocks), len(eu_stocks)),
    }

    # --- ETF BASICS (ISIN-basiert: EU-Prefixe) ---
    etf = PROCESSED / "etf_basics.csv"
    etf_present = 0
    etf_missing: List[str] = []
    if etf.exists() and etf.stat().st_size > 0:
        with etf.open(encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            # heuristik: symbol/ticker/isin spalten suchen
            cols = [c.lower() for c in (rdr.fieldnames or [])]
            sym_col = None
            for c in ("symbol","ticker","ric"):
                if c in cols: sym_col = c; break
            isin_col = "isin" if "isin" in cols else None

            rows = list(rdr)
            # Index symbol -> row vorhanden?
            have = set()
            if sym_col:
                for r in rows:
                    s = (r.get(sym_col) or "").strip()
                    if s: have.add(s)
            # Wenn ISIN verfügbar, dann zusätzlich EU über ISIN zählen
            eu_watch = set(eu_etf)
            if not eu_watch and isin_col:
                # keine EU-Ticker in WL → fallback: zähle EU-ISINs (berichtsweise)
                etf_present = sum(1 for r in rows if EU_ISIN.match((r.get(isin_col) or "").strip()))
                etf_missing = []  # kein Soll → N/A
            else:
                etf_present = sum(1 for s in eu_watch if s in have)
                etf_missing = [s for s in eu_watch if s not in have]
    write_txt("etf_basics_preview.txt", preview_rows(etf, 30))
    write_txt("etf_basics_missing.txt", etf_missing or ["<none missing>"])
    summary["datasets"]["etf_basics.csv"] = {
        "present": etf_present,
        "expected": len(eu_etf),
        "missing": etf_missing,
        "ampel": traffic_light(etf_present, len(eu_etf)),
    }

    # --- OPTIONS OI (optional; oft US-only) ---
    opt = PROCESSED / "options_oi_totals.csv"
    hdr, idx = load_csv_index_by_symbol(opt)
    present_stocks = [s for s in eu_stocks if s in idx]
    missing_stocks = [s for s in eu_stocks if s not in idx]
    write_txt("options_oi_totals_preview.txt", preview_rows(opt, 30))
    write_txt("options_oi_totals_missing.txt", [*missing_stocks] or ["<none missing>"])
    summary["datasets"]["options_oi_totals.csv"] = {
        "present": len(present_stocks),
        "expected": len(eu_stocks),
        "missing": missing_stocks,
        # Wenn 0 von 0 erwartet → blau; wenn EU-Stocks existieren, aber Optionsdaten
        # naturgemäß fehlen, wirst du Gelb/Rot sehen. Das ist OK als Signal.
        "ampel": traffic_light(len(present_stocks), len(eu_stocks)),
    }

    # --- FRED OAS (serienbasiert; nur Dateicheck) ---
    fred = PROCESSED / "fred_oas.csv"
    ok = fred.exists() and fred.stat().st_size > 0
    write_txt("fred_oas_preview.txt", preview_rows(fred, 30))
    summary["datasets"]["fred_oas.csv"] = {
        "present": 1 if ok else 0, "expected": 1, "missing": [] if ok else ["fred_oas.csv"],
        "ampel": "🟢" if ok else "🔴",
    }

    # --- Zusammenfassung ---
    # Ampel für alles (nur symbolbasierte Pflichtdatensätze werten: fundamentals + earnings + etf)
    mandatory = ("fundamentals_core.csv", "earnings_results.csv", "etf_basics.csv")
    reds = [k for k,v in summary["datasets"].items() if k in mandatory and v["ampel"] == "🔴"]
    ambers = [k for k,v in summary["datasets"].items() if k in mandatory and v["ampel"] in ("🟠","🟡")]
    if not reds and not ambers:
        overall = "🟢 ALLES DA"
    elif reds:
        overall = "🔴 FEHLER – fehlende Pflichtdaten"
    else:
        overall = "🟡 TEILWEISE – einige Lücken"

    summary["overall"] = overall

    # speichern
    (REPORTDIR / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [f"EU Coverage – {overall}", "", f"Watchlists: stocks(EU)={len(eu_stocks)} / etf(EU)={len(eu_etf)}", ""]
    for k, v in summary["datasets"].items():
        lines.append(f"{v['ampel']} {k}: {v['present']}/{v['expected']}")
        if v.get("missing"):
            miss = v["missing"][:10]
            if miss:
                lines.append("   fehlt: " + ", ".join(miss) + (" …" if len(v["missing"]) > 10 else ""))
    write_txt("summary.txt", lines)

if __name__ == "__main__":
    main()
