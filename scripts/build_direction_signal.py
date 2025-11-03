#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_direction_signal.py
Erzeugt data/processed/direction_signal.csv(.gz) aus:
- data/processed/options_signals.csv(.gz)
- data/processed/options_oi_by_strike.csv(.gz)

Ausgabe-Spalten (pro Symbol):
  symbol, dir, strength, next_expiry, nearest_dte,
  focus_strike_7, focus_strike_30, focus_strike_60
"""

import os
import gzip
import pandas as pd
from pathlib import Path

IN_SIGNALS = [
    "data/processed/options_signals.csv.gz",
    "data/processed/options_signals.csv",
]

IN_BY_STRIKE = [
    "data/processed/options_oi_by_strike.csv.gz",
    "data/processed/options_oi_by_strike.csv",
]

OUT_PATH = "data/processed/direction_signal.csv.gz"


def read_csv_auto(candidates, **kwargs):
    for p in candidates:
        if not Path(p).exists():
            continue
        if p.endswith(".gz"):
            with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
                return pd.read_csv(f, **kwargs)
        else:
            return pd.read_csv(p, **kwargs)
    return None


def main():
    os.makedirs("data/processed", exist_ok=True)

    # ---- 1) Grundsignal laden
    sig = read_csv_auto(IN_SIGNALS)
    if sig is None or sig.empty:
        raise SystemExit("options_signals.csv(.gz) nicht gefunden oder leer")

    # normalisieren
    sig.columns = [c.strip().lower() for c in sig.columns]
    for col in ("symbol", "dir", "strength"):
        if col not in sig.columns:
            raise SystemExit(f"Spalte '{col}' fehlt in options_signals")

    # nur relevante Spalten
    sig = sig[["symbol", "dir", "strength"]].copy()
    sig["symbol"] = sig["symbol"].astype(str)

    # robustes Numeric-Parsing
    sig["dir"] = pd.to_numeric(sig["dir"], errors="coerce").astype("Int64")
    sig["strength"] = pd.to_numeric(sig["strength"], errors="coerce").astype("Int64")

    # ---- 2) By-Strike für 7/30/60 Tage
    bs = read_csv_auto(IN_BY_STRIKE)
    if bs is None or bs.empty:
        # Falls nichts da ist, geben wir nur dir/strength zurück
        out = sig.copy()
        out["next_expiry"] = pd.NaT
        out["nearest_dte"] = pd.Series([pd.NA] * len(out), dtype="Int64")
        out["focus_strike_7"] = pd.NA
        out["focus_strike_30"] = pd.NA
        out["focus_strike_60"] = pd.NA
        write_out(out)
        return

    bs.columns = [c.strip().lower() for c in bs.columns]

    # Erwartete Spalten prüfen (tolerant bei zusätzlich vorhandenen)
    needed = {"symbol", "expiry", "dte"}
    if not needed.issubset(set(bs.columns)):
        raise SystemExit(f"Spalten fehlen in options_oi_by_strike: {needed - set(bs.columns)}")

    # Numerik & Datum konvertieren (ohne leere Strings!)
    bs["dte"] = pd.to_numeric(bs["dte"], errors="coerce").astype("Int64")
    for col in ("focus_strike_7", "focus_strike_30", "focus_strike_60", "focus_strike"):
        if col in bs.columns:
            bs[col] = pd.to_numeric(bs[col], errors="coerce")

    bs["expiry"] = pd.to_datetime(bs["expiry"], errors="coerce").dt.date

    # Wir aggregieren je Symbol den **nächsten** Eintrag (kleinste DTE >= 0)
    bs_valid = bs.loc[bs["dte"].notna() & (bs["dte"] >= 0)].copy()

    # Falls Datei keine getrennten 7/30/60-Spalten hat, aus fallback "focus_strike" ziehen
    for tgt in ("focus_strike_7", "focus_strike_30", "focus_strike_60"):
        if tgt not in bs_valid.columns:
            bs_valid[tgt] = pd.NA

    # je Symbol den kleinsten DTE-Record finden
    idx = bs_valid.groupby("symbol")["dte"].idxmin()
    bs_next = bs_valid.loc[idx, ["symbol", "expiry", "dte", "focus_strike_7", "focus_strike_30", "focus_strike_60"]].copy()
    bs_next = bs_next.rename(columns={"expiry": "next_expiry", "dte": "nearest_dte"})

    # Datentypen final setzen
    bs_next["nearest_dte"] = pd.to_numeric(bs_next["nearest_dte"], errors="coerce").astype("Int64")
    for col in ("focus_strike_7", "focus_strike_30", "focus_strike_60"):
        bs_next[col] = pd.to_numeric(bs_next[col], errors="coerce")

    # next_expiry wieder als ISO-String (yyyy-mm-dd) schreiben (Agena liest leichter)
    bs_next["next_expiry"] = pd.to_datetime(bs_next["next_expiry"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ---- 3) Join
    out = sig.merge(bs_next, on="symbol", how="left")

    # Finalreihenfolge
    out = out[
        [
            "symbol",
            "dir",
            "strength",
            "next_expiry",
            "nearest_dte",
            "focus_strike_7",
            "focus_strike_30",
            "focus_strike_60",
        ]
    ]

    write_out(out)


def write_out(df: pd.DataFrame):
    # .gz schreiben (UTF-8, Zeilenende = \n)
    with gzip.open(OUT_PATH, "wt", encoding="utf-8", newline="") as gz:
        df.to_csv(gz, index=False)
    print(f"wrote {OUT_PATH} rows={len(df)}")


if __name__ == "__main__":
    main()
