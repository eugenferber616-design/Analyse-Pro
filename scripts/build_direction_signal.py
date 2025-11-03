#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_direction_signal.py
Erzeugt data/processed/direction_signal.csv.gz aus:
- data/processed/options_signals.csv(.gz)
- data/processed/options_oi_by_strike.csv(.gz)

Robust: erkennt alternative Spaltennamen für dir/strength automatisch.
"""

import os, gzip
import pandas as pd
from pathlib import Path
from math import copysign

IN_SIGNALS = [
    "data/processed/options_signals.csv.gz",
    "data/processed/options_signals.csv",
]

IN_BY_STRIKE = [
    "data/processed/options_oi_by_strike.csv.gz",
    "data/processed/options_oi_by_strike.csv",
]

OUT_PATH = "data/processed/direction_signal.csv.gz"


# ---------- Helpers ----------
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


def pick_first_existing(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols:
            return cols[cand.lower()]
    return None


def to_int64_nullable(s):
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def to_float_nullable(s):
    return pd.to_numeric(s, errors="coerce")


# ---------- Main ----------
def main():
    os.makedirs("data/processed", exist_ok=True)

    # 1) options_signals
    sig = read_csv_auto(IN_SIGNALS)
    if sig is None or sig.empty:
        raise SystemExit("options_signals.csv(.gz) nicht gefunden oder leer")

    # lower header nur zum Suchen; Originalnamen behalten
    sig.columns = [c.strip() for c in sig.columns]

    # symbol
    col_symbol = pick_first_existing(sig, ["symbol", "ticker", "underlying"])
    if not col_symbol:
        raise SystemExit(f"Spalte 'symbol' (oder ticker/underlying) fehlt in options_signals. Vorhanden: {list(sig.columns)}")
    sig[col_symbol] = sig[col_symbol].astype(str)

    # dir – akzeptiere alternative Namen
    col_dir = pick_first_existing(
        sig,
        ["dir", "direction", "signal_dir", "signed_dir", "bias", "sign", "direction_bin"],
    )

    # strength – akzeptiere alternative Namen
    col_strength = pick_first_existing(
        sig,
        ["strength", "score", "strength_pct", "signal_strength", "confidence", "weight"],
    )

    if not col_dir and not col_strength:
        raise SystemExit(
            "Weder 'dir' noch eine alternative noch 'strength/score' gefunden. "
            f"Spalten sind: {list(sig.columns)}"
        )

    out_sig = pd.DataFrame({"symbol": sig[col_symbol].astype(str)})

    # Stärke zuerst parsen (kann für Ableitung von dir gebraucht werden)
    if col_strength:
        out_sig["strength"] = to_int64_nullable(sig[col_strength])
    else:
        out_sig["strength"] = pd.Series([pd.NA] * len(out_sig), dtype="Int64")

    # dir parsen/ableiten
    if col_dir:
        d = to_int64_nullable(sig[col_dir])
        # Falls dir nur -1/0/1 sein soll -> clamp
        d = d.clip(lower=-1, upper=1)
        out_sig["dir"] = d
    else:
        # Ableitung: dir = sign(strength) (NaN -> 0)
        s = pd.to_numeric(sig[col_strength], errors="coerce")
        d = s.fillna(0.0).apply(lambda x: 0 if x == 0 else int(copysign(1, x)))
        out_sig["dir"] = to_int64_nullable(d)

    # 2) by_strike (7/30/60 + nächster Verfall)
    bs = read_csv_auto(IN_BY_STRIKE)
    if bs is None or bs.empty:
        # nur Grundsignal ausgeben
        out = out_sig.copy()
        out["next_expiry"] = pd.NA
        out["nearest_dte"] = pd.Series([pd.NA] * len(out), dtype="Int64")
        out["focus_strike_7"] = pd.NA
        out["focus_strike_30"] = pd.NA
        out["focus_strike_60"] = pd.NA
        write_out(out)
        return

    bs.columns = [c.strip() for c in bs.columns]

    # flexible Spaltenfindung
    col_sym_bs = pick_first_existing(bs, ["symbol", "ticker", "underlying"])
    col_exp = pick_first_existing(bs, ["expiry", "expiration", "next_expiry"])
    col_dte = pick_first_existing(bs, ["dte", "days_to_expiry", "nearest_dte"])
    if not (col_sym_bs and col_exp and col_dte):
        raise SystemExit(
            "Benötigte Spalten in options_oi_by_strike fehlen. "
            f"Gefunden: symbol={col_sym_bs}, expiry={col_exp}, dte={col_dte}. "
            f"Vorhandene Spalten: {list(bs.columns)}"
        )

    # ggf. fehlen 7/30/60 – dann sind sie eben NaN
    col_fs7 = pick_first_existing(bs, ["focus_strike_7", "fs_7", "strike7"])
    col_fs30 = pick_first_existing(bs, ["focus_strike_30", "fs_30", "strike30"])
    col_fs60 = pick_first_existing(bs, ["focus_strike_60", "fs_60", "strike60"])

    # numerics / dates
    bs[col_dte] = to_int64_nullable(bs[col_dte])
    for c in [col_fs7, col_fs30, col_fs60]:
        if c:
            bs[c] = to_float_nullable(bs[c])

    bs[col_exp] = pd.to_datetime(bs[col_exp], errors="coerce").dt.date

    # nächstes Ablaufdatum je Symbol (kleinste DTE >=0)
    bs_valid = bs.loc[bs[col_dte].notna() & (bs[col_dte] >= 0)].copy()
    idx = bs_valid.groupby(col_sym_bs)[col_dte].idxmin()
    sel_cols = [col_sym_bs, col_exp, col_dte]
    rename_map = {col_sym_bs: "symbol", col_exp: "next_expiry", col_dte: "nearest_dte"}

    if col_fs7:  sel_cols.append(col_fs7);  rename_map[col_fs7]  = "focus_strike_7"
    if col_fs30: sel_cols.append(col_fs30); rename_map[col_fs30] = "focus_strike_30"
    if col_fs60: sel_cols.append(col_fs60); rename_map[col_fs60] = "focus_strike_60"

    bs_next = bs_valid.loc[idx, sel_cols].rename(columns=rename_map)
    # falls eine der Strike-Spalten fehlt, als NaN anlegen
    for c in ("focus_strike_7", "focus_strike_30", "focus_strike_60"):
        if c not in bs_next.columns:
            bs_next[c] = pd.NA

    bs_next["nearest_dte"] = to_int64_nullable(bs_next["nearest_dte"])
    for c in ("focus_strike_7", "focus_strike_30", "focus_strike_60"):
        bs_next[c] = to_float_nullable(bs_next[c])
    bs_next["next_expiry"] = pd.to_datetime(bs_next["next_expiry"], errors="coerce").dt.strftime("%Y-%m-%d")

    # 3) Join & Reihenfolge
    out = out_sig.merge(bs_next, on="symbol", how="left")
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
    with gzip.open(OUT_PATH, "wt", encoding="utf-8", newline="") as gz:
        df.to_csv(gz, index=False)
    print(f"wrote {OUT_PATH} rows={len(df)}")


if __name__ == "__main__":
    main()
