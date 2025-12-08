#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_sentiment_scores.py
-------------------------
Baut einen Player-Sentiment-Score pro Symbol aus:

- options_oi_summary.csv   (Options-/OI-Bias: Calls vs. Puts, Magnet)
- whale_alerts.csv         (Whale-Flow: mehr CALL- oder PUT-Flows?)
- short_interest.csv       (nur Borrow-Rate von iBorrowDesk)

Output:
  data/processed/sentiment_scores.csv

Spalten:
  symbol
  sentiment_score     (0-100, 0=bearish, 100=bullish)
  sentiment_label     ("Bullish", "Neutral", "Bearish")
  options_score       (0-100)
  whale_score         (0-100)
  borrow_score        (0-100)
  has_options_data    (True/False)
  has_whale_data      (True/False)
  has_borrow_data     (True/False)
"""

import os
from pathlib import Path
from typing import Optional, List, Dict, Any

import numpy as np
import pandas as pd

BASE = Path("data/processed")


# ------------------------------------------------------------
# Helper
# ------------------------------------------------------------

def _read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"[sentiment] WARN: Konnte {path} nicht lesen: {e}")
        return None


def _norm_symbol(s: Any) -> str:
    try:
        return str(s).strip().upper()
    except Exception:
        return str(s)


def _get_num(row: pd.Series, candidates: List[str]) -> float:
    for c in candidates:
        if c in row.index:
            try:
                v = row[c]
                if pd.isna(v):
                    continue
                return float(v)
            except Exception:
                continue
    return np.nan


# ------------------------------------------------------------
# Options-basierter Score
# ------------------------------------------------------------

def compute_options_scores(df_opt: Optional[pd.DataFrame]) -> Dict[str, float]:
    """
    Liefert pro Symbol einen Options-Score (0-100).
    Basis: Call/Put-OI-Imbalance + Magnet-Typ.
    """
    if df_opt is None:
        return {}

    df = df_opt.copy()
    if "symbol" not in df.columns:
        # Spalte raten
        df.rename(columns={df.columns[0]: "symbol"}, inplace=True)
    df["symbol"] = df["symbol"].apply(_norm_symbol)

    scores: Dict[str, float] = {}

    # Falls mehrere Zeilen pro Symbol: nimm die mit größtem oi_total / total_oi
    if "oi_total" in df.columns:
        df["_oi_total_tmp"] = pd.to_numeric(df["oi_total"], errors="coerce")
    elif "total_oi" in df.columns:
        df["_oi_total_tmp"] = pd.to_numeric(df["total_oi"], errors="coerce")
    else:
        df["_oi_total_tmp"] = np.nan

    for sym, grp in df.groupby("symbol"):
        g = grp.copy()

        # Zeile mit maximalem OI bevorzugen
        if g["_oi_total_tmp"].notna().any():
            row = g.sort_values("_oi_total_tmp", ascending=False).iloc[0]
        else:
            row = g.iloc[0]

        call_oi = _get_num(row, ["total_call_oi", "call_oi", "calls_oi"])
        put_oi  = _get_num(row, ["total_put_oi", "put_oi", "puts_oi"])

        if not np.isfinite(call_oi):
            call_oi = 0.0
        if not np.isfinite(put_oi):
            put_oi = 0.0

        tot = call_oi + put_oi
        if tot <= 0:
            options_score = 50.0  # neutral
        else:
            imbalance = (call_oi - put_oi) / tot  # -1 .. +1
            options_score = 50.0 + 50.0 * imbalance  # 0..100

        # Magnet-Typ leicht einbeziehen
        magnet_type = str(row.get("magnet_type", "")).upper()
        if magnet_type == "CALL":
            options_score += 5.0
        elif magnet_type == "PUT":
            options_score -= 5.0

        options_score = float(np.clip(options_score, 0.0, 100.0))
        scores[sym] = options_score

    return scores


# ------------------------------------------------------------
# Whale-Flow-Score
# ------------------------------------------------------------

def compute_whale_scores(df_whale: Optional[pd.DataFrame]) -> Dict[str, float]:
    """
    Whale-Flow: wie viele CALL- vs. PUT-Alarme?
    """
    if df_whale is None:
        return {}

    df = df_whale.copy()
    if "symbol" not in df.columns:
        df.rename(columns={df.columns[0]: "symbol"}, inplace=True)
    df["symbol"] = df["symbol"].apply(_norm_symbol)

    type_col = None
    for c in ["type", "option_type", "side"]:
        if c in df.columns:
            type_col = c
            break
    if type_col is None:
        return {}

    scores: Dict[str, float] = {}

    for sym, grp in df.groupby("symbol"):
        calls = 0
        puts = 0
        for _, r in grp.iterrows():
            t = str(r.get(type_col, "")).upper()
            if "CALL" in t:
                calls += 1
            elif "PUT" in t:
                puts += 1

        tot = calls + puts
        if tot == 0:
            whale_score = 50.0
        else:
            flow = (calls - puts) / float(tot)  # -1..+1
            whale_score = 50.0 + 50.0 * flow

        whale_score = float(np.clip(whale_score, 0.0, 100.0))
        scores[sym] = whale_score

    return scores


# ------------------------------------------------------------
# Borrow-Score (Fee -> Bearishness)
# ------------------------------------------------------------

def compute_borrow_scores(df_borrow: Optional[pd.DataFrame]) -> Dict[str, float]:
    """
    Borrow-Rate: hohe Gebühren = bearish, niedrige = eher neutral/bullish.
    0% Fee  -> ~100
    30% Fee -> ~0
    """
    if df_borrow is None:
        return {}

    df = df_borrow.copy()
    if "symbol" not in df.columns:
        df.rename(columns={df.columns[0]: "symbol"}, inplace=True)
    df["symbol"] = df["symbol"].apply(_norm_symbol)

    scores: Dict[str, float] = {}

    for sym, grp in df.groupby("symbol"):
        row = grp.iloc[0]
        rate = _get_num(row, ["borrow_rate", "fee", "ibd_fee"])

        if not np.isfinite(rate):
            borrow_score = 50.0
        else:
            r = float(np.clip(rate, 0.0, 30.0))
            borrow_score = 100.0 - (r / 30.0) * 100.0

        borrow_score = float(np.clip(borrow_score, 0.0, 100.0))
        scores[sym] = borrow_score

    return scores


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    BASE.mkdir(parents=True, exist_ok=True)

    df_opt    = _read_csv(BASE / "options_oi_summary.csv")
    df_whale  = _read_csv(BASE / "whale_alerts.csv")
    df_borrow = _read_csv(BASE / "short_interest.csv")

    if df_opt is None and df_whale is None and df_borrow is None:
        print("[sentiment] Keine Input-Dateien gefunden (options_oi_summary / whale_alerts / short_interest).")
        out = BASE / "sentiment_scores.csv"
        pd.DataFrame(columns=[
            "symbol","sentiment_score","sentiment_label",
            "options_score","whale_score","borrow_score",
            "has_options_data","has_whale_data","has_borrow_data"
        ]).to_csv(out, index=False)
        print(f"[sentiment] wrote EMPTY {out}")
        return

    opt_scores    = compute_options_scores(df_opt)
    whale_scores  = compute_whale_scores(df_whale)
    borrow_scores = compute_borrow_scores(df_borrow)

    # Alle Symbole aus allen Quellen
    symbols = set(opt_scores.keys()) | set(whale_scores.keys()) | set(borrow_scores.keys())
    symbols = sorted(symbols)

    rows = []

    for sym in symbols:
        o = opt_scores.get(sym)
        w = whale_scores.get(sym)
        b = borrow_scores.get(sym)

        has_opt    = o is not None
        has_whale  = w is not None
        has_borrow = b is not None

        # Gewichte, nur auf vorhandene Komponenten verteilen
        parts = []
        weights = []

        if has_opt:
            parts.append(o)
            weights.append(0.5)
        if has_whale:
            parts.append(w)
            weights.append(0.3)
        if has_borrow:
            parts.append(b)
            weights.append(0.2)

        if not parts:
            # Fallback: komplett neutral
            sentiment_score = 50.0
        else:
            w_sum = sum(weights)
            # Normieren, falls z.B. Borrow fehlt
            normed = [w_i / w_sum for w_i in weights]
            sentiment_score = float(np.clip(
                sum(p * w_n for p, w_n in zip(parts, normed)),
                0.0, 100.0
            ))

        if sentiment_score >= 60.0:
            label = "Bullish"
        elif sentiment_score <= 40.0:
            label = "Bearish"
        else:
            label = "Neutral"

        rows.append({
            "symbol": _norm_symbol(sym),
            "sentiment_score": round(sentiment_score, 2),
            "sentiment_label": label,
            "options_score": round(o, 2) if o is not None else np.nan,
            "whale_score": round(w, 2) if w is not None else np.nan,
            "borrow_score": round(b, 2) if b is not None else np.nan,
            "has_options_data": bool(has_opt),
            "has_whale_data": bool(has_whale),
            "has_borrow_data": bool(has_borrow),
        })

    out_df = pd.DataFrame(rows)
    out_path = BASE / "sentiment_scores.csv"
    out_df.to_csv(out_path, index=False)
    print(f"[sentiment] wrote {out_path} rows={len(out_df)}")


if __name__ == "__main__":
    main()
