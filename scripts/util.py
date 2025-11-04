# scripts/util.py
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Dict, Any, List, Tuple, Union

import os, json, yaml, csv as _csv, gzip, io, sys
import pandas as pd

# =========================================================
# Zeit & Pfad
# =========================================================

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(p: str | os.PathLike) -> None:
    """Stellt sicher, dass ein Verzeichnis existiert."""
    Path(p).mkdir(parents=True, exist_ok=True)

def ensure_parent(path: str | os.PathLike) -> None:
    """Erzeugt das Elternverzeichnis einer Datei."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)

# =========================================================
# Logging (leichtgewichtig)
# =========================================================

def _log(tag: str, *msg):
    print(f"[{tag} {now_utc_iso()}]", *msg, flush=True)

def log_info(*msg):  _log("INFO", *msg)
def log_warn(*msg):  _log("WARN", *msg)
def log_error(*msg): _log("ERR ", *msg)

# =========================================================
# Config / ENV / JSON
# =========================================================

def read_yaml(path: str | os.PathLike):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_env(keys: list[str] | None = None) -> dict[str, str]:
    """
    Liest relevante ENV-Variablen; keys=None -> Standard-Keys.
    """
    if keys is None:
        keys = ["FINNHUB_TOKEN", "FINNHUB_API_KEY", "FRED_API_KEY", "OPENAI_API_KEY"]
    return {k: os.getenv(k, "") for k in keys}

def env_get(key: str, default: Optional[str] = None) -> str:
    v = os.getenv(key, "")
    return v if v != "" else (default if default is not None else "")

def env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None: return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(key: str, default: int = 0) -> int:
    try: return int(os.getenv(key, "").strip())
    except Exception: return default

def env_float(key: str, default: float = 0.0) -> float:
    try: return float(os.getenv(key, "").strip())
    except Exception: return default

def write_json(path: str | os.PathLike, obj) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def read_json(path: str | os.PathLike, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default

# =========================================================
# CSV / GZ Utilities (pandas-freundlich)
# =========================================================

def read_csv_any(path: str | os.PathLike, **kwargs) -> pd.DataFrame:
    """
    Liest CSV oder CSV.GZ automatisch. Setzt standardmäßig parse_dates=['date'] wenn vorhanden.
    """
    path = str(path)
    if "parse_dates" not in kwargs:
        kwargs["parse_dates"] = [c for c in ("date", "Date") if _has_date_column(path, c)]
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return pd.read_csv(f, **kwargs)
    else:
        return pd.read_csv(path, **kwargs)

def write_csv_gz(df: pd.DataFrame, path: str | os.PathLike, float_format: str = "%.6f") -> None:
    """
    Schreibt DataFrame als .csv.gz (UTF-8).
    """
    ensure_parent(path)
    with gzip.open(str(path), "wt", encoding="utf-8", newline="") as gz:
        df.to_csv(gz, index=True, float_format=float_format)

def _has_date_column(path: str, candidate: str) -> bool:
    try:
        if path.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                head = "".join([next(f) for _ in range(1)])
        else:
            with open(path, "r", encoding="utf-8") as f:
                head = f.readline()
        cols = [c.strip() for c in head.split(",")]
        return candidate in cols
    except Exception:
        return False

# =========================================================
# Watchlist-Utilities (unverändert + kleine Robustheit)
# =========================================================

def _canon_symbol(s: str) -> str:
    """Erstes Token ohne Kommentar/Komma, getrimmt & UPPER."""
    if s is None:
        return ""
    s = str(s)
    s = s.split("#", 1)[0].split("//", 1)[0]  # Kommentare entfernen
    if "," in s:
        s = s.split(",", 1)[0]                 # CSV: erste Spalte
    s = s.strip()
    if not s:
        return ""
    return s.split()[0].upper()

def read_watchlists(root: str | Path) -> list[str]:
    """
    Liest alle *.txt/*.csv im Ordner `root`, säubert Ticker und gibt eine
    deduplizierte, sortierte Liste zurück.
    - TXT: eine Spalte mit optionalen Kommentarzeilen
    - CSV: bevorzugt Spalte 'symbol'/'ticker', sonst erste Spalte
    """
    root = Path(root)
    syms, seen = [], set()
    files = list(root.glob("*.txt")) + list(root.glob("*.csv"))
    for p in files:
        try:
            if p.suffix.lower() == ".csv":
                with p.open("r", encoding="utf-8", newline="") as f:
                    rdr = _csv.DictReader(f)
                    col = None
                    if rdr.fieldnames:
                        for c in rdr.fieldnames:
                            if c and c.strip().lower() in ("symbol", "ticker"):
                                col = c; break
                        if col is None:
                            col = rdr.fieldnames[0]
                    for row in rdr:
                        t = _canon_symbol(row.get(col, ""))
                        if t and t not in seen:
                            seen.add(t); syms.append(t)
            else:
                for ln in p.read_text(encoding="utf-8").splitlines():
                    t = _canon_symbol(ln)
                    if t and t.lower() not in ("symbol", "ticker") and t not in seen:
                        seen.add(t); syms.append(t)
        except Exception:
            # defekte Datei überspringen
            continue
    return sorted(syms)

# =========================================================
# Time-Series Helpers
# =========================================================

def ensure_date_index(df: pd.DataFrame, date_col: str = "date") -> pd.DataFrame:
    """Stellt sicher, dass der Index ein Datumsindex ist (naiv)."""
    if date_col in df.columns:
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], utc=False, errors="coerce")
        df = df.set_index(date_col)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=False, errors="coerce")
    df.index = df.index.tz_localize(None)
    return df

def to_daily_ffill(df: pd.DataFrame) -> pd.DataFrame:
    """Auf tägliche Frequenz bringen und vorwärts auffüllen."""
    if df.empty:
        return df
    df = ensure_date_index(df)
    rng = pd.date_range(df.index.min(), df.index.max(), freq="D")
    return df.reindex(rng).ffill()

def merge_on_date(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Horizontales Mergen mehrerer DataFrames auf Tagesindex (inner-join auf gemeinsames Intervall)."""
    cleaned = [ensure_date_index(d) for d in dfs if d is not None and not d.empty]
    if not cleaned:
        return pd.DataFrame()
    out = cleaned[0]
    for d in cleaned[1:]:
        out = out.join(d, how="outer")
    out = out.sort_index()
    return out

# =========================================================
# Guards / Reports
# =========================================================

def require_any(paths: Iterable[Union[str, os.PathLike]]) -> bool:
    """True, sobald mindestens eine Datei existiert und >0 Bytes hat."""
    ok = False
    for p in paths:
        p = Path(p)
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            ok = True
            break
    return ok

def save_report(path: str | os.PathLike, status: Dict[str, Any]) -> None:
    """Kleiner Helfer für JSON-Reports (mit Zeitstempel)."""
    status = dict(status)
    status["ts"] = now_utc_iso()
    write_json(path, status)

# =========================================================
# Kleinkram
# =========================================================

def head_csv_gz(path: str | os.PathLike, n: int = 5) -> str:
    """Gibt die ersten n Zeilen einer csv.gz als String zurück (für Logs)."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            lines = []
            for i, ln in enumerate(f):
                lines.append(ln.rstrip("\n"))
                if i + 1 >= n:
                    break
            return "\n".join(lines)
    except Exception as e:
        return f"<cannot read {path}: {e}>"

def list_dir(path: str | os.PathLike) -> List[str]:
    try:
        return [p.name for p in sorted(Path(path).iterdir())]
    except Exception:
        return []
