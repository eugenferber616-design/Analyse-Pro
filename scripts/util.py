# scripts/util.py
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import os, json, yaml, csv as _csv

# ---------- Zeit/FS ----------
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(p: str | os.PathLike) -> None:
    os.makedirs(p, exist_ok=True)

# ---------- Config/ENV/JSON ----------
def read_yaml(path: str | os.PathLike):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_env(keys: list[str] | None = None) -> dict[str, str]:
    """Liest relevante ENV-Variablen; keys=None -> Standard-Keys."""
    if keys is None:
        keys = ["FINNHUB_TOKEN", "FINNHUB_API_KEY", "FRED_API_KEY", "OPENAI_API_KEY"]
    return {k: os.getenv(k, "") for k in keys}

def write_json(path: str | os.PathLike, obj) -> None:
    ensure_dir(os.path.dirname(str(path)) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def read_json(path: str | os.PathLike, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default

# ---------- Watchlist-Utilities ----------
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
