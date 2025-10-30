# --- util.py (Ergänzung) ---
from pathlib import Path
import csv as _csv

def _canon_symbol(s: str) -> str:
    """Erstes Token ohne Kommentar/Komma, getrimmt & upper."""
    if s is None:
        return ""
    s = str(s)
    s = s.split("#", 1)[0].split("//", 1)[0]  # Kommentare
    if "," in s:                               # CSV-Zeile → 1. Spalte
        s = s.split(",", 1)[0]
    s = s.strip()
    if not s:
        return ""
    return s.split()[0].upper()

def read_watchlists(root: str | Path) -> list[str]:
    """
    Liest alle *.txt/*.csv im Ordner `root`, säubert Ticker und gibt
    eine deduplizierte Liste (sorted) zurück.
    - TXT: eine Spalte (Ticker) mit optionalen Kommentarzeilen
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
            # still – eine defekte Datei überspringen
            continue
    return sorted(syms)
