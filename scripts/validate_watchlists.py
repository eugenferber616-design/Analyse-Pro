from pathlib import Path
from util import read_watchlists, _canon_symbol

root = Path("watchlists")
all_syms = set()
dups = set()
per_file = {}

for p in list(root.glob("*.txt")) + list(root.glob("*.csv")):
    cur = set()
    for ln in p.read_text(encoding="utf-8").splitlines():
        t = _canon_symbol(ln)
        if t: cur.add(t)
    per_file[p.name] = len(cur)
    for t in cur:
        if t in all_syms: dups.add(t)
        all_syms.add(t)

print(f"TOTAL unique: {len(all_syms)}")
for k, v in sorted(per_file.items()):
    print(f"{k}: {v}")
if dups:
    print(f"Duplicates across files: {len(dups)}")
