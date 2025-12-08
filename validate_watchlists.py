# scripts/validate_watchlists.py
from __future__ import annotations
from pathlib import Path
import csv, argparse, json
from collections import defaultdict
from util import _canon_symbol

def read_file_symbols(p: Path) -> list[str]:
    syms: list[str] = []
    if p.suffix.lower() == ".csv":
        with p.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            col = None
            if rdr.fieldnames:
                for c in rdr.fieldnames:
                    if c and c.strip().lower() in ("symbol", "ticker"):
                        col = c; break
                if col is None:
                    col = rdr.fieldnames[0]
            for row in rdr:
                t = _canon_symbol(row.get(col, ""))
                if t: syms.append(t)
    else:
        for ln in p.read_text(encoding="utf-8").splitlines():
            t = _canon_symbol(ln)
            if t and t.lower() not in ("symbol","ticker"):
                syms.append(t)
    return syms

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="watchlists")
    ap.add_argument("--report", default="")   # optional JSON-Report
    ap.add_argument("--top-dups", type=int, default=20)
    args = ap.parse_args()

    root = Path(args.root)
    files = sorted(list(root.glob("*.txt")) + list(root.glob("*.csv")))
    all_syms: set[str] = set()
    per_file_count: dict[str,int] = {}
    occurs: defaultdict[str, list[str]] = defaultdict(list)

    for p in files:
        cur = set(read_file_symbols(p))
        per_file_count[p.name] = len(cur)
        for t in cur:
            occurs[t].append(p.name)
        all_syms |= cur

    dups = {t: srcs for t, srcs in occurs.items() if len(srcs) > 1}

    print(f"TOTAL unique: {len(all_syms)}")
    for k in sorted(per_file_count):
        print(f"{k}: {per_file_count[k]}")

    if dups:
        print(f"Duplicates across files: {len(dups)}")
        # zeige einige Beispiele
        shown = 0
        for t, srcs in sorted(dups.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            if shown >= args.top_dups: break
            print(f"  {t}: {', '.join(srcs)}")
            shown += 1

    if args.report:
        rep = {
            "root": str(root),
            "total_unique": len(all_syms),
            "files": per_file_count,
            "duplicates_count": len(dups),
            "duplicates_examples": dict(list(sorted(dups.items(), key=lambda kv: (-len(kv[1]), kv[0])))[:args.top_dups]),
        }
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(json.dumps(rep, indent=2), encoding="utf-8")
        print(f"[report] → {args.report}")

if __name__ == "__main__":
    main()
