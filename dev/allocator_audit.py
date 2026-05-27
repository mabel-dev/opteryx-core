#!/usr/bin/env python3
import os
import re

root = "."
report = []

for dirpath, dirnames, filenames in os.walk(root):
    # skip build dirs
    if "/.git" in dirpath or "/build" in dirpath or "/dist" in dirpath:
        continue
    for fn in filenames:
        if not fn.endswith((".pyx", ".pxi", ".py")):
            continue
        path = os.path.join(dirpath, fn)
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                txt = f.read()
        except Exception:
            continue
        has_malloc = "malloc(" in txt
        has_draken_own = (
            "draken_vector_own" in txt or "from_decoded(" in txt or "_vector_from_decoded" in txt
        )
        if has_malloc:
            report.append((path, has_malloc, has_draken_own))

with open("dev/allocator_audit.txt", "w") as out:
    out.write("path,has_malloc,has_draken_own_or_from_decoded\n")
    for p, m, d in sorted(report):
        out.write(f"{p},{m},{d}\n")

print("Wrote dev/allocator_audit.txt")
