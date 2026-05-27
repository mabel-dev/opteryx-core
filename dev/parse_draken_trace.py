#!/usr/bin/env python3
import re
from collections import defaultdict

log_path = "dev/draken_free_trace.log"
with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
    data = f.read()

# Regex to capture trace blocks
malloc_re = re.compile(
    r"DRAKEN_MALLOC TRACE: ptr=(0x[0-9a-fA-F]+) req=(\d+) size=(\d+)\n(.*?)-- end DRAKEN_MALLOC TRACE --\n",
    re.S,
)
free_re = re.compile(
    r"DRAKEN_FREE TRACE: ptr=(0x[0-9a-fA-F]+) size=(\d+)\n(.*?)-- end DRAKEN_FREE TRACE --\n", re.S
)

mallocs = {}
for m in malloc_re.finditer(data):
    ptr, req, size, body = m.groups()
    mallocs.setdefault(ptr, []).append({"req": int(req), "size": int(size), "body": body.strip()})

frees = {}
for m in free_re.finditer(data):
    ptr, size, body = m.groups()
    frees.setdefault(ptr, []).append({"size": int(size), "body": body.strip()})

# Identify pointers that were both malloc'd and free'd
common = set(mallocs.keys()) & set(frees.keys())

out = []
for ptr in sorted(common):
    out.append(
        f"PTR {ptr}\n  MALLOC events: {len(mallocs[ptr])}\n  FREE events: {len(frees[ptr])}\n"
    )
    # show first malloc and free bodies
    out.append(
        "  First MALLOC stack:\n"
        + "\n".join("    " + l for l in mallocs[ptr][0]["body"].splitlines()[:30])
    )
    out.append(
        "  First FREE stack:\n"
        + "\n".join("    " + l for l in frees[ptr][0]["body"].splitlines()[:30])
    )
    out.append("\n")

report = "\n".join(out)
print(report)
# also write to file
with open("dev/draken_trace_summary.txt", "w") as f:
    f.write(report)
print("Wrote dev/draken_trace_summary.txt")
