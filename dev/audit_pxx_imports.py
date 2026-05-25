"""
E30a — Audit: Python imports inside cdef/cpdef bodies in .pyx/.pxi files.

Walks opteryx/ and rugo/ (excluding tests/, scratch/, dev/).
For each file, tracks indent depth and function-def context to find
import statements inside cdef/cpdef bodies.

Outputs a structured report suitable for pasting into the design doc.
"""

import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

# Matches a cdef / cpdef function or method definition line.
# We capture the indentation and the function name.
FUNC_DEF_RE = re.compile(
    r'^(?P<indent>\s*)'
    r'(?:inline\s+)?(?:cdef|cpdef)\b'
    r'(?:\s+[^\(]+?)?\s+'           # optional return type(s)
    r'(?P<name>[A-Za-z_][A-Za-z0-9_]*)'
    r'\s*\('
)

# Also matches 'cdef class' — we want to NOT treat 'cdef class Foo:' as a func def.
CDEF_CLASS_RE = re.compile(r'^\s*cdef\s+class\b')

# Matches import statements.
IMPORT_RE = re.compile(r'^\s*(import\s+\S|from\s+\S+\s+import\b)')

# Checks for noexcept / nogil / inline on the def line (A-category signals).
HOT_SIGNALS_RE = re.compile(r'\b(inline|noexcept|nogil)\b')

# Names suggesting hot-path / per-row / per-morsel execution.
HOT_NAME_RE = re.compile(
    r'(^_push|^compare_|^vector_|^finalise|^collect|^_collect|'
    r'^_push_impl|^push_row|^process_row|^_process|^hash_into|'
    r'^compress_into|^emit|^consume|^accumulate|^update_|^_update)',
    re.IGNORECASE,
)

# Node types that have hot execute / next methods.
HOT_CLASS_CONTEXT_RE = re.compile(
    r'BasePlanNode|CollectorNode|Collector|Aggregate|GroupedAgg',
    re.IGNORECASE,
)

# Init-time method names.
INIT_NAME_RE = re.compile(
    r'^(__cinit__|__init__|bind|compile|__setup__|setup|_setup|'
    r'initialise|initialize|_init|from_arrow|from_morsel)$',
    re.IGNORECASE,
)


def classify(func_name: str, def_line: str, func_class: str, filepath: str) -> str:
    """Return category A / B / C / ? for a finding."""
    # (C) — deferred-import comment heuristic: if same line or nearby has a comment
    # explaining circular import — handled at call site, not here.

    # (A) hot-path signals
    if HOT_SIGNALS_RE.search(def_line):
        return "A"
    if HOT_NAME_RE.match(func_name):
        return "A"
    # collectors pxi — ALL methods in collectors are hot-path
    if "_collectors_" in filepath:
        return "A"
    # vector_ops
    if "vector_ops" in filepath or "/vector_" in filepath:
        return "A"

    # (B) init-time
    if INIT_NAME_RE.match(func_name):
        return "B"

    # class context clues
    if func_class and HOT_CLASS_CONTEXT_RE.search(func_class):
        return "A"

    return "?"


# ---------------------------------------------------------------------------
# File scanner
# ---------------------------------------------------------------------------

def scan_file(path: Path):
    """
    Yield dicts for each import-inside-cdef/cpdef found.
    Keys: lineno, func_name, func_def_line, func_class, import_stmt, def_line_text
    """
    try:
        src = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return

    lines = src.splitlines()

    # Stack of (indent_len, func_name, def_line_text, class_name)
    # representing active enclosing cdef/cpdef functions.
    func_stack: list[tuple[int, str, str, str]] = []

    # Track current class name (for context classification).
    current_class: str = ""

    for lineno, raw in enumerate(lines, start=1):
        # Strip trailing whitespace only; keep leading for indent measurement.
        line = raw.rstrip()

        # Skip blank lines and pure-comment lines.
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(stripped)

        # Pop function stack for anything at same or shallower indent.
        func_stack = [f for f in func_stack if f[0] < indent]

        # Track class context (class / cdef class).
        class_match = re.match(r'^\s*(?:cdef\s+)?class\s+([A-Za-z_][A-Za-z0-9_]*)', line)
        if class_match:
            current_class = class_match.group(1)

        # Check for cdef/cpdef function definitions (not class).
        if not CDEF_CLASS_RE.match(line):
            m = FUNC_DEF_RE.match(line)
            if m:
                func_stack.append((indent, m.group("name"), line, current_class))

        # Check for import statement.
        if IMPORT_RE.match(line) and func_stack:
            # We're inside at least one cdef/cpdef body.
            enclosing_indent, func_name, def_line_text, func_class = func_stack[-1]
            yield {
                "lineno": lineno,
                "func_name": func_name,
                "func_def_line": def_line_text.strip(),
                "func_class": func_class,
                "import_stmt": stripped,
                "def_line_text": def_line_text,
            }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ROOTS = ["opteryx", "rugo"]
DRAKEN_ROOTS = ["draken"]
EXCLUDE_DIRS = {"tests", "scratch", "dev", "__pycache__"}


def collect_files(roots):
    for root in roots:
        p = Path(root)
        if not p.exists():
            continue
        for f in sorted(p.rglob("*.pyx")) + sorted(p.rglob("*.pxi")):
            # Exclude non-production trees.
            parts = set(f.parts)
            if parts & EXCLUDE_DIRS:
                continue
            yield f


def run():
    findings = []
    draken_findings = []

    for path in collect_files(ROOTS):
        for hit in scan_file(path):
            hit["file"] = str(path)
            findings.append(hit)

    for path in collect_files(DRAKEN_ROOTS):
        for hit in scan_file(path):
            hit["file"] = str(path)
            draken_findings.append(hit)

    return findings, draken_findings


def format_report(findings, draken_findings):
    rows = []
    for i, h in enumerate(findings, start=1):
        cat = classify(
            h["func_name"],
            h["def_line_text"],
            h["func_class"],
            h["file"],
        )
        # Check for circular-import comment clue (C heuristic).
        if cat in ("A", "B", "?"):
            # If the import line itself has a "circular" or "deferred" comment, upgrade to C.
            if re.search(r'#.*(circular|deferred|defer|avoid.*import|import.*avoid)',
                         h["import_stmt"], re.IGNORECASE):
                cat = "C"
        rows.append({**h, "cat": cat, "seq": i})

    # Sort: A first, then B, then ?, then C.
    cat_order = {"A": 0, "B": 1, "?": 2, "C": 3}
    rows.sort(key=lambda r: (cat_order.get(r["cat"], 9), r["file"], r["lineno"]))
    # Re-number after sort.
    for i, r in enumerate(rows, start=1):
        r["seq"] = i

    lines = []
    lines.append("# E30a — Python Imports Inside `cdef`/`cpdef` Bodies: Audit Report")
    lines.append("")
    lines.append("> **Status:** Complete (read-only audit, no code changes).")
    lines.append("> **Scope:** `opteryx/**/*.pyx`, `opteryx/**/*.pxi`, `rugo/**/*.pyx`, `rugo/**/*.pxi`")
    lines.append("> **Excluded:** `tests/`, `scratch/`, `dev/`, generated `.c`/`.cpp` files")
    lines.append("")
    lines.append("## Findings Table")
    lines.append("")
    lines.append("| # | File | Line | Containing function/method | Import statement (verbatim) | Category | Notes |")
    lines.append("|---|------|------|----------------------------|----------------------------|----------|-------|")

    for r in rows:
        file_short = r["file"].replace("opteryx/", "opteryx/").replace("rugo/", "rugo/")
        func_display = r["func_name"]
        if r["func_class"]:
            func_display = f"{r['func_class']}.{r['func_name']}"
        import_escaped = r["import_stmt"].replace("|", "\\|")
        lines.append(
            f"| {r['seq']} | `{file_short}` | {r['lineno']} "
            f"| `{func_display}` "
            f"| `{import_escaped}` "
            f"| **{r['cat']}** | |"
        )

    # Draken section.
    if draken_findings:
        lines.append("")
        lines.append("### Draken `.pyx`/`.pxi` Findings (separate section per §2)")
        lines.append("")
        lines.append("| # | File | Line | Containing function/method | Import statement (verbatim) | Category | Notes |")
        lines.append("|---|------|------|----------------------------|----------------------------|----------|-------|")
        for i, h in enumerate(draken_findings, start=1):
            cat = classify(h["func_name"], h["def_line_text"], h["func_class"], h["file"])
            file_short = h["file"]
            func_display = h["func_name"]
            if h["func_class"]:
                func_display = f"{h['func_class']}.{h['func_name']}"
            import_escaped = h["import_stmt"].replace("|", "\\|")
            lines.append(
                f"| {i} | `{file_short}` | {h['lineno']} "
                f"| `{func_display}` "
                f"| `{import_escaped}` "
                f"| **{cat}** | |"
            )
    else:
        lines.append("")
        lines.append("### Draken `.pyx`/`.pxi` Findings")
        lines.append("")
        lines.append("No findings. (Expected: draken is C++-first with no Python imports in compiled code.)")

    # Summary.
    from collections import Counter
    cat_counts = Counter(r["cat"] for r in rows)
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- **Total findings:** {len(rows)}")
    lines.append(f"- **(A) Hot-path:** {cat_counts.get('A', 0)}")
    lines.append(f"- **(B) Init-time / once-per-query:** {cat_counts.get('B', 0)}")
    lines.append(f"- **(C) Defensible deferred:** {cat_counts.get('C', 0)}")
    lines.append(f"- **(?) Ambiguous:** {cat_counts.get('?', 0)}")
    if draken_findings:
        lines.append(f"- **Draken findings:** {len(draken_findings)}")
    else:
        lines.append(f"- **Draken findings:** 0")
    lines.append("")

    # Print raw data for architect review.
    return "\n".join(lines), rows, draken_findings


if __name__ == "__main__":
    import os
    # Run from repo root.
    repo_root = Path(__file__).parent.parent
    os.chdir(repo_root)

    findings, draken_findings = run()
    report_text, rows, _ = format_report(findings, draken_findings)

    print(report_text)
    print(f"\n--- RAW: {len(findings)} opteryx/rugo findings, {len(draken_findings)} draken findings ---")

    # Also print raw findings for review.
    if "--raw" in sys.argv:
        for r in findings:
            print(f"  {r['file']}:{r['lineno']} [{r['func_name']}] {r['import_stmt']}")
