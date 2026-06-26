#!/usr/bin/env python3
"""
Write measured function costs back into the catalog source.

Reads the JSON produced by ``estimate_function_costs.py`` and updates the
``cost`` / ``cost_us_per_million`` literals in
``opteryx/expression/functions/registrar/*.pyx``.

Costs live in two forms in the registrars, both handled here, keyed by the
overload id the estimator measured:

  * ``_make("UPPER", ..., cost=142.0)``          -> overload id "UPPER_default"
  * ``FunctionOverload(id="LENGTH_string", ...,  -> overload id "LENGTH_string"
        kernel=KernelSpec(..., cost_us_per_million=221.0))``

Matching is done with the Python AST (the leaf registrar files are plain Python),
so a cost literal is located by its exact source span — no brittle regex. Only
successful measurements are applied. Anything that can't be matched in either
direction is reported loudly; nothing is changed silently.

Usage:
    python import_function_costs.py function_costs.json            # dry-run preview
    python import_function_costs.py function_costs.json --apply    # write the files
"""

import argparse
import ast
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REGISTRAR_DIR = Path(
    os.path.join(os.path.dirname(__file__), "..", "opteryx", "expression", "functions", "registrar")
).resolve()


def _format_cost(value: float) -> str:
    """Render a cost literal the way the registrars do (always a float)."""
    if value < 1:
        return f"{value:.3f}"
    return f"{value:.2f}"


def _const_number(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(
        node.value, bool
    )


def _kw(call: ast.Call, name: str) -> Optional[ast.keyword]:
    for k in call.keywords:
        if k.arg == name:
            return k
    return None


class CostLocation:
    """A single cost literal in a source file, tagged with its overload id."""

    __slots__ = ("overload_id", "lineno", "col", "end_col", "old_text")

    def __init__(self, overload_id: str, value_node: ast.Constant, source_lines: List[str]):
        self.overload_id = overload_id
        self.lineno = value_node.lineno
        self.col = value_node.col_offset
        self.end_col = value_node.end_col_offset
        self.old_text = source_lines[self.lineno - 1][self.col : self.end_col]


def _find_kernelspec(node: ast.AST) -> Optional[ast.Call]:
    """Return the KernelSpec(...) call nested anywhere under `node`."""
    for child in ast.walk(node):
        if isinstance(child, ast.Call) and isinstance(child.func, ast.Name) and child.func.id == "KernelSpec":
            return child
    return None


def locate_costs(path: Path) -> List[CostLocation]:
    """Find every cost literal in a registrar file, tagged with its overload id."""
    src = path.read_text()
    lines = src.splitlines(keepends=True)
    tree = ast.parse(src)
    found: List[CostLocation] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue

        if node.func.id == "_make":
            if not node.args or not _is_str_const(node.args[0]):
                continue
            name = node.args[0].value
            cost_kw = _kw(node, "cost")
            if cost_kw is not None and _const_number(cost_kw.value):
                found.append(CostLocation(f"{name}_default", cost_kw.value, lines))

        elif node.func.id == "FunctionOverload":
            id_kw = _kw(node, "id")
            if id_kw is None or not _is_str_const(id_kw.value):
                continue
            overload_id = id_kw.value.value
            kernel = _find_kernelspec(node)
            if kernel is None:
                continue
            cost_kw = _kw(kernel, "cost_us_per_million")
            if cost_kw is not None and _const_number(cost_kw.value):
                found.append(CostLocation(overload_id, cost_kw.value, lines))

    return found


def _is_str_const(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def load_new_costs(costs_file: Path) -> Dict[str, float]:
    """overload_id -> measured cost, for successful measurements only."""
    data = json.loads(costs_file.read_text())
    out: Dict[str, float] = {}
    for overloads in data.get("functions", {}).values():
        for ov in overloads:
            if ov.get("success") and ov.get("cost_us_per_million") is not None:
                out[ov["overload_id"]] = float(ov["cost_us_per_million"])
    return out


def apply_file(path: Path, new_costs: Dict[str, float], apply: bool) -> Tuple[List[dict], List[str]]:
    """Return (updates, matched_overload_ids) for one file; writes it if `apply`."""
    locations = locate_costs(path)
    src_lines = path.read_text().splitlines(keepends=True)
    updates: List[dict] = []
    matched: List[str] = []

    # Apply edits last-to-first so earlier spans stay valid.
    edits: List[Tuple[int, int, int, str]] = []  # (lineno, col, end_col, new_text)
    for loc in locations:
        if loc.overload_id not in new_costs:
            continue
        matched.append(loc.overload_id)
        new_text = _format_cost(new_costs[loc.overload_id])
        if new_text == loc.old_text:
            continue
        edits.append((loc.lineno, loc.col, loc.end_col, new_text))
        updates.append(
            {
                "file": path.name,
                "overload_id": loc.overload_id,
                "old": loc.old_text,
                "new": new_text,
            }
        )

    if apply and edits:
        for lineno, col, end_col, new_text in sorted(edits, reverse=True):
            line = src_lines[lineno - 1]
            src_lines[lineno - 1] = line[:col] + new_text + line[end_col:]
        path.write_text("".join(src_lines))

    return updates, matched


def main() -> int:
    parser = argparse.ArgumentParser(description="Write measured costs into the registrars.")
    parser.add_argument("costs_file", type=Path)
    parser.add_argument("--apply", action="store_true", help="Write changes (default: preview only).")
    args = parser.parse_args()

    if not args.costs_file.exists():
        print(f"error: cost file not found: {args.costs_file}")
        return 1

    new_costs = load_new_costs(args.costs_file)
    print(f"loaded {len(new_costs)} measured cost(s) from {args.costs_file}\n")

    all_updates: List[dict] = []
    all_matched: set = set()
    for path in sorted(REGISTRAR_DIR.glob("*.pyx")):
        if path.name == "__init__.pyx":
            continue  # the _make helper's own default literals live here — never touch
        updates, matched = apply_file(path, new_costs, args.apply)
        all_matched.update(matched)
        all_updates.extend(updates)

    for u in sorted(all_updates, key=lambda d: (d["file"], d["overload_id"])):
        print(f"  {u['file']:<20} {u['overload_id']:<26} {u['old']:>12} -> {u['new']}")

    # Loud about anything we measured but could not place in source.
    unmatched = sorted(set(new_costs) - all_matched)
    print("\n" + "=" * 72)
    print(f"{len(all_updates)} literal(s) {'updated' if args.apply else 'to update'}; "
          f"{len(all_matched)} overload(s) matched in source")
    if unmatched:
        print("-" * 72)
        print(f"WARNING: {len(unmatched)} measured overload(s) had no source literal to update:")
        for oid in unmatched:
            print(f"  {oid}")

    if not args.apply:
        print("\n(dry-run — re-run with --apply to write the files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
