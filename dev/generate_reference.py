#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Regenerate all reference catalogs (JSON + Python catalog files).

Usage:
    python dev/generate_reference.py

Called by:
    make reference
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

# Add repo root to path for non-installed dev builds
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


class _PlannerFromSource:
    """Import `opteryx.planner` from its .py files, never its compiled builds.

    The catalogs read pure data out of the planner (the binder's OPERATOR_MAP), and
    the planner is Python by contract - Cython compiles it unchanged. Reading the
    source means the catalogs describe the code being committed, not the last build,
    and it takes the planner's stale-build guard out of the way. That guard is what
    broke this hook: pre-commit stashes unstaged edits by rewriting those .py files,
    which makes them newer than their builds, so any unbuilt planner work anywhere
    in the tree - even someone else's - failed every commit.

    The .pyx-backed catalogs (operator_catalog, the function registrars) still come
    from the compiled build, so `make c` before committing changes to those."""

    _root = _repo_root / "opteryx" / "planner"

    def find_spec(self, name, path=None, target=None):
        if name != "opteryx.planner" and not name.startswith("opteryx.planner."):
            return None
        base = self._root.joinpath(*name.split(".")[2:])
        package = base / "__init__.py"
        if package.is_file():
            return importlib.util.spec_from_file_location(
                name, package, submodule_search_locations=[str(base)]
            )
        module = base.with_suffix(".py")
        if module.is_file():
            return importlib.util.spec_from_file_location(name, module)
        return None


os.environ["OPTERYX_PLANNER_FROM_SOURCE"] = "1"
sys.meta_path.insert(0, _PlannerFromSource())


def main() -> None:
    from reference.aggregate_catalog import write_aggregate_catalog
    from reference.clauses_catalog import write_clauses_catalog
    from reference.expression_catalog import write_expression_catalog
    from reference.joins_catalog import write_joins_catalog
    from reference.operator_catalog import write_operator_catalog
    from reference.signatures import write_function_signatures
    from reference.type_catalog import write_type_catalog
    from reference.unary_ops_catalog import write_unary_ops_catalog
    from reference.variables_catalog import write_variables_catalog
    from reference.window_catalog import write_window_catalog

    ref = _repo_root / "reference"

    write_function_signatures(ref / "function_signatures.json")
    write_aggregate_catalog(ref / "aggregates.json")
    write_clauses_catalog(ref / "clauses.json")
    write_expression_catalog(ref / "expressions.json")
    write_joins_catalog(ref / "joins.json")
    write_operator_catalog(ref / "operators.json")
    write_type_catalog(ref / "types.json")
    write_unary_ops_catalog(ref / "unary_ops.json")
    write_variables_catalog(ref / "variables.json")
    write_window_catalog(ref / "windows.json")

    print("All catalogs regenerated.")


if __name__ == "__main__":
    main()
