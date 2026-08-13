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

import sys
from pathlib import Path

# Add repo root to path for non-installed dev builds
_repo_root = Path(__file__).resolve().parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


def main() -> None:
    from reference.aggregate_catalog import write_aggregate_catalog
    from reference.clauses_catalog import write_clauses_catalog
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
    write_joins_catalog(ref / "joins.json")
    write_operator_catalog(ref / "operators.json")
    write_type_catalog(ref / "types.json")
    write_unary_ops_catalog(ref / "unary_ops.json")
    write_variables_catalog(ref / "variables.json")
    write_window_catalog(ref / "windows.json")

    print("All catalogs regenerated.")


if __name__ == "__main__":
    main()
