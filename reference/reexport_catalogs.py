"""CLI helpers for rewriting generated reference catalogs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(1, str(Path(__file__).resolve().parents[2]))
    from opteryx.expression.functions.signatures import write_function_signatures
    from opteryx.reference.aggregate_catalog import write_aggregate_catalog
    from opteryx.reference.clauses_catalog import write_clauses_catalog
    from opteryx.reference.joins_catalog import write_joins_catalog
    from opteryx.reference.operator_catalog import write_operator_catalog
    from opteryx.reference.type_catalog import write_type_catalog
    from opteryx.reference.unary_ops_catalog import write_unary_ops_catalog
else:
    from opteryx.expression.functions.signatures import write_function_signatures

    from .aggregate_catalog import write_aggregate_catalog
    from .clauses_catalog import write_clauses_catalog
    from .joins_catalog import write_joins_catalog
    from .operator_catalog import write_operator_catalog
    from .type_catalog import write_type_catalog
    from .unary_ops_catalog import write_unary_ops_catalog


def reexport_reference_catalogs(base_path: str | Path | None = None) -> dict[str, Path]:
    root = Path(base_path) if base_path is not None else Path(__file__).resolve().parents[2]

    output_paths = {
        "aggregates": root / "opteryx/reference/aggregates.json",
        "clauses": root / "opteryx/reference/clauses.json",
        "joins": root / "opteryx/reference/joins.json",
        "operators": root / "opteryx/reference/operators.json",
        "unary_ops": root / "opteryx/reference/unary_ops.json",
        "types": root / "opteryx/reference/types.json",
        "functions": root / "opteryx/expression/functions/function_signatures.json",
    }

    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    write_aggregate_catalog(output_paths["aggregates"])
    write_clauses_catalog(output_paths["clauses"])
    write_joins_catalog(output_paths["joins"])
    write_operator_catalog(output_paths["operators"])
    write_unary_ops_catalog(output_paths["unary_ops"])
    write_type_catalog(output_paths["types"])
    write_function_signatures(output_paths["functions"])

    return output_paths


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rewrite the generated aggregate, clause, join, operator, unary-op, type, and function catalogs."
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=None,
        help="Repository root to write catalogs into. Defaults to the current project root.",
    )
    args = parser.parse_args()

    output_paths = reexport_reference_catalogs(args.base_path)
    for name, output_path in output_paths.items():
        print(f"{name}: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
