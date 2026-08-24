"""CLI helpers for rewriting generated reference catalogs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(1, str(Path(__file__).resolve().parents[1]))
    from reference.signatures import write_function_signatures
    from reference.aggregate_catalog import write_aggregate_catalog
    from reference.clauses_catalog import write_clauses_catalog
    from reference.expression_catalog import write_expression_catalog
    from reference.joins_catalog import write_joins_catalog
    from reference.operator_catalog import write_operator_catalog
    from reference.type_catalog import write_type_catalog
    from reference.unary_ops_catalog import write_unary_ops_catalog
    from reference.variables_catalog import write_variables_catalog
    from reference.window_catalog import write_window_catalog
else:
    from .signatures import write_function_signatures
    from .aggregate_catalog import write_aggregate_catalog
    from .clauses_catalog import write_clauses_catalog
    from .expression_catalog import write_expression_catalog
    from .joins_catalog import write_joins_catalog
    from .operator_catalog import write_operator_catalog
    from .type_catalog import write_type_catalog
    from .unary_ops_catalog import write_unary_ops_catalog
    from .variables_catalog import write_variables_catalog
    from .window_catalog import write_window_catalog


def reexport_reference_catalogs(base_path: str | Path | None = None) -> dict[str, Path]:
    root = Path(base_path) if base_path is not None else Path(__file__).resolve().parents[1]

    output_paths = {
        "aggregates": root / "reference/aggregates.json",
        "clauses": root / "reference/clauses.json",
        "expressions": root / "reference/expressions.json",
        "joins": root / "reference/joins.json",
        "operators": root / "reference/operators.json",
        "unary_ops": root / "reference/unary_ops.json",
        "variables": root / "reference/variables.json",
        "types": root / "reference/types.json",
        "functions": root / "reference/function_signatures.json",
        "windows": root / "reference/windows.json",
    }

    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    write_aggregate_catalog(output_paths["aggregates"])
    write_clauses_catalog(output_paths["clauses"])
    write_expression_catalog(output_paths["expressions"])
    write_joins_catalog(output_paths["joins"])
    write_operator_catalog(output_paths["operators"])
    write_unary_ops_catalog(output_paths["unary_ops"])
    write_variables_catalog(output_paths["variables"])
    write_type_catalog(output_paths["types"])
    write_function_signatures(output_paths["functions"])
    write_window_catalog(output_paths["windows"])

    return output_paths


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rewrite the generated aggregate, clause, expression, join, operator, unary-op, variable, type, function, and window catalogs."
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
