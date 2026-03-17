"""CLI helpers for rewriting generated reference catalogs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(1, str(Path(__file__).resolve().parents[2]))
    from opteryx.functions.signatures import write_function_signatures
    from opteryx.reference.operator_catalog import write_operator_catalog
    from opteryx.reference.type_catalog import write_type_catalog
else:
    from opteryx.functions.signatures import write_function_signatures

    from .operator_catalog import write_operator_catalog
    from .type_catalog import write_type_catalog


def reexport_reference_catalogs(base_path: str | Path | None = None) -> dict[str, Path]:
    root = Path(base_path) if base_path is not None else Path(__file__).resolve().parents[2]

    output_paths = {
        "operators": root / "opteryx/reference/operators.json",
        "types": root / "opteryx/reference/types.json",
        "functions": root / "opteryx/functions/function_signatures.json",
    }

    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    write_operator_catalog(output_paths["operators"])
    write_type_catalog(output_paths["types"])
    write_function_signatures(output_paths["functions"])

    return output_paths


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rewrite the generated operator, type, and function catalogs."
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
