# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.cli._render — the one shared output formatter every verb uses.

Two modes only: a plain-text table (default, human-facing) and `--json`
(machine-facing, for scripting). No verb invents its own layout.
"""

import json
import sys
from typing import Any, Dict, List, Sequence


def emit_json(obj: Any) -> None:
    json.dump(obj, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def emit_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    if not rows:
        print("  ".join(headers))
        return
    str_rows = [[_cell(v) for v in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    print("  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    print("  ".join("-" * w for w in widths))
    for row in str_rows:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))


def emit_kv(pairs: Sequence[Sequence[Any]]) -> None:
    if not pairs:
        return
    width = max(len(str(k)) for k, _ in pairs)
    for k, v in pairs:
        print(f"{str(k).ljust(width)} : {v}")


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def emit(headers: Sequence[str], rows: Sequence[Sequence[Any]], as_json: bool, json_key: str = "rows") -> None:
    if as_json:
        emit_json({json_key: [dict(zip(headers, row)) for row in rows]})
    else:
        emit_table(headers, rows)
