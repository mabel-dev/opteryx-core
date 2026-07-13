# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.cli._common — format detection and thin per-format read/write dispatch.

Every verb goes through here to turn a path into (reader_module, format_name)
rather than re-deriving the extension mapping itself.
"""

import os
from typing import Tuple

_EXT_TO_FORMAT = {
    ".parquet": "parquet",
    ".pqt": "parquet",
    ".csv": "csv",
    ".tsv": "csv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
}


class RugoCliError(Exception):
    """Raised for user-facing CLI errors (bad path, unsupported format, schema mismatch).

    main() catches this and prints the message to stderr — no traceback for
    expected, actionable failures.
    """


def detect_format(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    fmt = _EXT_TO_FORMAT.get(ext)
    if fmt is None:
        raise RugoCliError(
            f"cannot infer format from extension {ext!r} on {path!r} "
            f"(supported: {sorted(set(_EXT_TO_FORMAT.values()))})"
        )
    return fmt


def _require_exists(path: str) -> None:
    # The native parquet reader raises an uncaught C++ exception (process
    # abort, not a Python exception) when the path doesn't exist. Check here
    # so a typo'd path is a clean CLI error instead of a crash.
    if not os.path.isfile(path):
        raise RugoCliError(f"no such file: {path!r}")


def read_metadata(path: str, fmt: str = None):
    _require_exists(path)
    fmt = fmt or detect_format(path)
    if fmt == "parquet":
        from rugo import parquet
        return parquet.read_metadata(path)
    if fmt == "csv":
        from rugo import csv
        return csv.read_metadata(path)
    if fmt == "jsonl":
        from rugo import jsonl
        return jsonl.read_metadata(path)
    raise RugoCliError(f"unsupported format: {fmt!r}")


def open_reader(path: str, fmt: str = None, columns=None, predicates=None):
    """Return a context-managed reader yielding Morsels for `path`."""
    _require_exists(path)
    fmt = fmt or detect_format(path)
    if fmt == "parquet":
        from rugo import parquet
        return parquet.read_parquet(path, columns=columns, predicates=predicates)
    if fmt == "csv":
        from rugo import csv
        return csv.read_csv(path, columns=columns, predicates=predicates)
    if fmt == "jsonl":
        from rugo import jsonl
        return jsonl.read_jsonl(path, columns=columns, predicates=predicates)
    raise RugoCliError(f"unsupported format: {fmt!r}")


def write_morsel(morsel, path: str, fmt: str = None) -> None:
    fmt = fmt or detect_format(path)
    if fmt == "parquet":
        from rugo import parquet
        data = parquet.write_parquet(morsel)
    elif fmt == "csv":
        from rugo import csv
        data = csv.write_csv(morsel)
    elif fmt == "jsonl":
        from rugo import jsonl
        data = jsonl.write_jsonl(morsel)
    else:
        raise RugoCliError(f"unsupported format: {fmt!r}")
    with open(path, "wb") as f:
        f.write(data)


def schema_columns(meta) -> list:
    """Normalize ParquetMetadata/CsvMetadata/JsonlMetadata.schema_columns to
    a list of {"name", "type", "nullable"} dicts. Parquet uses
    physical_type/logical_type; csv/jsonl already use "type"."""
    out = []
    for col in meta.schema_columns:
        if isinstance(col, dict):
            out.append({
                "name": col["name"],
                "type": col.get("type") or col.get("logical_type") or col.get("physical_type"),
                "nullable": col.get("nullable", True),
            })
        else:
            out.append({
                "name": col.name,
                "type": col.logical_type or col.physical_type,
                "nullable": col.nullable,
            })
    return out


def file_size(path: str) -> int:
    return os.path.getsize(path)
