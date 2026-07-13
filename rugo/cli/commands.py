# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.cli.commands — one function per verb. Each takes a parsed argparse
Namespace and returns an int exit code. No verb touches sys.argv directly —
that's argparse's job in rugo.cli.__init__.
"""

from typing import List

from rugo.cli import _render
from rugo.cli._common import (
    RugoCliError,
    detect_format,
    file_size,
    open_reader,
    read_metadata,
    schema_columns,
    write_morsel,
)


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------

def cmd_info(args) -> int:
    fmt = detect_format(args.path)
    meta = read_metadata(args.path, fmt)
    cols = schema_columns(meta)
    info = {
        "path": args.path,
        "format": fmt,
        "size_bytes": file_size(args.path),
        "num_rows": meta.num_rows,
        "num_columns": len(cols),
    }
    if args.json:
        _render.emit_json(info)
    else:
        _render.emit_kv(list(info.items()))
    return 0


# ---------------------------------------------------------------------------
# schema / columns
# ---------------------------------------------------------------------------

def cmd_schema(args) -> int:
    fmt = detect_format(args.path)
    meta = read_metadata(args.path, fmt)
    cols = schema_columns(meta)
    headers = ["name", "type", "nullable"]
    rows = [[c["name"], c["type"], c["nullable"]] for c in cols]
    _render.emit(headers, rows, args.json, json_key="columns")
    return 0


def cmd_columns(args) -> int:
    fmt = detect_format(args.path)
    meta = read_metadata(args.path, fmt)
    names = [c["name"] for c in schema_columns(meta)]
    if args.json:
        _render.emit_json({"columns": names})
    else:
        for name in names:
            print(name)
    return 0


def cmd_count(args) -> int:
    fmt = detect_format(args.path)
    meta = read_metadata(args.path, fmt)
    if args.json:
        _render.emit_json({"num_rows": meta.num_rows})
    else:
        print(meta.num_rows)
    return 0


# ---------------------------------------------------------------------------
# preview / head
# ---------------------------------------------------------------------------

def _preview_rows(path: str, limit: int, columns: List[str] = None):
    fmt = detect_format(path)
    seen = 0
    header = None
    out_rows = []
    with open_reader(path, fmt, columns=columns) as reader:
        for morsel in reader:
            if header is None:
                header = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
            for row in morsel:
                if seen >= limit:
                    return header or [], out_rows
                out_rows.append(list(row))
                seen += 1
            if seen >= limit:
                break
    return header or [], out_rows


def cmd_preview(args) -> int:
    columns = args.columns.split(",") if args.columns else None
    header, rows = _preview_rows(args.path, args.limit, columns)
    _render.emit(header, rows, args.json, json_key="rows")
    return 0


# `head` is the Unix-friendly alias for `preview`; same implementation.
cmd_head = cmd_preview


# ---------------------------------------------------------------------------
# describe / stats  (Parquet only — footer statistics are a Parquet concept;
# CSV/JSONL carry no per-column min/max/null/distinct metadata to report).
# ---------------------------------------------------------------------------

def _require_parquet(path: str) -> None:
    fmt = detect_format(path)
    if fmt != "parquet":
        raise RugoCliError(
            f"{path!r} is {fmt}, not parquet — describe/stats/inspect read "
            "column statistics from the Parquet footer, which csv/jsonl don't have"
        )


def _aggregate_column_stats(row_groups: list) -> dict:
    """Reduce per-row-group column stats to one row per column.

    min/max are folded (min-of-mins, max-of-maxes) using the decoded values;
    null_count is summed. distinct_count is the parquet-reported per-row-group
    figure exposed as-is (no NDV estimation — that infra doesn't exist in
    rugo; ClickHouse-style cross-row-group unique counts are not computed).
    """
    from rugo.rugo_native import decode_value

    by_name = {}
    for rg in row_groups:
        for col in rg["columns"]:
            entry = by_name.setdefault(col["name"], {
                "name": col["name"],
                "type": col["logical_type"] or col["physical_type"],
                "null_count": 0,
                "min": None,
                "max": None,
                "distinct_count": None,
            })
            entry["null_count"] += col["null_count"]
            pt, lt = col["physical_type"].encode(), col["logical_type"].encode()
            if col["min"] is not None:
                mn = decode_value(pt, lt, col["min"], True)
                if entry["min"] is None or (mn is not None and mn < entry["min"]):
                    entry["min"] = mn
            if col["max"] is not None:
                mx = decode_value(pt, lt, col["max"], True)
                if entry["max"] is None or (mx is not None and mx > entry["max"]):
                    entry["max"] = mx
            if col["distinct_count"] is not None:
                entry["distinct_count"] = max(entry["distinct_count"] or 0, col["distinct_count"])
    return by_name


def cmd_describe(args) -> int:
    _require_parquet(args.path)
    from rugo.rugo_native import read_rowgroup_stats

    with open(args.path, "rb") as f:
        data = f.read()
    row_groups = read_rowgroup_stats(data)
    by_name = _aggregate_column_stats(row_groups)

    headers = ["name", "type", "null_count", "min", "max", "distinct_count"]
    rows = [
        [c["name"], c["type"], c["null_count"], c["min"], c["max"], c["distinct_count"]]
        for c in by_name.values()
    ]
    _render.emit(headers, rows, args.json, json_key="columns")
    return 0


cmd_stats = cmd_describe


# ---------------------------------------------------------------------------
# inspect — low-level footer/row-group/encoding dump, for debugging.
# ---------------------------------------------------------------------------

def cmd_inspect(args) -> int:
    _require_parquet(args.path)
    from rugo.rugo_native import read_rowgroup_stats

    with open(args.path, "rb") as f:
        data = f.read()
    meta = read_metadata(args.path, "parquet")
    row_groups = read_rowgroup_stats(data)

    report = {
        "path": args.path,
        "size_bytes": file_size(args.path),
        "num_rows": meta.num_rows,
        "num_row_groups": len(row_groups),
        "schema": schema_columns(meta),
        "row_groups": [
            {
                "num_rows": rg["num_rows"],
                "columns": [
                    {
                        "name": c["name"],
                        "physical_type": c["physical_type"],
                        "logical_type": c["logical_type"],
                        "null_count": c["null_count"],
                        "distinct_count": c["distinct_count"],
                        "has_bloom_filter": c["bloom_offset"] is not None and c["bloom_offset"] >= 0,
                        "has_min_max": c["min"] is not None and c["max"] is not None,
                    }
                    for c in rg["columns"]
                ],
            }
            for rg in row_groups
        ],
    }

    if args.json:
        _render.emit_json(report)
        return 0

    print(f"path            : {report['path']}")
    print(f"size_bytes      : {report['size_bytes']}")
    print(f"num_rows        : {report['num_rows']}")
    print(f"num_row_groups  : {report['num_row_groups']}")
    print()
    print("schema:")
    _render.emit_table(["name", "type", "nullable"],
                        [[c["name"], c["type"], c["nullable"]] for c in report["schema"]])
    for i, rg in enumerate(report["row_groups"]):
        print(f"\nrow group {i}  (rows={rg['num_rows']}):")
        _render.emit_table(
            ["name", "physical_type", "logical_type", "null_count", "distinct_count", "bloom", "min/max"],
            [[c["name"], c["physical_type"], c["logical_type"], c["null_count"],
              c["distinct_count"], c["has_bloom_filter"], c["has_min_max"]]
             for c in rg["columns"]],
        )
    return 0


# ---------------------------------------------------------------------------
# diff — metadata only (column added/removed, type/nullable differences).
# Row-level data diff is out of scope for v1.
# ---------------------------------------------------------------------------

def cmd_diff(args) -> int:
    left_cols = {c["name"]: c for c in schema_columns(read_metadata(args.left))}
    right_cols = {c["name"]: c for c in schema_columns(read_metadata(args.right))}

    added = sorted(set(right_cols) - set(left_cols))
    removed = sorted(set(left_cols) - set(right_cols))
    changed = []
    for name in sorted(set(left_cols) & set(right_cols)):
        l, r = left_cols[name], right_cols[name]
        if l["type"] != r["type"] or l["nullable"] != r["nullable"]:
            changed.append({
                "name": name,
                "left_type": l["type"], "right_type": r["type"],
                "left_nullable": l["nullable"], "right_nullable": r["nullable"],
            })

    report = {
        "left": args.left,
        "right": args.right,
        "columns_added": added,
        "columns_removed": removed,
        "columns_changed": changed,
        "identical": not (added or removed or changed),
    }

    if args.json:
        _render.emit_json(report)
    else:
        if report["identical"]:
            print("schemas are identical")
        else:
            if added:
                print(f"+ added:   {', '.join(added)}")
            if removed:
                print(f"- removed: {', '.join(removed)}")
            if changed:
                print("~ changed:")
                _render.emit_table(
                    ["name", "left_type", "right_type", "left_nullable", "right_nullable"],
                    [[c["name"], c["left_type"], c["right_type"], c["left_nullable"], c["right_nullable"]]
                     for c in changed],
                )
    return 0 if report["identical"] else 1


# ---------------------------------------------------------------------------
# convert — stream source format -> target format via the shared Morsel model.
# ---------------------------------------------------------------------------

def _read_all_morsels(path: str, fmt: str) -> list:
    with open_reader(path, fmt) as reader:
        return list(reader)


def _combine(morsels: list):
    from draken.morsels.morsel import Morsel
    if not morsels:
        raise RugoCliError("source produced no morsels (empty or unreadable file)")
    return Morsel.combine(morsels)


def cmd_convert(args) -> int:
    src_fmt = detect_format(args.source)
    dst_fmt = detect_format(args.dest)
    morsels = _read_all_morsels(args.source, src_fmt)
    morsel = _combine(morsels)
    write_morsel(morsel, args.dest, dst_fmt)
    if not args.json:
        print(f"{args.source} ({src_fmt}, {morsel.num_rows} rows) -> {args.dest} ({dst_fmt})")
    else:
        _render.emit_json({"source": args.source, "dest": args.dest,
                            "source_format": src_fmt, "dest_format": dst_fmt,
                            "num_rows": morsel.num_rows})
    return 0


# ---------------------------------------------------------------------------
# merge — concatenate N schema-identical files into one output file.
# ---------------------------------------------------------------------------

def _assert_same_schema(paths: List[str]) -> None:
    first_cols = None
    for path in paths:
        cols = [(c["name"], c["type"]) for c in schema_columns(read_metadata(path))]
        if first_cols is None:
            first_cols = cols
        elif cols != first_cols:
            raise RugoCliError(
                f"schema mismatch: {paths[0]!r} has {first_cols}, {path!r} has {cols} "
                "— merge requires identical column names, order, and types"
            )


def cmd_merge(args) -> int:
    _assert_same_schema(args.sources)
    dst_fmt = detect_format(args.dest)
    all_morsels = []
    for path in args.sources:
        all_morsels.extend(_read_all_morsels(path, detect_format(path)))
    morsel = _combine(all_morsels)
    write_morsel(morsel, args.dest, dst_fmt)
    if not args.json:
        print(f"merged {len(args.sources)} files ({morsel.num_rows} rows) -> {args.dest}")
    else:
        _render.emit_json({"sources": args.sources, "dest": args.dest, "num_rows": morsel.num_rows})
    return 0


# ---------------------------------------------------------------------------
# split — one input file into N output files, bounded by row count.
# ---------------------------------------------------------------------------

def cmd_split(args) -> int:
    if args.rows <= 0:
        raise RugoCliError("--rows must be a positive integer")
    src_fmt = detect_format(args.path)
    morsel = _combine(_read_all_morsels(args.path, src_fmt))

    import os
    stem, ext = os.path.splitext(args.path)
    dst_fmt = args.format or src_fmt
    out_ext = {"parquet": ".parquet", "csv": ".csv", "jsonl": ".jsonl"}[dst_fmt]

    outputs = []
    offset = 0
    part = 0
    total = morsel.num_rows
    while offset < total:
        length = min(args.rows, total - offset)
        chunk = morsel.slice(offset, length)
        out_path = f"{stem}.part{part:04d}{out_ext}"
        write_morsel(chunk, out_path, dst_fmt)
        outputs.append({"path": out_path, "num_rows": length})
        offset += length
        part += 1

    if args.json:
        _render.emit_json({"source": args.path, "outputs": outputs})
    else:
        for o in outputs:
            print(f"{o['path']}  ({o['num_rows']} rows)")
    return 0
