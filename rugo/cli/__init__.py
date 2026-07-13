# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
rugo.cli — the `rugo` command-line tool.

A Python entry point over the existing rugo.parquet / rugo.csv / rugo.jsonl
facades: a user interface on top of the engine, not part of it. The engine
(draken + rugo native extensions) still runs without Python; only this CLI
process requires it.

Verb names are reserved even where unimplemented (info, schema, diff, convert
are obvious enough that scripts will assume they exist) — see cmd table below.
"""

import argparse
import sys

from rugo.cli import commands
from rugo.cli._common import RugoCliError


def _add_json_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON instead of a text table")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rugo", description="Inspect and transform Parquet/CSV/JSONL files.")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("info", help="display high-level file metadata")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_info)

    p = sub.add_parser("schema", help="show the schema")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_schema)

    p = sub.add_parser("columns", help="list column names")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_columns)

    p = sub.add_parser("count", help="return the number of rows")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_count)

    p = sub.add_parser("preview", help="display the first N rows")
    p.add_argument("path")
    p.add_argument("-n", "--limit", type=int, default=10)
    p.add_argument("-c", "--columns", help="comma-separated column names to project")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_preview)

    p = sub.add_parser("head", help="alias for preview")
    p.add_argument("path")
    p.add_argument("-n", "--limit", type=int, default=10)
    p.add_argument("-c", "--columns", help="comma-separated column names to project")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_head)

    p = sub.add_parser("describe", help="per-column summary statistics (parquet only)")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_describe)

    p = sub.add_parser("stats", help="alias for describe")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_stats)

    p = sub.add_parser("inspect", help="low-level footer/row-group/encoding dump (parquet only)")
    p.add_argument("path")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_inspect)

    p = sub.add_parser("diff", help="compare schemas of two files (columns added/removed/changed)")
    p.add_argument("left")
    p.add_argument("right")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_diff)

    p = sub.add_parser("convert", help="convert between parquet/csv/jsonl")
    p.add_argument("source")
    p.add_argument("dest")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_convert)

    p = sub.add_parser("merge", help="combine multiple schema-identical files into one")
    p.add_argument("sources", nargs="+")
    p.add_argument("dest")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_merge)

    p = sub.add_parser("split", help="split a file into multiple files by row count")
    p.add_argument("path")
    p.add_argument("--rows", type=int, required=True, help="max rows per output file")
    p.add_argument("--format", choices=["parquet", "csv", "jsonl"], help="output format (default: same as input)")
    _add_json_flag(p)
    p.set_defaults(func=commands.cmd_split)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except RugoCliError as e:
        print(f"rugo: error: {e}", file=sys.stderr)
        return 1
    except FileNotFoundError as e:
        print(f"rugo: error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
