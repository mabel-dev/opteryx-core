#!/usr/bin/env python
# pragma: no cover

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A command line interface for Opteryx
"""

import argparse
import os
import shutil
import sys
import threading
import time
from typing import List
from typing import NamedTuple
from typing import Optional

sys.path.insert(1, os.path.join(sys.path[0], ".."))

if True:
    import opteryx
    from opteryx.constants import ResultType
    from opteryx.exceptions import MissingSqlStatement
    from opteryx.utils.display import format_jsonl
    from opteryx.utils.display import format_markdown
    from opteryx.utils.display import format_table

# Define ANSI color codes
ANSI_RED = "\u001b[31m"
ANSI_RESET = "\u001b[0m"


class QueryResult(NamedTuple):
    """The whole of a query's result, collected in memory for display."""

    column_names: List[str]
    column_types: List[str]
    rows: List[tuple]
    tabular: bool
    rowcount: int


def report_error(error, sql: str) -> str:
    """The error, plus an underline of the SQL it is about.

    A terminal has no editor to mark up, so the drawing has to happen here. Everywhere
    else - the web query surface above all - reads `error.position` and underlines in
    place, which is why the position is data on the exception and not text inside the
    message. This is the one surface that has to render it itself.
    """
    from opteryx.utils.sql import underline

    text = f"{ANSI_RED}Error{ANSI_RESET}: {error}"
    marked = underline(sql, getattr(error, "position", None))
    return f"{text}\n\n{marked}" if marked else text

def print_dots(stop_event):
    """
    Prints dots with pauses to indicate processing activity until the stop_event is set.
    """
    while not stop_event.is_set():  # pragma: no cover
        print(".", end="", flush=True)
        time.sleep(0.5)
        if not stop_event.is_set():
            print(".", end="", flush=True)
            time.sleep(0.5)
        if not stop_event.is_set():
            print(".", end="", flush=True)
            time.sleep(0.5)
        if not stop_event.is_set():
            print(".", end="", flush=True)
            time.sleep(0.5)
        if not stop_event.is_set():
            print(".", end="", flush=True)
            time.sleep(0.5)
        if not stop_event.is_set():
            print("\r     \r", end="", flush=True)
            time.sleep(0.5)


def execute(statement: str) -> QueryResult:
    """
    Execute `statement` and collect its entire result.

    The CLI renders a table and writes files, both of which need the result in
    one piece, so the morsel stream is drained into Python rows here. That cost
    is the command line's to pay - the engine underneath still streams.
    """
    session = opteryx.session()
    try:
        rows: List[tuple] = []
        for morsel in session.execute_to_morsels(statement):
            if morsel.num_rows == 0:
                continue
            columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
            rows.extend(zip(*columns))

        if session.result_type == ResultType.NON_TABULAR:
            # DDL and SET statements report an outcome, not a relation.
            return QueryResult([], [], [], False, session.rowcount or 0)

        # `description` reports the DBAPI type category (INTEGER, DECIMAL); the
        # schema carries the type the value actually has (INT8, DECIMAL(3, 1)).
        return QueryResult(
            list(session.column_names),
            [str(column.column_type) for column in session._schema.columns],
            rows,
            True,
            len(rows),
        )
    finally:
        session.close()


def console_width(limit_to_terminal: bool) -> Optional[int]:
    """
    The width to render a table into, or None to render it in full.

    Only a terminal has a width worth honouring - output redirected to a file or
    a pipe is not read through an eighty column window, so it is not cut to one.
    """
    if not limit_to_terminal or not sys.stdout.isatty():
        return None
    return shutil.get_terminal_size().columns


def render(result: QueryResult, args) -> str:
    """Render a result for the console, per the display options."""
    if not result.tabular:
        return ""
    return format_table(
        result.column_names,
        result.column_types,
        result.rows,
        display_width=console_width(args.table_width),
        colorize=args.color,
        max_column_width=args.max_col_width,
    )


def statistics(result: QueryResult, duration: int) -> str:
    """The row/column/timing line printed under a result."""
    if not result.tabular:
        return f"[ {result.rowcount} rows affected ] ( {duration / 1e9} seconds )"
    return (
        f"[ {result.rowcount} rows x {len(result.column_names)} columns ] "
        f"( {duration / 1e9} seconds )"
    )


def repl(args):  # pragma: no cover
    """Read statements from the user until they ask to leave."""
    import readline  # noqa: F401 - imported for line editing and history in input()

    print(f"Opteryx version {opteryx.__version__}")
    print("  Enter '.help' for usage hints")
    print("  Enter '.exit' to exit this program")

    while True:
        print()
        statement = input("opteryx> ")
        if statement in {".exit", ".quit"}:
            return
        if statement == ".help":
            print("  .exit        Exit this program")
            print("  .help        Show help text")
            continue

        stop_event = threading.Event()
        dot_thread = threading.Thread(target=print_dots, args=(stop_event,))
        dot_thread.start()
        try:
            start = time.monotonic_ns()
            result = execute(statement)
            stop_event.set()
            duration = time.monotonic_ns() - start
            print("\r     \r", end="", flush=True)
            if result.tabular:
                print(render(result, args))
            if args.stats:
                print(statistics(result, duration))
        except MissingSqlStatement:
            print(f"{ANSI_RED}Error{ANSI_RESET}: Expected SQL statement or dot command missing.")
        except Exception as e:
            print(report_error(e, statement))
        finally:
            stop_event.set()
            dot_thread.join()


def write(result: QueryResult, destination: str):
    """Write a result to a file, in the format named by its extension."""
    extension = destination.lower().split(".")[-1]

    if extension == "jsonl":
        with open(destination, mode="wb") as file:
            for row in result.rows:
                file.write(format_jsonl(result.column_names, row) + b"\n")
    elif extension == "md":
        with open(destination, mode="w") as file:
            file.write(format_markdown(result.column_names, result.rows) + "\n")
    else:
        raise ValueError(f"Unsupported output format '{extension}' (supported: jsonl, md)")


def main():
    parser = argparse.ArgumentParser(description="A command line interface for Opteryx")

    parser.add_argument(
        "--o", type=str, default="console", help="Output location (ignored by REPL)", dest="output"
    )

    # Mutually exclusive group for `--color` and `--no-color`
    color_group = parser.add_mutually_exclusive_group()
    color_group.add_argument(
        "--color", dest="color", action="store_true", default=True, help="Colorize the table."
    )
    color_group.add_argument(
        "--no-color", dest="color", action="store_false", help="Disable colorized output."
    )

    parser.add_argument(
        "--table_width",
        action="store_true",
        default=True,
        help="Limit console display to the screen width.",
    )
    parser.add_argument("--max_col_width", type=int, default=64, help="Maximum column width")

    # Mutually exclusive group for `--stats` and `--no-stats`
    stats_group = parser.add_mutually_exclusive_group()
    stats_group.add_argument(
        "--stats", dest="stats", action="store_true", default=True, help="Report statistics."
    )
    stats_group.add_argument(
        "--no-stats", dest="stats", action="store_false", help="Disable report statistics."
    )

    parser.add_argument("--cycles", type=int, default=1, help="Repeat Execution.")
    parser.add_argument("sql", type=str, nargs="?", help="Execute SQL statement and quit.")

    args = parser.parse_args()

    # Both entry points - `python -m opteryx` and the `opteryx` command - land
    # here, so the reporting of a failed query belongs here rather than in the
    # module's __main__ block. A query which failed exits non-zero.
    try:
        run(args)
    except Exception as e:
        print(report_error(e, args.sql or ""))
        sys.exit(1)


def run(args):
    """Do what the parsed arguments ask for."""
    # Run in REPL mode if no SQL is provided
    if args.sql is None:  # pragma: no cover
        if args.output != "console":
            raise ValueError("Cannot specify output location and not provide a SQL statement.")
        repl(args)
        return

    # Process the SQL query
    sql = args.sql

    if args.cycles > 1:  # Benchmarking mode
        print("[", end="")
        for i in range(args.cycles):
            start = time.monotonic_ns()
            sess = opteryx.session()
            for _ in sess.execute_to_morsels(sql):
                pass
            sess.close()
            print(
                f"{(time.monotonic_ns() - start) / 1e9:.3f}",
                flush=True,
                end=("," if (i + 1) < args.cycles else "]\n"),
            )
            sys.stdout.flush()
        return

    start = time.monotonic_ns()
    result = execute(sql)
    duration = time.monotonic_ns() - start

    if args.output == "console":
        if result.tabular:
            print(render(result, args))
        if args.stats:
            print(statistics(result, duration))
    else:
        write(result, args.output)
        print(statistics(result, duration))
        print(f"Written result to '{args.output}'")


if __name__ == "__main__":
    main()
