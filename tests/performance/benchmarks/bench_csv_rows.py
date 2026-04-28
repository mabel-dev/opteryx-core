"""
Benchmark native Draken CSV row serialization against Arrow materialization.

Run with:
    python tests/performance/benchmarks/bench_csv_rows.py
"""

import csv
import io
import os
import statistics
import sys
import time

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.io import morsel_to_csv_rows
from opteryx.compiled.io import morsel_to_csv_strings
from draken.morsels.morsel import Morsel


def _measure(fn, repeat: int = 5):
    fn()
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def _build_mixed_morsel(rows: int) -> Morsel:
    table = pa.table(
        {
            "id": list(range(rows)),
            "name": [f"user_{i}" for i in range(rows)],
            "score": [None if i % 11 == 0 else (i * 0.125) for i in range(rows)],
            "active": [i % 2 == 0 for i in range(rows)],
            "note": [f'v"{i},x' if i % 7 == 0 else f"line_{i}\nvalue" for i in range(rows)],
        }
    )
    return Morsel.from_arrow(table)


def _build_numeric_morsel(rows: int) -> Morsel:
    table = pa.table(
        {
            "c0": list(range(rows)),
            "c1": [i * 3 for i in range(rows)],
            "c2": [None if i % 13 == 0 else i * 1.5 for i in range(rows)],
            "c3": [i % 2 == 0 for i in range(rows)],
        }
    )
    return Morsel.from_arrow(table)


def _build_quoted_string_morsel(rows: int) -> Morsel:
    table = pa.table(
        {
            "name": [f'a"{i}' if i % 2 == 0 else f"c,{i}" for i in range(rows)],
            "note": [f"line\n{i}" if i % 3 == 0 else f"plain_{i}" for i in range(rows)],
            "kind": ["evt" if i % 2 == 0 else "log" for i in range(rows)],
        }
    )
    return Morsel.from_arrow(table)


def _normalize_for_csv(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".17g")
    if value is None:
        return ""
    return value


def _python_rows_to_csv_strings(rows, columns, include_header=False, separator=","):
    output = []
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=separator, lineterminator="")

    if include_header:
        writer.writerow(columns)
        output.append(buffer.getvalue())
        buffer.seek(0)
        buffer.truncate(0)

    for row in rows:
        writer.writerow([_normalize_for_csv(row[column]) for column in columns])
        output.append(buffer.getvalue())
        buffer.seek(0)
        buffer.truncate(0)

    return output


def _benchmark_case(name: str, morsel: Morsel, include_header=False, separator=","):
    table = morsel.to_arrow()
    columns = table.column_names

    native_bytes_ms = _measure(
        lambda: morsel_to_csv_rows(morsel, include_header=include_header, separator=separator)
    )
    native_strings_ms = _measure(
        lambda: morsel_to_csv_strings(morsel, include_header=include_header, separator=separator)
    )
    arrow_pylist_ms = _measure(lambda: table.to_pylist())
    arrow_pylist_csv_ms = _measure(
        lambda: _python_rows_to_csv_strings(
            table.to_pylist(),
            columns,
            include_header=include_header,
            separator=separator,
        )
    )

    native_rows = morsel_to_csv_strings(morsel, include_header=include_header, separator=separator)
    python_rows = _python_rows_to_csv_strings(
        table.to_pylist(),
        columns,
        include_header=include_header,
        separator=separator,
    )
    assert native_rows == python_rows

    native_vs_pylist = arrow_pylist_ms / native_strings_ms if native_strings_ms else float("inf")
    native_vs_end_to_end = arrow_pylist_csv_ms / native_strings_ms if native_strings_ms else float("inf")

    print()
    print("=" * 96)
    print(name)
    print("=" * 96)
    print(f"rows: {morsel.num_rows:,}")
    print(f"{'native rows -> StringVector (ms)':<42} {native_bytes_ms:>10.2f}")
    print(f"{'native rows -> list[str] (ms)':<42} {native_strings_ms:>10.2f}")
    print(f"{'Arrow table.to_pylist() (ms)':<42} {arrow_pylist_ms:>10.2f}")
    print(f"{'Arrow to_pylist() + csv.writer (ms)':<42} {arrow_pylist_csv_ms:>10.2f}")
    print(f"{'speedup vs Arrow to_pylist()':<42} {native_vs_pylist:>10.2f}x")
    print(f"{'speedup vs Arrow to_pylist()+csv':<42} {native_vs_end_to_end:>10.2f}x")


if __name__ == "__main__":
    print("DRAKEN CSV ROW SERIALIZATION BENCHMARK")
    _benchmark_case("numeric", _build_numeric_morsel(200_000))
    _benchmark_case("mixed+header", _build_mixed_morsel(100_000), include_header=True)
    _benchmark_case("quoted-strings", _build_quoted_string_morsel(100_000))
