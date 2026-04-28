"""
Benchmark native Draken JSON row serialization against Arrow materialization.

Run with:
    python tests/performance/benchmarks/bench_json_rows.py
"""

import json
import os
import statistics
import sys
import time

import pyarrow as pa

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.io import morsel_to_json_rows
from opteryx.compiled.io import morsel_to_json_strings
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
            "note": [f'v"{i}\\n' if i % 7 == 0 else f"value_{i}" for i in range(rows)],
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


def _build_raw_json_morsel(rows: int) -> Morsel:
    table = pa.table(
        {
            "id": list(range(rows)),
            "payload": [f'{{"a":{i},"b":[1,2,{i % 5}]}}' for i in range(rows)],
            "kind": ["evt" if i % 2 == 0 else "log" for i in range(rows)],
        }
    )
    return Morsel.from_arrow(table)


def _python_rows_to_json_strings(rows, raw_json_columns=None):
    return [
        json.dumps(
            _normalize_for_json_row(row, raw_json_columns),
            separators=(",", ":"),
            ensure_ascii=False,
        )
        for row in rows
    ]


def _normalize_for_json(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, list):
        return [_normalize_for_json(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalize_for_json(item) for key, item in value.items()}
    return value


def _normalize_for_json_row(row, raw_json_columns=None):
    normalized = _normalize_for_json(row)
    if raw_json_columns:
        for column in raw_json_columns:
            if column in normalized and normalized[column] is not None:
                normalized[column] = json.loads(normalized[column])
    return normalized


def _benchmark_case(name: str, morsel: Morsel, raw_json_columns=None):
    table = morsel.to_arrow()

    native_bytes_ms = _measure(
        lambda: morsel_to_json_rows(morsel, raw_json_columns=raw_json_columns)
    )
    native_strings_ms = _measure(
        lambda: morsel_to_json_strings(morsel, raw_json_columns=raw_json_columns)
    )
    arrow_pylist_ms = _measure(lambda: table.to_pylist())
    arrow_pylist_json_ms = _measure(
        lambda: _python_rows_to_json_strings(table.to_pylist(), raw_json_columns=raw_json_columns)
    )

    native_rows = morsel_to_json_strings(morsel, raw_json_columns=raw_json_columns)
    python_rows = _python_rows_to_json_strings(table.to_pylist(), raw_json_columns=raw_json_columns)
    assert len(native_rows) == len(python_rows)
    assert json.loads(native_rows[0]) == json.loads(python_rows[0])

    native_vs_pylist = arrow_pylist_ms / native_strings_ms if native_strings_ms else float("inf")
    native_vs_end_to_end = (
        arrow_pylist_json_ms / native_strings_ms if native_strings_ms else float("inf")
    )

    print()
    print("=" * 96)
    print(name)
    print("=" * 96)
    print(f"rows: {morsel.num_rows:,}")
    print(f"{'native rows -> StringVector (ms)':<42} {native_bytes_ms:>10.2f}")
    print(f"{'native rows -> list[str] (ms)':<42} {native_strings_ms:>10.2f}")
    print(f"{'Arrow table.to_pylist() (ms)':<42} {arrow_pylist_ms:>10.2f}")
    print(f"{'Arrow to_pylist() + json.dumps (ms)':<42} {arrow_pylist_json_ms:>10.2f}")
    print(f"{'speedup vs Arrow to_pylist()':<42} {native_vs_pylist:>10.2f}x")
    print(f"{'speedup vs Arrow to_pylist()+json':<42} {native_vs_end_to_end:>10.2f}x")


if __name__ == "__main__":
    print("DRAKEN JSON ROW SERIALIZATION BENCHMARK")
    _benchmark_case("numeric", _build_numeric_morsel(200_000))
    _benchmark_case("mixed", _build_mixed_morsel(100_000))
    _benchmark_case("raw-json", _build_raw_json_morsel(100_000), raw_json_columns=["payload"])
