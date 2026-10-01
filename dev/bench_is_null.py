#!/usr/bin/env python3
"""Benchmark c-native IS NULL and IS NOT NULL bytecode evaluation.

Measures the real whole-bytecode c-native evaluator over an INT64 morsel with a
one-in-three null pattern and a non-byte-aligned length. It deliberately includes
bytecode setup, load resolution, output allocation, and result wrapping, because
those are part of the operation's real per-morsel cost.

Run from the repository root:
    make bench-is-null

Pass options through BENCH_ARGS, for example:
    make bench-is-null BENCH_ARGS="--rows 1000003 --repetitions 11 --budget 0.5"
"""

import argparse
import gc
import json
import statistics
import time

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.compiled.expression.compiled_expression import build_bytecode, lower
from opteryx.compiled.structures.expressions import ExprArena, LogicalColumn, UnaryOperator
from opteryx.expression import NodeType
from opteryx.expression.evaluator import execute_bytecode
from opteryx.types.logical_type import INT64
from opteryx.compiled.planner.column_table import ColumnTable


def build_bytecode_for(op, schema_column):
    arena = ExprArena()
    column = LogicalColumn(
        node_type=NodeType.IDENTIFIER,
        source_column="value",
        schema_column=schema_column,
        arena=arena,
    )
    return build_bytecode(lower(UnaryOperator(value=op, centre=column, arena=arena)))


def measure(bytecode, morsel, repetitions, budget_seconds):
    for _ in range(5):
        execute_bytecode(bytecode, morsel)

    started = time.monotonic_ns()
    execute_bytecode(bytecode, morsel)
    one_iteration_ns = max(time.monotonic_ns() - started, 1)
    iterations = max(3, min(5000, int(budget_seconds * 1_000_000_000 / one_iteration_ns)))

    samples = []
    gc.disable()
    try:
        for _ in range(repetitions):
            gc.collect()
            started = time.monotonic_ns()
            for _ in range(iterations):
                execute_bytecode(bytecode, morsel)
            samples.append((time.monotonic_ns() - started) / iterations)
    finally:
        gc.enable()
    return iterations, samples


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_003)
    parser.add_argument("--repetitions", type=int, default=11)
    parser.add_argument("--budget", type=float, default=0.5)
    args = parser.parse_args()

    if args.rows < 1:
        raise ValueError("--rows must be at least 1")
    if args.repetitions < 3:
        raise ValueError("--repetitions must be at least 3")
    if args.budget <= 0:
        raise ValueError("--budget must be positive")

    schema_column = ColumnTable().relation_column("bench", "value", column_type=INT64)
    identity = schema_column.identity
    values = [None if row % 3 == 0 else row for row in range(args.rows)]
    morsel = Morsel.from_vectors(
        [identity], [vector_from_sequence(values, DrakenType.INT64)]
    )
    is_null = [value is None for value in values]
    expected = {
        "IsNull": is_null,
        "IsNotNull": [not flag for flag in is_null],
    }

    report = {"rows": args.rows, "null_fraction": "1/3", "operations": {}}
    for op, expected_rows in expected.items():
        bytecode = build_bytecode_for(op, schema_column)
        result = execute_bytecode(bytecode, morsel)
        if result.to_pylist() != expected_rows:
            raise RuntimeError(f"{op} result differs from the row-by-row expectation")
        iterations, samples = measure(bytecode, morsel, args.repetitions, args.budget)
        median_ns = statistics.median(samples)
        report["operations"][op] = {
            "iterations_per_sample": iterations,
            "median_ns_per_morsel": round(median_ns, 1),
            "ns_per_row": round(median_ns / args.rows, 4),
            "samples_ns_per_morsel": [round(sample, 1) for sample in samples],
        }

    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
