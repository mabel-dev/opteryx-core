#!/usr/bin/env python3
"""
Measure per-row execution cost of scalar function kernels.

This is a developer tool (dev/ — never imported by production code). It refreshes
the ``cost_us_per_million`` values stored in the function catalog
(opteryx/expression/functions/registrar/*.pyx), which the optimizer's predicate
ordering strategy reads via ``catalog.get_cost()``.

METHOD — measure through the real bytecode evaluator
----------------------------------------------------
A function's cost is whatever it actually costs the engine to evaluate it over a
morsel, *including* the real lowered execution path. Several functions never run
through their ``callable_ref`` at all (CONCAT etc. are lowered to specialised
bytecode), so calling the kernel callable directly would measure a fiction. We
therefore measure exactly what the engine does:

  1. Build a bound ``FUNC(col[, ...])`` expression node — an IDENTIFIER node per
     ordinary argument (resolved against a column in a synthetic test morsel) and
     a LITERAL node per ``constant_only`` argument — then resolve it through the
     function catalog (the same path the binder uses).
  2. ``lower()`` + ``build_bytecode()`` it once.
  3. Time ``execute_bytecode(bc, morsel)`` over a large morsel, many iterations,
     taking the median.
  4. Subtract an identity-expression baseline (a bare column load + return) to
     isolate the marginal per-row cost of the function itself.

The reported number is ``cost_us_per_million`` — microseconds to apply the
function to 1,000,000 rows — which is what the catalog stores.

Failures are recorded and printed loudly, never swallowed. A kernel that cannot
be driven (missing input shape, aggregate semantics, embedding model, ...) is
reported as a failure with its reason and produces NO cost — a wrong-but-honest
gap beats a fabricated number.

Usage:
    python estimate_function_costs.py [--functions UPPER,LOWER] [--output costs.json]
                                      [--sample-size 250000] [--budget 0.5]

Run from the dev/ directory (it inserts the repo root onto sys.path).
"""

import argparse
import json
import os
import random
import statistics
import string
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.compiled.expression.compiled_expression import build_bytecode, lower
from opteryx.expression import NodeType
from opteryx.expression.evaluator import execute_bytecode
from opteryx.expression.functions import get_catalog
from opteryx.models import Node
from opteryx.types.logical_type import (
    ARRAY,
    BOOLEAN,
    DATE,
    FLOAT64,
    INT64,
    NVARCHAR,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
    VARIANT,
)
from opteryx.types.schema import (
    ConstantColumn,
    FunctionColumn,
    SchemaColumn,
    mint_column_identity,
)

# Functions excluded by default: not scalar-expression kernels, or require an
# external resource we don't want a benchmark to pull in.
DEFAULT_EXCLUDE = {
    "EMBED",          # pulls an embedding model
    "_MATCH_AGAINST",  # semantic match via embeddings; volatile + model-dependent
}


class Unsupported(Exception):
    """Raised when a parameter type-family can't be driven by this harness."""


@dataclass
class FamilySpec:
    """How to manufacture test data for one parameter type-family."""

    dtype: str                       # vector_from_sequence dtype name
    column_type: Any                 # ColumnType for the SchemaColumn / scoring
    gen: Callable[[int], List[Any]]  # value generator: n -> list of element values
    literal: Any                     # representative scalar for constant_only params


# ---------------------------------------------------------------------------
# Value generators (stdlib only — no numpy/pyarrow needed for these shapes).
# ---------------------------------------------------------------------------
def _gen_strings(n: int) -> List[str]:
    pool = string.ascii_letters + " "
    return ["".join(random.choices(pool, k=12)) for _ in range(n)]


def _gen_floats(n: int) -> List[float]:
    return [random.uniform(1.0, 1000.0) for _ in range(n)]


def _gen_ints(n: int) -> List[int]:
    # Small positive ints: valid as substring positions, padding widths,
    # rounding precision, and as ordinary numeric input.
    return [random.randint(1, 8) for _ in range(n)]


def _gen_bools(n: int) -> List[bool]:
    return [bool(i & 1) for i in range(n)]


def _gen_dates(n: int) -> List[date]:
    base = date(2020, 1, 1)
    return [base + timedelta(days=i % 3650) for i in range(n)]


def _gen_timestamps(n: int) -> List[datetime]:
    base = datetime(2020, 1, 1)
    return [base + timedelta(seconds=(i % 86400) * 7) for i in range(n)]


def _gen_arrays(n: int) -> List[List[int]]:
    return [[1, 2, 3] for _ in range(n)]


def _gen_bytes(n: int) -> List[bytes]:
    return [bytes(random.randint(0, 255) for _ in range(8)) for _ in range(n)]


# Type-family -> how to build a column / literal for it.
FAMILIES: Dict[str, FamilySpec] = {
    "string": FamilySpec("VARCHAR", VARCHAR, _gen_strings, "AB"),
    "any": FamilySpec("VARCHAR", VARCHAR, _gen_strings, "AB"),
    "numeric": FamilySpec("DOUBLE", FLOAT64, _gen_floats, 2.0),
    "integer": FamilySpec("INT64", INT64, _gen_ints, 2),
    "boolean": FamilySpec("BOOLEAN", BOOLEAN, _gen_bools, True),
    "date": FamilySpec("DATE", DATE, _gen_dates, date(2021, 6, 1)),
    "timestamp": FamilySpec("TIMESTAMP", TIMESTAMP(), _gen_timestamps, datetime(2021, 6, 1, 12)),
    "temporal": FamilySpec("TIMESTAMP", TIMESTAMP(), _gen_timestamps, datetime(2021, 6, 1, 12)),
    "array": FamilySpec("ARRAY", ARRAY(VARIANT), _gen_arrays, [1, 2, 3]),
    "binary": FamilySpec("VARBINARY", VARBINARY, _gen_bytes, b"AB"),
}


# Some constant_only string parameters are *semantic tokens* (a date part, a
# truncation unit, a type name) — the generic per-family literal ("AB") is not a
# valid value and the kernel raises. Keyed by lowercase parameter name; the value
# is substituted for the family default when building that param's literal node.
CONST_PARAM_TOKENS: Dict[str, Any] = {
    "part": "day",        # EXTRACT, DATEDIFF
    "unit": "day",        # TRUNC(date/timestamp)
    "units": "day",       # TIME_BUCKET
    "mode": "words",      # HUMANIZE — the default ladder, so the cost is the
                          # one the no-mode call pays. The other modes walk a
                          # table of the same shape; none is the outlier.
}

# Non-const params whose data must satisfy a per-element shape the generic
# per-family column violates. Keyed by lowercase parameter name -> pseudo-family
# column key built in _build_morsel. (LPAD/RPAD `fill` must be a single char.)
IDENT_PARAM_COLUMNS: Dict[str, str] = {
    "fill": "_char1",
}


@dataclass
class OverloadResult:
    function_name: str
    overload_id: str
    kernel: str
    current_cost: float
    cost_us_per_million: Optional[float]
    ns_per_row: Optional[float]
    success: bool
    error: Optional[str] = None


class CostBench:
    """Builds a shared test morsel and benchmarks function overloads against it."""

    def __init__(self, sample_size: int, budget_s: float, reps: int):
        self.n = sample_size
        self.budget_s = budget_s
        self.reps = reps
        self.catalog = get_catalog()
        self._columns: Dict[str, Tuple[bytes, Any]] = {}  # family -> (identity, column_type)
        self._morsel = self._build_morsel()
        self._baseline_ns = self._measure(self._baseline_bytecode())

    # -- test data -------------------------------------------------------
    def _build_morsel(self) -> Morsel:
        names: List[bytes] = []
        vectors: List[Any] = []
        # One column per concrete dtype (families that share a dtype share a column).
        seen_dtype: Dict[str, bytes] = {}
        for family, spec in FAMILIES.items():
            if spec.dtype in seen_dtype:
                self._columns[family] = (seen_dtype[spec.dtype], spec.column_type)
                continue
            ident = mint_column_identity("bench", family)
            vec = vector_from_sequence(spec.gen(self.n), spec.dtype)
            names.append(ident)
            vectors.append(vec)
            seen_dtype[spec.dtype] = ident
            self._columns[family] = (ident, spec.column_type)
        # Single-character VARCHAR column for params (LPAD/RPAD `fill`) that
        # require exactly one character per element.
        char_ident = mint_column_identity("bench", "_char1")
        names.append(char_ident)
        vectors.append(vector_from_sequence(
            [random.choice(string.ascii_letters) for _ in range(self.n)], "VARCHAR"))
        self._columns["_char1"] = (char_ident, VARCHAR)
        return Morsel.from_vectors(names, vectors)

    def _identifier_node(self, family: str) -> Node:
        ident, column_type = self._columns[family]
        sc = SchemaColumn(name=family, identity=ident, column_type=column_type)
        return Node(NodeType.IDENTIFIER, schema_column=sc, value=ident)

    def _literal_node(self, family: str, param_name: Optional[str] = None) -> Node:
        spec = FAMILIES[family]
        value = spec.literal
        if param_name is not None and param_name.lower() in CONST_PARAM_TOKENS:
            value = CONST_PARAM_TOKENS[param_name.lower()]
        # VARCHAR/binary sequence edge is bytes-only; encode str literals.
        if isinstance(value, str):
            value = value.encode("utf-8")
        n = Node(NodeType.LITERAL, value=value)
        n.type = spec.column_type
        n.physical_type = spec.column_type.physical.value
        n.schema_column = ConstantColumn(name="lit", column_type=spec.column_type, value=value)
        return n

    def _baseline_bytecode(self):
        # Bare column load + return: the per-call VM + load/return overhead we
        # subtract from every function so the reported cost is marginal.
        return build_bytecode(lower(self._identifier_node("string")))

    # -- node construction ----------------------------------------------
    def _param_nodes(self, overload) -> List[Node]:
        nodes: List[Node] = []
        for p in overload.parameters:
            family = p.type_family
            if family not in FAMILIES:
                raise Unsupported(f"parameter type_family {family!r}")
            if p.constant_only:
                node = self._literal_node(family, p.name)
            else:
                col_key = IDENT_PARAM_COLUMNS.get(p.name.lower(), family)
                node = self._identifier_node(col_key)
            nodes.append(node)
            # A variadic param is exercised at its minimum arity (the single
            # instance above). Adding a second instance over-feeds kernels whose
            # trailing arg is really an *optional* scalar mis-tagged variadic
            # (CEILING/FLOOR/TRUNC scale), so the dispatch rejects the call.
        return nodes

    def _function_bytecode(self, func_name: str, overload):
        params = self._param_nodes(overload)
        resolved = self.catalog.resolve(func_name, params)
        if resolved is None:
            raise Unsupported("catalog.resolve returned None")
        fn = Node(NodeType.FUNCTION, value=func_name, parameters=params)
        fn.function_ref = resolved
        fn.schema_column = FunctionColumn(
            name=f"{func_name.lower()}_r", column_type=resolved.inferred_return_type
        )
        return build_bytecode(lower(fn)), resolved

    def _validate(self, result) -> None:
        """A scalar function must yield a length-N vector. Anything else (e.g. a
        placeholder lambda that returns a bare Python value, or a function whose
        real work is lowered elsewhere) is reported as a failure, never timed and
        emitted as a cost — a fabricated number is worse than an honest gap."""
        length = getattr(result, "length", None)
        if length != self.n:
            raise Unsupported(
                f"result is not a length-{self.n} vector "
                f"(got {type(result).__name__}, length={length}); "
                "kernel is likely a placeholder or the function is specially lowered "
                "— measure it through full SQL instead"
            )

    # -- timing ----------------------------------------------------------
    def _measure(self, bc) -> float:
        """Return median ns per execute_bytecode call (over the whole morsel)."""
        import gc

        morsel = self._morsel
        for _ in range(3):
            execute_bytecode(bc, morsel)
        t0 = time.monotonic_ns()
        execute_bytecode(bc, morsel)
        one = max(time.monotonic_ns() - t0, 1)
        iters = max(3, min(5000, int(self.budget_s * 1e9 / one)))
        samples: List[float] = []
        gc.disable()
        try:
            for _ in range(self.reps):
                gc.collect()
                t0 = time.monotonic_ns()
                for _ in range(iters):
                    execute_bytecode(bc, morsel)
                samples.append((time.monotonic_ns() - t0) / iters)
        finally:
            gc.enable()
        return statistics.median(samples)

    # -- public ----------------------------------------------------------
    def benchmark_function(self, func_def) -> List[OverloadResult]:
        results: List[OverloadResult] = []
        for overload in func_def.overloads:
            kernel = overload.kernel
            current = kernel.cost_us_per_million
            kernel_name = getattr(kernel.callable_ref, "__name__", repr(kernel.callable_ref))
            # Constant functions (NOW, PI, VERSION, ...) are defined via the _make
            # shorthand, whose placeholder kernel is `lambda *a: None`. They are
            # folded to a literal at bind time and have no per-row cost; executing
            # the placeholder through BC_FUNCTION returns None and segfaults the VM.
            # Skip them explicitly rather than risk a crash mid-sweep.
            if getattr(kernel.callable_ref, "__qualname__", "") == "_make.<locals>.<lambda>":
                results.append(
                    OverloadResult(
                        function_name=func_def.name,
                        overload_id=overload.id,
                        kernel=kernel_name,
                        current_cost=current,
                        cost_us_per_million=None,
                        ns_per_row=None,
                        success=False,
                        error="skipped: constant placeholder kernel (folded at bind time; no per-row cost)",
                    )
                )
                continue
            # try/except here RECORDS and REPORTS the failure (loudly); it never
            # swallows it to produce a fake cost.
            try:
                bc, _ = self._function_bytecode(func_def.name, overload)
                self._validate(execute_bytecode(bc, self._morsel))
                func_ns = self._measure(bc)
                ns_per_row = (func_ns - self._baseline_ns) / self.n
                cost = ns_per_row * 1000.0  # ns/row -> us per 1,000,000 rows
                results.append(
                    OverloadResult(
                        function_name=func_def.name,
                        overload_id=overload.id,
                        kernel=kernel_name,
                        current_cost=current,
                        cost_us_per_million=round(cost, 3),
                        ns_per_row=round(ns_per_row, 4),
                        success=True,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — recorded + reported, not swallowed
                results.append(
                    OverloadResult(
                        function_name=func_def.name,
                        overload_id=overload.id,
                        kernel=kernel_name,
                        current_cost=current,
                        cost_us_per_million=None,
                        ns_per_row=None,
                        success=False,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
        return results


def _selectable(func_def) -> Optional[str]:
    """Return a skip-reason if this function can't be measured as a scalar expr."""
    if func_def.category == "aggregate":
        return "aggregate (evaluated by aggregate operators, not the expression VM)"
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure scalar function execution costs.")
    parser.add_argument("--functions", type=str, help="Comma-separated names (default: all).")
    parser.add_argument("--output", type=Path, default=Path("function_costs.json"))
    parser.add_argument("--sample-size", type=int, default=250_000)
    parser.add_argument("--budget", type=float, default=0.5, help="Wall-clock seconds per timing.")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1234)
    args = parser.parse_args()

    random.seed(args.seed)
    bench = CostBench(args.sample_size, args.budget, args.reps)
    catalog = bench.catalog

    if args.functions:
        wanted = [f.strip().upper() for f in args.functions.split(",")]
        func_defs = [d for d in (catalog.get_definition(n) for n in wanted) if d is not None]
    else:
        func_defs = catalog.list_functions(include_deprecated=True)

    all_results: Dict[str, List[Dict[str, Any]]] = {}
    skipped: List[Tuple[str, str]] = []
    measured = 0
    failed = 0

    for func_def in sorted(func_defs, key=lambda d: d.name):
        if func_def.name in DEFAULT_EXCLUDE:
            skipped.append((func_def.name, "excluded by default"))
            continue
        reason = _selectable(func_def)
        if reason:
            skipped.append((func_def.name, reason))
            continue

        print(f"measuring {func_def.name} ...", flush=True)
        results = bench.benchmark_function(func_def)
        all_results[func_def.name] = [vars(r) for r in results]
        for r in results:
            if r.success:
                measured += 1
                print(
                    f"  {r.overload_id:<24} {r.kernel:<26} "
                    f"{r.current_cost:>10.2f} -> {r.cost_us_per_million:>10.2f} us/M"
                )
            else:
                failed += 1
                print(f"  FAIL {r.overload_id:<22} {r.error}")

    payload = {
        "method": "marginal per-row cost via bytecode evaluator (func - identity baseline)",
        "timestamp": time.time(),
        "sample_size": args.sample_size,
        "baseline_ns_per_call": round(bench._baseline_ns, 2),
        "measured_kernels": measured,
        "failed_kernels": failed,
        "functions": all_results,
    }
    args.output.write_text(json.dumps(payload, indent=2))

    print("\n" + "=" * 72)
    print(f"measured {measured} kernel(s), {failed} failed, {len(skipped)} function(s) skipped")
    if skipped:
        print("-" * 72)
        for name, why in skipped:
            print(f"  skip {name:<22} {why}")
    print(f"\nwritten to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
