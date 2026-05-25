# E30a — Python Imports Inside `cdef`/`cpdef` Bodies: Audit Report

> **Status:** Complete — read-only audit, no code changes.
> **Scope:** `opteryx/**/*.pyx`, `opteryx/**/*.pxi`, `rugo/**/*.pyx`, `rugo/**/*.pxi`
> **Excluded:** `tests/`, `scratch/`, `dev/`, generated `.c`/`.cpp` files
> **Audit script:** `dev/audit_pxx_imports.py` (raw scanner — classifications below are hand-verified against source)

---

## Findings Table

| # | File | Line | Containing function/method | Import statement (verbatim) | Category | Notes |
|---|------|------|----------------------------|----------------------------|----------|-------|
| 1 | `opteryx/compiled/expression/compiled_expression.pyx` | 67 | `_get_op_codes` | `from opteryx.expression.evaluator import _OP_CODE` | **C** | Lazy-init guard (`if _OP_CODES is None`); fires once. Comment above: "Bind-time lookups built lazily on first use." |
| 2 | `opteryx/compiled/expression/compiled_expression.pyx` | 75 | `_ensure_orso_types` | `from opteryx.types import OrsoTypes` | **C** | Same lazy-init pattern as #1; same comment block. |
| 3 | `opteryx/compiled/expression/compiled_expression.pyx` | 83 | `_ensure_set_types` | `from opteryx.compiled.structures.carchar_set import CarcharSetWrapper` | **C** | Same lazy-init pattern; both imports in one guard block. |
| 4 | `opteryx/compiled/expression/compiled_expression.pyx` | 84 | `_ensure_set_types` | `from opteryx.compiled.structures.perfect_hash_set import PerfectHashSet` | **C** | Companion import to #3, same guard. |
| 5 | `opteryx/compiled/vector_ops/vector_anyop_like.pyx` | 62 | `_regex_match_any_literal` | `from opteryx.utils.sql import sql_like_to_regex` | **A** | Vector op called per-morsel for LIKE/ILIKE on arrays; no guard — import fires every call. |
| 6 | `opteryx/compiled/vector_ops/vector_anyop_like.pyx` | 188 | `_regex_match_any_array_array` | `from opteryx.utils.sql import sql_like_to_regex` | **A** | Same as #5; different specialization (array×array). |
| 7 | `opteryx/compiled/vector_ops/vector_anyop_like.pyx` | 332 | `regex_match_any` | `from opteryx.utils.sql import sql_like_to_regex` | **A** | Same as #5; dispatch entry-point calling the above. |
| 8 | `opteryx/expression/evaluator/arithmetic.pyx` | 12 | `_to_string_vec` | `from draken.vectors.string_vector import StringVector` | **A** | Helper called from `_eval_binary_op_draken` (StringConcat path) per morsel; unconditional import. |
| 9 | `opteryx/expression/evaluator/arithmetic.pyx` | 26 | `_eval_binary_op_draken` | `from opteryx.types import OrsoTypes` | **A** | `cpdef`; called per morsel by expression evaluator; unconditional import on every call. |
| 10 | `opteryx/expression/evaluator/arithmetic.pyx` | 33 | `_eval_binary_op_draken` | `from draken.vectors.date32_vector import Date32Vector` | **A** | Conditional branch but no guard beyond type check; called per morsel on date arithmetic. |
| 11 | `opteryx/expression/evaluator/arithmetic.pyx` | 36 | `_eval_binary_op_draken` | `from draken.vectors.timestamp_vector import TimestampVector` | **A** | Same as #10; timestamp branch. |
| 12 | `opteryx/expression/evaluator/arithmetic.pyx` | 44 | `_eval_binary_op_draken` | `from draken.vectors.date32_vector import Date32Vector` | **A** | Right-operand date branch; same function. |
| 13 | `opteryx/expression/evaluator/arithmetic.pyx` | 47 | `_eval_binary_op_draken` | `from draken.vectors.timestamp_vector import TimestampVector` | **A** | Right-operand timestamp branch; same function. |
| 14 | `opteryx/expression/evaluator/arithmetic.pyx` | 68 | `_eval_binary_op_draken` | `from opteryx.compiled.nanobind.vector_selection_concat import vector_concat as _vc` | **A** | StringConcat fast-path; imports on every string concat morsel. |
| 15 | `opteryx/expression/evaluator/arithmetic.pyx` | 69 | `_eval_binary_op_draken` | `from draken.vectors.string_vector import StringVector as _SVT` | **A** | Companion to #14; same branch. |
| 16 | `opteryx/expression/evaluator/arithmetic.pyx` | 73 | `_eval_binary_op_draken` | `from opteryx.expression.binary_operators import BINARY_OPERATORS` | **A** | Fallback operator lookup; no guard; fires every call that reaches this branch. |
| 17 | `opteryx/expression/evaluator/arithmetic.pyx` | 115 | `_binary_op_from_vecs` | `from opteryx.types import OrsoTypes` | **A** | Bytecode executor calls this per `BC_BINARY_OP` instruction per morsel; unconditional import. |
| 18 | `opteryx/expression/evaluator/arithmetic.pyx` | 122 | `_binary_op_from_vecs` | `from draken.vectors.date32_vector import Date32Vector` | **A** | Date branch of `_binary_op_from_vecs`; per morsel. |
| 19 | `opteryx/expression/evaluator/arithmetic.pyx` | 125 | `_binary_op_from_vecs` | `from draken.vectors.timestamp_vector import TimestampVector` | **A** | Timestamp branch; same. |
| 20 | `opteryx/expression/evaluator/arithmetic.pyx` | 133 | `_binary_op_from_vecs` | `from draken.vectors.date32_vector import Date32Vector` | **A** | Right-operand date; same function. |
| 21 | `opteryx/expression/evaluator/arithmetic.pyx` | 136 | `_binary_op_from_vecs` | `from draken.vectors.timestamp_vector import TimestampVector` | **A** | Right-operand timestamp; same function. |
| 22 | `opteryx/expression/evaluator/arithmetic.pyx` | 157 | `_binary_op_from_vecs` | `from opteryx.compiled.nanobind.vector_selection_concat import vector_concat as _vc` | **A** | StringConcat path in bytecode variant; per morsel. |
| 23 | `opteryx/expression/evaluator/arithmetic.pyx` | 158 | `_binary_op_from_vecs` | `from draken.vectors.string_vector import StringVector as _SVT` | **A** | Companion to #22. |
| 24 | `opteryx/expression/evaluator/arithmetic.pyx` | 162 | `_binary_op_from_vecs` | `from opteryx.expression.binary_operators import BINARY_OPERATORS` | **A** | Fallback operator lookup in bytecode path; no guard. |
| 25 | `opteryx/expression/evaluator/case_eval.pyx` | 75 | `_decide` | `from opteryx.expression.evaluator.evaluation import evaluate_draken` | **C** | Explicit comment: "Lazy because evaluation.pyx imports this module's evaluate_case; the import cycle is broken by deferring to first call." |
| 26 | `opteryx/expression/evaluator/case_eval.pyx` | 126 | `_compute` | `from opteryx.expression.evaluator.evaluation import _eval_value` | **C** | Same circular-import situation as #25 (evaluation↔case_eval); no separate comment but same file and same reason. |
| 27 | `opteryx/expression/evaluator/evaluation.pyx` | 297 | `_get_legacy_helpers` | `from opteryx.expression import (` | **B** | Lazy-init guard (`if _legacy_helpers is None`); fires once per process. Called from `evaluate_and_append_draken` (legacy path). Not hot-path once loaded. |
| 28 | `opteryx/expression/evaluator/evaluation.pyx` | 1448 | `execute_bytecode` | `from opteryx.expression.binary_operators import MapAccessOp` | **A** | Inside hot bytecode interpreter loop; `BC_EXTRACTION` opcode handler; per-morsel, no guard. |
| 29 | `opteryx/expression/evaluator/evaluation.pyx` | 1452 | `execute_bytecode` | `from opteryx.expression.binary_operators import ArrowOp` | **A** | Same loop; Arrow operator branch. |
| 30 | `opteryx/expression/evaluator/evaluation.pyx` | 1456 | `execute_bytecode` | `from opteryx.expression.binary_operators import LongArrowOp` | **A** | Same loop; LongArrow operator branch. |
| 31 | `opteryx/expression/evaluator/temporal_ops.pyx` | 144 | `_date_interval_op_draken` | `from opteryx.expression.intervals import _as_interval_vector` | **A** | `cpdef`; called per morsel on date±interval arithmetic; unconditional import every call. (A comment nearby explains class-name detection, not this import.) |
| 32 | `opteryx/expression/evaluator/type_coercion.pyx` | 118 | `_coerce_timestamp` | `from opteryx.expression.casts import parse_timestamp_value` | **C** | Comment: "Lazy: parse_timestamp_value lives in opteryx.expression.casts which imports back through this package — keep the import inline to break the cycle." Conditional: only when value is string/bytes. |
| 33 | `opteryx/expression/evaluator/type_coercion.pyx` | 149 | `_coerce_temporal_scalar_for_arrow` | `from opteryx.expression.casts import parse_timestamp_value` | **C** | Comment: "Imported lazily for the same circular-import reason as _coerce_timestamp." |
| 34 | `opteryx/expression/evaluator/type_coercion.pyx` | 150 | `_coerce_temporal_scalar_for_arrow` | `from opteryx.types import OrsoTypes` | **C** | Part of the same deferred import block as #33. |
| 35 | `opteryx/compiled/structures/column_descriptor.pyx` | 69 | `serialize_descriptor` | `import json` | **B** | Serializes column metadata to bytes; called once per column per materialization event, not per row. |
| 36 | `opteryx/compiled/structures/column_descriptor.pyx` | 92 | `deserialize_descriptor` | `import json` | **B** | Mirror of #35 for deserialization. |
| 37 | `opteryx/operators/exit/exit.pyx` | 88 | `ExitNode._dispatch_push` | `from draken.interop.vector_sequence import vector_from_sequence` | **B** | Inside `if morsel is _EOS_SENTINEL and not self.at_least_one:` — fires at most once per query (empty result set path only). |
| 38 | `opteryx/operators/filter/filter.pyx` | 107 | `_build_constant_vector` | `from draken.vectors.integer64_vector import Integer64Vector` | **A** | Called per morsel for constant-folded filter comparisons; four unconditional imports at function entry every invocation. |
| 39 | `opteryx/operators/filter/filter.pyx` | 108 | `_build_constant_vector` | `from draken.vectors.float64_vector import Float64Vector` | **A** | Same as #38. |
| 40 | `opteryx/operators/filter/filter.pyx` | 109 | `_build_constant_vector` | `from draken.vectors.bool_vector import BoolVector` | **A** | Same as #38. |
| 41 | `opteryx/operators/filter/filter.pyx` | 110 | `_build_constant_vector` | `from draken.vectors.string_vector import StringVector` | **A** | Same as #38. |
| 42 | `opteryx/operators/filter_join/filter_join.pyx` | 170 | `_rebuild_carchar_from_phash` | `from draken.vectors.scalar_constructors import from_scalar as _build_scalar` | **B** | Docstring: "Called only when probe side turns out to have a column encoding the PerfectHashSet path can't handle." Fallback path; called at most once per join build. |
| 43 | `opteryx/operators/filter_join/filter_join.pyx` | 456 | `FilterJoinNode.push_right` | `from opteryx.exceptions import InvalidInternalStateError` | **B** | Inside `if phash is None: raise` — error path only; normal execution never reaches this import. |
| 44 | `opteryx/operators/nested_loop_join/nested_loop_join.pyx` | 159 | `NestedLoopJoinNode.push_left` | `from opteryx.compiled.structures.bloom_filter import create_bloom_filter_morsel` | **B** | Inside `if morsel is _EOS_SENTINEL:` block; fires once per query when left side is fully buffered. |
| 45 | `opteryx/operators/nested_loop_join/nested_loop_join.pyx` | 194 | `NestedLoopJoinNode.push_right` | `from opteryx.compiled.structures.bloom_filter import bloom_filter_check_morsel` | **A** | Called for every right-side morsel when a bloom filter exists (`if self.left_filter is not None:`); no guard; per-morsel import. |
| 46 | `opteryx/vectors/vector_math.pyx` | 190 | `row_as_fp32_array` | `from array import array as _array` | **A** | `cpdef`; called per vector row for FP16→FP32 conversion; unconditional import. (`array` is stdlib so cheap, but still a dict lookup per call.) |
| 47 | `opteryx/operators/grouped_aggregate_hashed/_collectors_approx.pxi` | 74 | `ApproxCountDistinctCollector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Collector `finalize` called once per group-aggregate finalization — but finalize is invoked per morsel batch in grouped aggregation. |
| 48 | `opteryx/operators/grouped_aggregate_hashed/_collectors_approx.pxi` | 136 | `ApproxPercentileCollector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Same as #47. |
| 49 | `opteryx/operators/grouped_aggregate_hashed/_collectors_approx.pxi` | 181 | `ArrayAggCollector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Same as #47. |
| 50 | `opteryx/operators/grouped_aggregate_hashed/_collectors_distinct.pxi` | 126 | `CountDistinctCollector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Original E.29 §9.4 finding. |
| 51 | `opteryx/operators/grouped_aggregate_hashed/_collectors_distinct.pxi` | 179 | `AnyValueInt64Collector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Original E.29 §9.4 finding. |
| 52 | `opteryx/operators/grouped_aggregate_hashed/_collectors_distinct.pxi` | 243 | `AnyValueFloat64Collector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Original E.29 §9.4 finding. |
| 53 | `opteryx/operators/grouped_aggregate_hashed/_collectors_distinct.pxi` | 305 | `AnyValueObjectCollector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Original E.29 §9.4 finding. |
| 54 | `opteryx/operators/grouped_aggregate_hashed/_collectors_numeric.pxi` | 863 | `MinMaxObjectCollector.finalize` | `from draken.interop.arrow import vector_from_sequence` | **A** | Original E.29 §9.4 finding. |

---

### Draken `.pyx`/`.pxi` Findings (separate section per §2)

| # | File | Line | Containing function/method | Import statement (verbatim) | Category | Notes |
|---|------|------|----------------------------|----------------------------|----------|-------|
| D1 | `draken/morsels/_morsel_shim.pyx` | 23 | `_make_morsel` | `from draken.draken_native import Morsel as NbMorsel` | **B** | Shim constructor; called once per morsel creation during bridge path. Not a hot inner loop. |

---

## Summary

- **Total findings (opteryx/rugo):** 54
- **(A) Hot-path:** 38
- **(B) Init-time / once-per-query:** 9
- **(C) Defensible deferred:** 7
- **(?) Ambiguous:** 0
- **Draken findings:** 1 (all category B)

---

## §6 Reporting — Top Three (A) Findings by Likely Call Frequency

**#1 — `arithmetic.pyx`: `_eval_binary_op_draken` and `_binary_op_from_vecs` (findings 9–24)**
These two `cpdef` functions are the primary binary arithmetic dispatch path and the bytecode executor variant respectively. Every query with arithmetic, string concatenation, or date arithmetic calls one of these per morsel. Together they account for 16 import statements across 2 functions — a `from X import Y` fires on every call that reaches each branch. `OrsoTypes` (findings 9, 17) is unconditional, meaning every arithmetic morsel pays for it.

**#2 — `filter.pyx`: `_build_constant_vector` (findings 38–41)**
Four unconditional imports at function entry, every invocation. Called per morsel for each constant-operand filter predicate. Queries with simple equality filters (extremely common) call this function on every morsel of every filtered relation.

**#3 — `_collectors_*.pxi` (findings 47–54) / `evaluation.pyx execute_bytecode` (findings 28–30)**
Eight collector `finalize` methods each import `vector_from_sequence` unconditionally; any grouped-aggregate query using APPROX_COUNT_DISTINCT, percentile, ARRAY_AGG, COUNT DISTINCT, or ANY_VALUE triggers these. The `execute_bytecode` BC_EXTRACTION handlers (findings 28–30) sit inside the tight bytecode interpreter loop and import `MapAccessOp`/`ArrowOp`/`LongArrowOp` on every map/JSON access operation.

---

## Patterns Not Covered by E.29 §9.4

E.29 §9.4 identified 4–5 hits in `_collectors_*.pxi`. This audit found **54 total**, with the following additional pattern clusters not previously called out:

1. **`arithmetic.pyx` — whole-function deferred imports (16 findings):** The two binary-op `cpdef` functions defer all their vector-type and operator imports into the function body. No lazy-init guard — imports fire on every call. This is the largest single cluster by finding count and by execution frequency.

2. **`filter.pyx _build_constant_vector` — 4 unconditional type imports at function entry:** A common filter helper that imports four draken vector types every call.

3. **`evaluation.pyx execute_bytecode` — inside a bytecode interpreter loop (3 findings):** `BC_EXTRACTION` opcode imports its legacy operators inline on every extraction instruction, inside the main evaluation loop.

4. **`type_coercion.pyx` — conditional circular-import workarounds (3 findings, all C):** Correctly deferred with comments; no action needed, but the pattern confirms that `parse_timestamp_value` in `opteryx.expression.casts` is a circular-import pressure point.

5. **`vector_anyop_like.pyx` — repeated import in three specializations (3 findings):** The same `sql_like_to_regex` import appears in the literal, array×array, and dispatch-entry versions. A module-level `cimport`/import would eliminate all three.

No `__import__()` calls, `importlib` usage, wildcard imports (`from X import *`), or conditional platform imports were found. The fire is entirely standard `from X import Y` inside function bodies.
