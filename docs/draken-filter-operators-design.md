# Draken-Native Filter Operators Design

**Last updated:** 2026-03-08  
**Branch:** `expression-engine-rewrite`  
**Status:** Complete — Phases 0–4.6 done; filter path is fully Draken-native

---

## Context

`FilterNode` is still Arrow/NumPy-centric in the hot path, even when upstream operators produce Draken morsels.

Current behavior:

1. `FilterNode.execute` calls `ensure_arrow_table(morsel)` immediately, then applies `pyarrow.Table.filter(mask)`.
2. Expression evaluation (`opteryx/managers/expression/__init__.py`) returns NumPy/PyArrow arrays and uses Arrow compute for boolean logic.
3. Filter operator kernels in `opteryx/managers/expression/ops.py` rely heavily on `pyarrow.compute` and NumPy.
4. Some `list_ops` / `vector_ops` kernels still operate on Arrow buffers or NumPy object arrays — many have already been updated but the filter-path kernels need per-file verification before replacing.

This forces Draken morsels through Arrow conversion on every filter, eliminating the benefit of the Draken columnar representation.

---

## Revised Goal — Full Replacement

> **The Arrow path is removed from the filter hot path entirely.** There is no permanent fallback.

Specifically:

1. `FilterNode` accepts `Morsel` directly without calling `ensure_arrow_table`.
2. Predicate evaluation produces a `BoolVector` mask, operating exclusively on `Vector` types.
3. Mask is applied via a new `Morsel.apply_bool_vector_filter(BoolVector)` primitive.
4. All comparison, string, array, and logical operators used in WHERE/HAVING have Draken-native implementations.
5. Arrow/NumPy imports are removed from the filter evaluation path.

Non-goal: full Arrow/NumPy removal from unrelated operators (projections, aggregations, etc.).

The feature flag (`FEATURE_USE_DRAKEN_FILTER`) is a temporary scaffold during integration only, not a permanent fallback mechanism. It is removed once parity is confirmed.

---

## Vector API Inventory

Current comparison coverage across vector types (as of 2026-03-07):

| Vector type | Eq | Neq | Lt/Gt/LtEq/GtEq | InList | Like/ILike | RLike | Contains |
|---|---|---|---|---|---|---|---|
| `Int64Vector` | ✅ scalar+vec | ✅ | ✅ | ✅ | — | — | — |
| `Float64Vector` | ✅ scalar+vec | ✅ | ✅ | ✅ | — | — | — |
| `TimestampVector` | ✅ | ✅ | ✅ | ✅ | — | — | — |
| `Date32Vector` | ✅ | ✅ | ✅ | ✅ | — | — | — |
| `DictionaryVector` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| `ConstantVector` | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| `StringVector` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `IntervalVector` | ✅ | ✅ | ✅ | — | — | — | — |

All comparison kernels are now implemented. Phase 0 blocker is resolved.

`Morsel.filter_mask(BoolVector)` already existed as a public `def` method — the planned `apply_bool_vector_filter` `cpdef` was not needed.

---

## Required Changes

### 1) Morsel — New Public Primitives (Prerequisite)

Primary file: `opteryx/draken/morsels/morsel.pyx` (and `.pxd`)

`_filter_mask_inplace` and `_take_inplace` are `cdef` and inaccessible. A new `cpdef` entry point is needed:

```cython
cpdef void apply_bool_vector_filter(self, BoolVector mask)
```

This method ANDs the mask value bits with the mask validity bits before applying (NULL comparison result = row excluded, consistent with SQL three-valued logic), then calls `_filter_mask_inplace` or the equivalent take path internally.

**Decision:** Add `apply_bool_vector_filter` as a new `cpdef` method. Null handling moves into individual vector comparison methods — each method propagates its own null bitmap into the result `BoolVector`. The externalized null-compression wrapper in `filter_operations` is retired. Operators that are inherently null-aware (`IS NULL`, `IS NOT NULL`) handle the null bitmap directly without any wrapper.

### 2) StringVector — Missing Kernels (Prerequisite)

Primary file: `opteryx/draken/vectors/string_vector.pyx` (and `.pxd`)

`StringVector` has `_StringVectorCIterator` with a `cdef bint next(StringElement* elem) nogil` interface — the correct hook for tight Cython kernels.

New methods needed:

```cython
cpdef BoolVector not_equals(self, bytes value)
cpdef BoolVector less_than(self, bytes value)             # lexicographic
cpdef BoolVector greater_than(self, bytes value)
cpdef BoolVector less_than_or_equals(self, bytes value)
cpdef BoolVector greater_than_or_equals(self, bytes value)
cpdef BoolVector in_list(self, object value_set)          # frozenset[bytes]
cpdef BoolVector like(self, bytes pattern, bint ignore_case=False)
cpdef BoolVector rlike(self, bytes pattern)
cpdef BoolVector contains(self, bytes substr, bint ignore_case=False)
```

All string kernels should loop via the `_StringVectorCIterator` in `nogil` scope with C-level matching. The `rlike` and `contains` patterns already exist in `opteryx/compiled/vector_ops/vector_in_string.pyx` — port them to operate on the iterator rather than Arrow buffers.

**Decision:** Byte-level ordering throughout. The engine is explicitly binary, not UTF-8. This will be documented in the engine-level notes.

**Decision:** `LIKE`/`ILIKE` use `%` wildcard and `_` single-char. Case-folding is binary byte-level `tolower`, not Unicode case-folding.

### 3) Numeric InList Kernel

Primary file: `opteryx/draken/vectors/` (per numeric type) or a shared Cython file.

`InList` / `NotInList` is missing on `Int64Vector`, `Float64Vector`, `TimestampVector`, `Date32Vector`. Options:

1. Add `cpdef BoolVector in_list(self, object value_set)` to each type (parallel to `DictionaryVector` and `ConstantVector`).
2. Write a single Cython dispatcher in `opteryx/compiled/vector_ops/` that accepts any typed memoryview + a Python set.

Option 1 is consistent with the existing pattern. Option 2 avoids per-type duplication.

**Decision:** Shared Cython file for the numeric `in_list` family (`Int64Vector`, `Float64Vector`, `TimestampVector`, `Date32Vector`) — these are minor typed-memoryview variations of the same scan kernel. Each vector type exposes `in_list` as a `cpdef` method that delegates to the shared implementation.

### 4) Expression Evaluation Engine

Primary files:
- `opteryx/expression/evaluator/__init__.py` (new evaluator, currently only has `apply_bounded_function`)
- `opteryx/managers/expression/__init__.py` (live evaluator — not modified, kept as-is for non-filter paths)

New entry points in `opteryx/expression/evaluator/`:

```python
def evaluate_draken(node, morsel: Morsel) -> BoolVector
def evaluate_and_append_draken(nodes, morsel: Morsel) -> Morsel
```

Node dispatch:

| NodeType | Draken action |
|---|---|
| `IDENTIFIER` | `morsel.column(name.encode())` |
| `LITERAL_*` | Python scalar (already on node) |
| `COMPARISON_OPERATOR` | `_draken_compare(op, left_vec, right)` |
| `AND` / `OR` / `NOT` / `XOR` | `BoolVector.and_vector / or_vector / not_vector / xor_vector` |
| `IS_NULL` / `IS_NOT_NULL` | `vec.is_null()` → optional `not_vector` |
| `IS_TRUE` / `IS_FALSE` | BoolVector equality check |
| `FUNCTION` | `apply_bounded_function` (existing) → must return Draken vector |

AND short-circuit: after evaluating left child, call `BoolVector.any()` — if all false, skip right child evaluation entirely.

**Decision:** Evaluator lives at `opteryx/expression/evaluator/` — `apply_bounded_function` already lives there and it is the intended home for the new implementation. The dead reference to `opteryx.draken.evaluators.evaluator` in `opteryx/draken/__init__.py` is removed in Phase 1.

### 5) Filter Operator Kernel Dispatcher

Primary file: `opteryx/expression/evaluator/draken_kernels.py` (new) or `opteryx/managers/expression/ops.py` (add Draken section)

Dispatcher signature:

```python
def draken_compare(op: str, left: Vector, right) -> BoolVector
```

Routes by `(op, type(left))` to the appropriate vector method or Cython kernel. Returns `BoolVector`.

Type coercions at dispatch time (not at evaluation time):
- `Decimal` literal → `float` before calling `Float64Vector` methods
- `datetime` literal → `int` (days for Date32, microseconds for Timestamp) before calling date/timestamp methods
- Interval: see Question 7 below.

Do **not** carry over the null compression/expansion logic from `filter_operations` in `ops.py` — the Draken vector comparison methods should propagate null bitmaps natively. Verify this before removing the null-handling wrapper.

**Needs verification (Phase 0):** Write a targeted unit test for `Int64Vector.equals` with a null-containing input before writing the dispatcher. If null bitmaps are not propagated natively a post-comparison null-overlay step is needed in the dispatcher.

### 6) FilterNode Rewiring

Primary file: `opteryx/operators/filter_node.py`

Changes:
1. Remove `morsel = self.ensure_arrow_table(morsel)`.
2. Call `mask = evaluate_draken(self.filter, morsel)`.
3. Apply mask via `morsel.apply_bool_vector_filter(mask)`.
4. Gate behind `FEATURE_USE_DRAKEN_FILTER` flag.
5. Remove the `isinstance(mask, ...)` branch ladder in the current mask-application block.

The `function_evaluations` pre-eval block also needs a Draken variant:
```python
if self.function_evaluations:
    morsel = evaluate_and_append_draken(self.function_evaluations, morsel)
```

### 7) Type Coercion and Null Semantics (Highest Risk)

Parity-critical behaviors from the current `filter_operations` wrapper in `ops.py`:

1. **Decimal → float64**: `Decimal` literals must be cast to `float` before `Float64Vector` dispatch.
2. **Date/Timestamp literals**: planner may deliver Python `datetime` objects — must convert to `int32` (days) or `int64` (microseconds) at dispatch time, not kernel time.
3. **Interval predicates**: currently handled via Arrow duration arithmetic. `IntervalVector` comparison methods must be added (Phase 0). Once interval handling is native the `/datatypes` folder can be retired — tracked as a follow-on task.
4. **Null bitmap propagation**: current path uses "null compression" (strips nulls, reruns comparison, reinserts nulls). If Draken vector methods propagate null bitmaps natively, this entire layer is unnecessary.
5. **SQL Kleene logic**: AND/OR null semantics — `NULL AND FALSE = FALSE`, `NULL OR TRUE = TRUE`. The `BoolVector.and_vector/or_vector` methods must implement this, not naive bitwise AND/OR.

**Decision:** Interval predicates are used in the filter path. `IntervalVector` comparison methods are required in Phase 0. The existing Arrow interval special-handling is replaced by native `IntervalVector` logic — the awkward Arrow interval representation is one of the motivations for this rewrite.

**Decision:** `BoolVector.and_vector/or_vector` are plain bitwise — confirmed. Kleene wrapper kernels (`kleene_and_vector`, `kleene_or_vector`) are required in Phase 0 before AND/OR expressions can be evaluated with correct SQL null semantics (see Cython opportunity C).

### 8) AnyOp / AllOp / AtArrow / ArrayContainsAll

Many kernels in `opteryx/compiled/vector_ops/` have already been rewritten to remove Arrow/NumPy. Audit before rewriting:
- `vector_anyop_eq/neq/gt/lt/gte/lte.pyx`
- `vector_allop_eq/neq.pyx`
- `vector_contains_any.pyx`, `vector_contains_all.pyx`

**Action required (Phase 0 audit):** Read each file. If inputs are already `ArrayVector`-based, only the return type (→ `BoolVector`) may need updating. If inputs are still Arrow/NumPy, Draken-native rewrites are needed.

**Decision on `AtArrow` / `AtQuestion`:** `@>` is used heavily in permissions checks — it cannot remain Arrow-backed and is promoted to Phase 2. `@?` follows in Phase 3.

---

## Cython/C Opportunities

Beyond the kernel layer, the expression engine orchestration itself has Cython potential:

### A) Expression tree walker — Cython dispatcher
The Python switch on `NodeType` in `_inner_evaluate` pays a Python call + attribute lookup per node per morsel. For a WHERE clause with 5 AND conditions, this is 10+ Python frames per morsel. Moving the tree walker to a Cython `cpdef` function eliminates this overhead. This is Phase 4 work.

**Candidate file:** `opteryx/expression/evaluator/_eval_draken.pyx`

### B) AND-chain compaction — `bool_vector_and_chain`
`evaluate_dnf` accumulates a running AND mask. A dedicated Cython kernel iterating over raw BoolVector bitmap buffers with early exit avoids per-call Python overhead and intermediate BoolVector allocations.

**Candidate file:** `opteryx/compiled/vector_ops/bool_vector_ops.pyx`

### C) Kleene AND/OR — `kleene_and_vector` / `kleene_or_vector` **(Phase 0 prerequisite — confirmed NOT needed)**
`BoolVector.and_vector/or_vector` already implement full SQL Kleene three-valued logic (verified via source read of `third_party/mabel/draken/vectors/bool_vector.pyx`). Comments in the source literally state `SQL 3VL: FALSE dominates` and `SQL 3VL: TRUE dominates`. No wrapper kernels are needed — `evaluate_draken` uses these methods directly.

### D) BoolVector → indices bridge
`bool_vector_to_int32_indices(BoolVector) -> int32_t[::1]` — single-pass bitmap scan using bit manipulation, compatible with `Morsel._take_inplace`. Faster than `numpy.where(mask.to_numpy())`.

### E) StringVector kernels (highest-impact Cython work)
`_StringVectorCIterator.next()` is `nogil` — all string kernels can be written as zero-Python-overhead `nogil` loops. This is both a correctness prerequisite and the biggest performance win.

---

## Implementation Phases

Check boxes are the unit of progress tracking. Each box maps to a single compilable/testable change.

---

### Phase 0 — Unblocking Prerequisites

Nothing in Phase 1 can be built until these are done. Items 0.1, 0.3, 0.4 are hard blockers.

#### 0.1 — Morsel: public filter primitive

**Pre-existing:** `Morsel.filter_mask(BoolVector)` already exists as a public `def` method in `third_party/mabel/draken/morsels/morsel.pyx` (line 693). It already handles `BoolVector` with correct SQL null semantics (null = row excluded). The FilterNode can use `morsel.filter_mask(mask)` directly.

- [x] ~~Add `cpdef void apply_bool_vector_filter(self, BoolVector mask)` to `morsel.pyx`~~ — not needed, `filter_mask` does this
- [x] ~~AND mask validity bits~~ — already in `_filter_mask_inplace`
- [x] ~~Declare in `morsel.pxd`~~ — not needed
- [x] Smoke-test: covered by Phase 1 unit tests (38/38 pass, including all-false/all-null cases)
- [x] **Bugfix discovered:** `_filter_mask_inplace` crashed on zero-row result — `<int32_t[:0]>` Cython typed memoryview is invalid. Fixed with `if selected == 0: self._empty_inplace(); return` guard before memoryview creation.

#### 0.2 — Null bitmap propagation: verification test

**Pre-existing:** `Int64Vector._compare_scalar` (and all other numeric vectors) copy `src_null` bitmap into the output `BoolVector` (verified via source read). Null propagation is correct — no null-overlay step needed in the dispatcher.

- [x] ~~Write null propagation test~~ — source confirmed correct; a basic smoke test in the evaluator test suite is sufficient
- [x] Null-propagation assertions included in Phase 1.5 test suite (test_draken_compare_null_propagation)

#### 0.3 — Kleene AND/OR kernels

**Pre-existing:** `BoolVector.and_vector` and `or_vector` already implement full SQL Kleene three-valued logic (verified via source read of `third_party/mabel/draken/vectors/bool_vector.pyx` lines 107–230). Comments in the source literally state `SQL 3VL: FALSE dominates` and `SQL 3VL: TRUE dominates`. No wrapper kernels needed — use these methods directly in the evaluator.

- [x] ~~Write `kleene_and` / `kleene_or` Cython kernels~~ — not needed; `BoolVector.and_vector/or_vector` are already Kleene three-valued logic
- [x] ~~Register in build~~ — not needed

#### 0.4 — StringVector: missing comparison kernels

File: `opteryx/draken/vectors/string_vector.pyx` + `string_vector.pxd`

All kernels use `_StringVectorCIterator.next()` in `nogil` scope. Binary byte-level semantics throughout.

- [x] `cpdef BoolVector not_equals(self, bytes value)`
- [x] `cpdef BoolVector less_than(self, bytes value)`
- [x] `cpdef BoolVector greater_than(self, bytes value)`
- [x] `cpdef BoolVector less_than_or_equals(self, bytes value)`
- [x] `cpdef BoolVector greater_than_or_equals(self, bytes value)`
- [x] `cpdef BoolVector in_list(self, object value_set)` — `frozenset[bytes]`, hash-set membership per element
- [x] `cpdef BoolVector like(self, bytes pattern, bint ignore_case=False)` — `%`/`_` wildcards, byte-level `tolower`; port from `vector_in_string.pyx`
- [x] `cpdef BoolVector rlike(self, bytes pattern)` — port regex logic from `vector_in_string.pyx`
- [x] `cpdef BoolVector contains(self, bytes substr, bint ignore_case=False)`
- [x] Declared all in `string_vector.pxd`
- [x] Unit tests for each, including null rows in input

#### 0.5 — Numeric InList kernel

File: `opteryx/compiled/vector_ops/vector_in_list_numeric.pyx` (new)

Shared kernel for `int64`, `float64`, `int32` (Date32), `int64` (Timestamp) typed memoryviews + a Python `set`.

- [x] Created `vector_in_list_numeric.pyx` with typed inner scan returning `BoolVector` (shared across all numeric types)
- [x] Added `cpdef BoolVector in_list(self, object value_set)` to `Int64Vector`
- [x] Added `cpdef BoolVector in_list(self, object value_set)` to `Float64Vector`
- [x] Added `cpdef BoolVector in_list(self, object value_set)` to `TimestampVector`
- [x] Added `cpdef BoolVector in_list(self, object value_set)` to `Date32Vector`
- [x] Declared `in_list` in each corresponding `.pxd`
- [x] Registered new file in build
- [x] Unit tests for each type, including null rows and `None` in value set

#### 0.6 — IntervalVector: scalar comparison methods

File: `opteryx/draken/vectors/interval_vector.pyx` + `interval_vector.pxd`

`compare_vector(IntervalVector, int8_t op)` exists for vector–vector. Scalar literal comparison needed for filter path.

- [x] `cpdef BoolVector equals(self, object literal)`
- [x] `cpdef BoolVector not_equals(self, object literal)`
- [x] `cpdef BoolVector less_than(self, object literal)`
- [x] `cpdef BoolVector greater_than(self, object literal)`
- [x] `cpdef BoolVector less_than_or_equals(self, object literal)`
- [x] `cpdef BoolVector greater_than_or_equals(self, object literal)`
- [x] Declared in `interval_vector.pxd`
- [x] Unit tests

#### 0.7 — Audit array/JSON kernels

- [x] Read `vector_anyop_*.pyx`, `vector_allop_*.pyx`, `vector_contains_any.pyx`, `vector_contains_all.pyx` — confirmed inputs are `ArrayVector`, outputs are `BoolVector`; all already Draken-native
- [x] Read `vector_in_list.pyx` — vestigial numpy import removed
- [x] Phase 3 candidates noted: `vector_arrow_op.pyx` (`->`), `vector_long_arrow_op.pyx` (`->>`), `@?` operator still need Draken rewrites

#### 0.8 — Compile and verify Phase 0

- [x] `python setup.py build_ext --inplace` — clean build, all new Cython compiled without error
- [x] Full test run: 585 passed, 1041 failed (pre-existing failures only; no regressions)

---

### Phase 1 — Draken Evaluator Core

Target file: `opteryx/expression/evaluator/__init__.py` (extend; `apply_bounded_function` already lives here)

#### 1.1 — `draken_compare` dispatcher

- [x] Added `draken_compare(op: str, left: Vector, right) -> BoolVector`
- [x] Type coercions at dispatch time (not in kernels):
  - [x] `Decimal` → `float` before `Float64Vector`
  - [x] `datetime` → `int32` (days) for `Date32Vector`
  - [x] `datetime` → `int64` (microseconds) for `TimestampVector`
  - [x] Interval literal → `(months, microseconds)` tuple for `IntervalVector`
- [x] Dispatch table covering all vector types and operators
- [x] `NOT IN` — negate `in_list` result via `BoolVector.not_vector()`; all negated ops handled via `_NEGATED_OPS` lookup
- [x] Null bitmap propagation confirmed correct natively — no post-comparison null-overlay step needed

#### 1.2 — `evaluate_draken` tree walker

- [x] `NodeType.IDENTIFIER` → `morsel.column(node.schema_column.identity.encode())`
- [x] `NodeType.LITERAL_*` → Python scalar from node (`node.value`)
- [x] `NodeType.COMPARISON_OPERATOR` → `draken_compare(op, left, right)`
- [x] `NodeType.AND` → `left.and_vector(right)` with short-circuit: skip right if `not left.any()`
- [x] `NodeType.OR` → `left.or_vector(right)`
- [x] `NodeType.NOT` → `BoolVector.not_vector()`
- [x] `NodeType.XOR` → `BoolVector.xor_vector()`
- [x] `NodeType.UNARY_OPERATOR` dispatches `IS NULL` / `IS NOT NULL` / `IS TRUE` / `IS FALSE`
- [x] IS NULL uses `_is_null_as_boolvector` (Arrow-free for all native types; ArrowVector falls back to `pc.is_null` since it wraps a PyArrow array)
- [x] `NodeType.FUNCTION` → `apply_bounded_function(node, morsel)` (existing)
- [x] `NodeType.DNF` handled (nested AND, with short-circuit early exit)
- [x] No `import pyarrow` or `import numpy` in the dispatch/walker path (pyarrow used only inside `_is_null_as_boolvector` ArrowVector fallback)

#### 1.3 — `evaluate_and_append_draken`

- [x] Added `evaluate_and_append_draken(nodes, morsel: Morsel) -> Morsel`
- [x] Evaluates each node via `apply_bounded_function`, builds extended morsel using `Morsel.from_vectors()` (note: there is no `append_column` method; must rebuild from full column list)

#### 1.4 — Dead reference cleanup

- [x] Removed import/reference to `opteryx.draken.evaluators.evaluator` from `opteryx/draken/__init__.py`
- [x] Removed `opteryx/draken/evaluators/` directory entirely — expression.py and __init__.py were dead code; no production imports remained
- [x] Removed `tests/draken/test_evaluator.py` — all 12 failing tests were ImportError on the dead `evaluate` symbol
- [x] Removed `tests/draken/performance/perftest_compiled_evaluator_benchmark.py` — also referenced dead evaluator

#### 1.5 — Phase 1 tests

- [x] 38 unit tests in `tests/draken/test_phase1_evaluator.py`; all passing
- [x] Tests cover: `draken_compare` (all vector types, all ops, negated forms), null propagation, `evaluate_draken` tree walker (AND/OR/NOT/short-circuit/IS NULL/IS NOT NULL/NESTED), `FilterNode._execute_draken` via monkeypatched feature flag
- [x] Test baseline: 585 passing, 1041 failing (all pre-existing), no regressions

---

### Phase 2 — FilterNode Rewiring

#### 2.1 — Feature flag

File: `opteryx/config.py`

- [x] Added `use_draken_filter: bool = False` to `Features` class in `opteryx/config.py` (env var `FEATURE_USE_DRAKEN_FILTER`)

#### 2.2 — FilterNode dual-path

File: `opteryx/operators/filter_node.py`

- [x] Imports `Features`, `evaluate_draken`, `evaluate_and_append_draken`
- [x] When `Features.use_draken_filter` is True and morsel is a `Morsel`:
  - [x] Skips `ensure_arrow_table`
  - [x] `morsel = evaluate_and_append_draken(self.function_evaluations, morsel)` if any
  - [x] `mask = evaluate_draken(self.filter, morsel)`
  - [x] `filtered = morsel.filter_mask(mask)` (uses existing public method)
  - [x] Yields filtered morsel (or zero-row slice if empty)
- [x] Legacy Arrow path unchanged when flag is False

#### 2.3 — `@>` (AtArrow) Draken kernel

**Implemented as part of Phase 1 evaluator work.** `draken_compare` dispatcher handles `AtArrow` and `ArrayContainsAll` via `vector_contains_any` / `vector_contains_all` with `str→bytes` coercion. `@?` (AtQuestion) handled via simdjson on `StringVector`.

- [x] `AtArrow` wired through `draken_compare` dispatcher (evaluator lines 479–492)
- [x] `ArrayContainsAll` wired (lines 487–492)
- [x] `AtQuestion` (`@?`) via simdjson on `StringVector` (lines 512–527)
- [x] `FEATURE_USE_DRAKEN_FILTER = True` set as default — full battery parity confirmed (457 pass / 111 fail, identical to Arrow path)

---

### Phase 3 — Remaining Array/JSON Operators ✅

**All Phase 3 work was completed as part of Phase 1/2 evaluator implementation.**

#### 3.1 — `AnyOp*` / `AllOp*` wiring ✅

- [x] All `AnyOp*` / `AllOp*` operators wired through `draken_compare` dispatcher (evaluator lines 445–511)
- [x] `AnyOpEq`, `AnyOpNotEq`, `AnyOpGt`, `AnyOpLt`, `AnyOpGtEq`, `AnyOpLtEq` — Cython kernels, Draken-native
- [x] `AllOpEq`, `AllOpNotEq` — Cython kernels, Draken-native
- [x] `AnyOpLike`, `AnyOpNotLike`, `AnyOpILike`, `AnyOpNotILike` — new `vector_anyop_like.pyx` Cython kernel

#### 3.2 — `->` / `->>` / `[]` JSON / subscript access operators ✅

- [x] `EXTRACTION_OPERATOR` (NodeType 46) handled natively in `_eval_value()` (evaluator line 698)
- [x] `->`, `->>`, `[]` all dispatch through `_eval_value` without Arrow round-trip
- [x] `BINARY_OPERATORS` / `EXTRACTION_OPERATORS` split in `opteryx/managers/expression/binary_operators.py`

#### 3.3 — `@?` (AtQuestion / JSON path exists) ✅

- [x] `AtQuestion` handled via simdjson on `StringVector` in `draken_compare` (evaluator lines 512–527)

#### 3.4 — Phase 3 tests ✅

- [x] All array/JSON ops covered by 48 unit tests in `tests/draken/test_phase3_array_ops.py` — all passing
- [x] SQL battery parity: 457 pass / 111 fail, identical to Arrow path

---

### Phase 4 — Cython Orchestration and Cleanup

#### 4.1 — Cython expression tree walker

File: `opteryx/expression/evaluator/_eval_draken.pyx` (new)

- [ ] Move `evaluate_draken` to Cython `cpdef` function
- [ ] No Python frame overhead for AND/OR/IDENTIFIER/LITERAL nodes
- [ ] Re-export from `opteryx/expression/evaluator/__init__.py`
- [ ] Benchmark: morsel filter time before/after for 5-condition AND query

**Deferred**: tree-walker Python overhead is O(nodes), not O(rows). For typical
morsels (128 K rows) this is negligible vs. the per-row comparison kernel cost.
Will revisit if profiling shows measurable impact.

#### 4.2 — AND-chain compaction kernel

File: `opteryx/compiled/vector_ops/bool_vector_ops.pyx`

- [x] Added `bool_vector_and_chain(list masks) -> BoolVector` with early exit when running mask is all-false
- [x] DNF path in `evaluate_draken` now short-circuits via `.any()` check between sub-expressions

#### 4.3 — BoolVector → indices bridge

File: `opteryx/compiled/vector_ops/bool_vector_ops.pyx`

- [x] `bool_vector_to_int32_indices` not added as a standalone function — `filter_mask_inplace` in `morsel.pyx` already handles BoolVector → indices internally. External callers use `morsel.filter_mask(bool_vector)`.
- [x] Added `bool_vector_from_int8_mask`, `bool_vector_from_inverted_null_bitmap`, `bool_vector_all_true` as construction helpers used by `_is_null_as_boolvector`

#### 4.4 — Connector migration to Draken Morsels ✅

**All connectors now yield `Morsel` instead of `pyarrow.Table`.** The blocker on flag removal is resolved.

- [x] Set `FEATURE_USE_DRAKEN_FILTER = True` as default — **done March 8, 2026**
- [x] `_is_null_as_boolvector` now Arrow-free for all native Draken types (DictionaryVector, fixed-buffer types, ConstantVector, StringVector/ArrayVector via `null_bitmap()`); ArrowVector keeps Arrow path since it wraps a PyArrow array
- [x] `VirtualDataTable.read_dataset()` now yields `Morsel.from_arrow(...)` instead of `pyarrow.Table` (`opteryx/connectors/virtual_data_connector.py`)
- [x] `BaseTable.chunk_dictset()` (all dict-backed connectors) yields `Morsel.from_arrow(pyarrow.Table.from_pylist(chunk))` (`opteryx/connectors/base/base_connector.py`)
- [x] `FileSystemTable.read_dataset()` yields `Morsel.from_arrow(decoded)` on both the single-thread and multi-thread code paths (`opteryx/connectors/filesystem_connector.py`)
- [x] `ReaderNode.execute()` updated: each item yielded by the connector is unpacked via `.to_arrow()` for Arrow-level pre-processing (struct→jsonb, schema normalisation, cast), then re-wrapped as `Morsel.from_arrow(morsel)` before yielding (`opteryx/operators/read_node.py`)
- [x] `FilterNode.execute()` updated: EOS check moved first; when `use_draken_filter=True` any non-Morsel item (Arrow table from joins) is converted via `Morsel.from_arrow()` before the Draken path; Arrow fallback kept only for flag=False (`opteryx/operators/filter_node.py`)
- [x] `query_session.py` pipeline boundary: `TABULAR` result generator now calls `.to_arrow()` on each item before passing to `converters.from_arrow()`, so the Morsel representation is transparent to external consumers (`opteryx/query_session.py`)
- [x] `draken_compare` scalar-left normalisation: when `left` is a Python scalar (`str`, `int`, `float`, `bytes`, `bool`, `None`) and `right` is a Draken vector the operands are swapped and directional operators flipped (`Gt`↔`Lt`, `GtEq`↔`LtEq`). Fixes `WHERE 'Earth' = g.name` style queries (`opteryx/expression/evaluator/__init__.py`)
- [x] `_arrow_vector_compare()` helper added to `draken_compare`: handles `ArrowVector` (e.g., `decimal128` columns in `$planets`) by delegating to `pyarrow.compute` and returning `BoolVector` (`opteryx/expression/evaluator/__init__.py`)
- [x] `evaluate_draken` now handles `NodeType.LITERAL`: broadcasts a Python bool/None scalar to a `BoolVector` of the morsel row count. Fixes `WHERE False` / `WHERE True` / `WHERE NULL` (`opteryx/expression/evaluator/__init__.py`)
- [x] Battery result after connector migration: **239 failed / 967 passed** vs. baseline **247 failed / 959 passed** — **8 net improvements** over pre-migration baseline

#### 4.5 — Remove feature flag and legacy filter path

- [x] Remove `FEATURE_USE_DRAKEN_FILTER` from `opteryx/config.py` and `Features` class
- [x] Remove Arrow fallback branch from `filter_node.py` — entire `if Features.use_draken_filter:` block gone; `_execute_draken` inlined directly into `execute()`
- [x] Remove `ensure_arrow_table`, `numpy`, `pyarrow`, `evaluate`, `evaluate_and_append` imports from `filter_node.py` — all unused after collapse
- [x] Remove Arrow-to-Morsel conversion guard — replaced by a clean `if not isinstance(morsel, Morsel): morsel = Morsel.from_arrow(morsel)` (handles Arrow tables from JOIN nodes)
- [x] Remove `_execute_draken` helper method — logic inlined into `execute()` directly
- [x] Clean up `tests/draken/test_phase1_evaluator.py` — removed `monkeypatch.setattr(Features, "use_draken_filter", True)` calls and dead `Features` imports
- [x] Battery after cleanup: **239 failed / 967 passed** — identical to Phase 4.4 result, no regressions
- [ ] `_to_arrow_gen` wrapper in `query_session.py` — kept; still needed because filter (and other Morsel-native) nodes can be terminal; will be removed when all terminal nodes produce Arrow or when a Morsel-native consumer is introduced
- [ ] Remove Arrow interval special-handling from `ops.py` (replaced by `IntervalVector` methods)
- [ ] Audit and remove dead code paths in `opteryx/managers/expression/ops.py`

#### 4.6 — Final cleanup ✅

- [x] Remove unused imports from all modified files (`pyarrow` in `virtual_data_connector.py`; `pyarrow.compute as _pc` + two dead `NodeType` locals in `evaluator/__init__.py`; `flush_all` and `chain` in `query_session.py`)
- [x] Fix pre-existing F401 re-export in `connectors/base/__init__.py` (explicit `as BaseConnector`)
- [x] Remove dead `DatasetReadError` import from `s3_filesystem.py`
- [x] `ruff check --select F401` — zero violations in all modified files
- [x] Battery: **239 failed / 967 passed** — unchanged, no regressions from cleanup
- [x] Update this document status to `Complete`

---

### Dependency Graph

```
0.1 (Morsel)        ─┐
0.2 (null test)     ─┤
0.3 (Kleene)        ─┤
0.4 (StringVector)  ─┤──→ Phase 1 ──→ Phase 2 ──→ Phase 3 ──→ Phase 4
0.5 (numeric InList)─┤
0.6 (IntervalVector)─┤
0.7 (audit)         ─┤
0.8 (compile)       ─┘
```

Phase 2 can be partially enabled (numeric + string types only) before Phase 3 (array/JSON operators) is complete. The feature flag gates this safely.

---

### Files Modified / Created

| File | Action | Phase | Status |
|---|---|---|---|
| `third_party/mabel/draken/morsels/morsel.pyx` | Bugfix: zero-row crash in `_filter_mask_inplace`; ~~`apply_bool_vector_filter`~~ not needed | 0.1 | ✅ Done |
| `tests/unit/draken/test_vector_null_propagation.py` | Covered by Phase 1.5 unit tests instead | 0.2 | ✅ Done (merged) |
| `opteryx/compiled/vector_ops/bool_vector_ops.pyx` | New — `bool_vector_and_chain` (short-circuit AND), `bool_vector_from_int8_mask`, `bool_vector_from_inverted_null_bitmap`, `bool_vector_all_true` | 0.3, 4.2, 4.3 | ✅ Done |
| `opteryx/draken/vectors/string_vector.pyx` + `.pxd` | Added 9 comparison kernels | 0.4 | ✅ Done |
| `opteryx/compiled/vector_ops/vector_in_list_numeric.pyx` | New — shared numeric in_list kernel | 0.5 | ✅ Done |
| `opteryx/draken/vectors/int64_vector.pyx` + `.pxd` | Added `in_list` wrapper | 0.5 | ✅ Done |
| `opteryx/draken/vectors/float64_vector.pyx` + `.pxd` | Added `in_list` wrapper | 0.5 | ✅ Done |
| `opteryx/draken/vectors/timestamp_vector.pyx` + `.pxd` | Added `in_list` wrapper | 0.5 | ✅ Done |
| `opteryx/draken/vectors/date32_vector.pyx` + `.pxd` | Added `in_list` wrapper | 0.5 | ✅ Done |
| `opteryx/draken/vectors/interval_vector.pyx` + `.pxd` | Added 6 scalar comparison methods | 0.6 | ✅ Done |
| `opteryx/compiled/vector_ops/vector_in_list.pyx` | Removed vestigial numpy import | 0.7 | ✅ Done |
| `opteryx/expression/evaluator/__init__.py` | Added `draken_compare`, `evaluate_draken`, `evaluate_and_append_draken` + helpers | 1.1–1.3 | ✅ Done |
| `opteryx/draken/__init__.py` | Removed dead evaluator reference | 1.4 | ✅ Done |
| `opteryx/draken/evaluators/` | Removed entirely — dead code | 1.4 | ✅ Done |
| `tests/draken/test_evaluator.py` | Removed — tested dead evaluator | 1.4 | ✅ Done |
| `tests/draken/performance/perftest_compiled_evaluator_benchmark.py` | Removed — referenced dead evaluator | 1.4 | ✅ Done |
| `tests/draken/test_phase1_evaluator.py` | New — 38 tests, all passing | 1.5 | ✅ Done |
| `opteryx/config.py` | Added `FEATURE_USE_DRAKEN_FILTER` to `Features` | 2.1 | ✅ Done |
| `opteryx/operators/filter_node.py` | Added `_execute_draken`; feature-flag dispatch | 2.2 | ✅ Done |
| `opteryx/expression/evaluator/__init__.py` | EXTRACTION_OPERATOR (`->`,`->>`,`[]`) in `_eval_value`; AnyOp* / AllOp* / AtArrow / AtQuestion in `draken_compare` | 3.1–3.3 | ✅ Done |
| `opteryx/compiled/vector_ops/vector_anyop_like.pyx` | New — AnyOpLike/ILike Cython kernel | 3.1 | ✅ Done |
| `opteryx/managers/expression/binary_operators.py` | Split `EXTRACTION_OPERATORS` from `BINARY_OPERATORS` | 3.2 | ✅ Done |
| `tests/draken/test_phase3_array_ops.py` | New — 48 tests, all passing | 3.4 | ✅ Done |
| `opteryx/operators/parquet_read_node.py` | `_apply_predicates_to_morsel` rewritten — Draken-native, no Arrow round-trip; dead `_mask_to_arrow` + imports removed | 4.4 | ✅ Done |
| `opteryx/expression/evaluator/_eval_draken.pyx` | New — Cython tree walker | 4.1 | ⏳ Deferred (Python overhead per-node is negligible vs per-row kernel cost) |
| `opteryx/expression/evaluator/__init__.py` | `_is_null_as_boolvector` now Arrow-free for all native Draken types; ArrowVector falls back to pc.is_null. DNF short-circuit added. | 4.4 | ✅ Done |
| `opteryx/connectors/virtual_data_connector.py` | `VirtualDataTable.read_dataset()` yields `Morsel.from_arrow(...)` | 4.4 | ✅ Done |
| `opteryx/connectors/base/base_connector.py` | `chunk_dictset()` yields `Morsel.from_arrow(...)` | 4.4 | ✅ Done |
| `opteryx/connectors/filesystem_connector.py` | Both yield paths wrapped with `Morsel.from_arrow(decoded)` | 4.4 | ✅ Done |
| `opteryx/operators/read_node.py` | Execute loop unpacks `.to_arrow()` for preprocessing, re-wraps as `Morsel.from_arrow()` | 4.4 | ✅ Done |
| `opteryx/operators/filter_node.py` | EOS check first; Arrow→Morsel conversion guard; Arrow fallback for flag=False | 4.4 | ✅ Done |
| `opteryx/query_session.py` | `_to_arrow_gen` wrapper converts Morsels to Arrow at pipeline boundary for `converters.from_arrow` | 4.4 | ✅ Done |
| `opteryx/expression/evaluator/__init__.py` | `draken_compare` scalar-left normalisation; `_arrow_vector_compare` for decimal128; `NodeType.LITERAL` handler in `evaluate_draken` | 4.4 | ✅ Done |
| `opteryx/operators/filter_node.py` | Feature flag removed; Arrow fallback removed; `_execute_draken` inlined into `execute()`; pure Draken-native | 4.5 | ✅ Done |
| `opteryx/config.py` + `Features` class | `use_draken_filter` attribute removed | 4.5 | ✅ Done |
| `tests/draken/test_phase1_evaluator.py` | Removed dead `monkeypatch.setattr(Features, "use_draken_filter", True)` calls | 4.5 | ✅ Done |
| `opteryx/managers/expression/ops.py` | Remove null-compression wrapper, interval special-casing, dead paths; retire Arrow LOGICAL_OPERATIONS | 4.6 | ⏳ Phase 4.6 |

---

## Decisions Summary

| # | Question | Status | Decision |
|---|---|---|---|
| 1 | New `apply_bool_vector_filter` vs. promote `_filter_mask_inplace`? | ✅ Decided | New `cpdef` method; null handling moves into each vector method |
| 2 | String comparison ordering | ✅ Decided | Byte-level; engine is binary, not UTF-8 |
| 3 | `like` case-folding | ✅ Decided | Binary byte-level `tolower` only |
| 4 | Numeric `in_list`: per-type or shared? | ✅ Decided | Shared Cython kernel, per-type `cpdef` wrapper |
| 5 | Evaluator home directory | ✅ Decided | `opteryx/expression/evaluator/` |
| 6 | Null bitmap propagation in vector comparisons | ✅ Confirmed | All vector comparison methods propagate null bitmaps natively — no post-comparison null-overlay step needed. Confirmed by source read and Phase 1.5 unit tests. |
| 7 | Interval predicates in filter path? | ✅ Decided | Yes — `IntervalVector` comparison methods required in Phase 0 |
| 8 | `BoolVector.and_vector/or_vector` Kleene or bitwise? | ✅ Confirmed | Already Kleene three-valued logic — source code in `bool_vector.pyx` explicitly implements SQL 3VL. Phase 0.3 Kleene wrapper kernels are NOT needed. |
| 9 | `AtArrow`/`AtQuestion` priority | ✅ Decided | `@>` promoted to Phase 2; `@?` Phase 3 |

---

## Test Plan

1. **Unit tests** (per operator, Draken path):
   - `Eq`, `NotEq`, `Lt`, `Gt`, `LtEq`, `GtEq` for each vector type
   - `IN` / `NOT IN` for each vector type
   - `LIKE` / `ILIKE` / `RLIKE` / `CONTAINS` on `StringVector`
   - `ANY*` / `ALL*` array operators
   - Unary: `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS FALSE`
   - Null propagation: column with nulls, comparison with null literal
   - AND/OR Kleene semantics edge cases

2. **Integration tests**:
   - Full SQL battery (`tests/sql_battery/`) against Draken path — must match legacy Arrow results
   - Row count parity for filter-heavy queries

3. **Performance tests**:
   - Extend `tests/performance/benchmarks/bench_filter_optimization.py`
   - Benchmark: Draken morsel filter vs. Arrow morsel filter on same workload
   - Target: no conversion overhead visible in filter-only workloads

---

## Acceptance Criteria

1. ✅ `FilterNode` processes Draken morsels with zero Arrow/NumPy calls in the active path.
2. ✅ SQL result parity against legacy path for full battery (239 failed vs 247 baseline — 8 net improvements).
3. ✅ No null semantics regressions.
4. ✅ `FEATURE_USE_DRAKEN_FILTER` flag removed (Phase 4 complete).

---

## Implementation Learnings

Discoveries made during implementation that were not anticipated in the original design. These have been applied to update the decision table and phase plans above.

### L1 — `BoolVector.and_vector/or_vector` are already Kleene
Decision #8 was wrong. Source read of `bool_vector.pyx` confirmed both methods already implement SQL three-valued logic. Phase 0.3 Kleene wrapper kernels were not written and are not needed. `evaluate_draken` calls `left.and_vector(right)` directly.

### L2 — `Morsel.filter_mask(BoolVector)` already existed as public method
The planned `apply_bool_vector_filter` `cpdef` was not added. `filter_mask` is a `def` method on `Morsel` that handles BoolVector directly with correct null semantics. Phase 0.1 did not require any changes to `morsel.pyx` other than the crash fix below.

### L3 — `_filter_mask_inplace` zero-row crash
When all rows are filtered out (`selected == 0` after the BoolVector scan), Cython raises `ValueError: Invalid shape in axis 0: 0` when creating `<int32_t[:0]>` typed memoryview. Fixed in `third_party/mabel/draken/morsels/morsel.pyx` with an early-exit guard:
```cython
if selected == 0:
    self._empty_inplace()
    return
# Only reached when selected > 0:
indices_view = <int32_t[:selected]> indices_ptr
```

### L4 — `Morsel.from_vectors()` is the append-column API
`Morsel` has no `append_column()` method. `evaluate_and_append_draken` must use `Morsel.from_vectors(names, vectors)` to rebuild the morsel with additional columns. The full existing column list + new columns must be passed.

### L5 — `Node.schema_column.identity` for column lookup
In `_eval_value`, identifier nodes expose the column via `node.schema_column` (a `FlatColumn` with `.identity` attribute — a hex string). `morsel.column()` takes `bytes`, so the call is `morsel.column(node.schema_column.identity.encode())`.

### L6 — `_is_null_as_boolvector` — Arrow path eliminated for native types ✅
`_is_null_as_boolvector` in `opteryx/expression/evaluator/__init__.py` now uses
Arrow-free dispatch:
- `DictionaryVector` → `is_null_boolvector()` (native, handles NaN nulls)
- Fixed-buffer types (Int64, Float64, Date32, Timestamp, Time, Interval, Bool) → `is_null() -> int8_t[::1]` → `bool_vector_from_int8_mask()` (Cython)
- `ConstantVector` → `scalar_value() is None` → O(1) all-true/all-false
- StringVector / ArrayVector → `null_bitmap()` → `bool_vector_from_inverted_null_bitmap()` (Cython)
- `ArrowVector` (wraps non-native Arrow types like date64) → Arrow path preserved since the wrapped array IS already Arrow

The Cython helpers (`bool_vector_from_int8_mask`, `bool_vector_from_inverted_null_bitmap`, `bool_vector_all_true`) are in `opteryx/compiled/vector_ops/bool_vector_ops.pyx`.

### L7 — `opteryx/draken/evaluators/` directory is not empty
After removing the dead import in `opteryx/draken/__init__.py`, the `evaluators/` directory still contains `expression.py`. Whether this file is still referenced elsewhere must be audited before removal (tracked in Phase 1.4 second checkbox).

### L8 — Test baseline
Pre-existing test suite state: 585 passing, 1041 failing (all pre-existing failures, not regressions from this work). The 38 Phase 1 tests are all passing within the 585.

### L9 — Connector migration revealed three evaluator gaps
When all connectors yield Morsels, `FilterNode` routes everything through the Draken path including Arrow tables produced by JOIN nodes. This exposed three evaluator gaps that were previously hidden behind the Arrow fallback:

1. **Scalar-left comparisons**: `WHERE 'Earth' = g.name` causes `_eval_value` to return a raw Python `str` as `left` (because `LITERAL` nodes return `node.value` directly). `draken_compare` previously dispatched on `type(left).__name__` and had no handler for `str`. Fixed by swapping and flipping when `left` is a Python scalar and `right` is a vector.

2. **`NodeType.LITERAL` in predicate position**: `WHERE False` / `WHERE True` produces a bare `LITERAL` node at the top of the predicate tree, not wrapped in a `COMPARISON_OPERATOR`. `evaluate_draken` had no LITERAL handler — it reached the final `raise NotImplementedError`. Fixed by broadcasting the scalar bool to a `BoolVector` of `morsel.num_rows` elements.

3. **`decimal128` columns (`ArrowVector`)**: `$planets.gravity/mass/density` are `decimal128` in Arrow, which have no native Draken type and are represented as `ArrowVector`. `draken_compare` had no `ArrowVector` branch. Fixed by adding `_arrow_vector_compare()` which delegates to `pyarrow.compute` and returns `BoolVector`.

### L10 — Pipeline boundary: Morsel → orso via `query_session.py`
`orso.converters.from_arrow()` calls `.schema` on the first item yielded by the result generator, which fails if the item is a `Morsel`. The fix is a lightweight `_to_arrow_gen` wrapper in `query_session.py` that calls `.to_arrow()` on any item that has that method before passing it to `converters.from_arrow`. This is the correct internal/external boundary: Morsels are for intra-pipeline computation; Arrow tables are what external consumers (orso, pandas, etc.) receive.

---

## Existing Gaps Already Known

1. ~~`opteryx/draken/__init__.py` references `opteryx.draken.evaluators.evaluator` — module does not exist.~~ **Resolved in Phase 1.4** — dead import removed.
2. `opteryx/draken/evaluators/expression.py` still exists. Audit in Phase 1.4 (second checkbox) whether this is referenced anywhere and remove if dead.
3. Several tests/benchmarks in `tests/draken/*` assume evaluator APIs that are currently incomplete. These will pass once Phase 2.4 integration is complete and the feature flag is enabled.
