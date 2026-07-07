# Native SELECT-path residual close-out plan (A0 acceptance gate)

The native C++ engine runs plain `SELECT` end-to-end **except** for parquet scans
that fall back to the per-morsel Python trampoline (`StreamingScanSource`). That
trampoline (`engine.hpp`'s scan-pull callback) is the *only* per-morsel Python
touch left on the native SELECT path; removing every fallback finishes
Python-free execution for `SELECT`.

Every fallback is one `return None` guard in
[`_native_scan_plan`](../opteryx/managers/execution/compiler.py) (`_Compiler`).
Each guard now records a stable **residual reason code** on query telemetry
(`scan_residual_reasons`, keyed by scan identity — parallel to `scan_sources`),
so the frontier is measurable, not guessed.

* **Census tool:** `dev/native_residual_census.py`
* **Acceptance gate / frontier test:** `tests/unit/operators/test_native_scan_residual_gate.py`
* **Instrumentation context:** [`ENGINE_INSTRUMENTATION.md`](ENGINE_INSTRUMENTATION.md) (WP-INSTR)

This doc is measurement + a worklist. **No admission decision was changed** — the
same scans go native/trampoline as before; the fallbacks are only annotated.

## Census (corpus = the `.run_tests` SQL battery: clickbench + tpch)

143 queries → 158 parquet scans observed (16 queries raised on unsupported /
non-scan reasons — whole-query native-support gaps, out of scope for A0).

| Source | scans |
|---|---:|
| `NativeParquetScanSource` (zero-Python) | 145 |
| `StreamingScanSource` (trampoline) | **13** |

Residual reasons for the 13 fallbacks:

| Reason (guard) | battery count | reachable? |
|---|---:|---|
| `footer_gate` (R7b) | **10** | ✔ observed |
| `zero_projection` (R1) | 2 | ✔ observed |
| `fused_topn` (R3) | 1 | ✔ observed |
| `pushed_limit` (R2) | 0 | ✔ hand-set |
| `unlowerable_predicate` (R4) | 0 | ✔ hand-set (regex) |
| `bool_predicate_input` (R5) | 0 | ✔ hand-set |
| `non_admissible_kind:<T>` (R6) | 0 | ✔ hand-set (ARRAY) |
| `no_manifest` (R7a) | 0 | ✖ no SQL trigger |

`footer_gate` alone is **77% of all fallbacks** — one close-out clears the
majority of the residual.

## Corrections to the A0 residual enumeration (findings)

The task's enumeration was validated against the live code. Three items differ
from reality:

1. **R6 example "a TIME column projected" is STALE.** Post-WP-11, TIME is
   *admitted* (routed through the int path as INT64) — projecting a TIME column
   goes **native**, not to the trampoline. The real R6 trigger is a column whose
   `ColumnType` has **no physical `DrakenType`** — e.g. `ARRAY` (tagged
   `non_admissible_kind:NONE`; `_physical_type` returns `None`). The reason code
   carries the offending type name (or `NONE`) so R6 sub-censuses by type.

2. **R4 is reachable but narrow, and is partly masked by a HARD ERROR.** A
   non-c-native predicate triggers R4 (`unlowerable_predicate`) **only when it is
   pushed to the scan** — in practice a *regex* (`RLIKE`) predicate. A non-c-native
   predicate that does **not** push (a function over a column, e.g.
   `LEVENSHTEIN(col,'x')=0`, `SOUNDEX`, `COALESCE`) becomes a standalone `Filter`
   and **hard-errors at `compiler.py:454`** ("a … predicate outside the c-native
   kernel set") — it never reaches this guard. So the true non-c-native-predicate
   residual is split across R4 (trampoline) *and* a class of whole-query hard
   failures. Widening the c-native kernels closes both.

3. **R7a (`no_manifest`) has no SQL trigger.** An empty/missing dataset fails at
   bind time before this guard; it is a defensive check for a manifest that
   resolves to zero files post-construction. It is correct as-is and needs no
   close-out — documented, not exercised.

Everything else in the enumeration matched. `footer_gate` is heterogeneous — see
R7b below.

## Ordered close-out worklist

Ordered by evidence (census count + independence), cheapest-highest-payoff first;
R4 last because it overlaps uncommitted WIP.

### 1. `footer_gate` (R7b) — **census 10** — *medium* — **DO FIRST**
`native_scan_supported()` (C-side footer gate,
`pool_reader.pyx:1629`) rejected the scan. Column-level probing on
`clickbench_tiny`: bare-INT64 `UserID` passes, but `AdvEngineID`,
`ResolutionWidth` (int-family) and `EventDate` (UINT16) **reject**. The int
branch of the gate admits only physical int32/int64 with a logical annotation
that is empty / `int32` / `int64` / `time[...]` (`pool_reader.pyx:1711-1721`) —
so **narrow / unsigned integer logical annotations** (`INTEGER(bits,signed)`) and
UINT16 are refused even though the value widens to INT64 on decode.
* **Needs:** widen the footer int-gate + native int decode to admit narrow/unsigned
  integer logical annotations (emit the same widened INT64 the trampoline does).
* **First step:** add a C-side rejection reason to `native_scan_supported` so the
  census can sub-tally *which* annotation rejects (the Python census cannot see
  the C reason today). Highest payoff, largely mechanical, independent.

### 2. `zero_projection` (R1) — **census 2** — *small–medium*
`not scan.columns` — a scan with an empty projection (`COUNT(*)` **with** a
`WHERE`; a bare `COUNT(*)` short-circuits to a statistics response and never
scans). The predicate column is read as a role-3 column, but no column is emitted.
* **Needs:** a native zero-column/count Source that emits row-count-only morsels
  while still decoding + applying the predicate columns. Self-contained.

### 3. `pushed_limit` (R2) — **census 0** (reachable) — *small–medium*
`scan.limit is not None` — LIMIT semantics currently live in the trampoline scan.
* **Needs:** early-stop (stop after N rows) in `NativeParquetScanSource`.
  Independent, small.

### 4. `non_admissible_kind:<T>` (R6) — **census 0** (ARRAY reachable) — *structural (per type)*
A read-set column of a kind with no native decode. Only `ARRAY` (`:NONE`)
observed. Per-type and independent; ARRAY implies **nested decode** → structural.
* **Needs:** a native decode for each offending type (start with the type the
  prod corpus actually hits; ARRAY is the known one).

### 5. `bool_predicate_input` (R5) — **census 0** (reachable) — *small–medium*
A `BOOL` column used as a **predicate input** (WP-11 fail-closed; bool comparison
raises err_op=11). BOOL columns that are only *projected* already decode natively.
* **Needs:** BOOL in the c-native span (a c-native bool-comparison kernel).
  **Overlaps R4** (both are c-native kernel coverage).

### 6. `fused_topn` (R3) — **census 1** — *structural*
`scan._topn_sort_name is not None` — an `ORDER BY … LIMIT` fused into the scan
(observed on `SELECT * … ORDER BY … LIMIT`).
* **Needs:** the WP-02 §9 two-pass **late-materialization** (rank on sort keys,
  then materialize the top-N rows). Structural; independent of R4.

### 7. `unlowerable_predicate` (R4) — **census 0** (regex reachable) — *structural* — **DO LAST**
A pushed predicate that does not lower to a c-native span (regex / `RLIKE`).
* **Needs:** widened c-native expression-kernel coverage (regex + the kernels
  behind the `compiler.py:454` hard-error class).
* **⚠ Dependency / overlap — DO NOT TOUCH from A0:** this overlaps the uncommitted
  `draken/draken_native.cpp` / `opteryx/expression/evaluator/evaluation.pyx` WIP
  and the native `draken_if_then_else` / `join2` correctness bugs. Close only
  after that WIP lands and those bugs are fixed. Overlaps R5.

### (not a close-out) `no_manifest` (R7a)
Defensive guard, unreachable from SQL. Leave as-is.

## How a close-out chip uses this gate

`tests/unit/operators/test_native_scan_residual_gate.py` carries one
`@pytest.mark.xfail(strict=True)` per open category (`test_category_now_native`).
While a category is on the trampoline the test **xfails**. When a close-out chip
admits that shape natively, the scan goes native, the assertion passes → the
strict-xfail **turns RED (xpass)** — the signal to delete that marker and record
the category closed. The parallel `test_residual_reason_reachable` proves each
reason code stays reachable and correctly wired.
