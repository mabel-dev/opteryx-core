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

**A0 baseline** (before the A1 close-out):

| Source | scans |
|---|---:|
| `NativeParquetScanSource` (zero-Python) | 145 |
| `StreamingScanSource` (trampoline) | **13** |

Residual reasons for the 13 A0 fallbacks:

| Reason (guard) | A0 count | after A1 | after A2 | reachable? |
|---|---:|---:|---:|---|
| `footer_gate` (R7b) | **10** | **1** | 1 | ✔ observed |
| `zero_projection` (R1) | 2 | 2 | **0** | ✖ closed (A2) |
| `fused_topn` (R3) | 1 | 1 | 1 | ✔ observed |
| `pushed_limit` (R2) | 0 | 0 | 0 | ✔ hand-set |
| `unlowerable_predicate` (R4) | 0 | 0 | 0 | ✔ hand-set (regex) |
| `bool_predicate_input` (R5) | 0 | 0 | 0 | ✔ hand-set |
| `unsigned_predicate_input` (R5b, A1) | — | 0 | 0 | ✔ hand-set |
| `non_admissible_kind:<T>` (R6) | 0 | 0 | 0 | ✔ hand-set (ARRAY) |
| `no_manifest` (R7a) | 0 | 0 | 0 | ✖ no SQL trigger |
| `temporal_predicate_input` (new, A2) | — | — | 0 | ✔ observed (missions dataset, not in battery) |

`footer_gate` was **77% of all A0 fallbacks**. **A1 closed the integer sub-case**:
after A1 the battery has 4 trampoline scans (native 154 / 158), and `footer_gate`
drops **10 → 1**. The single remainder is NOT an integer case — it is
`EventTime::TIMESTAMP[ms]`, a column whose CAST retags an int64 footer column to
TIMESTAMP the footer's `int64` annotation cannot satisfy (a temporal-cast gap,
distinct from integer admission).

**A2 closed `zero_projection`** (see the ordered worklist entry below): the battery
now has 2 trampoline scans (native 156 / 158). A2 also introduced
`temporal_predicate_input`, a NEW fail-closed guard for a pre-existing gap it
uncovered (not present in the battery census, but reachable — see below).

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

### 1. `footer_gate` (R7b) — census 10 → **1** — **CLOSED for integers (A1)**
`native_scan_supported()` (C-side footer gate, `pool_reader.pyx`) rejected the
scan. A0 finding: bare-INT64 `UserID` passed, but `AdvEngineID`,
`ResolutionWidth` (int16) and `EventDate` (uint16) **rejected** — the int branch
admitted only empty / `int32` / `int64` / `time[...]` logical annotations, so
narrow / unsigned integer annotations were refused.

**A1 close-out (done).** Three coordinated changes admit every integer width and
signedness byte-identically to the trampoline:
1. **Footer gate** (`native_scan_supported`, `pool_reader.pyx`) — the "int" branch
   now also admits `int8`/`int16`/`uint8`/`uint16`/`uint32`/`uint64` logical
   annotations.
2. **Native decode** (`safe_logical` in `rugo/src/parquet/io_pipeline.hpp`) — signed
   `int8`/`int16` are now direct-eligible (widen to `DK_INT64` like `int32`); before
   A1 they fell to `DK_POOL`, which the native Source cannot decode for a numeric
   column. Unsigned widths were already direct (`DK_UINT{8,16,32,64}`, E33).
3. **Native Source** (`native_parquet_scan_source.hpp`) — `direct_kind_supported` /
   `draken_type_for` / the numeric-dict branch now handle `DK_UINT{8,16,32,64}` and
   their dict shapes, tagging exact-width `DRAKEN_UINT*` (matching the trampoline's
   `_wrap_direct`).

The compiler classifier needed **no** width/signedness packing: the type system
collapses every integer width to canonical `INT64`
(`_CATEGORY_TO_CANONICAL[INTEGER]`), so the classifier already tags these "int";
the exact width is recovered at decode from the parquet IntType annotation.

**Fail-closed (A1, `unsigned_predicate_input` / R5b).** An UNSIGNED column used as a
c-native **predicate input** stays on the trampoline: it decodes to an exact-width
`DK_UINT` vector the relocated ExprFilter's bytecode VM cannot read (`err_op=11`;
the uint compare kernel is out-of-scope R4/R5 follow-on). Detected at plan time via
`any_column_unsigned` (the footer, since the schema collapses to INT64). Signed
narrow ints widen to INT64 and work in every role, including as predicate inputs.
No width is left fail-closed for projection/aggregation — UINT64 has a native
`DRAKEN_UINT64` vector (no truncation).

**Remaining `footer_gate` (census 1):** `EventTime::TIMESTAMP[ms]` — a temporal-cast
column, not an integer case (out of A1 scope).

### 2. `zero_projection` (R1) — census 2 → **0** — **CLOSED (A2)**
`not scan.columns` — a scan with an empty projection. Inventory finding (A2):
the task's assumed "common no-predicate `COUNT(*)`" shape is a NON-ISSUE — a bare
`SELECT COUNT(*) FROM t` never reaches a scan at all (`StatisticsOnlyResponseStrategy`
rewrites it to a manifest-count literal over `$no_table` at the optimizer level, before
any scan node exists). The ENTIRE reachable residual is `COUNT(*)` **WITH** a `WHERE`:
the predicate column is read as a role-3 column, but no column is emitted.

**A2 close-out (done).** No engine change was needed — the WP-02 relocated-filter
machinery already degenerates correctly at emit-set = ∅: `emit_ids`/`emit_indices`
are naturally empty when `scan.columns` is `[]`, and `ColumnSelectOperator`
(`src/cpp/engine/engine.hpp`) already handles a zero-index Select, emitting a
genuine zero-column morsel whose row count rides on `zero_col_rows` — the exact
contract `UngroupedAggSink`'s CountStar already reads for the trampoline path. The
ONLY change was removing `_native_scan_plan`'s unconditional bail on
`not scan.columns` (`compiler.py`): it now only bails when there is ALSO no
predicate to build a read-set from (a shape with no SQL trigger for parquet scans,
analogous to `no_manifest`/R7a).

**A2 also uncovered and fail-closed a pre-existing, unrelated gap.** Running the
newly-native `COUNT(*) WHERE <predicate>` shape against `make q` surfaced
`SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at >= '1957-10-04'::DATE`
crashing (`ExprFilterOperator ... err_op=11`). Confirmed pre-existing and
independent of A2 (the identical crash reproduces on
`SELECT Mission FROM testdata.missions WHERE Lauched_at >= ...`, a plain projection
untouched by the A2 guard change): a DATE/TIMESTAMP column used as a c-native
**predicate input** is not safely evaluable by the relocated ExprFilter kernel —
the same failure class WP-11 already fail-closes for BOOL (R5) and unsigned
integers (R5b), but no equivalent guard existed for DATE32/TIMESTAMP64. A2 adds
that guard (`temporal_predicate_input`, alongside the BOOL/unsigned checks in
`_native_scan_plan`); DATE/TIMESTAMP columns that are only *projected* are
unaffected. A native DATE/TIMESTAMP-comparison kernel is a follow-on
(R4/R5-adjacent, not part of A2's scope).

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
