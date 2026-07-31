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

**Latest census (after the R2 + R7b-temporal + R5 + R6 close-outs): 165 scans,
164 native, 1 trampoline — `fused_topn` only.**

⚠ The census corpus (clickbench + tpch) contains **no array columns**, so it
never reported R6 at all and its 164/165 flattered the frontier. On ordinary
data ARRAY was the single biggest real-world source of the trampoline — a plain
`SELECT *` over `testdata.astronauts` or `testdata/flat/formats/parquet` dropped
the WHOLE scan — which is why R6's close-out below is measured against a
purpose-built corpus, not this battery.

| Reason (guard) | A0 count | after A1 | after A2 | after A3 | now | reachable? |
|---|---:|---:|---:|---:|---:|---|
| `footer_gate` (R7b) | **10** | **1** | 1 | 1 | **0** | ✔ schema evolution only |
| `zero_projection` (R1) | 2 | 2 | **0** | 0 | 0 | ✖ closed (A2) |
| `fused_topn` (R3) | 1 | 1 | 1 | 1 | 1 | ✔ observed (composed shape — see below) |
| `pushed_limit` (R2) | 0 | 0 | 0 | 0 | **0** | ✖ closed (R2 close-out) |
| `unlowerable_predicate` (R4) | 0 | 0 | 0 | 0 | 0 | ⚠ marker stale — see below |
| `bool_predicate_input` (R5) | 0 | 0 | 0 | 0 | 0 | ✖ retired (R5 close-out) |
| `unsigned_predicate_input` (R5b, A1) | — | 0 | 0 | 0 | 0 | ✖ retired (A1) |
| `non_admissible_kind:<T>` (R6) | 0 | 0 | 0 | 0 | 0 | ✖ retired (R6 close-out) |
| `no_manifest` (R7a) | 0 | 0 | 0 | 0 | 0 | ✖ no SQL trigger |
| `temporal_predicate_input` (new, A2) | — | — | 0 | 0 | 0 | ✔ observed (missions dataset, not in battery) |

⚠ **`unlowerable_predicate` (R4) — the frontier marker is STALE, unverified.**
`test_category_now_native[unlowerable_predicate]` and its reachability twin are
currently RED (strict-xfail xpass): the hand-set trigger
(`... WHERE text RLIKE 'a'`) now selects the **native** Source, i.e. the regex
predicate lowers to a c-native span. That was NOT done by the R2/R7b close-outs —
neither touches `bytecode_is_all_c_native`, and the trigger has no LIMIT so the R2
guard never applied to it — so R4 appears to have been closed incidentally by the
native RLIKE kernel work. **Not investigated or retired here**, because item 7
below marks R4 hands-off pending other WIP. Someone owning R4 should confirm the
whole category (not just this one trigger) and retire the marker.

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

**A3 closed `fused_topn` for the NO-predicate sub-case only** (R3, see the
ordered worklist entry below). The battery count is UNCHANGED (native 156 /
158, 2 trampoline scans) because the battery's one `fused_topn` trigger is
ClickBench Q24 — `SELECT * ... WHERE URL LIKE '%google%' ORDER BY EventTime
LIMIT 10` — which is exactly the composed (predicate-bearing) sub-case that
A3 deliberately did NOT admit (see below: admitting it was tried and reverted
after measuring a ~400% regression on Q24). The no-predicate sub-case has no
trigger in this battery, so its close-out is only visible via the dedicated
unit tests, not this census.

## Corrections to the A0 residual enumeration (findings)

The task's enumeration was validated against the live code. Three items differ
from reality:

1. **R6 example "a TIME column projected" is STALE.** Post-WP-11, TIME is
   *admitted* (routed through the int path as INT64) — projecting a TIME column
   goes **native**, not to the trampoline. The real R6 trigger was `ARRAY`.
   ⚠ This finding ALSO said ARRAY has no physical `DrakenType` and is tagged
   `non_admissible_kind:NONE`; **that was wrong**. `ColumnType.physical` for an
   array column is `DrakenType.ARRAY`, and the observed reason code was always
   `non_admissible_kind:ARRAY`. The guard's own inline comment carried the same
   error and is corrected in the R6 close-out below. The reason code carries the
   offending type name (or `NONE` when a column genuinely has no physical tag),
   so R6 sub-censuses by type.

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

**Remaining `footer_gate` (census 1 → 0) — CLOSED (temporal-cast close-out).**
`EventTime::TIMESTAMP[ms]` is a column whose parquet footer carries **no temporal
annotation at all** (a bare int64): the temporal-ness comes from a SQL `CAST`, not
the file. The compiler classifies the read column as kind `"timestamp"`, but
`native_scan_supported`'s timestamp branch required the FOOTER's own logical type
to already start with `timestamp[` — so the scan failed closed.

This was NOT a decode gap. The native path is already unit-parametrized end to end
(`logical_coerce` → `LC_TIMESTAMP` + unit → `build_temporal_column`;
`safe_logical` already treats a bare int64 as direct-eligible), so the only change
was widening the gate's kind-classification to also admit an int64 column whose
logical annotation is empty or `int64`. Anything else still fails closed.

Verified the cast is a pure bit-REINTERPRET with no epoch rescale, so admitting it
cannot change values: `EventTime` holds Unix SECONDS, and the trampoline reads
`::TIMESTAMP[s]` → 2013-07-27, `[ms]` → 1970-01-16, `[us]` → 1970-01-01. The native
scan now reproduces all three **byte-identically** (SHA-matched against output
captured from the trampoline before the change) — see
`test_cast_driven_timestamp_now_native` and
`test_cast_driven_timestamp_matches_trampoline`.

`footer_gate` stays reachable as a residual only via **schema evolution** (a
projected column absent from some files — `HAND_SET["footer_gate"]`), which is a
distinct, still-open structural gap.

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

### 3. `pushed_limit` (R2) — census 0 → **CLOSED**
`scan.limit is not None` — LIMIT semantics used to live in the trampoline scan
(`_records_to_read` / `_limit_exhausted`, sliced in `_commit_morsel_cxx`).

**Close-out (done).** `NativeParquetScanSource` now carries `row_limit`
(`-1` == unlimited; threaded compiler → `NativePlan.set_native_scan_source` →
`Engine::set_native_scan_source` → the Source):

1. **Correctness — the scan enforces the cap itself.** This is NOT merely an I/O
   optimization: `LimitPushdownStrategy._apply_to_scan` **removes the Limit node
   from the plan** when it pushes into a scan (`remove_node(heal=True)`), so
   there is no downstream `LimitOperator` left to truncate. `get_morsel` claims
   each morsel's share of the quota under the Source's existing global mutex
   (`rows_emitted`, guarded together with the submit/receive counters so the
   claim and the submit decision see one consistent view) and truncates the
   morsel that crosses the boundary via `cxx_slice_c`. A worker that finds the
   quota already filled drains its row group and emits nothing.
2. **I/O — uncontributing row groups are never decoded.** The footer already
   carries every row group's exact row count (`RowGroupStats::num_rows`), so
   `limit_submit_cap()` walks the work list ONCE in `make_global` and caps the
   submit frontier at the first row group that satisfies the limit. Without this
   the prefetch window still ran ahead: `in_flight_limit` (== workers+2) **plus
   one row group per worker that races in before the first morsel is emitted** —
   measured 31 row groups for `LIMIT 5` over `tpch_1.lineitem`. With the cap it
   decodes **1**.

Measured on `testdata.tpch_1.lineitem` (96 row groups, 6,001,215 rows):

| query | row groups decoded | bytes fetched |
|---|---:|---:|
| `LIMIT 5` | **1** | 13,414 |
| `LIMIT 100000` | 3 | 40,252 |
| `LIMIT 5000000` | 80 | 1,622,461 |
| full scan | 96 | 1,937,327 |

Row identity is unspecified for a LIMIT without ORDER BY, and pushdown only
fires with no pushed predicate and no OFFSET (`limit_pushdown.py` refuses on
`scan_node.predicates`), so nothing is lost by the native path's completion
order — the trampoline is equally order-nondeterministic at dop>1 (concurrent
`_single_pass_next` pulls commit under `_scan_mtx` in completion order, not file
order).

Gates: `make q` 216/216, `make tpch` 22/22, census `pushed_limit` 0. Exact
row-count parity verified for limits below / at / above a row-group boundary and
above the whole table — see `test_pushed_limit_now_native`,
`test_pushed_limit_row_count_exact`, and
`test_pushed_limit_skips_uncontributing_row_groups` in
`tests/unit/operators/test_native_scan_residual_gate.py`. The reason code is
retired from `HAND_SET` (no longer reachable), same convention as R5b.

### 4. `non_admissible_kind:<T>` (R6) — census 0 → **CLOSED (ARRAY)**
A read-set column (projected OR role-3 filter-only) of a kind with no native
decode. `ARRAY` was the only type ever observed on it — and, unlike the census
count suggests, it was the **dominant real-world residual**: the battery has no
array columns, but on ordinary data a plain `SELECT *` over
`testdata.astronauts`, `testdata/flat/formats/parquet` or
`testdata/flat/struct_array` failed the whole scan closed on one list column.

**Inventory finding: the producer side was already native; only the consumer was
not.** A parquet LIST column carries repetition levels, and rugo's
`direct_kind_for` (`rugo/src/parquet/io_pipeline.hpp`) routes *any* column with
rep levels to `DK_POOL` — there is no direct list kind, regardless of encoding.
`ipc_serialize.hpp::serialize_list_column` then writes it as the recursive
**TAG_ARRAY (11)** wire format. Both scan paths share that producer verbatim; the
trampoline's only extra step was parsing TAG_ARRAY in **Cython**
(`column_deserializer.pyx::_build_array_vector{,_nested,_string,_numeric}`) and
boxing the result through the PyObject-returning `draken_vector_own_array*`
family. So this was never "nested decode" work — it was one missing consumer.

**Close-out (done).** Four coordinated changes:

1. **New native decoder — `src/cpp/engine/native_array_pool_decode.hpp`.** A
   faithful, PyObject-free port of those four Cython functions, plus the
   `draken_vector_own_array{,_numeric,_child}` bodies they hand off to. It builds
   a dense `DRAKEN_ARRAY` parent over int32 offsets whose
   `VectorOwner::child_owner` (`draken/core/vector_owner.h`) owns the element
   vector outright — the destructor chains, so no lifetime plumbing of its own
   was invented. Every child tag `serialize_list_column` can emit is handled:
   `CHILD_INT64/INT32/UINT64/FLOAT32/FLOAT64/BOOL/STRING`, and `CHILD_ARRAY`,
   which recurses for `list<list<...>>` of arbitrary depth. Unlike the Cython
   reference it bounds-checks every read against the pool blob's length: a
   native worker thread has no Python exception to unwind into, so a malformed
   blob must surface as an `ErrCtx`, not an OOB read.
2. **Native Source** (`native_parquet_scan_source.hpp`) — a new
   `array_columns[i]` plan flag (parallel to `column_names`, the same mechanism
   as `decimal_columns` / `varchar_columns`) routes the DK_POOL blob to that
   decoder. The flag is load-bearing, not decoration: all three pool shapes
   (decimal / varchar / array) are indistinguishable from the `DirectKind`
   alone. A column planned ARRAY that arrives on any other DirectKind fails
   loud rather than reading a direct buffer as a list.
3. **Footer gate** (`native_scan_supported`, `pool_reader.pyx`) — a new `"array"`
   kind admits a column whose rugo footer logical type starts with `array<`,
   whose `max_repetition_level >= 1`, and whose (leaf) physical type is one
   `serialize_list_column` can actually emit. `int96` and
   `fixed_len_byte_array` leaves stay closed — rugo *throws* on them, on both
   paths, so admitting them would only move where the same error surfaces.
4. **Compiler** (`_native_scan_plan`, `compiler.py`) — `DrakenType.ARRAY` gets
   its own classifier branch instead of falling into the R6 bail, and builds
   `array_columns`. The bail's own comment was **stale and wrong** (it claimed
   ARRAY has no physical tag and reports `:NONE`; the observed code was always
   `:ARRAY`) — corrected, along with a note of what is actually left behind it.

**The one coercion.** `_wp11_logical_coerce`'s scalar retags do not apply to a
list, but `ARRAY<TIMESTAMP>` does need one: parquet stores the leaf as physical
int64 and the IPC list format carries no logical type, so without a retag the
elements read back as raw micros. The trampoline fixes this with
`vector_retag_array_child_as_timestamp64` driven by `_sp_array_ts_unit_map`
(`parquet_read.pyx`); the native path mirrors it exactly via a new
`LC_ARRAY_TIMESTAMP` packing. `ARRAY<DATE>` gets **no** retag — because the
trampoline gives it none either. Parity is the bar, not judgement.

**Deliberately still closed.** MAP and STRUCT are unchanged and were never behind
this guard: STRUCT is annotated `json` by rugo's footer and binds as a string
column (it already went native), and MAP is refused by the footer gate as
`footer_gate`. Both verified against real files rather than assumed. The
`varchar` branch's LIST rejection was NOT relaxed — an `array<...>`-annotated
column is classified `"array"` by the compiler and never reaches that branch.

**Verification.** The trampoline's answers were captured BEFORE the change and
compared after: 31 queries over 6 datasets, hashing **every row's full nested
value** (not row counts), all **SHA-identical**. On top of that,
`tests/unit/operators/test_wp_r6_array_scan.py` is an A/B harness running each
query natively and forced-trampoline **in one process**, over every element type,
`SELECT *`, mixed projections, role-3 (filter-only) arrays, zero-projection
`COUNT(*)`, and the operators that actually consume a list (`UNNEST`,
`ARRAY_CONTAINS`, `LENGTH`, `ARRAY_AGG`). The four null-ish shapes are pinned
*by value* because they are genuinely different and a decoder can collapse them:
a NULL list (`None`), an EMPTY list (`[]`), a list of NULLs (`[None]`), and a
NULL *inner* list inside a nested one (`[[7, None], None, []]`).

The pre-existing ARRAY datasets are all VARCHAR-element and mostly uniform, which
is exactly the trap that makes a broken decoder look correct, so
`dev/generate_array_testdata.py` writes `testdata/flat/array_types` — every
element type, both string arenas (inline and >12-byte), NULL/empty/NULL-bearing
lists, and `list<list<int64>>`, across two row groups.

Gates: `make q` 217/217, `make tpch` 22/22, census `non_admissible_kind` **0**
(165 scans, 164 native), ASan clean on the array query set. `tests/unit/operators`
95 failures before and after (all pre-existing). The reason code is retired from
`HAND_SET` — same convention as R2 / R5 / R5b — because it now has **no reachable
SQL trigger**: a sweep of every parquet dataset under `testdata/` finds none.
What remains behind the guard (VARIANT, INTERVAL, VECTOR_FP16, a DECIMAL/temporal
column with an unusable logical descriptor) is defensive, like `no_manifest`/R7a.

### 5. `bool_predicate_input` (R5) — census 0 → **CLOSED**
A `BOOL` column used as a **predicate input** failed the whole scan closed
(WP-11 fail-closed). BOOL columns that are only *projected* already decoded
natively and were never affected.

**Root cause was one missing switch branch, not a missing engine capability.**
`bytecode_is_all_c_native` correctly reported the predicate lowerable, and the
relocated `ExprFilter` ran the same bytecode VM as everything else — but
`draken_compare_dv`'s type switch (`draken/ops/compare_dv.cpp`) had no
`DRAKEN_BOOL` case, so every bool comparison hit `default: return nullptr`
("declined, use the caller's fallback"). The native ExprFilter **has** no
fallback, so it surfaced that as `err_op=11`. Same failure shape as the A1
`unsigned_predicate_input` (R5b) retirement, and it is retired the same way.

**Close-out (done).** Two changes:

1. **New kernel — `draken/ops/bool_compare.h` (`bool_compare_vector`).** Unlike
   R5b, BOOL could **not** be closed by literal coercion + dispatching an
   existing kernel: both operands already arrive tagged `DRAKEN_BOOL` (the
   literal via `BC_LOAD_LIT_BOOL`, which materialises a dense bitmap directly —
   it never goes through `_coerce_literal_physical`), so the type-match gate was
   already passing. What was missing is a kernel that can *read* a bool vector:
   BOOL is **bit-packed**, so `data` is a bitmap and `data[selection[i]]` means
   *bit* `selection[i]` — no fixed-width compare kernel can address it.
   The kernel is ONE uniform loop over the bitmap, per CLAUDE.md §11 — dense,
   constant (`selection` = the global zero vector) and dict (owned codes) all
   read correctly through the same path, with **no shape discriminant**.
   `validity` is indexed by the LOGICAL row `i` (the vector contract), not by
   `selection[i]`. Ordering is SQL's `FALSE < TRUE`; nulls follow the
   `compare_vector` contract (result row NULL when **either** operand row is
   NULL — *not* Kleene AND/OR, which is `bool_logical.h`'s job).
2. **Guard removed** (`_native_scan_plan`, `compiler.py`) — the
   `_physical_type(sc) == DrakenType.BOOL` bail is gone; the reason code is
   retired from `HAND_SET` (no longer reachable), same convention as R2 / R5b.

**Verification.** The trampoline's answers were captured BEFORE the change and
compared after: every query's survivor set is **SHA-identical**, on `= TRUE`,
`= FALSE`, `<> TRUE`, `!= FALSE`, projected-and-filtered, role-3 (filter-only),
composed with an int predicate, zero-projection `COUNT(*)`, and a NULL-bearing
bool column (80 TRUE / 40 FALSE / 80 NULL — the NULL rows survive neither
polarity, on either path). Gates: `make q` 217/217, `make tpch` 22/22, census
`bool_predicate_input` **0** (165 scans, 164 native). See
`test_bool_predicate_input_now_native` /
`test_bool_predicate_survivor_count_matches_trampoline` in
`tests/unit/operators/test_native_scan_residual_gate.py`, the A/B parity harness
(native vs forced-trampoline in one run) in
`tests/unit/operators/test_wp11_decimal_temporal_bool_scan.py` — which is where
WP-11's two `*_fails_closed` bool tests lived and are now `*_now_native` — and
the C++ bitmap assertions in `_compare_dv_smoke_test`
(`draken/tests/native/test_compare_dv.py`).

**Deliberately NOT done (would need architect agreement).** The kernel has no
byte-wise dense fast path. The overwhelmingly common shape — a dense-identity
bool column against `BC_LOAD_LIT_BOOL`'s dense literal bitmap — could compare 8
rows per instruction with plain word ops, the way `bool_and`/`bool_or` in
`bool_logical.h` already do for their identity-selection inputs. That is
encoding-shape-specialized dispatch, which CLAUDE.md §11 says must be surfaced
before implementing, so only the uniform path is here.

**Adjacent, NOT closed by this (separate gap):** `WHERE <bool col> IS TRUE` /
`IS FALSE` still hard-errors at `compiler.py:454` ("a … predicate outside the
c-native kernel set") — a different opcode, unchanged by this work, and part of
the R4-adjacent hard-error class described in finding 2 above. A bare
`WHERE <bool col>` is rejected earlier still, by the planner, as unsupported
syntax.

### 6. `fused_topn` (R3) — census 1 → **1** — **PARTIALLY CLOSED (A3)**
`scan._topn_sort_name is not None` — an `ORDER BY … LIMIT` fused into the scan
(observed on `SELECT * … ORDER BY … LIMIT`).

**Inventory finding.** `scan._topn_sort_name`/`_topn_limit`/`_topn_descending`
is a **trampoline-only decode-skip hint**, not the mechanism that produces
correctness. `TopNScanPushdownStrategy` (`opteryx/planner/optimizer/strategies/
topn_scan_pushdown.py`) stamps it purely so the trampoline's own two-pass
late-materialization (`_apply_topn` in `parquet_read.pyx`, which used Python
`to_pylist()` + `sorted()`) can shrink pass-2 decode to just the rows that can
survive the cut. Separately, `HeapSortNode` **always** compiles to a real
native `set_topn_sink` operator (`compiler.py::_compile_scan`, generically over
the incoming layout) that performs the actual sort / limit / tie-break /
null-order — this already ran downstream of `StreamingScanSource` before A3,
independent of scan Source. So the scan-level hint changes nothing about
*which rows reach the client*; the open question was only ever about decode
cost, not correctness.

**First attempt (reverted): admit unconditionally.** The first cut removed the
guard unconditionally and let native ignore the hint in every case, reasoning
that the no-predicate case is genuinely free (the trampoline's own
`two_pass_eligible` never activates without a pushed WHERE, so neither path
decode-skips). **This missed the composed case.** ClickBench Q24 —
`SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` — IS
a fused-TopN-with-predicate query, and there the trampoline's two-pass
late-mat is doing real, load-bearing work: it decodes only the predicate
(`URL`) and sort-key (`EventTime`) columns for the whole 100M-row table, then
the other ~100 projected columns only for the handful of `LIKE`-surviving
rows. Sending this shape native (ignoring the hint) forces a full single-pass
decode of **every** column of **every** row — **measured ~400% slower on Q24
and ~20% slower on the ClickBench suite overall**. This was caught after
landing (not before — the pre-land benchmark only exercised a synthetic
no-predicate case) and reverted the same day.

**A3 close-out (as landed).** `_native_scan_plan` (`compiler.py`) admits a
fused-TopN scan to native **only when it carries no predicate**:
```python
if (getattr(scan, "_topn_sort_name", None) is not None
        and getattr(scan, "predicates", None)):
    self.scan_residual_reasons[scan.identity] = "fused_topn"
    return None
```
No new native TopN/heap-select kernel was needed for the no-predicate
sub-case — the "hard part" (native heap-select) already existed and already
ran on this exact plan shape, unconditionally, regardless of scan Source. The
composed (predicate-bearing) sub-case remains an **open, fail-closed residual**
— it is the one case the census still reports, and it is the correct,
measured decision, not an oversight.

**Future performance follow-on (not a correctness/residual gap):** closing the
composed sub-case natively requires full WP-02 §9 two-pass late-materialization
— pass-1 decode of predicate/sort-key columns → native heap-select survivor
mask → masked pass-2 decode of the remaining projected columns via
`submit_work_native_masked`. That is real, structural work (the native source
does not do masked pass-2 decode today) — do not attempt to close it by simply
removing the predicate check above again without that machinery in place; that
is precisely what produced the Q24 regression.

See `tests/unit/operators/test_wp_a3_fused_topn_scan.py` for the A/B
correctness parity harness (ascending/descending, ties, NULLs, N above/below a
row-group boundary, and a large-N edge — all no-predicate) plus the fail-closed
assertion for the composed shape, and
`tests/unit/operators/test_native_scan_residual_gate.py::test_fused_topn_no_predicate_now_native`
/ `test_fused_topn_with_predicate_stays_trampoline` for the acceptance-gate
assertions.

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
