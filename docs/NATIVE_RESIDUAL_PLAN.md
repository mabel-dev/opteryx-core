# Native SELECT-path residual close-out plan (A0 acceptance gate)

> ## ✅ MILESTONE — the census frontier is empty (R3 close-out, 2026-08-01)
>
> **`dev/native_residual_census.py` now reports 165 scans, 165 native, 0
> trampoline.** `fused_topn` (R3) was the last reachable residual in the battery
> and is closed: the composed `WHERE … ORDER BY … LIMIT` shape runs on
> `LatmatScanSource`, a native two-pass late-materialization scan
> (`src/cpp/engine/native_latmat_scan_source.hpp`) that keeps the decode-skip
> instead of losing it. See item 6.
>
> **`StreamingScanSource` is NOT dead** — do not delete it. It is still selected
> by the one residual with a live SQL trigger, `footer_gate` via schema evolution
> (a projected column absent from some files), which `HAND_SET["footer_gate"]`
> exercises. It is also still the fixture several instrumentation tests force via
> `native_scan_supported`. What HAS gone is any *battery* query reaching it.
>
> `unlowerable_predicate` (R4) is now **CLOSED** — see item 7. It was the last
> entry in the strict-xfail frontier, which is now empty; `footer_gate` via schema
> evolution remains the one residual with a live SQL trigger.

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

**Latest census (after the R2 + R7b-temporal + R5 + R6 + R3 close-outs): 165
scans, 165 native, 0 trampoline — the frontier is empty.** "Native" now covers
two Source classes, both zero-Python: `NativeParquetScanSource` (single-pass) and
`LatmatScanSource` (R3's two-pass late-materialization scan). The census's
`_NATIVE_SOURCES` is the single list that defines this, read by both the tool and
the acceptance gate.

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
| `fused_topn` (R3) | 1 | 1 | 1 | 1 | **0** | ✖ retired (R3 close-out) |
| `pushed_limit` (R2) | 0 | 0 | 0 | 0 | **0** | ✖ closed (R2 close-out) |
| `unlowerable_predicate` (R4) | 0 | 0 | 0 | 0 | **0** | ✖ retired (R4 close-out) |
| `bool_predicate_input` (R5) | 0 | 0 | 0 | 0 | 0 | ✖ retired (R5 close-out) |
| `unsigned_predicate_input` (R5b, A1) | — | 0 | 0 | 0 | 0 | ✖ retired (A1) |
| `non_admissible_kind:<T>` (R6) | 0 | 0 | 0 | 0 | 0 | ✖ retired (R6 close-out) |
| `no_manifest` (R7a) | 0 | 0 | 0 | 0 | 0 | ✖ no SQL trigger |
| `temporal_predicate_input` (new, A2) | — | — | 0 | 0 | 0 | ✔ observed (missions dataset, not in battery) |

✔ **`unlowerable_predicate` (R4) — CLOSED and retired.** The marker was stale for
some months: the hand-set trigger (`... WHERE text RLIKE 'a'`) selects the
**native** Source, i.e. the regex predicate lowers to a c-native span. No R4
close-out chip was ever written — the native regex kernel work closed it
**incidentally**, which is exactly why the marker outlived the category.

Confirmed as a whole category, not just the one trigger:

* the battery census reports **165/165 scans native, 0 trampoline, no residual
  reasons of any kind**;
* a 43-shape hand sweep found no SQL that tags R4 — the regex family (`RLIKE`,
  `NOT RLIKE`, `SIMILAR TO`, `NOT SIMILAR TO`, `~`, `!~`, composed with
  `AND`/`OR`/`NOT`), string transforms, hashing/encoding, `SPLIT`, `SOUNDEX`,
  `LEVENSHTEIN`, `ARRAY_CONTAINS`, `CASE`, `COALESCE`/`NULLIF`, casts and
  arithmetic all either go native or raise.

Retired from `_OPEN_CATEGORIES` and `HAND_SET`; replaced by real passing
assertions in `test_regex_predicate_now_native` plus a native-vs-forced-trampoline
survivor-count parity check. The `bytecode_is_all_c_native` guard in `compiler.py`
**stays** as the structural fail-closed, exactly as R6's does — what is retired is
the claim that SQL can still reach it.

R4 was the last entry in the strict-xfail frontier, so `_OPEN_CATEGORIES` is now
empty. That is **not** the same as "nothing reaches the trampoline":
`footer_gate` via schema evolution still does, and keeps its `HAND_SET` entry.

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
ordered worklist entry below). The battery count was UNCHANGED at the time
(native 156 / 158, 2 trampoline scans) because the battery's one `fused_topn`
trigger is ClickBench Q24 — `SELECT * ... WHERE URL LIKE '%google%' ORDER BY
EventTime LIMIT 10` — which is exactly the composed (predicate-bearing) sub-case
A3 deliberately did NOT admit (admitting it as a single-pass scan was tried and
reverted after measuring a ~400% regression on Q24). The no-predicate sub-case
has no trigger in this battery, so its close-out is only visible via the
dedicated unit tests, not this census.

**The R3 close-out then took the composed sub-case as well**, via a native
two-pass late-materialization Source that keeps the decode-skip rather than
losing it — which is what finally emptied the battery's frontier. Item 6 below.

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

### 6. `fused_topn` (R3) — census 1 → **0** — **CLOSED**
`scan._topn_sort_name is not None` — an `ORDER BY … LIMIT` fused into the scan
(observed on `SELECT * … WHERE … ORDER BY … LIMIT`).

**What the hint is.** `scan._topn_sort_name`/`_topn_limit`/`_topn_descending` is
a **decode-skip hint**, not the mechanism that produces correctness.
`TopNScanPushdownStrategy` stamps it so the scan can shrink pass-2 decode to the
rows that can survive the cut. `HeapSortNode` **always** compiles to a real native
`set_topn_sink` operator (`compiler.py::_compile_scan`) that performs the actual
sort / limit / tie-break / null-order generically over the incoming layout,
independent of scan Source. So the hint never changes *which rows reach the
client* — only how much has to be decoded to find them.

**A3 closed the NO-predicate sub-case** (no late-materialization happens on either
path without a pushed WHERE, so a single-pass native scan is exactly equivalent).
**A first attempt at the composed sub-case admitted it as a plain single-pass
scan and was reverted**: that forces a full decode of ~105 columns × 100M rows and
measured ~400% slower on ClickBench Q24. The lesson stands — the shape may only be
admitted by something that PRESERVES the decode-skip.

**Close-out (done, 2026-08-01).** A new native Source,
`LatmatScanSource` (`src/cpp/engine/native_latmat_scan_source.hpp`), performs the
two passes in C++:

```
pass 1  decode predicate columns + sort key for the whole table; evaluate the
        predicate per row group into a survivor bitmap; keep the survivors
reduce  across ALL row groups, find the LIMIT boundary in the sort key and drop
        every survivor strictly worse than it (n rows plus ties at the boundary);
        row groups with no candidate left are never read again
pass 2  decode the remaining projected columns, MASKED to those rows, and zip
        them back onto their pass-1 columns
```

Four of the five pieces already existed natively and were only ever *driven* from
Python — this is assembly plus one new kernel, not a from-scratch build:

| piece | reused from |
|---|---|
| decode + **masked** decode | `rugo::ParquetIOPipeline::submit_row_group(…, row_mask)` |
| pass-1 predicate on the decode workers | rugo `Pass1Pred` / `pass1_run_predicate` |
| the same predicate as a fallback | `opteryx_pass1_predicate_eval`'s C ABI (`Pass1PredResolver`) |
| column materialization | `NativeScanColumnBuilder` — extracted verbatim out of `NativeParquetScanSource`, now shared by both Sources |
| row gather | draken `gather_rows` |

**The one new kernel: the boundary reduction (`reduce_to_topn`).** It does NOT
implement an ordering. It builds draken's own normalized sort keys
(`build_sort_keys`) over the pass-1 survivors and uses draken's own comparator
(`SortKeyCmp`) — the SAME definition the downstream `TopNSink` sorts with — then:

```cpp
nth_element(idx, idx + n - 1, cmp);   b = idx[n-1];
keep[r] = !cmp(b, r);                 // r is not strictly worse than the boundary
```

Because the comparator is shared, this is correct **by construction** for every
key type, for ties (a tied row compares neither-before-nor-after `b`, so it is
kept — "n rows plus ties at the boundary"), and for NULLs. No null rule and no
type switch is written in the reduction at all. `n >= total` skips it entirely.

**⚠ Building this surfaced a NULL-ordering bug that BOTH paths had.**
`_apply_topn` (`parquet_read.pyx`) hard-coded "NULLs sort last" in both
directions. draken orders NULL **below every value** (`SortKeyCmp`:
`cmp = va ? 1 : -1`), i.e. NULLs come **FIRST ascending**, last descending — so
for `ORDER BY <nullable> ASC LIMIT n` with more than n non-null survivors,
`_apply_topn` dropped NULL rows that belong in the answer. Verified on a 3-NULL
fixture: it returned `[1003…1012]` where the correct answer is
`[NULL,NULL,NULL,1003…1009]`.

**Both sides are fixed, and neither now writes a null rule by hand** — that is the
point, because a rule stated twice is a rule that drifts. The native Source
reduces with draken's own comparator. `_apply_topn` reduces on `_topn_rank`
(`(0,)` for NULL, `(1, value)` otherwise), so its boundary test is a single rank
comparison with no null branch and no separate "fewer than n non-null" case; it
also drops the old full `sorted()` for `heapq.nsmallest/nlargest`, O(m log n)
instead of O(m log m) over every pass-1 survivor in the table.

**A FLOAT sort key containing NaN had the same class of bug, fixed the same way.**
draken sorts NaN HIGHEST regardless of sign (`sort_num_key` → `UINT64_MAX`), but
Python defines EVERY comparison against NaN as False, so `_apply_topn`'s old
`v <= boundary` / `v >= boundary` test was silently False for a NaN survivor no
matter where the boundary sat — and the `nlargest`/`nsmallest` boundary
selection itself saw the same broken comparisons. Observed: an ASC top-10 over
3 NaN keys plus several thousand real ones collapsed to a **single** returned
row. `_topn_rank` now maps NaN to the tag above every real value (`(2,)`, vs
`(0,)` for NULL and `(1, v)` for a real value), so the boundary test never
compares a NaN payload directly. The native path never had this bug — it
reduces with draken's own `sort_num_key`, which already maps NaN to
`UINT64_MAX`. See `test_latmat_nan_sort_key` and
`test_trampoline_apply_topn_keeps_nan_rows`.

**Eligibility mirrors the trampoline's own `two_pass_eligible` + `topn_active`**
(`_latmat_scan_plan`, `compiler.py`): a pushed, all-c-native predicate; the
late-materialization feature flag; a non-empty pass-2 column set; and the same
manifest **selectivity gate**
(`PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY`, 0.7). That mirroring is the whole
safety argument for declining: every refused shape falls through to the ordinary
single-pass native scan, which is exactly the work the trampoline would have done
for it — never a silently lost decode-skip. A fused-TopN scan whose sort key is
not in its own projection **raises** rather than degrading; the
`TopNScanPushdownStrategy` invariant (HeapSort reads directly from this scan)
makes that unreachable, and a broken invariant is not a thing to guess around.

**Threading.** Pass 1 is a barrier — no boundary exists until every row group's
sort key has been seen. The first worker into `get_morsel` runs it to completion
under the Source's global mutex (rugo's decode workers still parallelise the
decode and the pushed predicate); the others park there, then all of them stream
pass 2 concurrently, claiming work items the same way `NativeParquetScanSource`
does.

**Measured** (`scratch.hits_rugo_262k`, the ClickBench `FULL_SPLIT_RUGO_262K`
dataset, 99 files / ~100M rows; peak RSS is `ru_maxrss` of a one-query process):

| Q24 `SELECT * … WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10` | time | peak RSS |
|---|---:|---:|
| trampoline (the old path) | 1233 ms | 2.37 GB |
| **native `LatmatScanSource`** | **1206 ms** | **2.31 GB** |
| native single-pass (the reverted approach) | 4018 ms | 10.10 GB |

Output is **SHA-identical** to the single-pass reference.

`make clickbench` on the same dataset (`FULL_SPLIT_RUGO_262K`), min-of-3 matched
back-to-back runs, box settled between campaigns:

| | Q24 | Σ suite |
|---|---:|---:|
| before (trampoline) | 960.3 ms | 16 116 ms |
| after (`LatmatScanSource`) | 892.4 ms | 15 822 ms |

⚠ Read that as **no measurable change**, not a win: the within-campaign spread was
960 / 1041 / 1005 before and 989 / 961 / 892 after — ~10%, comfortably wider than
the difference. The acceptance criterion here is the ABSENCE of the ~400%
regression the single-pass approach caused, which is unmissable at this
resolution.

**Memory at the barrier — measured, not assumed.** Pass 1 holds every survivor's
pass-1 columns until the boundary exists, so a weak predicate is the risk case.
The selectivity gate catches most of it (`WHERE URL <> ''` is refused and takes
the single-pass path). The honest worst case is a predicate the estimator
under-rates on a WIDE string column — `WHERE URL LIKE '%h%'`, which matches
essentially every row:

| `… WHERE URL LIKE '%h%' ORDER BY EventTime LIMIT 10` | time | peak RSS |
|---|---:|---:|
| trampoline | 31 018 ms | 18.32 GB |
| **native `LatmatScanSource`** | **5 043 ms** | **12.74 GB** |
| native single-pass | 4 857 ms | 10.40 GB |

So in the degenerate case the native two-pass path costs ~4% time and ~22% memory
over a single pass — and is **6x faster and 5.6 GB lighter than the trampoline it
replaces**. The exposure is inherited, bounded, and strictly improved; no new
threshold was invented for it.

**Verification.** `tests/unit/operators/test_wp_r3_latmat_scan.py` is the
correctness matrix: every case runs three ways in one process — natively, on the
forced trampoline (so `_apply_topn`'s independent implementation of the same
reduction is checked too), and with late-materialization off (the un-pushed
single-pass ground truth) — over ties at
the boundary, tie blocks spanning row groups, all-NULL keys, fewer-than-n non-null
rows, ASC and DESC, N above the survivor count, string and float keys,
zero survivors, sort-key-is-the-predicate-column, and an explicit pass-1/pass-2
row-alignment check. Comparison is row count + sort-key multiset + "every returned
row is a real survivor row", NOT a row sequence: `ORDER BY … LIMIT n` over a tie
block wider than the cut has no defined row order, and asserting one would be
asserting something SQL never promised.

Gates: `make q` 217/217, `make tpch` 22/22, census `fused_topn` **0** (165 scans,
165 native, 0 trampoline). The reason code is retired from `HAND_SET` — same
convention as R2 / R5 / R5b / R6.

**Two test files had to be re-pointed, deliberately, not silently.**
`test_parquet_late_materialization.py` and `test_parquet_latmat_dict_skip.py` read
the TRAMPOLINE's own latmat telemetry counters, which the native Source does not
emit; the first now forces the trampoline via an autouse
`native_scan_supported` fixture so it keeps testing the (still live) code it is
named after. `test_wp_a3_fused_topn_scan.py`'s two "stays trampoline" assertions
became "now native".

**Perf follow-on (2026-08-01): `pass1_build_dv_view` shape coverage.** At close-out
`pass1_build_dv_view` (rugo `io_pipeline.hpp`) handled exactly one shape —
plain `DK_VARCHAR` — and declined everything else, leaving `survivor_mask` empty
so the consumer (`LatmatScanSource` / the trampoline) evaluated the predicate
serially on its own thread instead of in parallel on the decode workers. Measured
on the full ClickBench dataset (`scratch.hits_rugo_262k`, 99 files, ~100M rows,
`URL LIKE '%google%' ORDER BY EventTime LIMIT 10`): only 75/396 row groups took
the worker-side path; 321 fell back.

Extended `pass1_build_dv_view` to every direct-path shape rugo can emit for a
predicate column: `DK_VARCHAR_DICT` (dict-shaped strings — the actual gap; 100%
of the 321 declines were this one kind), the fixed-width dense kinds
(`DK_INT8/16/32/64`, `DK_UINT8/16/32/64`, `DK_FLOAT32/64`, `DK_BOOL`), and their
`_DICT` counterparts. `DK_POOL` (a serialized IPC blob, not a viewable buffer)
and `DK_DECIMAL128` (precision/scale live in a logical descriptor attached
OUTSIDE the `DrakenVector`) are still refused — a view of either would answer a
different question, not a cheaper one.

**The tag problem.** rugo tags a view from the decoded buffers' own physical
kind — the only thing it can know, since rugo stays opteryx-free. Some columns
are RETAGGED after decode by plan-time state rugo has no access to: DATE /
TIMESTAMP (physical int → `DRAKEN_DATE32`/`DRAKEN_TIMESTAMP64`, the latter with a
mandatory unit descriptor), int64-backed DECIMAL / DECIMAL128, and NVARCHAR /
VARBINARY (share VARCHAR's byte layout, not its semantics — case folding, LENGTH,
regex all dispatch on the tag). A worker-side view of one of those would be a
wrong answer, not a fast path — CLAUDE.md §11's "a fast path whose result
differs from the uniform path is a bug, never an optimization" applies directly.
rugo cannot detect this, so the predicate is now only HANDED to the workers at
all when every predicate column reaches it on its NATURAL physical tag —
`pass1_worker_predicate_admissible`
(`opteryx/connectors/parquet_io/pass1_predicate_gate.py`), called from both
registration sites (`_latmat_scan_plan` and the trampoline's
`_ensure_scan_started`). Refusing is free: the consumer already evaluates the
identical program itself whenever `survivor_mask` comes back empty.

**Verification.** A dedicated fixture drove every admitted shape (dense +
dict, nullable + non-nullable, single-column and multi-column AND'd predicates)
through the native Source with a temporary harness that additionally ran the
serial fallback program over the SAME decoded columns and `memcmp`'d the two
masks. Every case: worker-side mask taken, byte-identical to the fallback,
zero mismatches. (FLOAT32 was left out of the fixture — it hits the
pre-existing, unrelated `float32_declared_vs_actual_type_divergence` gap where
the binder declares float32 columns as FLOAT64.) `make q` 217/217, `make tpch`
22/22 unaffected.

**Re-measured** on the same dataset and query: worker-side coverage 396/396 row
groups (was 75/396). Wall time min-of-3, matched back-to-back, jemalloc
preloaded, box settled: 972.3 / 1016.7 / 1054.4 ms — statistically unchanged
from the 892–1041 ms noise band already on record for this query. Read that the
same way the original close-out did: the acceptance criterion here is closing
the shape-coverage gap (and proving it byte-identical), not a Q24 speedup —
pass-1 predicate evaluation was evidently never the bottleneck for this
query/dataset (IO + pass-2 decode of the ~100 survivor columns dominates), so
routing 100% instead of 19% of row groups through the parallel path doesn't
move a wall clock it was never gating. The gap closed is architectural
(uniform coverage per §11, no silent single-threaded fallback for the common
dict-encoded-string case) and should matter more for CPU-bound predicates or
smaller-selectivity queries where pass-1 eval is a larger share of the work —
not proven here, just the honest scope of what this change does and doesn't
show.


### 7. `unlowerable_predicate` (R4) — **CLOSED** (retired)
A pushed predicate that does not lower to a c-native span (regex / `RLIKE`).

**Closed incidentally by the native regex kernel work**, not by a close-out chip;
retired 2026-08-03 after confirming the whole category (census 165/165 native +
a 43-shape hand sweep — see the R4 note above the census table). The old
"DO NOT TOUCH from A0 / overlaps uncommitted `draken_native.cpp` +
`evaluation.pyx` WIP" dependency no longer applies: that WIP landed, and closing
R4 required no engine change at all — only retiring the marker and re-pointing
the tests that used its trigger as their canonical trampoline fixture.

The `bytecode_is_all_c_native` guard stays as the structural fail-closed. The
adjacent hard-error class (a non-lowerable predicate that never *pushes* becomes
a standalone Filter and raises in `_lower_expression`) is unchanged and was never
tagged R4 — see finding 2 above.

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
