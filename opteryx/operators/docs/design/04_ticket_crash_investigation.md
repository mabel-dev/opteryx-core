# 04 — Ticket: Investigate intermittent crash in `make q`

> Status: focused diagnosis ticket. **Investigation only — do not change
> code in the suspect ranges below until you have a backtrace and the
> architect has reviewed the root cause.**
>
> Self-contained. You don't need to read other PM logs to start, but
> `03_pm_log.md` (entry dated 2026-05-26, section
> "D-E taken; intermittent crash exposed; STOP") has the full backstory.

---

## Goal

Identify the root cause of the intermittent crash in `make q`. Produce:

1. A native backtrace (LLDB or ASAN report) of the crash site.
2. A one-paragraph diagnosis pointing at the exact file:line that's
   wrong, and whether it's a race, use-after-free, double-free, heap
   overflow, or uninit read.
3. A proposed fix sketch (do not implement — surface to the architect).

**Do not** start "fixing" by editing code in the suspect ranges before
you have the backtrace. Every fix without the trace is a guess, and
the previous PM (me) stopped specifically to avoid that.

## Background — what got us here

The draken rebuild deleted typed-Vector subclasses (`Integer64Vector`,
`Float64Vector`, `StringVector`, `DecimalVector`). Operator-PM (me)
migrated the operator tree to uniform `Vector` + `DrakenType`
dispatch. The migration:

- Replaced `isinstance(vec, Integer64Vector)` with
  `vec.unified().type == DRAKEN_INT64`.
- Replaced `Integer64Vector(0, True); vec.ptr = buf; vec._unified_view = ...`
  finalize-handoff pattern with new "copy-and-handoff" helpers
  (`_consume_int64_buffer`, `_consume_float64_buffer`,
  `_materialize_fixed_buffer`, `_ks_consume_int64_buffer`).
- Wrapped nb Vectors in the Cython shim at appropriate boundaries
  (filter.pyx `_build_constant_vector`).
- Unwrapped Cython shims to `_nb` at the nanobind boundary
  (parquet_read.pyx `_coerce_logical_types`).

After all that, `make q` reaches **119/133 passing** when it doesn't
crash. The crash rate is **~60%** of clean runs, **~100%** under
`MallocScribble=1`. Pre-rebuild stale-`.so` baseline was also 119/133
(no documented intermittency from before — could be pre-existing race
finally surfacing on a fresh build, or fresh bug from the migration).

## Reproducer

The crash is sequence-dependent. None of the queries crash in
isolation; the sequence needs many queries through a fresh session.

**Fastest deterministic reproducer** (~100% crash rate):

```bash
MallocScribble=1 make q
```

`MallocScribble=1` on macOS writes `0xAA` to freed memory and `0x55`
to fresh `malloc`. It turns probabilistic reads-of-freed-memory into
deterministic SEGVs.

**A smaller scripted reproducer** (the previous PM extracted the first
20 queries into a standalone script that crashes under MallocScribble
~50% of the time):

```python
# /tmp/run_qs.py — fresh-session-per-query, 20 queries from test_shapes_basic.py
# See 03_pm_log.md for the exact query list, or extract from
# tests/integration/sql_battery/test_shapes_basic.py STATEMENTS[0:20].
```

The scripted form is preferable for LLDB because it has no pytest
overhead.

**Characterisation observed:**

- Single query alone: never crashes.
- Same query 20× repeated in fresh sessions: never crashes.
- Full 20-query mixed sequence: crashes sometimes.
- Sequence consistently survives through query 18; crash typically
  fires around queries 19-20 or later.
- Query 19 is a GROUP BY:
  `SELECT * FROM (SELECT COUNT(*), name FROM testdata.astronauts GROUP BY name ORDER BY COUNT(*)) AS SQ LIMIT 5`.
  Query 20 is a multi-predicate filter:
  `SELECT * FROM testdata.astronauts WHERE name LIKE '%o%' AND ` + "`year`" + ` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)`.
- `PYTHONFAULTHANDLER=1` masks the crash via timing changes, so
  faulthandler tracebacks are not useful here.

## Suspect ranges

In order of likelihood:

### 1. New producer helpers (operator-PM's recent code)

The most likely culprits. The migration replaced typed-Vector
finalize-handoff with these:

- `opteryx/operators/grouped_aggregate_hashed/_collectors_numeric.pxi`
  — `_materialize_fixed_buffer`, `_consume_int64_buffer`,
  `_consume_float64_buffer`, `_slice_int64_buffer`,
  `_slice_float64_buffer` (~lines 120-200). Called from every numeric
  collector's `finalize()` / `finalize_slice()`.
- `opteryx/operators/grouped_aggregate_hashed/_collectors_buffered.pxi`
  — the inline producer in `MedianFloat64Collector.finalize_slice()`
  (~lines 135-185).
- `opteryx/operators/grouped_aggregate_hashed/_key_store.pxi`
  — `_ks_consume_int64_buffer` (~lines 67-105) used in
  `reconstruct_vectors()` for the multi-column GROUP BY path
  (~line 1003).

**What they do.** Allocate a `draken_malloc`'d output buffer + an
optional `draken_malloc`'d validity bitmap, copy the contents from a
libc-malloc'd `DrakenFixedBuffer*` (collector internal state) into
them, then call `_vector_from_decoded(data, validity, length, dtype)`
to transfer ownership to a Vector. The Vector's GC will eventually
`draken_free` the buffers.

**What might be wrong.** I reviewed them on paper and they look right,
but the intermittency tracks the GROUP BY path. The architect's
intuition (`03_pm_log.md`) was race or uninit memory. Specific
sub-suspicions:

- Validity bitmap bounds: I allocate `(length + 7) >> 3` bytes,
  memset 0xFF, then clear specific bits. If `length == 0` I take an
  early-return that passes NULL to `_vector_from_decoded`. Worth
  checking that `draken_vector_own_raw(NULL, NULL, 0, dtype)` is
  well-defined when callers later do `.unified()`/etc. on it.
- The `_consume_*` helpers call `free_fixed_buffer(buf, True)` AFTER
  `_materialize_fixed_buffer` returns. If `_materialize_fixed_buffer`
  raises (e.g., MemoryError mid-allocation), `buf` is leaked but not
  crashed — should be fine. But if it returns a Vector that
  *aliases* `buf` somehow, freeing `buf` would UAF. I don't think it
  aliases (the copy is via `memcpy`), but a backtrace will confirm.

### 2. Cross-allocator ownership transfer

Collectors keep state in **libc `malloc`** (via
`draken/core/fixed_vector.pxd::alloc_fixed_buffer`, which uses
`malloc(...)`). Result buffers are **mimalloc-allocated** (via
`draken_malloc` from `draken/core/alloc.h`). Vectors transfer-own
those via `_vector_from_decoded` → `draken_vector_own_raw`, and the
Vector's destructor calls `draken_free` (= `mi_free`).

If any path accidentally:
- Hands a libc-malloc'd buffer to `draken_vector_own_raw`: `mi_free`
  on a libc pointer = corruption.
- Calls `free_fixed_buffer` (libc `free`) on a draken_malloc'd
  pointer: same.

Check: in the new helpers I always pair the allocator with its free,
but the `seen` bitmap path in `SumInt64Collector.finalize()` does
`out.null_bitmap = seen` (where `seen` is libc-malloc'd via
`_grow_bitmap`/`malloc`) and then hands `out` to
`_consume_int64_buffer`. That helper passes `out.null_bitmap` to
`_materialize_fixed_buffer`, which **copies** it into a draken_malloc'd
bitmap. Then `free_fixed_buffer(out, True)` libc-frees the original.
So the allocators are paired correctly in my reading. Worth verifying.

### 3. Process-wide caches / thread pools

Module-level state that survives across fresh sessions:

- `opteryx/operators/parquet_read/parquet_read.pyx:72`:
  `_FOOTER_CACHE = ParquetFooterBytesCache()`.
- `opteryx/connectors/parquet_io/thread_pool_manager.py`: process-wide
  thread pools (HTTP, GCS, parquet decode). The
  `parquet_io_parallelization_progress.md` and
  `part2_layer_complete.md` memory notes indicate active parallel IO
  via these.

If a Vector handle or a borrowed `DrakenVector*` leaks into one of
these caches (or a worker thread holds a reference past the morsel's
lifetime), a later query gets a freed pointer.

**This is the "race condition" half of the architect's hypothesis.**
If the crash disappears with `OPTERYX_MAX_IO_WORKERS=1` or similar
single-threaded forcing (if such a knob exists), the bug is here.

## Tools

### LLDB

Foreground LLDB invocation that the previous PM tried (was
interrupted by rebuild churn — should work cleanly from a tree where
`make c` already passed):

```bash
PYBIN=$(python -c "import sys; print(sys.executable)")
cat > /tmp/lldb.cmd <<'EOF'
settings set target.env-vars MallocScribble=1
run
bt 50
register read
quit
y
EOF
lldb -s /tmp/lldb.cmd -- $PYBIN /tmp/run_qs.py
```

If LLDB's `target create` complains about the python being a shim
(pyenv), pass the resolved python path explicitly (the `$PYBIN`
trick).

### AddressSanitizer

If LLDB doesn't catch a clean trace, build with ASAN:

```bash
CFLAGS="-fsanitize=address -fno-omit-frame-pointer -g" \
CXXFLAGS="-fsanitize=address -fno-omit-frame-pointer -g" \
LDFLAGS="-fsanitize=address" \
make compile
```

Then run the reproducer with
`DYLD_INSERT_LIBRARIES=$(clang -print-file-name=libclang_rt.asan_osx_dynamic.dylib)`
(macOS) or use the python wrapper that already loads asan.

ASAN will report UAF / heap-overflow / double-free with file:line and
both the use site and the free site. That's almost certainly enough
to localise the bug.

### Mimalloc cross-thread free validation

Per the draken rebuild's stated risk (`feedback_no_false_green_clean_break.md`
and `draken_rebuild_delivery_plan.md`): "Validate mimalloc
CROSS-THREAD free, not just same-thread." If the reproducer crashes
only when IO is multi-threaded, this is the smoking gun.

## What not to do

- Don't edit any file in `opteryx/operators/grouped_aggregate_hashed/`
  before you have a backtrace.
- Don't disable mimalloc by default — it's the production allocator.
  Toggling it is a hypothesis test only.
- Don't extend `draken/` files (CLAUDE.md §1 + briefing §8).
- Don't bypass the shim/nb seam with `cdef object` workarounds —
  those are recognised anti-patterns
  (`feedback-consumer-edge-pattern`).
- Don't change the producer pattern documented in
  `03_pm_log.md` ("D-C resolved by self-service" section) without
  architect agreement; it was settled deliberately.
- Don't try to "improve" the row-count regressions noted in
  `03_pm_log.md` (the `_apply_constant_replacements` mutation through
  `morsel._columns[idx]`). Those are pre-existing in shape; chasing
  them now will drift the investigation.

## STOP conditions

Stop and surface to the architect if any of these happen:

- You've spent more than 90 minutes without a backtrace.
- Your edit count exceeds 0 (this ticket is investigation-only).
- You find yourself wanting to "just try" a fix in a suspect range.
- The build state diverges from `make c` clean (revert your tree).
- ASAN can't be built locally (then surface and ask for a build).

## Acceptance

Investigation is complete when you've produced:

1. A native backtrace pointing at file:line.
2. A diagnosis paragraph naming the bug class and the suspect ranges
   it implicates.
3. (Optional) A minimum-viable reproducer smaller than the 20-query
   sequence, ideally a single query or a 2-3 query sequence that
   triggers the same crash.

Hand the trace + diagnosis to the architect. **Do not implement the
fix yourself unless explicitly told to** — the fix may cross lane
boundaries (operator-PM vs draken-PM) and the architect decides.

## Quick reference — paths and helpers

Cython files that contain the new producer helpers and are the
primary suspect range:

```
opteryx/operators/grouped_aggregate_hashed/_collectors_numeric.pxi
opteryx/operators/grouped_aggregate_hashed/_collectors_buffered.pxi
opteryx/operators/grouped_aggregate_hashed/_key_store.pxi
```

Cross-allocator surfaces:

```
draken/core/alloc.h                          # draken_malloc / draken_free (mimalloc)
draken/core/fixed_vector.pxd                 # alloc_fixed_buffer / free_fixed_buffer (libc)
draken/vectors/_vector_shim.pyx              # from_decoded → draken_vector_own_raw
draken/core/draken_bridge.h                  # draken_vector_unwrap / draken_vector_own_*
draken/draken_native.cpp (line ~2020)        # draken_vector_own_raw implementation
draken/core/vector_alloc.cpp                 # draken_vector_from_dense (the struct fill)
```

Process-wide state:

```
opteryx/operators/parquet_read/parquet_read.pyx:72
opteryx/connectors/parquet_io/thread_pool_manager.py
```

Reproducer script template: `03_pm_log.md` has the 20-query list in
the "Bisecting" section; or extract from
`tests/integration/sql_battery/test_shapes_basic.py` (the
`STATEMENTS` list, first ~20 entries).

---

## Update — 2026-05-26: strong new lead

While the crash investigation was being scoped, the operator-PM
diagnosed a separate (?) bug that **may be the same bug**.
See `03_pm_log.md` § "parquet_read pass-1/pass-2 merge — DIAGNOSED".

**Summary.** The parquet_read two-pass late-materialisation path emits
morsels with **columns of different lengths**:

```
col[0] name  length=41    ← pass-1 column, filtered to 41 rows
col[3] status length=4441 ← pass-2 column, full row-group length
col[7] alma_mater length=46
col[15] space_walks_hours length=4531
```

Reproducer (single query, single session — no need for the 20-query
sequence):
```python
SELECT * FROM testdata.astronauts
WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%'
      AND group IN(1, 2, 3, 4, 5, 6)
```

This query is **also the one that segfaults under `MallocScribble`**
in the bisect runs (query 0020 in the test suite).

**Diagnostic hypothesis.** A morsel with mismatched column lengths
downstream causes operators to iterate by one column's length and
read off the end of shorter columns. That's the classic UAF/uninit
read profile that `MallocScribble` exposes.

**The merge site is parquet_read.pyx:884-908.** Pass-1 columns are
correctly filtered. Pass-2 columns come back from
`iter_pass2_row_groups_ipc()` with apparently unfiltered (and
inconsistent) lengths. The mask is passed to the C++ pipeline via
`pool_reader.pyx:715:submit_work_native_masked(..., bytes(mask_bytes))`
but either isn't applied or the merge across row groups is broken.

**Test before chasing other suspects.** Before deep-diving into the
producer helpers (suspect range 1) or cross-allocator transfer
(suspect range 2), verify this hypothesis:

```bash
# Quick check: does fixing column-length inconsistency eliminate the crash?
# Add this to parquet_read.pyx:_coerce_logical_types or the merge site:
#   assert len({v.length for v in combined_vectors}) == 1, "column-length mismatch"
# Then run the make q reproducer. If the assertion fires before SIGSEGV,
# the merge bug is the crash.
```

If confirmed, the fix lives in **draken/rugo C++ pipeline**, not in
operator-PM helpers. That redirects the investigation entirely.
