# Ticket: Parquet column-chunk decode parallelism (dict + data pages)

## Problem

When decoding a single parquet column chunk today, two latency sources sit on
the critical path that don't have to:

1. **Dictionary page decode is fully synchronous** before any data page work
   begins. For URL in ClickBench the dict is large (hundreds of MB
   compressed); decompressing and parsing it serially adds 100–300 ms of
   pure-wait time before the worker pool sees any data tasks.
2. **Data pages within one column chunk are decompressed serially.** Each page
   becomes a single work unit on the pool. URL has many fat ZSTD pages, so
   one column chunk does not fan out at all — we leave cores idle.

This ticket: get both onto the worker pool. Dict page becomes a pool task
that data tasks await via a latch; data page decompression fans out across
workers within a chunk.

## Why this matters

URL column read is ~3.3 s end-to-end. After dict-aware aggregation lands
(separate ticket) some queries skip materialization entirely, but the **read
itself** still pays the serial dict cost and the serial decompress cost on
every URL-touching query. This ticket attacks the read latency directly.

## Scope

In scope:
- `rugo/src/parquet/decode_column.cpp` — dictionary decode and per-page
  dispatch.
- `rugo/src/parquet/thread_pool.hpp` — only if a small primitive (latch /
  countdown event) needs to be added; prefer using std primitives.

Out of scope:
- Cross-column or cross-row-group parallelism (already exists at a higher
  layer).
- SIMD changes to the materialization path (separate ticket).
- Codec changes (LZ4 support, ZSTD parameter tuning).

## Approach

### Part A — concurrent dict + data pages

Today's flow in `decode_column.cpp` (verify line numbers — drift expected):

1. Read dict page bytes.
2. Decompress dict (blocking).
3. Parse dict (blocking).
4. Prescan data pages.
5. Enqueue data page decode tasks to the pool.

Change to:

1. Read dict page bytes.
2. **Enqueue** a task that decompresses + parses the dict, then sets a
   `dict_ready` latch (use `std::atomic<bool>` + `std::condition_variable`,
   or `std::latch` if C++20 is available — check the build).
3. Prescan data pages.
4. Enqueue data page tasks. Each task:
   - Decompresses its page (no dict needed).
   - **Before** touching dict (during decode of dict-referenced values),
     waits on `dict_ready`. Plain (non-dict) data pages don't wait.
5. Join all tasks at the end as today.

For most data pages on a dict-encoded column, the dict is needed during
decode; the latch wait will usually be a no-op by the time decode reaches it
because decompression takes long enough for the dict task to finish first.
The wait is correctness insurance, not the optimization itself — the
optimization is that dict decompress overlaps with data decompress instead of
serializing.

Edge cases:
- Column chunk with no dict page: skip the dict task entirely; latch starts
  in the "ready" state.
- Dict task fails: store the exception, mark latch ready, propagate when a
  data task observes the failure. Fail fast — do not silently degrade.
- Single-threaded pool / pool size 1: the optimization degenerates to the
  current behavior. That's fine; no special-case needed.

### Part B — fan-out page decompression within a chunk

Currently each data page is one pool task that does
`decompress → decode → emit`. For wide chunks with many pages, that already
parallelizes across pages. The gap is per-page work being serial.

Option B1 (preferred, smaller change): **leave page-level granularity alone**
but verify that all data pages in a chunk are enqueued up-front (not produced
lazily during a serial walk). If the current code prescans then enqueues in
one shot, Part B is already done by the existing pool — confirm and close
this part.

Option B2 (only if B1 shows pages are dispatched serially or in waves):
split decompress and decode into two pipelined task stages:
- Stage 1 task: decompress page N → buffer.
- Stage 2 task: decode page N from buffer → output.
- Stage 2 for page N depends on stage 1 for page N (and dict_ready for
  dict-referenced columns).

Stage 1 is pure CPU; stage 2 is pure CPU. Pipelining them lets the pool keep
all cores busy when the chunk has more pages than workers.

**Decide between B1 and B2 with a measurement first.** Add temporary
instrumentation: per-chunk, log (#pages, total decompress wall, total decode
wall, pool-busy %). If pool-busy is already >80% on URL chunks, B1 is enough.

### Part C — instrumentation (required, not optional)

Add an env-var-gated tracing hook (e.g. `RUGO_DECODE_TRACE=1`) that emits
per-column-chunk timings: dict decompress, dict parse, data decompress total,
data decode total, wall time. Without this we cannot validate the change.
Remove or leave behind a feature flag — the user's call.

## Constraints (from CLAUDE.md)

- **No Python in this path.** This is C++; keep it that way.
- **Release the GIL** if any Cython glue is touched.
- **Fail fast.** Propagate exceptions from worker tasks; do not swallow.
- **No fallback duplication.** Don't keep both serial and parallel dict
  decode paths around — replace, with the single-threaded pool case being
  the natural degenerate behavior.
- **Architecture targets**: ARM (NEON) for dev, x86 (AVX2) for prod. No
  arch-specific code needed in this ticket, but don't introduce
  arch-assumptions.
- **Do not commit.**

## Files (verify before editing)

- `rugo/src/parquet/decode_column.cpp` — dict decode path and page dispatch
  loop.
- `rugo/src/parquet/thread_pool.hpp` — `SimpleThreadPool` and any latch
  primitive that needs to be added.
- `rugo/src/parquet/compression.cpp` — only if dict task needs a separate
  decompress entry point.

## Tests

- Existing parquet read tests must pass byte-for-byte. Materialized
  StringVector output for URL must be identical before/after.
- Add a stress test: a column chunk with many small pages and one with few
  large pages — both should decode correctly and faster.
- `make q` must pass.
- `make clickbench` URL queries (Q15, Q20, Q22, Q28, Q29) measured before
  and after; report wall-time deltas in the PR. Goal: 10–20% reduction on
  the read phase for the URL column.

## Definition of done

- Dict page decode runs as a pool task with a latch gating dict-dependent
  data tasks.
- Page dispatch verified to be fan-out (B1) or pipelined into
  decompress+decode stages (B2), based on measurement.
- `RUGO_DECODE_TRACE` instrumentation lands (kept or removed per user
  decision).
- `make q` passes.
- ClickBench URL-touching queries measured before/after; numbers in PR.
- No fallback path duplication; no swallowed exceptions.
