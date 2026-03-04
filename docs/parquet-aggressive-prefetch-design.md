# Aggressive Parquet Prefetch — Design Proposal

## Observed Behaviour

From `scratch/io_trace.jsonl` (100 files, 325 row groups, `SELECT COUNT(*) FROM scratch.hits WHERE URL LIKE '%google%'`):

| Metric | Value |
|--------|-------|
| Total query time | 34.16 s |
| Download phase span | 11.14 s (t=0.46 → 11.79 s) |
| Avg concurrent downloads **during download phase** | 7.51 |
| Peak concurrent downloads | 23 |
| % of download phase with ≤ 1 concurrent download | 28.7 % |
| Decode phase span | 17.22 s (t=0.36 → 17.58 s) |
| Post-decode gap to query end | 16.58 s |

The download phase achieves only ~31 % of the peak concurrency on average, with nearly a third of its span having at most one download in-flight.  A dedicated prefetch layer could bring average concurrency much closer to the 24-reader cap that is already configured.

The 16.58 s post-decode gap is the predicate-filter / aggregation pipeline and is outside the IO layer; it is not addressed here.

---

## Design Goals

1. Keep downloads running at the configured bandwidth cap (`PARQUET_GLOBAL_RANGE_READERS`) with as little idle time as possible.
2. Allow the decode pipeline to fall behind without throttling IO.
3. Require no changes to callers of `iter_row_groups`.
4. Introduce as few new knobs as possible; defaults should be safe for both local-disk and object-storage connectors.

---

## Proposed Changes

### 1. Two-tier buffer: raw download ring decoupled from decode admission

**Problem.**  `_dispatch_reads` currently checks `len(decode_pending) + len(decode_futures) < decode_buffer_cap` before submitting a new read.  When the decode pool is saturated, no new reads are started even when read-pool threads are idle.

**Proposal.**  Split the single `decode_buffer_cap` gate into two independent limits:

```
              ┌─────────────────────┐
              │   raw download ring │   ← capped by RAW_RING_CAP (new)
              │  (raw bytes, not    │     independently of decode
              │   yet decoded)      │
              └──────────┬──────────┘
                         │ decoded one-at-a-time by decode pool
              ┌──────────▼──────────┐
              │   ready-to-emit     │   ← capped by existing PARQUET_READY_
              │   (decoded)         │     ROWGROUP_QUEUE_CAP
              └─────────────────────┘
```

(`RAW_RING_CAP` rather than `PREFETCH_RING_CAP` to avoid name collision with `io_process_ring`.)

`_dispatch_reads` checks only `ranges_in_flight < global_ranges_cap` and `len(raw_ring) < RAW_RING_CAP`.  The decode pipeline drains from the raw ring at its own rate.

`RAW_RING_CAP` default: `PARQUET_GLOBAL_RANGE_READERS * 2` — enough to keep the read pool fed two rounds ahead.  For the hits dataset (~8.9 MB average column batch, 24 readers) the peak unprocessed memory is `24 * 2 * 8.9 MB ≈ 427 MB`.  This is intentional backpressure: if decode falls far behind, IO slows to match.

**Memory accounting note.**  `len(raw_ring)` counts row groups, not bytes.  Variable-sized row groups mean memory usage is not tightly bounded by `RAW_RING_CAP` alone.  A future improvement is a secondary soft byte-based cap (`RAW_RING_BYTES_CAP`, unset by default) alongside the count cap.  For the initial implementation the count cap is sufficient.

**Implementation sketch (`_iter_row_groups_v2`)**

```python
# Replace:
while (
    ranges_in_flight < global_ranges_cap
    and pending_dispatch
    and (len(decode_pending) + len(decode_futures)) < decode_buffer_cap
):

# With:
while (
    ranges_in_flight < global_ranges_cap
    and pending_dispatch
    and len(raw_ring) < raw_ring_cap
):
```

`raw_ring` is a `deque` that stores completed read results awaiting decode — it is what `decode_pending` already is, renamed and no longer counted against the read-dispatch gate.

---

### 2. Read-ahead admission: admit row groups from more files simultaneously

**Problem.**  The round-robin over `active_file_indices` only advances one row group per file per cycle, and `_admit_rowgroups` stops early when `admitted_this_cycle == 0` (typically because some files still lack footers).  This produces bursts of admits followed by idle gaps.

**Proposal.**  Change the admission loop so that within a single call to `_admit_rowgroups` it will admit row groups from any file whose footer is ready, even if that means admitting multiple row groups from fewer files, as long as the total `pending_dispatch + ranges_in_flight` remains under `rowgroups_in_flight_cap`.

The round-robin fairness goal (avoid one large file starving others) is preserved by capping at `rowgroups_per_file` per file per call.  The key change is removing the `admitted_this_cycle == 0` early-exit so that a file blocked on its footer does not silently stall the iteration.

```python
# Replace:
if admitted_this_cycle == 0:
    break

# With:
# (remove the early exit entirely; the outer while condition handles termination)
```

This alone can fill the 28.7 % low-concurrency windows that arise from footer-blocked files holding a slot in the round-robin.

**Fairness note.**  Without the early exit, files whose footers arrived first will be admitted more aggressively than those still waiting.  For local-disk workloads where all 100 footers arrive within 0.186 s this bias is negligible.  For high-latency object storage with staggered footer arrivals the bias could be noticeable.  The `rowgroups_per_file` cap limits the damage, but a future refinement could track per-file admission counts and skip a file that has already consumed its fair share within a single `_admit_rowgroups` call, re-queuing it at the back of `active_file_indices`.

---

### 3. Small-file whole-read path

**Problem.**  For files smaller than a configurable threshold, the two round trips (footer + columns) can be replaced by a single whole-file read.  The footer is parsed from the tail, and all column chunk buffers are sliced directly from the single in-memory buffer.

**Proposal.**  In `_read_footer_payload`, extend the speculative read to the full file when `file_size <= PARQUET_SMALL_FILE_THRESHOLD`.  Return the full buffer alongside the footer envelope.  In `_iter_row_groups_v2`, if the full buffer is present in the returned footer result, resolve column chunk byte ranges immediately (without a second `read_ranges` call) by slicing from the buffer.

```python
PARQUET_SMALL_FILE_THRESHOLD: int = int(get("PARQUET_SMALL_FILE_THRESHOLD", 4 * 1024 * 1024))  # 4 MB default
```

Callers of `fetch_footer` are unaffected.  The full buffer is stored alongside the footer cache entry; `fetch_columns` checks for it before calling `read_ranges`.

**Threshold calibration note.**  The hits dataset files average ~29 MB (2,876 MB ÷ 100 files), so the 4 MB default does not help this trace at all — the earlier claim of "~2.45 MB per file" was incorrect.  The 4 MB default is appropriate for workloads that produce many small partition files (e.g., high-cardinality hourly partitions).  For large analytical files the threshold would need to rise accordingly, but that increases peak memory proportionally.  Consider making the threshold configurable per-connector class rather than as a single global: local SSD connectors can afford a lower threshold; object-store connectors with high per-request overhead benefit from a higher one.

**Cache lifetime.**  The full file buffer is pinned in the cache entry until all row groups from that file have been processed, at which point it should be dropped.  Track a per-file "row groups remaining" counter; set `__file_bytes__ = None` in the cache entry when it reaches zero.  Without this, large whole-file buffers accumulate in memory for the lifetime of the cache.

---

### 4. Pre-populate prefetch ring during footer phase

**Problem.**  There is a strict happens-before dependency: footers must arrive before row groups can be admitted.  In practice footers arrive within 0.186 s for 100 files, so the gap is small.  However, for large datasets on high-latency object storage the gap can be significant.

**Proposal.**  When a file size is known in advance (from manifest or a prior scan), submit a speculative read for the first `PARQUET_SPECULATIVE_RG_BYTES` bytes of the file concurrently with the footer read.  If the footer reveals that the first row group starts within those bytes, the data is already available without waiting.

This is only worthwhile for connectors with RTT > ~5 ms (S3, GCS, Azure Blob).  Gate it behind a connector capability flag (`connector.supports_speculative_prefetch`).  Default: **off** for local-disk connectors to avoid reading wasted bytes on fast storage.

**Known-size dependency.**  The speculative read must be sized before the footer arrives, so the file size must be known in advance — either from a manifest, a catalog, or a prior `HEAD`/list operation.  If the size is unknown, no speculative read is issued.  The implementation must check `file_sizes.get(path)` before submitting the speculative future; if absent, fall back to the normal footer-then-columns path.  Do not perform an additional stat call just to enable speculation — that would cost more than it saves.

**Minimum read size.**  Enforce a minimum speculative read of 64 KiB (matching `_FOOTER_PREFETCH`) internally, regardless of what `PARQUET_SPECULATIVE_RG_BYTES` is set to.  A speculative read smaller than the footer prefetch buys nothing and risks not covering the first page header.

---

### 5. Footer-phase parallelism: keep read pool full during footer phase

**Problem.**  Footer reads are submitted all at once, but the read pool has 32 threads and `global_ranges_cap = 24`.  For 100 files, the footer phase will process 24, then 24, then 24, then 28 — saturating the pool.  Column reads can begin for files in the first batch as soon as those footers arrive without waiting for the remaining footer reads to complete.

This already happens in `_pipeline_rowgroups` / `_mark_footer_ready` — footers pipeline immediately on arrival.  However, `_dispatch_reads` will not be called until the scheduler's main loop resumes, which only happens when `_drain_completions(block=True)` returns.  If the blocking wait wakes on a footer completion (rather than a read completion), `_dispatch_reads` is called but may find only a small `pending_dispatch` queue because `_admit_rowgroups` hasn't had a chance to fill it.

**Proposal.**  Call `_admit_rowgroups()` and `_dispatch_reads()` inside the `_drain_completions` loop immediately after `_mark_footer_ready`, before returning to the outer scheduler loop.  This avoids the round-trip through the outer loop and eliminates a scheduling latency of one scheduler iteration per footer arrival.

```python
# Inside _drain_completions, after _mark_footer_ready(footer_path, meta):
_admit_rowgroups()
_dispatch_reads()
```

**Recursion / re-entrancy safety.**  `_admit_rowgroups` and `_dispatch_reads` only enqueue new futures; they do not call `_drain_completions` themselves and do not block.  Newly submitted futures will not complete before the current `_drain_completions` call returns, so there is no re-entrancy risk.

---

### 6. Decouple the scheduler from the downstream consumer (steady-flow)

**Problem — the fundamental one.**  The v1/v2 scheduler loops in `reader.py` are generator coroutines.  When they execute `yield row_group`, the entire loop is suspended until the caller calls `next()`.  During that suspension — while the downstream operator is filtering, aggregating, or otherwise processing the row group — **no reads are dispatched, no decode tasks are submitted, and no completions are drained**.  IO only advances during the brief window between `yield` returning and the next `yield`.

The downstream operator in the hits query is a CPU-heavy LIKE filter (`URL LIKE '%google%'`).  The 16.58 s post-decode gap in the trace is the direct result: the last download completed at t=11.79 s, the generator was suspended inside `yield` for most of the following seconds while the filter ran, so the IO pipeline ran dry well before the query finished.

This is also why §1–§5 above cannot alone prevent idle IO time in the v1/v2 paths: the scheduler must be running to dispatch new reads, and it cannot run while the downstream holds the generator.

**This problem is already solved in `opteryx/parquet_io/io_process_ring.py`.**  That module spawns a dedicated worker `Process` that runs the full IO loop independently — downloading, decoding, and writing serialised row groups into a shared-memory slot ring.  The consumer generator only calls `event_q.get()` to be notified of completed slots; when it is suspended at `yield`, the worker process keeps running and filling ring slots.  The ring slot count provides natural backpressure: the worker blocks waiting for a FREE slot only when the consumer is far behind.

The `io_process_rowgroup_ring` feature flag routes `iter_row_groups` through this path.  The flag is currently off by default.

**The remaining question is therefore not architecture but operability.**  The ring path carries serialisation cost: decoded `DrakenVector` columns must be pickled/serialised into the shared-memory frames by the worker and deserialised by the consumer.  Whether this overhead is smaller than the gap it eliminates depends on column count, column types, and downstream processing cost.

**Proposal for v2 path.**  As a lighter-weight alternative that avoids serialisation cost, apply the same structural fix to `_iter_row_groups_v2` by moving the scheduler loop into a `threading.Thread` with a bounded `queue.Queue`:

```
  ┌──────────────────────────────────────────────────┐
  │  Scheduler Thread  (thread, not process)         │
  │   _drain_completions / _admit / _dispatch_reads  │
  │   runs continuously — never suspended by caller  │
  │                                                  │
  │   ready_queue.put(decoded_row_group)             │
  └──────────────────────────────────────────────────┘
                         │  queue.Queue(maxsize=PARQUET_READY_ROWGROUP_QUEUE_CAP)
  ┌────────────────────── ▼──────────────────────────┐
  │  iter_row_groups generator                       │
  │    while True: yield ready_queue.get()           │
  └──────────────────────────────────────────────────┘
```

No serialisation is needed because threads share the process address space — decoded vectors pass through the queue as Python object references.

**Critical: the IO thread must never block on `queue.put()`.**  `queue.Queue.put()` blocks when the queue is full.  If the IO thread calls it directly, the entire scheduler — including `_drain_completions`, `_admit_rowgroups`, and `_dispatch_reads` — is frozen while the downstream consumer is slow.  That re-introduces exactly the problem §6 is meant to solve.

The fix is a two-step transfer: the scheduler thread maintains its own internal `_pending_output` deque and pushes completed row groups there first.  Each iteration of the scheduler loop (after `_drain_completions`) drains `_pending_output` into the consumer queue using `put_nowait`, stopping at the first `queue.Full`.  Items that did not transfer stay in `_pending_output` for the next iteration.  Admission control (`_admit_rowgroups`) is gated on `len(_pending_output) + output_queue.qsize() < output_cap` so in-flight work is bounded whether or not the consumer is keeping up.  This check must be explicit in the scheduler thread loop — not a side-effect of other caps — to prevent unbounded growth of `_pending_output` when the consumer is stuck.

```python
# Inside scheduler thread loop:
while _pending_output:
    try:
        output_queue.put_nowait(_pending_output[0])
        _pending_output.popleft()
    except queue.Full:
        break  # try again next iteration; do not block
```

**Thread ownership.**  The background thread owns all scheduler state exclusively: `active_rowgroups`, `read_futures`, `decode_futures`, `raw_ring`, `_pending_output`, and all `_FileState`/`_RowGroupState` objects.  The main generator thread touches only `output_queue` (via `get`) and the stop `Event`.  No locking is required beyond what `queue.Queue` itself provides.

**Error propagation.**  The scheduler thread wraps its main loop in a `try/except BaseException`.  On any uncaught exception, it places a sentinel `(type=_SENTINEL_ERROR, exc=exc)` onto the output queue and exits.  The generator's `get` loop detects the sentinel and re-raises.

**Cancellation.**  A `threading.Event` (`_stop_event`) is checked at the top of each scheduler loop iteration.  When the generator is closed or garbage-collected, it sets `_stop_event` and calls `thread.join(timeout=2)`.  The thread also calls `future.cancel()` on all in-flight read and decode futures before exiting, consistent with the existing `finally` block in `_iter_row_groups_v2`.

**Shutdown drain.**  When `_stop_event` is set, the scheduler thread should **not** attempt to drain `_pending_output` into the consumer queue before exiting.  If the consumer is gone (generator closed or broken out of), there is no receiver; attempting to drain can block or silently discard anyway.  Drop `_pending_output` and cancel all futures immediately.

**Rollout.**  The background-thread scheduler should be opt-in for at least one release cycle via a feature flag (`FEATURE_PARQUET_THREAD_SCHEDULER`, default `0`), mirroring the existing `FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2` flag.  Switch the default to `1` in a subsequent release after soak time and slow-consumer stress testing.

**Relationship to `io_process_ring`.**  The thread approach and the process ring are complementary.  Thread approach: lower overhead, no serialisation, cannot exploit a separate GIL-free CPU core for IO.  Process approach: true process isolation, avoids CPython GIL contention for decode-heavy workloads, higher transfer cost.  Both solve the `yield`-suspension problem.  The thread approach is the right default for `_iter_row_groups_v2`; the process ring remains preferable for high-latency remote object storage where IO is truly bound by network and not the GIL.

---

## Configuration Summary

| Key | Default | Description |
|-----|---------|-------------|
| `PARQUET_RAW_RING_CAP` | `PARQUET_GLOBAL_RANGE_READERS * 2` | Max undecoded row groups in the raw download ring before reads are throttled |
| `PARQUET_SMALL_FILE_THRESHOLD` | `4 * 1024 * 1024` (4 MB) | Files at or below this size are read whole; footer+columns in one call. Default chosen for small-partition workloads; tune upward for large analytical files. |
| `PARQUET_SPECULATIVE_RG_BYTES` | `0` (off) | Bytes to speculatively prefetch per file during footer phase (network connectors only; requires known file size) |

**`PARQUET_READ_DECODE_BUFFER_CAP` — semantic change.**  This existing key currently acts as a combined gate on both in-flight reads and pending decodes.  Under §1 its role narrows to a hard cap on `raw_ring` size — a ceiling before OOM rather than an operational throttle.  Existing deployments that raised this value to permit more decode concurrency will not be broken (the value still bounds ring depth) but the meaning has shifted.  The key will be documented as deprecated in favour of `PARQUET_RAW_RING_CAP`; both are honoured during a transition period, with `RAW_RING_CAP` taking precedence if set.  Emit a deprecation warning at startup if `PARQUET_READ_DECODE_BUFFER_CAP` is set in the environment and `PARQUET_RAW_RING_CAP` is not, directing users to migrate.

---

## Expected Impact

| Change | Estimated impact |
|--------|------------------|
| **§6 — thread scheduler for v2** | **Eliminates suspend-on-yield in v1/v2 paths; equivalent to what `io_process_ring` already provides** |
| §1 — raw download ring (`RAW_RING_CAP`) | Eliminates most 28.7 % ≤1-concurrent windows during decode saturation |
| §2 — admission fix | Removes round-robin stalls from footer-pending files |
| §3 — small-file whole-read | Eliminates column range reads for files ≤ threshold; no impact on the hits dataset (29 MB avg) at the 4 MB default |
| §5 — inline admit on footer | Reduces first-row-group latency per file by one scheduler iteration |

§1–§5 are improvements to the v1/v2 scheduler body and apply whether it runs as a coroutine (current) or background thread (§6).  Without §6, they improve IO throughput *between* yields but cannot prevent the pipeline draining while the downstream is processing.  The `io_process_ring` path already provides this decoupling via process isolation; §6 brings the same property to `_iter_row_groups_v2` at lower cost (no serialisation).

---

## Implementation Order (suggested)

1. **§5** — inline admit/dispatch immediately after `_mark_footer_ready`.  One-line change; confirmed safe from re-entrancy.
2. **§2** — remove `admitted_this_cycle` early exit.  Small change, directly addresses the 28.7 % low-concurrency windows.
3. **§1** — raw download ring (`RAW_RING_CAP`): decouple `decode_pending` depth from read-dispatch gate.  Rename `decode_pending` → `raw_ring` internally.  Deprecate `PARQUET_READ_DECODE_BUFFER_CAP` in favour of `PARQUET_RAW_RING_CAP`.
4. **§6** — background scheduler thread for `_iter_row_groups_v2`.  Structural change; implement after §1–§2 and §5 are validated.  Key requirements: IO thread uses `put_nowait` + `_pending_output` deque — never blocks on `output_queue.put()`; admission gate includes `len(_pending_output) + output_queue.qsize() < output_cap`; shutdown drops `_pending_output` without draining.  Gate behind `FEATURE_PARQUET_THREAD_SCHEDULER` (default off) for the first release; promote to default after slow-consumer stress testing passes.  Enable `io_process_ring` as the default for remote connectors once §6 is stable.
5. **§3** — small-file whole-read.  Useful for small-partition workloads; calibrate threshold per-connector.  Add row-group-remaining counter to drop the whole-file buffer once all chunks are consumed.
6. **§4** — speculative prefetch.  Only issue when `file_size` is already known; do not add a stat call.
