# Parquet Local Serial Fast-Path Design

**Last updated:** 2026-03-08  
**Status:** Draft for review  
**Scope:** Local Parquet reads only

---

## Objective

Add a feature-flagged fast path for local storage that avoids the current
worker-heavy Parquet scan pipeline.

For local files, the design target is:

- lower time to first morsel
- lower CPU overhead in the scan layer
- no IPC or shared-memory transport
- no worker-pool scheduling overhead
- same correctness and projection behavior as the existing Parquet path

This fast path is intentionally simple:

- process one row group at a time
- process one column chunk at a time
- only read projected columns
- keep predicate-based row-group pruning

---

## Problem

The current Parquet scan stack is optimized around remote and concurrent I/O:

1. footer planning
2. row-group admission
3. worker dispatch
4. range reads
5. decode
6. optional process-ring transfer
7. morsel emission

That architecture is appropriate for object storage, where latency hiding and
decoupling from the consumer matter more than scheduler overhead.

It is a poor fit for local storage.

For local files:

- read latency is already low
- there is no network round-trip to hide
- shared-memory ring transport adds copy/coordination cost without benefit
- process startup, queues, and IPC dominate small and medium scans
- thread-pool scheduling overhead is often larger than the actual file read

The result is that local storage is substantially slower than it should be.

---

## Non-Goals

This design does not attempt to:

- replace the remote/object-storage path
- remove the existing v2 scheduler
- change Parquet decoding semantics
- introduce broad local-file prefetching
- maximize total parallel local throughput across many files

This is a latency and overhead reduction path, not a new global scheduler.

---

## Why Serial Is Correct For Local

For local storage, the core requirement is not maximum concurrency. The main
requirement is to avoid doing unnecessary work.

The fast path stays serial on purpose:

1. It still reads only row groups that survive pruning.
2. It still reads only projected columns.
3. It still reads one column chunk at a time, so wide files do not become
   whole-row-group reads.
4. It avoids fanout into `files x rowgroups x columns` tasks.
5. It avoids queueing work that local disk can serve faster directly.

This is the simplest path with the fewest moving parts for local reads.

---

## Proposed Routing Rule

Use an explicit reader-selection setting rather than an opt-in flag.

- `FEATURE_USE_SERIAL_READER`

Recommended values:

- `LOCAL` (default)
- `NONE`
- `ALL`
- comma-separated connectors, for example `LOCAL,S3,GS`

This makes the optimized local path the default behavior instead of something
that must be turned on manually.

Reader selection order:

1. If the connector matches `FEATURE_USE_SERIAL_READER`:
   use serial fast path.
2. Else if `FEATURE_IO_PROCESS_ROWGROUP_RING=1`:
   use process-ring path.
3. Else if `FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2=1`:
   use v2 scheduler.
4. Else:
   use v1 scheduler.

These are not different dataset categories. They are different Parquet scan
implementations within the storage-backed read path.

Current Parquet scan implementations:

- `v1`: older in-process scheduler
- `v2`: newer in-process scheduler with row-group-focused admission
- `io_process_rowgroup_ring`: dedicated IO-process/shared-memory-ring path

`FEATURE_IO_PROCESS_ROWGROUP_RING` gates the Parquet-specific process-ring
transport. It is not a separate top-level storage model; it is an alternate
implementation of Parquet row-group production for storage-backed scans.

Local storage detection should be based on either:

- `connector == "LOCAL"`
- `filesystem` is `OpteryxLocalFileSystem`

This keeps the override narrow and explicit.

Preferred initial configuration:

- `FEATURE_USE_SERIAL_READER=LOCAL`

Useful overrides for testing:

- `FEATURE_USE_SERIAL_READER=NONE`
- `FEATURE_USE_SERIAL_READER=ALL`
- `FEATURE_USE_SERIAL_READER=LOCAL,S3,GS`

### Recommended Deployment Profiles

#### Local Deployments

Recommended defaults for local development, notebooks, and single-node local
execution:

- `FEATURE_USE_SERIAL_READER=LOCAL`
- `FEATURE_IO_PROCESS_ROWGROUP_RING=0`
- `FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2=1`

Recommended interpretation:

- local Parquet uses the serial reader by default
- remote-style process transport is disabled
- v2 remains the fallback for non-local storage

Why:

- local disk does not benefit enough from worker fan-out to justify IPC
- the process-ring path adds overhead without hiding any network latency
- the serial reader should be the normal local path, not a test mode

#### Cloud Deployments

Recommended starting point for cloud/object-storage-heavy deployments:

- `FEATURE_USE_SERIAL_READER=LOCAL`
- `FEATURE_IO_PROCESS_ROWGROUP_RING=1`
- `FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2=1`

Recommended interpretation:

- local scratch disks still use the serial reader
- object storage can use the process-ring path
- v2 remains available as the in-process fallback path

Why:

- cloud deployments are where decoupled IO and worker overlap are most useful
- remote latency is the case the process-ring path is trying to optimize
- local and remote should not share the same default reader strategy

#### What Could Be Removed

If we want a cleaner long-term configuration surface, the current design points
to a likely simplification:

- for local-focused deployments, `FEATURE_IO_PROCESS_ROWGROUP_RING` is not a
  useful knob and could be ignored or retired there
- if v2 fully supersedes v1, then `v1` should be retired rather than remaining
  as a permanent third scheduler choice
- if the process-ring path consistently wins for cloud object storage and loses
  for local, the system could eventually reduce to two meaningful Parquet
  reader modes:
  - serial local reader
  - remote fan-out reader

That simplification should be driven by benchmark and operational evidence, not
assumed in this change.

---

## Proposed Execution Model

### Stage 1: Footer Resolution

For each unique file path:

- reuse prefetched footer when available
- else reuse cached footer when available
- else fetch footer directly on the caller thread
- parse footer on the caller thread

This stage remains per-file and low cost.

### Stage 2: Row-Group Walk

For each file in scan order:

- walk row groups in file order
- apply existing `row_group_may_satisfy(...)` pruning
- skip pruned row groups immediately

No row-group admission queue is needed.

### Stage 3: Column Walk

For each surviving row group:

- iterate projected columns in projection order
- resolve `(offset, length)` for the column chunk
- issue a single `filesystem.read_ranges(path, [(offset, length)])`
- decode immediately on the caller thread
- place result into the row-group assembly dict

No thread pool and no decode pool are used.

### Stage 4: Emit

When all projected columns for a row group are decoded:

- attach the same metadata fields expected by `ParquetReadNode`
- emit the row group immediately

The consumer sees the same logical unit of work as in the existing path.

---

## Why Column-By-Column Instead Of Batched `read_ranges`

The requested shape is:

- row-group by row-group
- column by column
- limit reads

That implies no row-group-wide vectored read.

Reasons to preserve that shape:

1. It avoids speculative over-read when the consumer stops early.
2. It keeps memory footprint small for wide row groups.
3. It keeps local behavior easy to reason about.
4. It avoids rebuilding a mini-scheduler inside the fast path.

This is a deliberate tradeoff:

- less throughput than an aggressively batched local reader in some cases
- much lower coordination overhead
- better early-stop behavior
- simpler and safer rollout

The initial implementation should include one simple combine-read heuristic:

- if projected bytes are already >= 50% of row-group bytes, combine the row
  group into a single read

Rationale:

- for local storage, reducing syscall and dispatch overhead matters more than
  minimizing over-read
- when most of the row-group bytes are already needed, many tiny column reads
  are the wrong tradeoff

Below that threshold, the reader remains strictly column-by-column.

---

## Telemetry Contract

The fast path should preserve the telemetry keys already consumed by
`ParquetReadNode` and query stats, even when many values collapse to serial
constants.

It should also explicitly report which Parquet reader strategy was chosen, for
example:

- `parquet_scan_strategy = local_serial`

Required fields per emitted row group:

- `__path__`
- `__row_group__`
- `__bytes_fetched__`
- `__footer_bytes__`
- `__footer_fetch_ns__`
- `__range_request_count__`
- `__range_bytes_requested__`
- `__time_read_ranges_ns__`
- `__time_decode_columns_ns__`
- `__cache_column_hits__`
- `__cache_column_misses__`
- `__task_queue_wait_ns__`
- `__task_total_ns__`
- `__scheduler_wait_ns__`
- `__rowgroup_completion_latency_ns__`
- `__rowgroup_peak_in_flight__`
- `__ranges_in_flight_peak__`
- `__active_files_peak__`
- `__active_rowgroups_peak__`
- `__rowgroups_in_flight_cap__`
- `__emit_wait_ns__`
- `__emit_queue_depth_at_ready__`
- `__scheduler_empty_wait_ns__`
- `__scheduler_empty_wait_events__`
- `__time_to_first_rowgroup_ns__`
- `__row_groups_pruned__`

For the local serial path, these should usually collapse to:

- active files peak = `1`
- active row groups peak = `1`
- rowgroups in flight cap = `1`
- scheduler wait = `0`
- task queue wait = `0`
- emit wait = `0`

That lets downstream telemetry stay stable while making the behavior obvious.

---

## Correctness Requirements

The local fast path must preserve:

1. Footer caching behavior.
2. Prefetched footer reuse.
3. Predicate-based row-group pruning.
4. Projection pruning.
5. Column decode behavior.
6. Output row-group schema and column order.
7. Early-stop behavior from the consumer.

It must not:

- read non-projected columns
- bypass row-group pruning
- change emitted row-group contents

---

## Expected Outcome

For local storage, expected wins are:

- much lower fixed scan overhead
- lower time to first row group
- lower CPU spent in scheduling/transport
- better performance on small and medium local scans
- better early-stop behavior for `LIMIT` and selective queries

Expected losses:

- less overlap across files/row groups than v2
- lower peak throughput on workloads that genuinely benefit from local parallelism

This is acceptable because the fast path is the default only for the connector
classes where that tradeoff is sensible, starting with `LOCAL`.

---

## Rollout Plan

1. Add `FEATURE_USE_SERIAL_READER`, default `LOCAL`.
2. Route matching connectors to the serial path before the process-ring path.
3. Keep remote storage on existing schedulers unchanged.
4. Add unit tests for:
   - route selection precedence
   - serial one-column-at-a-time reads
   - combine-read threshold behavior
   - footer reuse
   - row-group pruning parity
   - connector-selection parsing
5. Add benchmark coverage comparing:
   - local serial fast path
   - v2 scheduler
   - process-ring path
6. Add explicit telemetry indicating which reader strategy was selected.
7. Keep `LOCAL` as the default unless benchmark data shows a clear regression.

---

## Benchmark Plan

Benchmark local datasets with:

- single file, few columns
- single file, many columns
- many small files
- few large files
- early-stop `LIMIT`
- selective predicate with row-group pruning
- full scan with wide projection

Metrics:

- first row group latency
- total elapsed time
- rows per second
- row groups per second
- CPU time in scan stage
- total bytes read

The main decision metric is:

> Does local serial reduce overhead enough to beat the existing local path on
> realistic developer and notebook workloads?

---

## Open Questions

1. For cloud deployments, should the recommended remote path be the
   process-ring reader immediately, or should cloud also prefer v2 until a
   benchmark threshold is met?

---

## Decision Summary

For Parquet reads, the system should support a dedicated serial fast path
selected by connector, with `LOCAL` as the default target.

That path should:

- bypass workers and IPC entirely
- keep row-group pruning
- keep projection pruning
- read one row group at a time
- read one column chunk at a time
- combine a row group into a single read when projected bytes are already
  >= 50% of row-group bytes
- emit the same row-group contract as the existing path
- expose the selected reader strategy in telemetry

This is the simplest high-confidence way to remove the largest sources of local
scan overhead without destabilizing the remote-storage architecture.
