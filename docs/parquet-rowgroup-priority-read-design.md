# Parquet Row-Group Priority Read Design

## Objective

Prioritize fast completion of full row groups so execution can be fed with
ready morsels as early and as steadily as possible.

The optimization target is:

- time to first morsel
- sustained morsels per second

Not total scan completion time alone.

---

## Why This Design

In the split Parquet read path, a row group is the minimum useful execution
unit. Reading fewer bytes is not enough if I/O is fragmented into many
high-latency requests that delay completion of full row groups.

This design makes the scheduler row-group completion-first.

---

## Scheduling Principles

1. Keep a small active file window.
2. Keep a bounded number of in-flight row groups per active file.
3. Prioritize finishing already-started row groups before admitting new ones.
4. Bound parallel column range reads to avoid request explosion.
5. Enforce a global request cap across all files/row groups.
6. Yield row groups immediately on completion.

---

## Concurrency Model

Use a hierarchical set of caps:

- `files_in_flight`: maximum active files.
- `rowgroups_per_file_in_flight`: maximum active row groups per active file.
- `ranges_per_rowgroup_in_flight`: maximum in-flight range reads per row group.
- `global_ranges_in_flight`: hard cap across the whole query.

Suggested starting values for larger files (for example ~512 MB):

- `files_in_flight = 2`
- `rowgroups_per_file_in_flight = 2`
- `ranges_per_rowgroup_in_flight = 10`
- `global_ranges_in_flight = 24`

Baseline startup allocation at `t=0`:

- first two row groups receive `10 + 10` readers
- the next queued row group receives the remaining `4` readers

This creates a stable "2 almost-completing + 1 warming" pattern that favors
consistent morsel throughput.

---

## Execution Pipeline

### Stage 1: Footer Planning

- Fetch footer bytes in parallel across files.
- Parse footer metadata safely in the caller thread.
- Build per-file row group work queues.

### Stage 2: Row Group Assembly

For each admitted row group:

- Plan projected column chunk ranges.
- Fetch ranges with bounded parallelism.
- Decode projected columns.
- Assemble complete row group payload.

### Stage 3: Emit

- Convert assembled row group into a morsel.
- Yield immediately to downstream operators.
- Release scheduler credits.

---

## Admission And Priority Policy

Row-group completion-first policy:

1. Admit files until `files_in_flight` is reached.
2. For each active file, admit row groups until
   `rowgroups_per_file_in_flight` is reached.
3. For admitted row groups, grant range read credits up to
   `ranges_per_rowgroup_in_flight`, constrained by `global_ranges_in_flight`.
4. If credits are limited, favor row groups already in progress over new ones.
5. When a row group completes, emit it and then admit the next queued row group.
6. Burn through files and row groups in order, but with bounded overlap from
   the active windows above.

This avoids a broad fanout pattern like:

`files * rowgroups * columns`

which can overwhelm object storage and increase latency.

---

## Backpressure And Cancellation

On limit reached or early stop:

- stop admitting new files/row groups
- cancel pending non-started range requests
- drain essential completions only
- exit quickly after current critical section

This keeps the system focused on useful units of work.

---

## Telemetry Requirements

The scheduler should be evaluated with rowgroup-focused telemetry:

- `time_to_first_rowgroup_ns`
- `rowgroups_completed`
- `rowgroups_completed_per_s`
- `rowgroup_completion_latency_ns` (p50/p95)
- `ranges_in_flight_peak`
- `scheduler_credit_wait_ns`
- `files_active_peak`
- `rowgroups_active_peak`

Complement with existing metrics already in place:

- `time_parquet_read_ranges_ns`
- `time_parquet_task_queue_wait_ns`
- `time_parquet_decode_columns_ns`
- `time_parquet_footer_fetch_ns`
- `parquet_range_request_count`

---

## Expected Outcome

With bounded hierarchical concurrency and completion-first priority:

- lower time to first morsel
- steadier flow of complete morsels
- reduced latency spikes from request trample
- more predictable behavior as file size grows

---

## Rollout Plan

1. Introduce scheduler caps and defaults behind config flags.
2. Implement admission/credit logic in `iter_row_groups`.
3. Add rowgroup-priority queueing policy.
4. Add cancellation behavior for early termination.
5. Benchmark against current path using first-morsel and morsel-rate metrics.

---

## Implementation TODO

1. [x] Add config knobs in `opteryx/config.py`:
   `PARQUET_FILES_IN_FLIGHT=2`, `PARQUET_ROWGROUPS_PER_FILE_IN_FLIGHT=2`,
   `PARQUET_GLOBAL_RANGE_READERS=24`, `PARQUET_RANGE_READERS_PER_ROWGROUP=10`.
2. [x] Add feature flag `FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2` and keep current scheduler as fallback.
3. [x] Refactor `iter_row_groups` in `opteryx/parquet_io/reader.py` into explicit stages:
   footer planning, admission, range dispatch, completion emit.
4. [x] Implement `FileState` and `RowGroupState` structs/classes for scheduler state.
5. [x] Implement bounded file window admission (`files_in_flight <= 2`) in file order.
6. [x] Implement bounded rowgroup admission per file (`rowgroups_per_file_in_flight <= 2`) in rowgroup order.
7. [x] Implement global range credit cap (`global_in_flight_ranges <= 24`) with semaphore/counter enforcement.
8. [x] Implement per-rowgroup range credit cap (`ranges_per_rowgroup_in_flight <= 10`) with enforcement.
9. [x] Implement startup allocation target (`10 + 10 + 4`) across first three admitted rowgroups.
10. [x] Dispatch range work at scheduler layer as single-range tasks so global caps are authoritative.
11. [x] Prioritize reads for in-progress rowgroups before admitting/feeding new rowgroups.
12. [x] Within a rowgroup, sort pending column reads by descending remaining bytes.
13. [x] Emit rowgroup immediately when complete; do not wait for submission order.
14. [x] Add early termination behavior: stop admission, cancel not-started futures, fast-drain completions.
15. [x] Add scheduler telemetry:
    `time_to_first_rowgroup_ns`, `ranges_in_flight_peak`, `active_files_peak`,
    `active_rowgroups_peak`, `scheduler_wait_ns`, `rowgroups_completed_per_s`.
16. [x] Keep existing parquet telemetry and ensure new telemetry appears in `ReadRel` operation stats.
17. [x] Add unit tests for cap invariants (2/2/24/10), startup distribution, and completion-first behavior.
18. [x] Add integration tests for correctness parity vs current path and LIMIT/early-stop cancellation.
19. [x] Add benchmark script/profile for first-morsel latency and morsel throughput under mixed file sizes.
20. [x] Run A/B benchmark with `FEATURE_PARQUET_ROWGROUP_SCHEDULER_V2` on/off and record results in docs.

## A/B Benchmark (2026-02-27)

Benchmark script: `tests/performance/benchmarks/bench_parquet_rowgroup_scheduler.py`  
Dataset sample: 8 local parquet files, 8 projected columns.

- v1:
  - rowgroups: 349
  - first morsel: 249.18 ms
  - elapsed: 460.99 ms
  - rowgroups/s: 757.07
- v2:
  - rowgroups: 349
  - first morsel: 22.55 ms
  - elapsed: 466.34 ms
  - rowgroups/s: 748.38

Observed behavior:

- Major reduction in time-to-first-morsel.
- Similar total throughput/scan completion time in this profile.

---

## Decision Summary

Use a row-group completion-first scheduler with bounded hierarchical
parallelism, rather than unconstrained fanout or strict one-file-at-a-time
execution.

---

## Alternative Reviewed: Column-Worker Pool (Usenet-style)

### Proposal Summary

- Use `N` workers reading column chunks.
- Prioritize larger columns first.
- Workers take the next chunk from whichever row group has pending work.
- Emit any row group immediately when all projected columns are complete.

### What Is Strong About This

- Naturally caps request fanout to `N`.
- Good utilization under variable object-store latency.
- Largest-first can reduce long-tail straggler effects for row-group completion.
- Simple operational mental model.

### Scalability Nuance

Strict single-file processing is usually not the most scalable policy for
remote object storage:

- one cold or throttled file can head-of-line block the entire pipeline
- no cross-file latency hiding
- weaker throughput when each file has few row groups

Strict single-rowgroup processing is also suboptimal:

- can underutilize workers if one row group has low projected-column count
- can inflate time to first morsel if one large column dominates

### Revised Recommendation (Hybrid)

Keep the column-worker pool idea, but apply it over a bounded multi-file
window:

- `file_window = 2` (or small bounded value)
- row groups from active files share a global worker queue
- work item = `(file, row_group, column_chunk)`
- per-rowgroup in-flight cap to avoid over-fragmenting completion
- global in-flight cap for all reads

This preserves your model while avoiding single-file bottlenecks.

### Priority Rule

Use completion-first with size-aware tie-break:

1. Prefer row groups already in progress.
2. Among them, prioritize row groups with least remaining bytes to completion.
3. Within a row group, read largest remaining column first.

This yields fast morsel completion while still preventing stragglers.

### Practical Initial Settings

- `range_workers = 24`
- `file_window = 2`
- `rowgroups_per_file_in_flight = 2`
- `ranges_per_rowgroup_in_flight = 10`
- `global_in_flight_ranges = 24`

Expected first-wave distribution:

- rowgroup A: 10 reads
- rowgroup B: 10 reads
- rowgroup C: 4 reads

These are safer than unconstrained fanout and more scalable than strict
single-file or strict single-rowgroup modes.
