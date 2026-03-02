# Parquet Continuous Feed Scheduler V3 Design

## Status

Draft for design review.

---

## Objective

Eliminate staircase/batch IO behavior and maintain a steady feed of decoded row groups into execution.

Primary optimization targets:

- sustained row groups per second (low variance)
- minimal idle gaps in read dispatch
- low time to first row group

Not just total query wall time.

---

## Problem Statement

Observed waterfall traces show repeated cohorts of starts followed by large idle gaps.

This indicates true scheduling burstiness, not only visualization artifacts.

The engine is not continuously refilling IO as work completes; work is admitted and consumed in batches.

---

## Root Causes (Current Model)

1. Pull-coupled scheduling loop
- Progress in scheduling is tied to generator iteration and row-group emission.
- When downstream is busy, scheduler refill cadence degrades.

2. Coarse work unit
- Current unit is often "entire row group read + decode".
- Coarse tasks create long critical sections and synchronized completions.

3. Single mixed resource queue
- Read and decode share the same practical work stream.
- Slow decode can reduce effective read refill rate.

4. Admission by count only, but without the right count limits
- Existing limits do not directly guard the number of partially completed row groups.

5. Footer/read/decode phases not modeled as independent pipelines
- Footer floods and row-group bursts can phase-lock.

---

## Design Goals

1. Continuous refill
- Whenever any read slot is freed, new read work is eligible immediately.

2. Decoupled scheduling loop
- Scheduling progress should not depend on downstream pulling the next row group.

3. Slot-based control model
- Use slot/count limits, not byte-budget limits, for the first implementation.

4. Queue-order driven behavior
- Keep a deterministic queue order so work stays grouped by row group (Usenet-style model).

5. Explicit cap on partially in-flight row groups
- Bound row groups in progress (for example 3-5) to avoid memory pressure from skewed column runtimes.

6. Deterministic cancellation
- Fast shutdown for LIMIT/early-stop with bounded tail work.

7. Optional later optimization
- Sub-row-group `ReadTask` granularity is a later phase if traces still justify it.

---

## Non-Goals

- Replacing Parquet decode implementation.
- Changing SQL semantics or row-group correctness rules.
- Solving all object-store variance in one iteration.
- Introducing byte-based admission budgets in phase 1.

---

## Architecture

### 1) Pipeline Stages

- `FooterPlanner`
  - Fetch + parse footers.
  - Produces row-group plans.

- `ReadScheduler`
  - Admits row groups in queue order.
  - Maintains global read slots and row-group in-flight limits.

- `Decode`
  - Decodes completed reads and assembles row groups.
  - Can remain in current worker path initially; dedicated decode queue is optional phase 2.

- `Emitter`
  - Delivers completed row groups to downstream.
  - Honors downstream backpressure credits.

### 2) Dedicated Queues (Bounded By Count)

- `planned_rowgroups` (metadata only)
- `pending_reads`
- `ready_to_decode`
- `emit_ready_rowgroups`

All queues are bounded by count, with a hard cap on active row groups.

### 3) Work Unit

Phase 1 work unit remains row-group oriented.

- Scheduler unit: `(file, rg_idx)`
- Read dispatch within the row group follows deterministic queue order.

Optional later phase:

- Introduce finer `ReadTask` units only if phase 1 still shows staircase behavior.

---

## Scheduling Policy

### Admission Limits (Phase 1)

Use explicit slot/count limits:

- `MAX_READ_SLOTS_IN_FLIGHT`
- `MAX_FILES_IN_FLIGHT`
- `MAX_ROWGROUPS_IN_FLIGHT` (new, primary guardrail; target 3-5)
- `MAX_ROWGROUPS_PER_FILE_IN_FLIGHT`
- `MAX_READY_ROWGROUPS`

A read is dispatchable only if all relevant slot limits allow it.

### Queue-Order Policy (Usenet-Style)

- Maintain a queue of admitted row groups.
- Dispatch reads from the queue head first.
- Keep each row group moving toward completion before spreading to many new row groups.
- Rotate fairly only when head row group is blocked or has reached per-row-group slot cap.

This keeps related work together and avoids wide fanout across too many partial row groups.

### Completion Bias

When multiple row groups are dispatchable:

1. Prefer row groups already in progress.
2. Prefer the earliest admitted row group.
3. Admit new row groups only when active row-group limit allows.

### Fairness Guardrails

- At least one active row group per active file if work exists.
- Hard cap of simultaneously active row groups per file.

---

## Backpressure Model

Two control signals:

1. Downstream consumption credits
- `emit_ready_rowgroups` cannot grow beyond `MAX_READY_ROWGROUPS`.

2. Row-group in-flight cap
- `MAX_ROWGROUPS_IN_FLIGHT` prevents accumulation of many partially complete row groups.

This controls memory risk without introducing byte-based admission complexity in phase 1.

---

## Control Loop (High-Level)

A dedicated scheduler loop runs independently from row-group emission.

1. Poll completions (footer/read/decode).
2. Update file and row-group state.
3. Recompute dispatchable reads under slot limits.
4. Dispatch up to available read slots.
5. Push completed row groups to `emit_ready_rowgroups`.
6. Yield from `emit_ready_rowgroups` when downstream pulls.

Key property: step 1-4 continue even if step 6 is temporarily slow.

---

## State Model

### FileState

- `footer_ready`
- `rowgroups_total`
- `rowgroups_admitted`
- `rowgroups_completed`

### RowGroupState

- `reads_total`
- `reads_done`
- `decode_done`
- `status` (`planned|reading|decoding|ready|emitted|cancelled`)
- timestamps for latency accounting

### TaskState

- `queued_at`, `dispatched_at`, `completed_at`
- `request_count`
- `attempt`, `error`

---

## Telemetry Requirements

Existing telemetry is necessary but not sufficient.

Add scheduler-centric metrics:

- `scheduler_loop_iterations`
- `scheduler_idle_wait_ns`
- `read_slot_utilization_pct`
- `read_dispatch_gap_ms` (p50/p95/p99)
- `pending_reads_depth`
- `emit_ready_depth`
- `rowgroups_in_flight_peak`
- `rowgroup_time_planned_to_first_read_ns`
- `rowgroup_time_first_read_to_ready_ns`

Waterfall support:

- distinguish `footer`, `rowgroup_read`, `rowgroup_decode`, `rowgroup_ready`, `rowgroup_emit`
- include `(file, rg_idx)` tags consistently

---

## Acceptance Criteria

For representative multi-file scans (local + object storage):

1. `read_dispatch_gap_ms_p95` significantly reduced vs current baseline
2. `read_slot_utilization_pct >= 85%` during steady-state phase
3. coefficient of variation for rowgroups/s reduced by >= 40% vs current
4. no regression in correctness tests
5. no material memory regression under same workload profile
6. no trace evidence of wide fanout beyond configured `MAX_ROWGROUPS_IN_FLIGHT`

---

## Rollout Plan

### Phase 0: Instrumentation First

- Add missing queue/gap metrics to current scheduler.
- Validate bottleneck signatures on current traces.

### Phase 1: Decouple Loop + Slot Controls

- Introduce dedicated scheduler loop + bounded emit queue.
- Add `MAX_ROWGROUPS_IN_FLIGHT`.
- Keep row-group-oriented work units.

### Phase 2: Queue-Order Refinement

- Implement/adjust deterministic head-first queue policy.
- Tune fairness rotation and per-file caps.

### Phase 3: Optional Granularity Upgrade

- Introduce finer `ReadTask` units only if phase 2 still shows staircase behavior.

### Phase 4: Default Switch

- Enable via feature flag default-on after burn-in.
- Keep fallback path for one release cycle.

---

## Testing Strategy

### Unit Tests

- Slot invariants (read slots, file slots, row-group slots)
- No starvation across files
- Deterministic cancellation behavior
- Queue bound enforcement

### Simulation Tests

- Inject variable read/decode latencies
- Verify no large dispatch gaps under synthetic slow downstream pull
- Verify row-group in-flight cap never exceeded

### Integration Tests

- Current correctness parity with v2
- LIMIT/early-close cancellation tail work limits
- Trace-based assertions on gap metrics and row-group fanout

### Performance Benchmarks

- cold and warm cache
- local filesystem and object storage
- narrow projection and wide projection
- small and large row groups

---

## Risks And Mitigations

1. Complexity growth
- Mitigation: explicit state machines + strict telemetry contracts.

2. Memory blow-up from skewed columns
- Mitigation: strict `MAX_ROWGROUPS_IN_FLIGHT` and `MAX_READY_ROWGROUPS`.

3. Queue-order over-bias (reduced fairness)
- Mitigation: per-file caps and bounded fairness rotation.

4. Optional phase creep
- Mitigation: phase gates require telemetry-based evidence.

---

## Open Questions

1. Default `MAX_ROWGROUPS_IN_FLIGHT`: 3 or 5?
2. Should decode stay in current worker path for phase 1, or be split in phase 1.5?
3. Should footer planning remain eager for all files, or be windowed by active-file budget?
4. What exact trace threshold defines "staircase still present" for triggering phase 3?

---

## Decision Summary

Implement a decoupled, slot-limited, queue-order scheduler first.

Do not add byte-budget admission or sub-row-group tasking in phase 1.

Only add finer-grained read tasks if post-phase-2 traces still show staircase behavior.
