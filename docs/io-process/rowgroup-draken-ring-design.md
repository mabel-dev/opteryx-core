# Opteryx IO Process V2: RowGroup Draken Ring Design

## 1. Overview

This design isolates the scan IO pipeline into a separate process and
transfers data to EXEC through a fixed-size shared-memory ring.

Unlike the prior blob-transfer design, the transfer unit is a Parquet row group
encoded in Draken serialization format.

If a row group payload does not fit in one slot, it is fragmented across
multiple slots. If fragmentation is excessive, the row group is sliced into
multiple row-group slices before transfer.

Target deployment:

- one query per container
- Cloud Run
- fixed memory budget

---

## 2. Goals

- keep IO and network variance off the EXEC critical path
- remove large-payload Python IPC copies
- transfer row-group-ready payloads, not raw blobs
- enforce strict bounded backpressure with slot accounting
- expose explicit producer/consumer stall metrics

---

## 3. Non-Goals

- multi-query arbitration in one container
- variable-size shared memory allocator
- distributed shuffle protocol
- changing Draken serialization format itself

---

## 4. Delta From Existing IO-Process Design

Existing design (`docs/io-process/design.md`) transfers raw blobs and decodes in EXEC.

This design changes to:

- slot size: `32 MiB` (was `64 MiB`)
- transfer unit: row-group Draken payload (was full blob bytes)
- decode location: IO process (Parquet decode) then Draken serialize
- EXEC side: Draken deserialize and execute

---

## 5. Process Model

Two processes per query:

- `EXEC`
  - plan query
  - send scan work to IO process
  - consume row-group payload frames from shared memory
  - Draken-deserialize to Morsel and run operators

- `IO`
  - fetch footers and column ranges
  - decode row groups
  - serialize row groups using Draken format
  - publish frames into shared-memory slots

Control plane uses small messages (`Pipe`/`Queue`).
Data plane uses shared memory only.

---

## 6. Ring Buffer Geometry

### 6.1 Slot Size

- `SLOT_BYTES = 32 * 1024 * 1024`
- `SLOT_HEADER_BYTES = 256` (cache-line aligned)
- `SLOT_PAYLOAD_BYTES = SLOT_BYTES - SLOT_HEADER_BYTES`

### 6.2 Slot Count

Default: `64` slots (`2 GiB` total ring).

This is configurable; production tuning should keep ring usage under memory SLO.

### 6.3 Slot State Machine

- `FREE`
- `WRITING`
- `READY`
- `READING`
- `ERROR`

Ownership:

- IO process: `FREE -> WRITING -> READY`
- EXEC process: `READY -> READING -> FREE`

Single writer, single reader per slot.

---

## 7. Transfer Unit: RowGroupFrame

Each slot carries one `RowGroupFrame`:

- header (fixed)
- payload bytes (Draken fragment)

Header fields:

- `query_id`
- `transfer_id` (unique per row-group transfer)
- `file_id_hash`
- `row_group_index`
- `slice_index` (0 for unsliced row group)
- `fragment_index`
- `fragment_count`
- `rows_in_slice`
- `payload_bytes`
- `payload_crc32`
- `flags` (`LAST_FRAGMENT`, `SLICED_ROWGROUP`, `ERROR`)

`transfer_id` groups fragments that belong to one logical transfer
(`file`, `row_group`, optional `slice_index`).

---

## 8. Row Group Fit, Fragment, Slice

Given serialized Draken payload size `N`:

1. If `N <= SLOT_PAYLOAD_BYTES`, write one frame.
2. Else fragment:
   - `fragment_count = ceil(N / SLOT_PAYLOAD_BYTES)`.
3. If `fragment_count <= MAX_FRAGMENTS_PER_TRANSFER`, transfer as fragments.
4. Else slice row group into row slices and serialize each slice separately.

Defaults:

- `MAX_FRAGMENTS_PER_TRANSFER = 8`
- `TARGET_SLICE_BYTES = 16 MiB`

Slicing rule:

- preserve row order within row group
- produce deterministic contiguous row ranges
- each slice is a complete Draken morsel fragment group

This prevents one huge row group from monopolizing ring memory.

---

## 9. Control Plane Messages

### 9.1 EXEC -> IO

- `ScanStart`
  - query id
  - files
  - projection
  - pushed predicates
  - scheduler caps
- `ScanCancel`
- `Shutdown`

### 9.2 IO -> EXEC

- `FrameReady`
  - `slot_id`
  - `transfer_id`
  - `fragment_index`
  - `fragment_count`
  - `rows_in_slice`
  - `payload_bytes`
- `TransferError`
- `ScanComplete`

---

## 10. Producer / Consumer Flow

### 10.1 IO Producer

1. Build next row-group work item using scheduler policy.
2. Fetch + decode + Draken-serialize row group.
3. Fit/fragment/slice according to section 8.
4. For each frame:
   - wait for `FREE` slot
   - write header + payload
   - transition `WRITING -> READY`
   - emit `FrameReady`

If no `FREE` slot, producer blocks and records `producer_full_stall`.

### 10.2 EXEC Consumer

1. Receive `FrameReady`.
2. Mark slot `READING`.
3. Accumulate fragment references by `transfer_id`.
4. When all fragments arrive:
   - assemble payload (zero-copy memoryviews where possible)
   - Draken-deserialize to Morsel
   - emit morsel downstream
5. Return consumed slots to `FREE`.

If no `READY` frame, consumer blocks and records `consumer_empty_stall`.

---

## 11. Scheduling Policy In IO Process

Use completion-first queueing with bounded active row groups:

- `MAX_FILES_IN_FLIGHT`
- `MAX_ROWGROUPS_IN_FLIGHT`
- `MAX_ROWGROUPS_PER_FILE`
- `MAX_READ_SLOTS_IN_FLIGHT`

Priority:

1. row groups already in progress
2. earliest admitted row group
3. bounded fairness across active files

This keeps transfer stream steady while avoiding wide partial-row-group fanout.

---

## 12. Backpressure and Stall Signals

### 12.1 Full-Side Stall

Producer cannot acquire `FREE` slot.

Metrics:

- `io_ring_producer_full_wait_ns`
- `io_ring_producer_full_wait_events`
- `io_ring_ready_slots_peak`

### 12.2 Empty-Side Stall

Consumer has no `READY` frames.

Metrics:

- `io_ring_consumer_empty_wait_ns`
- `io_ring_consumer_empty_wait_events`
- `io_ring_free_slots_peak`

### 12.3 Transfer Backlog

Completed transfers waiting in EXEC before operator consumption.

Metrics:

- `io_transfer_ready_backlog_peak`
- `io_transfer_emit_wait_ns`

These metrics replace the old implicit buffer stall diagnostics.

---

## 13. Memory and Safety Constraints

- hard ring bound: `SLOT_BYTES * SLOT_COUNT`
- no dynamic growth
- frame checksum validation before deserialize
- enforce max in-memory transfer assembly bytes per transfer
- reject malformed fragment sequences

On corruption or mismatch:

- mark slot `ERROR`
- emit `TransferError`
- fail query

---

## 14. Cancellation and Failure Handling

### 14.1 Query Cancel

- EXEC sends `ScanCancel`
- IO stops admitting new row groups
- IO stops publishing new frames
- EXEC drains/returns `READY` slots then tears down

### 14.2 IO Crash

- EXEC detects process death
- fail query
- force-reset non-`FREE` slots during cleanup
- unlink shared memory

### 14.3 EXEC Crash

- container teardown handles cleanup in production
- dev startup performs best-effort stale shm unlink

---

## 15. Configuration Knobs

- `FEATURE_IO_PROCESS_ROWGROUP_RING`
- `IO_RING_SLOT_BYTES` (default `33554432`)
- `IO_RING_SLOT_COUNT` (default `64`)
- `IO_MAX_FRAGMENTS_PER_TRANSFER` (default `8`)
- `IO_TARGET_SLICE_BYTES` (default `16777216`)
- `IO_MAX_FILES_IN_FLIGHT`
- `IO_MAX_ROWGROUPS_IN_FLIGHT`
- `IO_MAX_ROWGROUPS_PER_FILE`
- `IO_MAX_READ_SLOTS_IN_FLIGHT`

---

## 16. Telemetry Contract

Query-level:

- `io_ring_slot_bytes`
- `io_ring_slot_count`
- `io_ring_total_bytes`
- `io_ring_producer_full_wait_ns`
- `io_ring_producer_full_wait_events`
- `io_ring_consumer_empty_wait_ns`
- `io_ring_consumer_empty_wait_events`
- `io_transfer_ready_backlog_peak`
- `io_transfer_emit_wait_ns`
- `io_transfer_fragment_count_p50/p95/max`
- `io_transfer_payload_bytes_p50/p95/max`
- `io_rowgroup_slice_count`
- `io_deserialize_ns`
- `io_serialize_ns`

Trace events:

- `ring_frame_write_start/complete`
- `ring_frame_read_start/complete`
- `ring_transfer_ready`
- `ring_transfer_emit`
- `ring_stall_full_start/stop`
- `ring_stall_empty_start/stop`

---

## 17. Implementation Plan

### Phase 0: Interfaces and Metrics

- define control/data structs
- add telemetry plumbing
- add feature flag and config defaults

### Phase 1: Ring Transport Skeleton

- shared memory ring with slot transitions
- control-plane messages
- synthetic payload transfer test

### Phase 2: RowGroup Payload Path

- integrate Parquet row-group scheduler in IO process
- Draken serialize in IO
- Draken deserialize in EXEC
- fit/fragment logic

### Phase 3: Slicing and Fairness

- add row-group slicing when fragment cap exceeded
- tune scheduling fairness

### Phase 4: Cloud Run Hardening

- crash recovery checks
- throttling and retry observability
- soak tests

---

## 18. Testing Strategy

Unit tests:

- slot state transitions
- fragment reassembly correctness
- checksum failure behavior
- slicing determinism

Integration tests:

- parity with current row counts and results
- LIMIT cancellation
- induced producer-full and consumer-empty stalls

Performance tests:

- local and Cloud Run
- narrow and wide projections
- mixed row-group sizes
- p50/p95 stall time and throughput

---

## 19. Open Decisions

1. Should IO process own Parquet decode immediately, or only fetch+coalesce in phase 1?
2. Can Draken deserialize consume fragmented memoryviews directly to avoid full re-copy?
3. What slot count is safe by default in Cloud Run memory classes?
4. Do we permit out-of-order transfer emit or force strict row-group sequence?

---

## 20. Decision Summary

Adopt a two-process architecture with a fixed 32 MiB shared-memory slot ring.

Transfer row-group Draken payloads across the ring, fragment when required, and
slice row groups when fragmentation is excessive.

Use explicit producer-full and consumer-empty stall telemetry as first-class
diagnostics for bottleneck attribution.
