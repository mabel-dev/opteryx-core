# Opteryx IO / Execution Process Isolation Design

## 1. Overview

This document describes the design for isolating I/O from execution in Opteryx by introducing a dedicated I/O worker process and a shared-memory ring buffer.

The objective is to:

> Prevent remote blob fetch latency and variance from stalling the execution engine, while eliminating large-payload IPC overhead.

This design assumes:

- One query per container
- Cloud Run deployment
- 8 GB container memory (configurable)
- Blobs up to ~50 MB compressed
- 8 in-flight reads
- Decode occurs in EXEC (not IO)

---

## 2. Goals

- Decouple I/O latency from execution pipeline
- Eliminate large payload copying between processes
- Introduce bounded backpressure
- Provide measurable signals for I/O vs EXEC bottlenecks
- Keep implementation simple and predictable

---

## 3. Non-Goals

- Multi-query scheduling
- Cross-query shared buffer pool
- Async rewrite of engine
- Replacing GCS client implementation
- Variable-sized shared memory allocator
- NUMA / CPU affinity tuning

---

## 4. Architecture

Single container, two OS processes:

~~~
+——————————————————+
| Cloud Run Container                                 |
|                                                      |
|  EXEC Process (Parent)                              |
|    - Query planning                                 |
|    - Decode                                         |
|    - Operator execution                             |
|    - Shared memory consumer                         |
|    - Issues read requests                           |
|                                                      |
|  IO Worker Process                                  |
|    - Receives read requests                         |
|    - Fetches blobs from GCS                         |
|    - Writes payload into shared memory              |
|    - Signals completion                             |
|                                                      |
|  Shared Memory Region                               |
|    - Fixed-size slot ring                           |
+——————————————————+
~~~

Communication model:

- **Control plane**: small messages via `multiprocessing.Pipe` or `Queue`
- **Data plane**: large payloads via shared memory ring buffer

### 4.1 Process Lifecycle

**Startup order:**

1. EXEC process allocates shared memory region
2. EXEC spawns IO worker (spawn, not fork) passing the shared memory name
3. IO worker attaches to shared memory
4. IO worker signals ready; EXEC waits before issuing any reads

> **Decision:** Use **spawn**. Fork would inherit open GCS client state and file descriptors, which can cause unpredictable behaviour. Both Linux (Debian) and macOS support spawn; macOS defaults to spawn from Python 3.8 onwards. Startup latency (~10–50 ms) is negligible relative to Cloud Run cold-start time.

**Teardown order:**

1. EXEC sends a shutdown sentinel to IO worker
2. IO worker flushes any in-flight writes, then exits
3. EXEC joins IO worker process
4. EXEC releases shared memory
5. Shared memory is unlinked

> **Decision:** No production requirement to guarantee `unlink()` on crash — the container recycles. However, on macOS (dev environment) POSIX shared memory objects survive process death and can accumulate between test runs. Add a best-effort `unlink()` at startup (swallow `FileNotFoundError`) as a development-hygiene measure.

---

## 5. Memory Configuration

### 5.1 Ring Parameters

| Parameter   | Value      |
|-------------|-----------|
| Slot size   | 64 MB     |
| Slot count  | 24        |
| Total size  | ~1.5 GB   |

Rationale:

- Max compressed blob ~50 MB → fits in single slot
- 8 in-flight reads
- 3× buffering depth for jitter tolerance
- Leaves sufficient memory for engine + runtime

If memory pressure observed, reduce to 16 slots (~1 GB).

---

## 6. Shared Memory Layout

Shared memory is allocated per query and freed on completion.

Memory is divided into fixed-size slots.

### 6.1 Slot Structure

Each slot consists of metadata and payload:

~~~
struct Slot {
uint32 state;       // atomic; governs ownership
uint32 length;      // set by IO before WRITING → READY
uint64 request_id;  // echo of the originating ReadRequest
uint8  _pad[48];    // pad header to 64 bytes (cache-line aligned)
uint8  payload[64MB];
}
~~~

The payload field must start on a 4 KB page boundary to allow zero-copy decode paths. Verify alignment when allocating the shared memory region.

> **Action:** Add an assertion in the Phase 1 initialisation path to verify page alignment at runtime. This will answer the question the first time tests are run and catch any platform-specific issues early.

### 6.2 Slot State Machine

~~~
FREE     = 0
WRITING  = 1
READY    = 2
READING  = 3
~~~

Ownership rules:

- IO process:
  - FREE → WRITING → READY
- EXEC process:
  - READY → READING → FREE

Single writer per slot.
Single reader per slot.
No shared writes.

---

## 7. Control Plane Messages

### 7.1 EXEC → IO: ReadRequest

Fields:

- `request_id` (uint64) — generated by EXEC; monotonically increasing per query; wraparound not a concern at realistic blob counts
- `blob_id` — opaque string identifier for the GCS object

Byte-range (partial-blob) reads are not in scope. The IO worker always fetches the full object. Partial-read support is listed in §16 as a possible future enhancement.

Sent via `multiprocessing.Pipe`. IO worker count is fixed at 8 for the foreseeable future, so the one-to-one topology of `Pipe` is preferred over `Queue` for its lower overhead. `Queue` would only be justified if the worker pool became dynamic.

---

### 7.2 IO → EXEC: ReadComplete

Fields:

- `request_id`
- `slot_id`
- `length`
- Optional error flag

Payload size is small.

---

## 8. Slot Lifecycle

### 8.1 IO Worker

1. Receive `ReadRequest`
2. Wait for FREE slot
3. Transition: FREE → WRITING
4. Fetch blob from GCS
5. Write payload into slot
6. Set `length`
7. Transition: WRITING → READY
8. Send `ReadComplete`

If no FREE slot:
- IO blocks (natural backpressure)

---

### 8.2 EXEC

Concurrency is bounded by a semaphore initialised to 8 (the max in-flight reads). EXEC acquires the semaphore before issuing each `ReadRequest` and releases it after freeing the corresponding slot.

1. Acquire inflight semaphore (blocks if 8 reads already outstanding)
2. Issue `ReadRequest`
3. On `ReadComplete`:
   - Transition READY → READING
4. Decode from shared memory
5. After decode:
   - Transition READING → FREE
   - Release inflight semaphore

If no READY slots:
- EXEC blocks (indicates I/O bound)

> **⚠ TO TEST:** Verify that the semaphore correctly prevents more than 8 simultaneous outstanding requests under load.

---

## 9. Synchronization Strategy

Initial implementation:

- Simple synchronization primitives (semaphores or condition variables)
- Atomic state transitions
- No global locks

Optimization (only if profiling demands):

- Lock-free signaling
- Eventfd / futex (Linux)

---

## 10. Backpressure Model

System guarantees:

- IO cannot exceed bounded memory
- EXEC cannot consume unbounded memory
- No large-payload pickling
- No unbounded buffering

Metrics reveal bottleneck:

| Condition                   | Interpretation                        |
|-----------------------------|---------------------------------------|
| READY == 0, FREE > 0       | IO / network bound                    |
| FREE == 0, READY > 0       | EXEC / decode bound                   |
| READY > 0 and FREE > 0     | Balanced; neither side is limiting    |
| READY == 0 and FREE == 0   | All slots in WRITING or READING state |

---

## 11. Error Handling

### 11.1 IO Failure

If blob fetch fails:

- Send `ReadComplete` with error flag
- Slot remains FREE
- EXEC handles failure

### 11.2 Process Failure

If IO process dies:

- Parent detects failure (poll returncode or sentinel on control pipe)
- Query fails fast
- Shared memory cleaned up

### 11.3 Stranded WRITING Slot

If the IO process crashes while a slot is in state `WRITING`, that slot is permanently unreachable under normal operation (no writer will ever transition it to READY or FREE).

Recovery:

- On IO worker restart or query teardown, EXEC scans all slots and forcibly resets any `WRITING` → `FREE`
- This scan is safe because a dead IO process holds no concurrent write

> **Decision:** SIGKILL terminates the container, so in production the recovery scan is only needed at query teardown (not restart). The scan is still worth implementing and testing during Phase 1 to confirm correct slot accounting — use a controlled in-process simulation rather than SIGKILL.

---

## 12. Expected Performance Impact

Eliminates:

- Large-payload pickling
- IPC megabyte transfers
- Python allocator churn
- GIL coordination overhead

Does not eliminate:

- GCS latency
- TLS/network variability
- Cloud provider throughput limits

If performance unchanged after implementation, bottleneck is upstream.

---

## 13. Metrics

Instrument:

- Slot state counts (FREE / READY)
- EXEC wait time for READY
- IO wait time for FREE
- GCS latency histogram (p50 / p95 / p99)
- Bytes transferred per query

These determine true limiting factor.

---

## 14. Implementation Phases

### Phase 1
- Implement ring using `multiprocessing.shared_memory`
- Use simple synchronization primitives
- Validate correctness

### Phase 2
- Move slot metadata + state transitions into Cython/C
- Use atomic state transitions (`__atomic_compare_exchange` via a Cython `cdef` wrapping GCC/Clang built-ins — Python-level atomics are not sufficient)
- Profile

### Phase 3 (Optional)
- Lock-free signaling
- Only if profiling demonstrates need

---

## 15. Safety Considerations

- Bounds-check payload writes
- Validate state transitions
- Clear slot metadata before reuse
- Assert slot state correctness during transitions

---

## 16. Future Enhancements

- Dynamic slot depth tuning
- Adaptive inflight read tuning
- Central IO service for multi-query containers
- Prefetch prioritization
- Smaller slots if blob distribution skews lower

---

## 17. Open Questions

Items that must be resolved before or during implementation:

| # | Question | Section | Status |
|---|----------|---------|--------|
| 1 | spawn vs fork for IO worker process | §4.1 | **Resolved: spawn** |
| 2 | `unlink()` guaranteed on EXEC crash? | §4.1 | **Resolved: not required; add dev-hygiene unlink at startup** |
| 3 | Page alignment guaranteed by `shared_memory`? | §6.1 | **Open — verify via assertion in Phase 1 init** |
| 4 | Byte-range reads in scope for Phase 1? | §7.1 | **Resolved: never; always full-blob reads** |
| 5 | `Pipe` vs `Queue` for control plane | §7.1 | **Resolved: Pipe** |
| 6 | Cython vs Rust for Phase 2 transitions | §14 | **Resolved: Cython/C** |

---

## 18. Summary

This design introduces:

- Process-level isolation between IO and EXEC
- Bounded shared-memory buffering
- Clear backpressure semantics
- Deterministic memory usage (~1.5 GB)
- Predictable slot lifecycle
- Elimination of large IPC copies

Topology:

- 2 processes
- 1 shared memory ring
- 8 in-flight reads
- 24 fixed 64 MB slots

The design prioritizes correctness, predictability, and observability over cleverness.

---

## 19. Glossary

| Term | Definition |
|------|------------|
| **Blob** | A single compressed object stored in GCS (typically a Parquet partition), up to ~50 MB |
| **Morsel** | A decoded, in-memory batch of rows produced from a blob; the unit of work consumed by the execution engine |
| **Slot** | One fixed-size region within the shared memory ring, capable of holding one blob payload |
| **EXEC process** | The parent process responsible for query planning, decoding morsels, and executing operators |
| **IO worker** | The child process responsible only for fetching blobs from GCS and writing them into slots |
| **Ring buffer** | The fixed set of slots cycled through WRITING → READY → READING → FREE |
| **Inflight read** | A `ReadRequest` that has been issued by EXEC but whose corresponding slot has not yet been freed |