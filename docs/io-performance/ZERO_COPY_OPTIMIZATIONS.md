# Parquet IO Transport: MemoryPool Architecture

## Overview

This document describes the current architecture of Opteryx's Parquet IO transport layer
(`opteryx/connectors/parquet_io/io_process_ring.py`). The system uses an in-process
`MemoryPool` to pass row group data between the IO worker thread and the query consumer,
minimising data copies while keeping control flow simple and explicit.

---

## Architecture

### Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│  iter_row_groups_io_process_v2()  (consumer — main thread)              │
│                                                                         │
│   MemoryPool.read(ref_id, zero_copy=True, latch=True)  ◄───────────┐   │
│   read_morsel(memoryview)                                           │   │
│   pool.unlatch(ref_id)                                              │   │
│   pool.release(ref_id)                                              │   │
│                                                                     │   │
│   event_q.get()  ◄──────────────────────────────────────────────┐  │   │
└──────────────────────────────────────────────────────────────────│──│───┘
                                                                   │  │
                              ROWGROUP_READY event {ref_id, meta} ─┘  │
                              data committed at ref_id ────────────────┘
┌─────────────────────────────────────────────────────────────────────────┐
│  _emit_loop()  (emitter thread, one per scan)                           │
│                                                                         │
│   write_morsel(None, morsel)  →  bytes                                  │
│   pool.commit(data)           →  ref_id                                 │
│   event_q.put({ROWGROUP_READY, ref_id, metadata})                       │
└─────────────────────────────────────────────────────────────────────────┘
                        ▲
                  ready_queue (bounded)
                        │
┌─────────────────────────────────────────────────────────────────────────┐
│  _io_worker()  (worker thread, one per session lifetime)                │
│                                                                         │
│   Persistent ThreadPoolExecutor (read + decode pools)                   │
│   Footer fetch → Column range reads → Draken decode → Morsel assembly   │
│   _complete_rowgroup() → ready_queue.put(state)                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Threading Model

| Thread | Lifecycle | Responsibility |
|--------|-----------|----------------|
| Main (consumer) | Per query | Reads events, reads pool, yields row groups |
| IO worker | Per session | Persistent; footer fetch, column IO, decode dispatch |
| Emitter | Per scan | Serialises morsels, commits to pool, signals consumer |
| Read pool | Per session | `PARQUET_GLOBAL_RANGE_READERS` threads; range reads |
| Decode pool | Per session | `PARQUET_DECODE_WORKERS` threads; Draken column decode |

The read and decode pools are **persistent** across scans to avoid thread-creation
overhead and preserve HTTP keep-alive connections (important for GCS).

---

## Data Flow and Copy Count

For each row group, data moves as follows:

| Step | Operation | Copies |
|------|-----------|--------|
| 1 | Draken decode: compressed bytes → vector buffers | 1 (unavoidable: decompress) |
| 2 | `write_morsel(None, morsel)` → `bytes` via BytesIO | 1 (serialise vectors) |
| 3 | `pool.commit(data)` → MemoryPool segment | 1 (copy into pool) |
| 4 | `pool.read(ref_id, zero_copy=True, latch=True)` → `memoryview` | **0** |
| 5 | `read_morsel(memoryview)` via `MemoryViewStreamOptimized` | 1 (decode into new vectors) |

**Total: 4 copies.** Steps 2–3 are the next reduction target (reserve-and-write, see below).

### Historical comparison

| Era | Copies per row group |
|-----|----------------------|
| Multi-process IPC (original) | 7–9 |
| In-process threading + ring | 5–6 |
| **MemoryPool transport (current)** | **4** |
| Reserve-and-write (planned) | 2 |

---

## Key Components

### `MemoryPool` (`opteryx.compiled.structures.memory_pool`)

Compiled Cython class. Manages a contiguous block of memory with segment tracking,
compaction, and thread-safe access. Used here for the IO transport but also backs the
`memory://` KV store layer.

Relevant API for the IO transport:

```python
pool = MemoryPool(size=N, name="parquet-io-pool", auto_resize=True, alignment=8)

# Producer side
ref_id = pool.commit(data: bytes) -> int   # -1 on failure (pool full, auto_resize=False)

# Reserve-and-write (future; avoids the bytes intermediary)
ref_id, ptr, capacity = pool.reserve_for_write_ptr(estimated_size: int)
# write directly to ptr via _PoolWriter / ctypes memoryview
pool.finalize_commit(ref_id, actual_bytes_written: int)

# Consumer side
mv = pool.read(ref_id, zero_copy=True, latch=True)  # latches: prevents compaction
pool.unlatch(ref_id)    # release latch after read_morsel completes
pool.release(ref_id)    # free the segment
```

Pool is initialised with `size = IO_RING_SLOT_BYTES × IO_RING_SLOT_COUNT` and
`auto_resize=True`. Since `read_morsel` copies data into vector-owned buffers,
`unlatch` and `release` are called immediately after `read_morsel` returns.

### `_PoolWriter` (`morsel_io.pyx`)

A Cython `cdef class` that implements the file-like interface (`write`, `tell`, `close`)
expected by `write_morsel`, but writes directly into a caller-supplied pre-allocated
buffer via a raw `char*` pointer and a cursor.

Used today only when `write_morsel` is called with a `memoryview` sink
(`SINK_MEMORYVIEW`). In that path there is **no intermediate BytesIO allocation**:
each `write()` call `memcpy`s directly into the target. On overflow it raises
`ValueError("pool write overflow: ... too small")`.

```
write_morsel(some_writable_memoryview, morsel)
  → _open_writer detects memoryview → constructs _PoolWriter(mv)
  → all handle.write(...) calls go directly into the buffer
  → bytes_output = handle.bytes_written  (no getvalue(), no copy)
```

### `_emit_loop`

Per-scan emitter thread. Drains `ready_queue` (bounded, provides back-pressure),
serialises each completed `_IORowGroupState` into a morsel, commits to the pool,
and puts a `_EVENT_ROWGROUP_READY` event on `event_q`.

```python
data   = write_morsel(None, morsel)     # serialize
ref_id = pool.commit(data)              # into pool
event_q.put({"type": _EVENT_ROWGROUP_READY, "ref_id": ref_id, "row_group_meta": ...})
```

If `pool.commit` returns `-1` (only possible with `auto_resize=False`), the emitter
sets `cancel_event` and raises immediately — no silent degradation.

### Consumer (`iter_row_groups_io_process_v2`)

```python
if event_type == _EVENT_ROWGROUP_READY:
    ref_id   = event["ref_id"]
    metadata = event["row_group_meta"]

    mv     = pool.read(ref_id, zero_copy=True, latch=True)
    morsel = read_morsel(mv)
    pool.unlatch(ref_id)
    pool.release(ref_id)

    row_group = {_decode_column_name(c): morsel.column(...) for c in morsel.column_names}
    row_group.update(metadata)
    yield row_group
```

No fragment assembly, no slot state machine, no bitmap scan.

---

## Back-pressure

Back-pressure is provided by two bounded queues:

1. **`ready_queue`** (`maxsize = PARQUET_READY_ROWGROUP_QUEUE_CAP`): between the IO
   worker and the emitter. If the emitter falls behind, the IO worker blocks here,
   preventing unbounded memory accumulation of decoded vectors.

2. **`event_q`**: between the emitter and the consumer. `event_q.put(event, timeout=0.1)`
   will raise `queue.Full` after 100 ms; the emitter treats this as a fatal error,
   releases the pool segment, and cancels the scan. In normal operation the consumer
   reads events faster than they are produced.

---

## Configuration

| Variable | Default | Meaning |
|----------|---------|---------|
| `IO_RING_SLOT_BYTES` | 32 MB | Initial pool size factor (`× slot_count`) |
| `IO_RING_SLOT_COUNT` | 64 | Initial pool size factor (`× slot_bytes`) |
| `PARQUET_READY_ROWGROUP_QUEUE_CAP` | 2 | Bound on ready_queue depth |
| `PARQUET_ROWGROUPS_IN_FLIGHT` | 24 | Max concurrent row groups in IO worker |
| `PARQUET_GLOBAL_RANGE_READERS` | 16 | Read thread pool size |
| `PARQUET_DECODE_WORKERS` | 16 | Decode thread pool size |
| `PARQUET_READ_QUEUE_CAP` | 64 | Max in-flight column range reads |
| `PARQUET_DECODE_QUEUE_CAP` | 128 | Max pending + in-flight decode tasks |

The effective initial pool size is `IO_RING_SLOT_BYTES × IO_RING_SLOT_COUNT` (default
32 MB × 64 = 2 GB). With `auto_resize=True` the pool grows on demand; this is the
safe default since row group sizes are variable and can exceed any fixed estimate.

---

## Metrics

Metrics are collected per scan and attached to the final row group as `__key__` fields.

| Metric | Meaning |
|--------|---------|
| `__io_serialize_ns__` | Time spent in `write_morsel` per scan |
| `__io_consumer_empty_wait_ns__` | Consumer wait time (event queue empty) |
| `__io_consumer_empty_wait_events__` | Number of consumer poll timeouts |
| `__io_transfer_emit_wait_ns__` | Time from row group ready to emission start |
| `__io_transfer_ready_backlog_peak__` | Peak depth of ready backlog |
| `__io_pool_commits__` | Total `pool.commit` calls this scan |
| `__io_pool_bytes_committed__` | Total bytes committed to pool this scan |
| `__time_read_ranges_ns__` | Time in range read tasks |
| `__time_decode_columns_ns__` | Time in column decode tasks |
| `__rowgroup_completion_latency_ns__` | Admitted → completed latency |
| `__ranges_in_flight_peak__` | Peak concurrent in-flight ranges |

---

## IO Worker Internals (unchanged by transport migration)

The IO scheduling logic inside `_io_worker` was not modified by the ring→MemoryPool
migration. It continues to:

- Fetch parquet footers in parallel (via the persistent read pool)
- Admit row groups up to `PARQUET_ROWGROUPS_IN_FLIGHT` concurrently
- Dispatch column range reads via `_dispatch_columns` (cost-aware ordering)
- Dispatch Draken column decodes via `_dispatch_decodes`
- Apply `_pick_dispatch_state` with estimated decode cost per codec (GZIP > ZSTD > SNAPPY > LZ4 > PLAIN)
- Handle row-group pruning via predicate push-down
- Manage per-file and global in-flight caps

---

## Future Work

### 1. Reserve-and-Write (eliminates 1 copy)

Replace `write_morsel(None, morsel)` + `pool.commit(data)` with:

```python
ref_id, ptr, capacity = pool.reserve_for_write_ptr(estimated_size)
buf = (ctypes.c_char * capacity).from_address(ptr)
mv  = memoryview(buf)
stats = write_morsel(mv, morsel)          # writes via _PoolWriter directly into pool
pool.finalize_commit(ref_id, stats["bytes_output"])
```

`_PoolWriter` is already in `morsel_io.pyx` and `write_morsel` with a `memoryview`
sink already uses it. The remaining work is wiring up the `reserve_for_write_ptr` call
in `_emit_loop` with a size estimation strategy and an overflow fallback to `commit`.

**Expected improvement**: eliminates copy step 3 above → 3 total copies per row group.

**Size estimation**: use a rolling average of recent serialised row group sizes × 1.5
safety margin. On `ValueError` overflow from `_PoolWriter`, `pool.release(ref_id)` and
fall back to `pool.commit(bytes)`.

### 2. Fix dtype-3 serialiser gap

`DRKM v1` serialiser (`morsel_io.pyx`) does not handle `dtype 3` (numeric dictionary
columns). Two `test_morsel_io_round_trip_*` tests are blocked on this.

### 3. Per-session pool

Currently a new `MemoryPool` is created at the start of each call to
`iter_row_groups_io_process_v2`. A pre-warmed pool shared across scans (like the
persistent thread pools) would eliminate pool initialisation latency for short queries.

### 4. Zero-copy decode into pool-backed vectors

For `CODEC_NONE` columns, `read_morsel` currently `memcpy`s compressed block data into
vector-owned buffers. With pool-backed vectors that borrow a reference to the pool
segment (and hold a latch until the vector is freed), this copy could be eliminated.
Requires changes to the Draken vector ownership model.

---

## Design Decisions

### MemoryPool over SharedMemoryRing

The `_SharedMemoryRing` used `multiprocessing.shared_memory` — designed for cross-process
data passing. After the previous session migrated to in-process threading, the shared
memory overhead (ring state machine, bitmap scan, fragment slicing/assembly, struct
pack/unpack) was pure waste. `MemoryPool` is already compiled, in-process, and provides
exactly the right API (`reserve_for_write_ptr` / `finalize_commit` / `read(zero_copy)` /
`unlatch` / `release`). Replacing the ring removed ~500 lines of complexity.

### `auto_resize=True`

Row group sizes vary widely (kilobytes for narrow projections, hundreds of megabytes for
wide scans). A fixed-size pool would require tuning and could stall on unusual workloads.
`auto_resize=True` delegates the memory management decision to the pool and lets the
existing `ready_queue` back-pressure control the rate. This is safe for in-process use.

### One morsel per pool segment

The ring required row groups to be sliced into fixed-size fragments and reassembled.
The pool stores each morsel as a single variable-size segment, removing all fragment
tracking code and simplifying the consumer to a single `pool.read` call.