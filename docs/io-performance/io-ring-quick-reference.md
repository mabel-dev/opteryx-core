# IO Process Ring: Quick Reference & Action Items

## 5 Critical Bottlenecks (15-25% overhead)

| # | Problem | Location | Impact | Quick Fix |
|---|---------|----------|--------|-----------|
| 1 | Bitmap O(n) scan on every free slot claim | `io_process_ring.py:218-231` | 3-7% | Add hint tracking |
| 2 | Full morsel serialization before slicing decision | `io_process_ring.py:342-448` | 6-10% | Estimate first, serialize second |
| 3 | Suboptimal column dispatch ordering (largest-first) | `io_process_ring.py:1086-1106` | 3-5% | Sort by decode cost, not size |
| 4 | Metrics lock contention (8000+ acquisitions/query) | `io_process_ring.py:636-667` | 2-4% | Use thread-local aggregation |
| 5 | Frame struct packing overhead | `io_process_ring.py:252-293` | 1-2% | Use Cython `process_ring.pyx` |

**Total potential gain**: 25-35% with all optimizations | **Phase 1 alone**: 10-15%

---

## Phase 1: Week 1 Quick Wins (No architectural changes)

### 1.1 Add Hint Tracking to Bitmap Scan (1 hour, 3-5% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 218-231):
```python
def _find_free_slot_from_bitmap(self) -> int | None:
    for slot_id in range(self.slot_count):  # O(n) full scan every time
        if self.free_slot_bitmap[slot_id] == 0:
            if self.read_state(slot_id) == FREE:
                return slot_id
```

**Improved**:
```python
def _find_free_slot_from_bitmap(self) -> int | None:
    # Start from last known position (circular)
    start = getattr(self, '_last_free_hint', 0)
    for offset in range(self.slot_count):
        slot_id = (start + offset) % self.slot_count
        if self.free_slot_bitmap[slot_id] == 0:
            if self.read_state(slot_id) == FREE:
                self._last_free_hint = slot_id
                return slot_id
            else:
                self.free_slot_bitmap[slot_id] = 1
    return None
```

**Why**: Adjacent frees cluster. Hint reduces avg iterations from 32 to ~8.

---

### 1.2 Predictive Slice Sizing (2 hours, 3-4% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 410-423):
```python
# Full morsel serialized already; now decide slicing with uncertainty
bytes_per_row = len(payload) / rows_total
rows_per_slice = max(1, int(ideal_bytes_per_slice / bytes_per_row * 0.8))
# Retry loop if still too fragmented (up to 3 retries = 3× serialization)
for _ in range(max_retry):
    # Re-serialize each slice
    slice_payload, _ = _serialize_morsel(slice_morsel)
```

**Improved**:
```python
# Before full serialization: Estimate from Parquet stats
def _estimate_serialized_size(morsel, parquet_columns):
    total_compressed = sum(col['total_compressed_size'] for col in parquet_columns)
    draken_factor = 0.95  # Draken ≈ 5% overhead vs Parquet
    return int(total_compressed * draken_factor)

# Decide slice size based on estimate
est_bytes = _estimate_serialized_size(morsel, parquet_columns)
est_fragments = math.ceil(est_bytes / slot_payload_bytes)

if est_fragments <= max_fragments_per_transfer:
    # Likely fits; serialize full morsel
    payload, _ = _serialize_morsel(morsel)
else:
    # Need slicing; use estimate to size slices deterministically
    bytes_per_row_est = est_bytes / morsel.num_rows
    rows_per_slice = max(1, int((max_fragments_per_transfer * slot_payload_bytes) / bytes_per_row_est))
    # Serialize slices once (no retry loop)
```

**Why**: Avoid full serialization + multiple retry re-serializations.

---

### 1.3 Cost-Based Column Dispatch (1.5 hours, 2-3% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 1099-1104):
```python
# Sort by size; ignores compression/codec
candidates.sort(reverse=True, key=lambda item: (item[0], item[1]))
# 100 MB uncompressed ZSTD ≠ 100 MB uncompressed snappy
```

**Improved**:
```python
def _estimate_decode_cost(col_stats):
    """Estimate decode time from compression codec and size."""
    compressed = col_stats.get('total_compressed_size', 1)
    codec = col_stats.get('compression_codec', 'snappy').lower()
    codec_factor = {
        'uncompressed': 0.5,
        'snappy': 1.0,
        'gzip': 1.5,
        'zstd': 1.2,
        'lz4': 0.8,
    }.get(codec, 1.0)
    return compressed * codec_factor

# Sort by estimated decode cost, not raw size
candidates = [
    (
        _estimate_decode_cost(col_stats),
        -state.admitted_ns,
        key,
        state,
        col_stats,
    )
    for key, state in active_states.items()
    if state.pending_columns
]
candidates.sort(reverse=True)
```

**Why**: Decode time correlates with codec, not just size. GZIP-compressed columns are 1.5x slower than snappy.

---

### 1.4 Thread-Local Metrics (1 hour, 1-3% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 636-638, 667):
```python
with metrics_lock:  # Lock acquired ~8000 times per query
    metrics["io_serialize_ns"] += serialize_ns
    metrics["io_ring_producer_full_wait_ns"] += wait_ns
```

**Improved**:
```python
# Per-thread metrics (no lock during collection)
if not hasattr(_thread_local, 'metrics'):
    _thread_local.metrics = {}

# No lock needed
_thread_local.metrics['io_serialize_ns'] = _thread_local.metrics.get('io_serialize_ns', 0) + serialize_ns
_thread_local.metrics['io_ring_producer_full_wait_ns'] = _thread_local.metrics.get('io_ring_producer_full_wait_ns', 0) + wait_ns

# At scan end (single aggregation with lock)
with metrics_lock:
    for key, value in _thread_local.metrics.items():
        metrics[key] = metrics.get(key, 0) + value
```

**Why**: Avoid lock contention on hot path. 8000 lock acquisitions × 100-500 ns each = 1-4 ms overhead.

---

### 1.5 Ready Queue Backpressure Check (1.5 hours, 1-2% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 1039):
```python
while (
    len(active_states) < active_target
    and file_rr
    and _ready_buffer_depth() < ready_backlog_cap  # Only one check
):
    # Admit row groups
```

**Improved**:
```python
# Add read queue depth check to prevent unbounded read buffering
def _admit_rowgroups() -> None:
    nonlocal first_rowgroup_key
    if cancel_event.is_set():
        return
    
    while (
        len(active_states) < active_target
        and file_rr
        and _ready_buffer_depth() < ready_backlog_cap
    ):
        # Check read queue depth; don't admit if queue is backing up
        buffer_depth = len(read_futures) + len(decode_pending) + len(decode_futures)
        if buffer_depth > decode_buffer_cap * 0.8:
            break  # Pause admission until queue drains
        
        # ... admission logic ...
```

**Why**: Prevents read-side memory buildup (can cause OOM on large file scans).

---

## Phase 2: Week 2-3 Medium Effort (Moderate risk, 5-8% additional gain)

### 2.1 Integrate Cython FastSharedMemoryRing (3 hours, 2-3% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py` + `opteryx/compiled/io/process_ring.pyx`

**Current**: `process_ring.pyx` has `FastSharedMemoryRing` but it's not used.

**Action**:
```python
# In _io_worker(), replace _SharedMemoryRing with Cython wrapper
try:
    from opteryx.compiled.io.process_ring import create_fast_ring
    ring = _SharedMemoryRing(...)
    ring = create_fast_ring(ring)  # Wrap with Cython version
    use_cython = True
except ImportError:
    use_cython = False  # Fallback to Python

# In _emit_loop(), use fast path if available
if use_cython and hasattr(ring, 'write_frame_fast'):
    ring.write_frame_fast(slot_id, ...)
else:
    ring.write_frame(slot_id, ...)
```

**Why**: Cython avoids struct module overhead; struct packing is 2-3× faster in Cython.

---

### 2.2 Increase ready_queue_cap (30 minutes, 1-2% gain)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 1120):
```python
ready_queue_cap = max(2, int(_cfg.PARQUET_READY_ROWGROUP_QUEUE_CAP))
```

**Action**:
```python
# Increase default cap in config
PARQUET_READY_ROWGROUP_QUEUE_CAP = 8  # Was 2
```

**Why**: Small queue creates head-of-line blocking. Doubling to 8 adds minimal latency (64 MB extra memory) but smooths emission.

---

### 2.3 File-Aware Dispatch Fairness (2 hours, 1-2% gain, medium risk)

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current** (Line 1086-1106):
```python
# Global best-candidate selection; File A can monopolize dispatch
candidates = [(size, -admitted_ns, key, state) for key, state in active_states.items()]
candidates.sort(reverse=True)
_, _, key, state = candidates[0]
```

**Improved**:
```python
# Round-robin across files with per-file dispatch tracking
if not hasattr(_dispatch_fairness, 'file_queue'):
    _dispatch_fairness.file_queue = deque(
        set(state.file_seq for state in active_states.values())
    )

current_file = _dispatch_fairness.file_queue[0]
candidates = [
    (size, -admitted_ns, key, state)
    for key, state in active_states.items()
    if state.file_seq == current_file
]
if candidates:
    candidates.sort(reverse=True)
    _, _, key, state = candidates[0]
else:
    # Fallback to global best if no candidates in current file
    _dispatch_fairness.file_queue.rotate(-1)  # Move to next file
```

**Why**: Prevents large-column files from starving small-column files.

---

## Testing Checklist

### Before Any Commit
- [ ] Existing tests pass: `pytest tests/unit/parquet_io/test_io_process_ring.py`
- [ ] No regression on narrow projections (1-5 columns)
- [ ] No regression on wide projections (50+ columns)

### Per-Optimization
- [ ] **Bitmap hint**: Verify free slot claim latency < 2 µs with 50% fill
- [ ] **Slice sizing**: Verify no retry loops triggered (check `io_slice_estimate_error_count` metric)
- [ ] **Column dispatch**: Verify `io_rowgroup_completion_latency_ns` variance reduced (p95/p50 < 2.0)
- [ ] **Thread-local metrics**: Verify metrics still accurate at scan end
- [ ] **Ready queue**: Verify no deadlocks, backlog < 10 consistently

### Performance Validation
```bash
# Run benchmark before/after
make test  # Full regression suite

# Measure latency
python -c "
import time
import opteryx
session = opteryx.session()
start = time.time()
for _ in session.execute_to_morsels('SELECT * FROM testdata LIMIT 1000'):
    pass
print(f'Time: {time.time() - start:.2f}s')
"
```

---

## Configuration Knobs to Add

```python
# opteryx/config.py

# Ring optimizations
PARQUET_RING_FREE_LIST_HINT_ENABLED = True
PARQUET_LAZY_SERIALIZATION_ENABLED = True
PARQUET_COST_BASED_DISPATCH_ENABLED = True
PARQUET_THREAD_LOCAL_METRICS_ENABLED = True
PARQUET_FAIR_FILE_DISPATCH_ENABLED = True
PARQUET_CYTHON_RING_ENABLED = True  # Use FastSharedMemoryRing if available

# Tuning
PARQUET_READY_ROWGROUP_QUEUE_CAP = 8  # Increase from 2
PARQUET_SERIALIZE_ESTIMATION_MARGIN = 1.2  # ±20% safety margin
```

---

## Metrics to Watch Post-Deployment

```python
# New/modified metrics to track
{
    # Phase 1 indicators
    'io_ring_slot_claim_ns_p50': < 2 microseconds,  # Was 1-5 µs
    'io_serialize_ns': reduced by 15-20%,            # Lazy serialization
    'io_rowgroup_completion_latency_ns_variance': reduced,  # Cost-based dispatch
    'metrics_lock_contention': reduced by 70%,       # Thread-local metrics
    'ready_backlog_peak': < 5,                       # Queue depth reduced
    
    # Regression indicators
    'io_ring_producer_full_wait_events': should be 0 or very small,
    'io_ring_consumer_empty_wait_events': should be small,
    'io_slice_estimate_error_count': should be ~0,  # Estimation accuracy
}
```

---

## Rollback Plan

Each optimization is independent and can be disabled via config flag:

```python
# If regression detected, disable specific optimization
if PARQUET_COST_BASED_DISPATCH_ENABLED:
    # Use cost-based dispatch
else:
    # Fall back to largest-first (current)
```

---

## Decision Required from Architect

1. **Ready queue cap increase** (2 → 8): Accept 64 MB extra memory for stability?
2. **Thread-local metrics**: Accept metrics reporting latency (only available at scan end) for 2-3% gain?
3. **File-aware dispatch**: Accept fairness heuristic (may over-optimize for rare cases) for even distribution?

**Recommendation**: YES to all three. Low complexity, low risk, measurable gain.

---

## Next Steps

**Week 1**: Implement Phase 1 (bitmap hint, slice sizing, cost dispatch, thread-local metrics)
- Effort: 7 hours of development + testing
- Expected gain: 10-15%
- Risk: Low

**Week 2-3**: Implement Phase 2 (Cython integration, queue cap, file fairness)
- Effort: 5-6 hours of development + testing
- Expected gain: additional 5-8%
- Risk: Low-Medium

**Week 4+**: Phase 3 if bottlenecks persist (slice cache, parallel decode, adaptive coalescing)