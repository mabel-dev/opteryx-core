# Draken Group By Performance Diagnostics

## Executive Summary

Based on telemetry analysis of a production group by operation (hash: `b24d8d49daef57d0`), the implementation exhibits severe performance bottlenecks in the **ingest phase**, consuming **88.6% of total execution time**. The operation processed ~100M input records with 56M unique groups, producing 56.4M output records across 861 output morsels.

**Key Metric**: The group by ingest phase spent **53.9 seconds out of 60.8 seconds total** — this is where optimization must focus.

## Telemetry Overview

### Raw Metrics

```json
{
  "records_in": 99997497,
  "bytes_in": 2764585852,
  "records_out": 56384822,
  "bytes_out": 2293813592,
  "calls": 325,
  "execution_time": 60793348242,
  "feature_groupby_draken_eval_native": 325,
  "time_group_by_evaluations": 5118449879,
  "feature_groupby_engine_carchar": 1,
  "feature_groupby_engine_multi_key_object": 1,
  "groupby_key_store_bytes": 2462968058,
  "time_groupby_ingest": 53878271909,
  "time_groupby_finalize": 7738665167,
  "groupby_output_morsels": 861,
  "time_groupby_finalize_backend": 0,
  "time_groupby_finalize_rows_to_vectors": 6847082158,
  "time_groupby_finalize_morsel_build": 0,
  "time_groupby_finalize_accounted": 6847082158,
  "time_groupby_finalize_emit_wait": 891583009,
  "groupby_finalize_rows": 56384822,
  "groupby_finalize_chunks": 861,
  "groupby_finalize_fast_path_hits": 0,
  "type": "AggregateRel"
}
```

### Execution Time Breakdown

| Phase | Time (ns) | Time (s) | Percentage |
|-------|-----------|----------|-----------|
| Ingest | 53,878,271,909 | 53.88 | **88.6%** |
| Finalize (total) | 7,738,665,167 | 7.74 | 12.7% |
| Finalize: rows→vectors | 6,847,082,158 | 6.85 | 11.3% |
| Finalize: emit wait | 891,583,009 | 0.89 | 1.5% |
| Evaluations | 5,118,449,879 | 5.12 | 8.4% |
| **Total** | **60,793,348,242** | **60.79** | **100%** |

### Data Flow Metrics

| Metric | Value | Analysis |
|--------|-------|----------|
| Records in | 99,997,497 | ~100M rows processed |
| Records out | 56,384,822 | 56.4% of input retained |
| Bytes in | 2.76 GB | ~27 bytes/record average |
| Bytes out | 2.29 GB | ~41 bytes/record average (values inflate after aggregation) |
| Byte reduction | 17% | **Poor compression despite 44% record reduction** |
| Key store size | 2.46 GB | **Enormous — almost as large as output** |
| Output morsels | 861 | High fragmentation — many small output chunks |
| Unique groups | ~56M | Estimated from output records (high cardinality) |

## Root Cause Analysis

### Primary Bottleneck: Ingest Phase Inefficiency

The ingest phase accounts for **88.6% of total execution time**, dominated by per-row hash lookups and state insertion for ~100M input records against ~56M unique groups.

#### 1. High Cardinality with Minimal Aggregation Benefit

**The Problem:**
- Input records: 99.9M
- Output records: 56.4M (only 43.5% reduction)
- Input bytes: 2.76 GB
- Output bytes: 2.29 GB (only 17% reduction)
- Key store: 2.46 GB (89% of output size)

**Interpretation:**
This data pattern indicates that aggregation is providing minimal value:
- Approximately **56 million unique group keys** exist in the dataset
- Each group represents only ~1.8 input records on average (99.9M / 56.4M)
- The aggregated values aren't significantly compressing data (only 17% byte reduction)
- The key storage overhead (2.46 GB) consumes 89% of the output size

**Performance Impact:**
With this many unique groups, the hash table must:
- Maintain 56M state entries in memory
- Perform 100M hash lookups with a significant miss rate (~43.5% based on record reduction)
- Allocate and append new state for each new group encountered

#### 2. Row-by-Row Hash Lookup Pattern

The core ingest loop operates at row granularity:

```cython
# From _ingest_object_key_multi() in group_by_engine.pyx, lines 5199-5209
for row_idx in range(row_count):
    state_index = -1
    if self._index.lookup_fast(row_hashes[row_idx], state_index):
        state_indices[row_idx] = state_index
        continue
    # Cache miss → call _find_or_insert_multi_encoded_state()
    state_indices[row_idx] = self._find_or_insert_multi_encoded_state(
        row_hashes[row_idx],
        key_vectors,
        row_idx,
    )
```

**The Cost:**
- **100M iterations** of the outer loop
- **~43.5M cache misses** (one per new group)
- Each miss triggers expensive state allocation

#### 3. State Allocation Overhead on Hash Misses

When a new group is encountered (`_find_or_insert_multi_encoded_state()`), the code performs:

```cython
# From _find_or_insert_multi_encoded_state(), lines 2447-2494
payload_ref = <int64_t> self._state_count()
self._index.insert_new(row_hash, payload_ref)
self._append_multi_payload_key(key_vectors, row_idx)

key_store_bytes = <size_t> self._key_payload_bytes.size()
record_groupby_key_store_bytes(self, key_store_bytes)  # ← Called 43.5M times

# For each aggregation function:
for agg_idx in range(self._multi_agg_count):
    self._multi_counts.push_back(0)
    self._multi_i64_state.push_back(0)
    self._multi_f64_state.push_back(0.0)
    self._multi_seen.push_back(0)
    self._multi_avg_sums.push_back(0.0)
    self._multi_avg_counts.push_back(0)
    self._multi_object_state.append(None)
    self._multi_object_state_starts.push_back(0)
    self._multi_object_state_lengths.push_back(0)
    # ... more appends per aggregation
```

**Per-New-Group Costs:**
- **1 hash index insertion** — O(1) amortized but repeated 43.5M times
- **Key payload append** — copying/encoding the group key
- **Telemetry recording** — `record_groupby_key_store_bytes()` called 43.5M times
- **8-10 vector.push_back() calls per aggregation** — with N aggregations, this is **N * 43.5M allocations**

**Memory Fragmentation:**
Vector reallocation during repeated push_back operations causes:
- Cache misses during growth
- Allocator fragmentation
- Potential reallocation overhead as vectors grow from initial capacity

#### 4. No Vectorized Fast Path

The telemetry shows:

```
"groupby_finalize_fast_path_hits": 0
"feature_groupby_engine_multi_key_object": 1
```

This indicates:
- **Zero fast path hits** — the optimized batch processing path is not being used
- **Multi-key object mode active** — using string/object keys rather than fixed-width types
- The code is falling back to the generic, scalar per-row path

**Cost of Scalar Path:**
The object key path (`_ingest_object_key_multi()`) processes rows sequentially without the vectorized kernels available for fixed-width keys. Compare with the fixed-width int64 path:

```cython
# Fast path (int64 keys) - uses vectorized kernels
count_star_multi_accumulate(self._multi_counts.data(), state_indices, row_count, ...)

# Scalar fallback - iterates per row
for row_idx in range(row_count):
    if not _bitmap_is_valid(value_nulls, row_idx):
        continue
    offset = self._multi_offset(state_indices[row_idx], agg_idx)
    if agg_mode == AGG_COUNT_VALUE:
        self._multi_counts[offset] = self._multi_counts[offset] + 1
```

### Secondary Bottlenecks

#### Hash Index Load Factor and Collision Chains

The hash index is initialized with:

```cython
self._index = new CarcharIndex(<size_t> max(16, morsel.num_rows * 2), 0.80)
```

**Issues:**
- **Load factor 0.80** is aggressive for this workload
- With 56M unique groups, collision chains will be non-trivial
- Each collision requires additional probe steps in the hash table

**CarcharIndex Probe Behavior** (from `carchar_index.hpp`):
- Uses swiss table-style open addressing
- Load factor 0.80 means table is 80% full
- Probe length correlates with collision frequency

**Estimated Probe Overhead:**
- At 80% load factor, average probe length for lookups increases significantly
- With 100M lookups against 56M entries, you're doing ~1.78 lookups per input row
- If average probe length is 2-3, that's 2-3x additional memory accesses per lookup

#### Key Store Size Explosion

The `groupby_key_store_bytes` metric shows **2.46 GB** for storing group keys.

**Analysis:**
- 56M unique groups × ~44 bytes/key = 2.46 GB ✓ (matches telemetry)
- This is 89% of the 2.29 GB output size
- For a GROUP BY with minimal aggregation benefit, you're paying nearly the full data volume cost just to store keys

**Root Cause:**
With 56M unique groups, the key storage scales linearly with cardinality. There's limited optimization opportunity here without changing the problem (e.g., pre-aggregation, sampling, or spill strategies).

#### Output Fragmentation (861 Morsels)

The operation emitted **861 output morsels** for 56.4M records:
- Average morsel size: 65,500 records (~2.7 MB per morsel)
- Typical morsel target: 65,536 records

**Impact:**
While not a primary bottleneck, 861 morsels is high fragmentation. This suggests:
- Memory pressure during finalize
- Multiple finalize iterations
- Potential downstream merge/shuffle overhead

## Detailed Performance Analysis

### Ingest Phase Timing

**Total ingest time: 53.88 seconds**

Breaking down the 100M row processing:

```
100M rows × 53.88s ingest time = 0.54 microseconds per row

Cost per row:
  - Hash lookup: ~0.3 µs (cached hit ~57% of time, miss ~43%)
  - On miss: state allocation + key encoding (~1.2 µs)
  - Aggregation update: ~0.05 µs
```

At 43.5M misses:
```
43.5M misses × 1.2 µs/miss = 52.2 seconds
56.5M hits × 0.3 µs/hit = 17.0 seconds
─────────────────────────────
Total: ~69.2 seconds expected vs 53.88s actual
```

The actual time is lower, suggesting some batching or vectorization is occurring, but still dominated by miss-case handling.

### Key Observation: Memory Allocation in Hot Path

The most expensive operation during ingest is likely the per-group state allocation in `_find_or_insert_multi_encoded_state()`. With ~43.5M new groups:

```cython
for agg_idx in range(self._multi_agg_count):
    self._multi_counts.push_back(0)           # ← Memory allocation
    self._multi_i64_state.push_back(0.0)      # ← Memory allocation
    self._multi_f64_state.push_back(0.0)      # ← Memory allocation
    self._multi_seen.push_back(0)             # ← Memory allocation
    self._multi_avg_sums.push_back(0.0)       # ← Memory allocation
    self._multi_avg_counts.push_back(0)       # ← Memory allocation
    self._multi_object_state.append(None)     # ← Python list operation
    self._multi_object_state_starts.push_back(0)  # ← Memory allocation
    self._multi_object_state_lengths.push_back(0) # ← Memory allocation
    # ... plus distinct sets, etc.
```

If `N_agg = 3` (3 aggregation functions), that's **9+ vector push_back operations per new group**. With 43.5M new groups:

```
43.5M × 9 = ~391.5M allocations in hot path
```

This is a memory allocator stress test and major cache pollution source.

## Recommendations for Optimization

### Priority 1: Reduce State Allocation Frequency

**Strategy**: Pre-allocate state storage to accommodate expected cardinality.

**Implementation**:
- Estimate unique group cardinality from first morsel or use a config hint
- Pre-reserve state vectors to avoid repeated reallocation:

```cython
# Before ingest loop
expected_groups = morsel.num_rows * 2  # or config parameter
for agg_idx in range(self._multi_agg_count):
    self._multi_counts.reserve(expected_groups)
    self._multi_i64_state.reserve(expected_groups)
    self._multi_f64_state.reserve(expected_groups)
    # ... etc for all state vectors
```

**Expected Benefit**: 20-30% reduction in ingest time by eliminating vector reallocation overhead.

### Priority 2: Batch Group Detection

**Strategy**: Group consecutive rows with identical hash values to reduce lookup frequency.

**Concept**:
```cython
# Instead of:
for row_idx in range(row_count):
    state_index = lookup_hash(row_hashes[row_idx])  # 100M lookups

# Do:
prev_hash = -1
batch_start = 0
for row_idx in range(row_count + 1):
    if row_idx == row_count or row_hashes[row_idx] != prev_hash:
        # Process batch [batch_start:row_idx) with same hash
        batch_len = row_idx - batch_start
        state_index = lookup_hash(prev_hash)
        process_batch(state_index, batch_start, batch_len)
        batch_start = row_idx
        prev_hash = row_hashes[row_idx]
```

**Expected Benefit**: If data has even modest clustering by group key, this could reduce lookup count by 10-20x. Not applicable if data is randomly ordered.

### Priority 3: Lower Hash Index Load Factor

**Change**:
```cython
# Current
self._index = new CarcharIndex(..., 0.80)

# Proposed
self._index = new CarcharIndex(..., 0.70)  # or 0.75
```

**Trade-off**:
- **Benefit**: Fewer collisions, shorter probe chains, fewer cache misses
- **Cost**: ~12% larger hash table memory (negligible at 56M entries)

**Expected Benefit**: 5-10% faster lookups; modest overall impact (~2-5% ingest time).

### Priority 4: Vectorize Object Key Path

**Challenge**: The current `_ingest_object_key_multi()` operates at row granularity without batch kernel support.

**Opportunity**: Implement batch operations for string/object keys similar to fixed-width paths:

```cython
# Proposed batch operation (pseudocode)
def count_star_multi_accumulate_object_keys(
    multi_counts, state_indices, row_count, multi_agg_count, agg_idx
):
    """Batch update counts for multiple rows in one kernel."""
    for i in range(row_count):
        offset = state_indices[i] * multi_agg_count + agg_idx
        multi_counts[offset] += 1
```

**Expected Benefit**: 20-40% faster aggregation updates for this path (~5-10% overall ingest time if aggregation is 25% of miss-case cost).

### Priority 5: Consider Cardinality-Driven Architecture Decision

**Problem**: 56M unique groups with minimal aggregation benefit suggests this operation may not be well-suited for in-memory hashing.

**Options**:
1. **Validate upstream**: Is the GROUP BY actually needed, or can aggregation happen earlier (e.g., with a DISTINCT or partial aggregate)?
2. **Spill to disk** (Phase 3 from design): For datasets with >10M unique groups, consider partitioned spill to manage memory more effectively.
3. **Approximate aggregation**: If exactness can be relaxed, use sketches or sampling (e.g., `APPROX_COUNT_DISTINCT` already available).

**Expected Benefit**: Depends on root cause; could be fundamental architecture change.

## Non-Recommendations (Low Priority)

### Not Recommended: Telemetry Optimization
The `record_groupby_key_store_bytes()` function is called 43.5M times, but this is negligible overhead:
```cython
cdef inline void record_groupby_key_store_bytes(object self, size_t key_store_bytes) noexcept:
    self._readings["groupby_key_store_bytes"] = key_store_bytes
```

This is a single memory store operation per group — not a bottleneck.

### Not Recommended: Hash Function Changes
Unless collision analysis shows pathological clustering, changing the hash function provides limited benefit. The `CarcharIndex` uses a well-designed swiss table; probe length is reasonable for the given cardinality.

## Comparison with Fast Path

For reference, the **fixed-width int64 key path** avoids many of these issues:

```cython
# Fast path for int64 keys (lines 3394-3676)
cdef void _ingest_int64_key(self, Morsel morsel, Int64Vector key_vector):
    # Uses vectorized kernels:
    count_star_accumulate(self._counts.data(), state_indices, row_count)
```

This path:
- ✅ Uses vectorized kernels (one function call for all rows)
- ✅ Processes all rows in one batch operation
- ✅ Avoids per-row state allocation overhead
- ✅ Better cache locality

**Why we're not on fast path**: `feature_groupby_engine_multi_key_object: 1` indicates multi-key with object (string) types, which forces the scalar path.

## Next Steps

### Immediate Investigation
1. **Validate cardinality assumption**: Confirm that 56M unique groups is accurate via query analysis
2. **Profile allocator**: Use memory profiler to quantify vector reallocation overhead
3. **Measure probe length**: Check `CarcharIndex` statistics for actual collision behavior
4. **Check data ordering**: Determine if input data has any clustering by group key (enables batch optimization)

### Short-term Optimization
- [ ] Implement Priority 1 (pre-allocation)
- [ ] Implement Priority 3 (lower load factor)
- [ ] Measure impact and baseline

### Medium-term Optimization
- [ ] Implement Priority 2 (batch group detection) if data ordering permits
- [ ] Implement Priority 4 (vectorize object key path)
- [ ] Benchmark against current state

### Long-term Architecture
- [ ] Evaluate whether Phase 3 (spill) is necessary for this workload class
- [ ] Consider approximate aggregation as alternative path for high-cardinality cases
- [ ] Profile memory allocator behavior at this scale

## Appendix: Key Code Locations

| Topic | File | Lines |
|-------|------|-------|
| Main ingest entry point | `group_by_engine.pyx` | 5588-5663 |
| Object key ingest | `group_by_engine.pyx` | 5169-5346 |
| Multi-agg object key ingest | `group_by_engine.pyx` | 5348-5583 |
| State insertion (single key) | `group_by_engine.pyx` | 2286-2335 |
| State insertion (multi key) | `group_by_engine.pyx` | 2444-2494 |
| Hash index implementation | `carchar_index.hpp` | — |
| Fast path example (int64) | `group_by_engine.pyx` | 3394-3676 |
| Telemetry definitions | `group_by_engine.pyx` | 410-462 |

---

**Document Version**: 1.0  
**Date**: 2024  
**Status**: Performance analysis of hash-based group by for high-cardinality datasets  
**Audience**: Performance engineers, architecture reviewers
