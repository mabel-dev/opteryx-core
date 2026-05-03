# IO Stack Performance Review & Optimization Opportunities

**Date**: May 2026  
**Focus**: Clickbench benchmarking with identified quick wins and deeper optimizations  
**Status**: Recommendations for immediate implementation + longer-term improvements

---

## Executive Summary

The IO stack has substantial recent optimization work (Tier 1-4 parquet decoding, HTTP async, thread pool unification). Current state is **solid but tunable**. This review identifies 11 optimization opportunities across three impact tiers, ranging from quick-win config tuning to deeper architectural improvements. All are benchmarkable via clickbench (43 queries, realistic data sizes).

---

## Current State (Baseline)

### What's Working Well

1. ✅ **C++ Pipeline** (`pool_reader.pyx`): Lock-free, zero-Python in hot path
   - ZSTD decompression optimized (thread-local context, avoid per-call malloc)
   - Memory prefetch tuning already applied
   - GIL release early in execution paths

2. ✅ **Thread Pool Unification** (`thread_pool_manager.py`)
   - 7 global pools (decode, range, footer, filesystem pools)
   - Lazy initialization with caching (16.5x speedup on reuse vs. creation)
   - C++ BS::thread_pool backend (lock-free, work-stealing)

3. ✅ **Range Read Coalescing** (`reader.py:_coalesce_ranges`)
   - 64-byte gap merging + 32MB span limit prevents oversized reads
   - Reduces small-file overhead

4. ✅ **Filesystem Abstractions**
   - Local: pread() + mmap for zero-copy
   - GCS: libcurl-based HTTP client with connection pooling
   - HTTP: Range requests via libcurl get_many() (concurrent in C++)

5. ✅ **Footer Caching**
   - `ParquetFooterBytesCache`: In-memory per-query cache
   - Prefetch pool (64 workers) parallelizes multi-file footer reads

---

## Performance Opportunities (Prioritized)

### TIER 1: Quick Wins (Low Risk, Immediate Measurement)

#### 1.1: Range Read Coalescing Tuning
**Impact**: 2-5% on clickbench (query 29, 41, 43 — multi-file scans)  
**Effort**: 1-2 hours (benchmarking + tuning)  
**Risk**: Low

**Current behavior** (`reader.py:173`):
```python
gap = offset - last_end
if gap >= 0 and gap <= 64 and next_span <= 32 * 1024 * 1024:
    # Merge ranges
```

**Opportunity**: Clickbench has specific file layouts (e.g., hits_10M in multiple row groups). The 64-byte threshold and 32MB span may not be optimal:
- **For local SSD**: Gaps <100 bytes are "free" (single disk head movement); could relax to 100-200 bytes
- **For GCS/HTTP**: Larger merges reduce HTTP roundtrips; could relax span to 48MB or 64MB
- **For small row groups**: The 32MB limit might be too conservative on queries with small column chunks

**Action**:
1. Add instrumentation to `_coalesce_ranges()` to log actual gaps/spans for clickbench queries
2. Profile: measure coalescing efficiency (bytes merged / total ranges)
3. Test variants: gap=[64, 128, 256], span=[32MB, 48MB, 64MB, 96MB]
4. Benchmark each variant on clickbench subset (queries 1, 5, 29, 41, 43)

**File**: `opteryx/connectors/parquet_io/reader.py:146-188`

---

#### 1.2: Footer Prefetch Size Heuristic
**Impact**: 1-3% on queries with many small files  
**Effort**: 1 hour  
**Risk**: Very low

**Current behavior** (`reader.py:228`):
```python
_FOOTER_PREFETCH = 65536  # Fixed 64KB
prefetch_size = min(_FOOTER_PREFETCH, file_size)
```

**Opportunity**: For clickbench (~1GB parquet files), 64KB is oversized; many row-group metadata fits in 8KB. For small files, 64KB might be underfetching.

**Action**:
1. Change to adaptive heuristic:
   ```python
   def _footer_prefetch_size(file_size):
       if file_size < 10_000_000:  # <10MB: small file
           return min(32768, file_size)  # 32KB
       elif file_size < 1_000_000_000:  # <1GB: medium
           return min(65536, file_size)  # 64KB
       else:  # >1GB: large
           return min(131072, file_size)  # 128KB
   ```
2. Measure reduction in footer bytes fetched / query

**File**: `opteryx/connectors/parquet_io/reader.py:28-29, 204-230`

---

#### 1.3: Connection Pool Saturation Testing
**Impact**: 5-10% on GCS/HTTP queries (queries 1, 5, 10, etc. on GCS)  
**Effort**: 2-3 hours (empirical testing)  
**Risk**: Low (purely config change)

**Current behavior**:
- Local: 48 workers (tuned from 64)
- GCS HEAD: 96 workers
- HTTP HEAD: 16 workers

**Opportunity**: Memory in notes suggests GCS optimal ~96 (confirmed). But:
- Local might benefit from **32-40 workers** on spinning disks (lower SSD assumption)
- HTTP might benefit from **32-48 workers** (matches typical server connection limits)
- Decode pool (cpu_count-2) is tuned; footer pool (64) is fixed

**Action**:
1. Run clickbench with local workload, test workers=[32, 40, 48, 56]
2. Run GCS subset (10-15 queries), confirm 96 is optimal or refine
3. For HTTP: test 16 vs 32 vs 48
4. Measure: task latency, queue depth, CPU utilization

**Files**:
- Local: `opteryx/connectors/io_systems/local_filesystem.py:17`
- GCS: `opteryx/connectors/io_systems/gcs_filesystem.py:23`
- HTTP: `opteryx/connectors/io_systems/http_filesystem.py:43`

---

#### 1.4: GCS Token Refresh Batching
**Impact**: <1% (micro-optimization, but reduces lock contention)  
**Effort**: 1 hour  
**Risk**: Very low

**Current behavior** (`gcs_filesystem.py:140-146`):
```python
if not self.client_credentials.valid:
    with self._token_lock:
        if not self.client_credentials.valid:
            self.client_credentials.refresh(...)
```

**Opportunity**: Under high concurrency, all threads may race to the token-refresh condition simultaneously. Can batch refreshes by checking at **token creation** time rather than access time:

**Action**:
1. Pre-compute token expiry time at initialization
2. Refresh proactively before expiry (e.g., 1 min before expiry) in a background task
3. Eliminates synchronous refresh wait in hot path

**File**: `opteryx/connectors/io_systems/gcs_filesystem.py:133-146`

---

### TIER 2: Medium Impact (Moderate Effort, Measurable Gains)

#### 2.1: Dynamic Memory Pool Sizing in C++ Pipeline
**Impact**: 3-8% on queries with very large row groups or many columns  
**Effort**: 4-6 hours  
**Risk**: Medium (C++/IPC changes)

**Current behavior** (`pool_reader.pyx:67`):
```python
pool = MemoryPool(pool_size=256*1024*1024, auto_resize=False)
```

**Opportunity**: 256MB is tuned for typical clickbench workloads, but:
- Very wide projections (20+ columns) may exceed this
- Single-column queries are wasting 256MB
- Dynamic sizing could adapt per-query

**Action**:
1. Add `estimate_pool_size(columns, row_group_count)` function that calculates needed memory based on column types and row counts
2. Set `auto_resize=True` with soft limit (256MB) → hard limit (512MB) to allow overflow gracefully
3. Measure pool utilization across clickbench queries

**File**: `opteryx/connectors/parquet_io/pool_reader.pyx:60-72`

---

#### 2.2: Decode Parallelism Threshold for Multi-Column Groups
**Impact**: 5-15% on queries with many columns per row group  
**Effort**: 3-5 hours  
**Risk**: Medium (Cython changes, testing required)

**Current behavior** (`reader.py:443-486`):
```python
if len(misses) == 1:
    # Single column: decode inline
    col_name, decoded = _decode_one(col_name, raw_buffers[0])
else:
    # Multiple columns: inline sequential decode (comment says pool causes serialization)
    for col_name, raw_buffer in zip(misses, raw_buffers):
        col_name, decoded = _decode_one(col_name, raw_buffer)
```

**Opportunity**: Comment acknowledges that row-group-level parallelism (via pool) would be slower because many RGs are in flight. But **per-column parallelism within a single row group** can help when:
- Many columns (10+) with varied decode times
- Decode is CPU-bound (PLAIN/dictionary, not compressed)

**Action**:
1. Add threshold: if `len(misses) > 4` AND sum(compressed_sizes) > 10MB:
   - Submit columns to decode pool with `max_workers=4` (not 64, to avoid contention)
2. Measure: decode latency for wide vs. narrow row groups
3. Benchmark on clickbench queries 1 (few columns), 6 (many columns)

**File**: `opteryx/connectors/parquet_io/reader.py:405-487`

---

#### 2.3: Row-Mask Filtering Pushdown
**Impact**: 2-8% on queries with row-masks (predicates that eliminate rows)  
**Effort**: 4-6 hours  
**Risk**: Medium (requires careful testing of correctness)

**Current behavior** (`reader.py:421`):
```python
decoded = decoder(raw_bytes_arg, _col_stats, row_mask)  # Row mask applied in C++ decoder
```

**Opportunity**: Row mask **is** pushed down to C++ decoder, which is good. But:
- Filter columns are decoded, then rows are masked
- Could skip decoding filtered-out rows entirely (early termination in parquet decoder)
- Requires exposing "early mask" to C++

**Action**:
1. Measure current behavior: how much of column decode is wasted (row_mask selectivity)
2. For very selective row masks (<10% selectivity), consider two-pass approach:
   - Pass 1: Decode filter columns only
   - Pass 2: Build row mask, decode projection columns with mask
3. This is complex; defer unless profiling shows row-mask overhead is significant

**File**: `opteryx/connectors/parquet_io/reader.py:415-421`

---

#### 2.4: Footer Cache Persistence Across Queries
**Impact**: 5-15% on repeated scans of same dataset  
**Effort**: 3-4 hours  
**Risk**: Low (cache coherency, not correctness)

**Current behavior** (`reader.py:204-310`):
```python
footer_cache: Optional[ParquetFooterBytesCache] = None  # Per-query in-memory cache
```

**Opportunity**: Clickbench runs 43 queries on the same dataset. Footer is immutable (file doesn't change). Could persist cache across queries:
- In memory: global cache with LRU eviction (10,000 entries = ~100MB for typical files)
- Or disk: SQLite cache keyed by (path, mtime)

**Action**:
1. Create global `_FOOTER_CACHE = {}` (LRU with 10k entries) in thread_pool_manager
2. Pass to fetch_footer() by default
3. Test on clickbench: measure footer bytes fetched (should plateau after first 2-3 queries)

**File**: `opteryx/connectors/parquet_io/reader.py:291-310`, `thread_pool_manager.py`

---

### TIER 3: Deeper Optimizations (Larger Effort, Higher Risk)

#### 3.1: Speculative Footer Prefetch
**Impact**: 3-5% on multi-file scans  
**Effort**: 6-8 hours  
**Risk**: Medium (requires careful implementation)

**Concept**: While decoding row group N from file A, speculatively fetch footer for file B in the background.

**Current behavior**:
1. Scan starts
2. Fetch footer for file 1 (blocking)
3. Dispatch row groups
4. Decode row groups
5. Fetch footer for file 2 (blocking)

**Opportunity**: Stage 2 can happen in parallel with stages 3-4.

**Action**:
1. In `ParquetReadNode._execute()`, pre-submit footer fetches for all files to footer pool before row-group loop
2. Use `as_completed()` pattern: yield row groups as they finish, fetch next file footer while yielding
3. Requires careful sequencing to avoid reading file footer before previous file is fully processed

**File**: `opteryx/operators/parquet_read/parquet_read.pyx`

---

#### 3.2: Compression Codec-Aware Decoding Pool Sizing
**Impact**: 2-5% on mixed compression (SNAPPY + ZSTD)  
**Effort**: 4-6 hours  
**Risk**: Medium (requires codec analysis)

**Concept**: ZSTD is slower than SNAPPY; columns with different codecs benefit from different parallelism levels.

**Action**:
1. Analyze footer to detect codec distribution (e.g., 80% ZSTD, 20% SNAPPY)
2. Adjust decode pool `max_workers` accordingly:
   - ZSTD-heavy: use full cpu_count
   - SNAPPY-heavy: reduce to cpu_count/2 (less CPU-bound, more I/O-bound)
3. Measure decode latency by codec on clickbench

**File**: `opteryx/connectors/parquet_io/thread_pool_manager.py:224-239`

---

#### 3.3: Adaptive Range Coalescing Based on Filesystem Type
**Impact**: 2-4% depending on filesystem  
**Effort**: 4-6 hours  
**Risk**: Low

**Concept**: Local disk and GCS have very different I/O characteristics:
- Local SSD: high throughput, low latency; large merges are good
- GCS: HTTP round-trip overhead; want to minimize request count
- HTTP: Similar to GCS but with connection pool limits

**Action**:
1. Parameterize coalescing: `_coalesce_ranges(ranges, filesystem_type)`
   - Local: gap=128, span=64MB
   - GCS: gap=256, span=96MB
   - HTTP: gap=512, span=48MB
2. Measure: range request count and total latency per filesystem

**File**: `opteryx/connectors/parquet_io/reader.py:146-188`

---

#### 3.4: Column Chunk Batching for Small Projections
**Impact**: 1-3% on queries with few columns  
**Effort**: 5-7 hours  
**Risk**: Medium

**Concept**: For queries reading <5 columns, batch them into a single read_ranges() call even across different row groups to amortize overhead.

**Current behavior**:
- Per row-group, per-column range reads
- Many small requests for narrow projections

**Opportunity**:
- Batch columns across 2-4 row groups in one read_ranges() call
- Decode separately to maintain correct semantics

**Action**:
1. In `ParquetReadNode`, collect pending columns to read
2. When batch reaches threshold (5+ columns), submit single read_ranges()
3. Deserialize into per-RG buffers
4. Measure on clickbench narrow-projection queries (1-3 columns)

**File**: `opteryx/operators/parquet_read/parquet_read.pyx`

---

#### 3.5: GCS HTTP/2 Multiplexing (If libcurl supports it)
**Impact**: 5-10% on GCS  
**Effort**: 2-3 hours (if available, otherwise 0)  
**Risk**: Very low

**Concept**: If `opteryx.compiled.http_client` (C++ wrapper) uses libcurl with HTTP/2 support, parallel range requests can multiplex on a single connection.

**Action**:
1. Check if HttpClient is built with HTTP/2 (libcurl -V → "HTTP2")
2. If yes, reduce `_MAX_PARALLEL_HEAD_REQUESTS` from 96 → 32 (HTTP/2 multiplexing reduces need)
3. Measure: connection count and latency on GCS queries

**File**: `opteryx/connectors/io_systems/gcs_filesystem.py:131`

---

## Benchmarking Strategy for ClickBench

### Baseline Run
```bash
make clickbench 2>&1 | tee baseline.log
# Captures: per-query time, total time, memory usage
```

### Instrumentation Points
Add these to measure each optimization:

1. **Coalescing efficiency**: Log in `_coalesce_ranges()`
   ```python
   total_original = sum(len(r) for r in ranges)
   total_merged = sum(len(r) for r in merged_ranges)
   logger.info(f"Coalesce: {total_original}B → {total_merged}B "
               f"({100*total_merged/total_original:.1f}%), gaps={gaps}, spans={spans}")
   ```

2. **Footer cache hits**: Log in `fetch_footer()`
   ```python
   if cached:
       logger.info(f"Footer cache HIT: {path}")
   else:
       logger.info(f"Footer cache MISS: {path}, prefetch={prefetch_size}B")
   ```

3. **Coalescing params**: Log gaps/spans distribution
   ```python
   logger.debug(f"Gap distribution: min={min_gap}, max={max_gap}, median={median_gap}")
   logger.debug(f"Span distribution: min={min_span}, max={max_span}, median={median_span}")
   ```

### Recommended Test Sequence
1. **Quick wins first** (1.1-1.4): Easy config tuning, immediate feedback
2. **Tier 2 next** (2.1-2.4): Moderate effort, measurable impact
3. **Tier 3 last** (3.1-3.5): Defer unless Tier 1-2 show continued gains

### Measurement Points
For each optimization:
- Total clickbench time (43 queries)
- Per-query breakdown (important to catch regressions)
- Memory usage (peak, avg)
- GIL contention (if profiling is added)

---

## Quick-Win Checklist

| ID | Optimization | Effort | Est. Impact | Risk | Priority |
|---|---|---|---|---|---|
| 1.1 | Range coalescing tuning | 1-2h | 2-5% | Low | ⭐⭐⭐ |
| 1.2 | Footer prefetch heuristic | 1h | 1-3% | V.Low | ⭐⭐⭐ |
| 1.3 | Pool saturation testing | 2-3h | 5-10% | Low | ⭐⭐⭐ |
| 1.4 | GCS token batching | 1h | <1% | V.Low | ⭐⭐ |
| 2.1 | Dynamic pool sizing | 4-6h | 3-8% | Medium | ⭐⭐ |
| 2.2 | Decode parallelism | 3-5h | 5-15% | Medium | ⭐⭐⭐ |
| 2.3 | Row-mask pushdown | 4-6h | 2-8% | Medium | ⭐⭐ |
| 2.4 | Footer cache persistence | 3-4h | 5-15% | Low | ⭐⭐⭐ |
| 3.1 | Speculative prefetch | 6-8h | 3-5% | Medium | ⭐⭐ |
| 3.2 | Codec-aware pool sizing | 4-6h | 2-5% | Medium | ⭐ |
| 3.3 | Adaptive coalescing | 4-6h | 2-4% | Low | ⭐⭐ |

---

## Caveats & Assumptions

1. **Baseline assumption**: Current clickbench runs complete successfully with no regressions
2. **ClickBench composition**: Queries vary in width (columns), depth (row groups), and filters
   - Narrow queries (<5 cols): benefit from opts 1.3, 2.4, 3.4
   - Wide queries (15+ cols): benefit from opts 2.2, 3.2
   - Filter-heavy queries: benefit from opts 2.3, 2.4
3. **Hardware**: Optimizations tuned for Apple Silicon (ARM NEON) and x86 (AVX2); verify on both
4. **Filesystem**: Local tests should run on actual SSD, not ramdisk

---

## Next Steps

1. **Establish baseline**: Run clickbench with current code, save per-query times
2. **Implement Tier 1**: Start with 1.1 (coalescing tuning) — highest ROI/effort ratio
3. **Measure after each change**: Track total time and per-query variance
4. **Iterate Tier 2**: Pick highest-impact options based on baseline findings
5. **Document results**: Update memory with findings and optimal tuning parameters

---

**Generated**: May 2026  
**Review Scope**: Opteryx IO stack (parquet_io, connectors, thread pools)  
**Benchmarking**: ClickBench 43-query suite on realistic workload  
**Status**: Ready for implementation
