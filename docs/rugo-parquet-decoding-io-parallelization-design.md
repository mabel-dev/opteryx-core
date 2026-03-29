# Rugo Parquet Decoding & IO Parallelization Design

**Objective**: Improve parquet IO throughput from current ~20% bandwidth utilization to 80-95% by combining single-threaded decode efficiency gains (memory + SIMD) with multi-CPU parallelization (page-level threading, native async HTTP, C++-based thread pools).

**Status**: Part 1 (Tier 1A-1E memory optimizations) ✅ COMPLETE. Tiers 2-4 and Part 2 planned.

---

## Part 1: Parquet Decoding Optimization (4 Tiers)

### Overview

The parquet decode pipeline (`DecodeColumnFromChunk` in `decode_column.cpp`) is single-threaded at the C++ level and dominated by memory allocation overhead. While Python-level parallelism exists (thread pools for inter-column/inter-file work), actual decoding runs sequentially. The telemetry (`telemetry.hpp`) identified `val_expand_s` (value expansion) as a hot phase.

**Four optimization layers**:
1. **Memory management** – eliminate per-element allocation (Tier 1A-1E) ✅ DONE
2. **SIMD acceleration** – vectorize hot loops (Tier 2A-2C)
3. **Intra-column page parallelism** – BS::thread_pool for work-stealing (Tier 3A-3B)
4. **Zero-copy and IPC** – direct buffer writes (Tier 4A-4B)

---

## Tier 1: C++ Memory Management (Completed)

**Impact**: ~15-30% improvement on value expansion phase. Zero API changes.

### 1A. Pre-reserve output vectors at column-chunk start ✅

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp` (lines 323-341)

**Status**: Implemented and tested. Pre-reserves `dict_indices`, type-specific vectors before page loop, eliminating per-page incremental reserve calls.

### 1B. Batch insert for dict_indices ✅

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp` (lines 522-540)

**Status**: Implemented. Created `batch_append_dict_indices()` lambda with single bounds-check pass (auto-vectorizable min/max) followed by bulk insert instead of per-element validate + push_back. Applies to all 5 dict-mode blocks (int32, int64, byte_array, float32, float64).

### 1C. memcpy for PLAIN fixed-width on little-endian ✅

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp` (lines 685-914)

**Status**: Implemented. Added `#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__` guards with memcpy for dict parsing and PLAIN fixed-width decoding in both ext_* paths and internal vector paths.

### 1D. SIMD-accelerated row-mask page-skip scan ✅

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp` (lines 401-421)

**Status**: Implemented. Replaced per-byte scan with word-at-a-time (8 bytes) memcpy + OR pattern with 1ms sleep fallback. Planned SIMD upgrade below in Tier 2.

### 1E. Row-mask decoded_row_mask memcpy ✅

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp` (line 417)

**Status**: Implemented. Replaced per-element push_back with resize + memcpy for decoded_row_mask.

**Telemetry Updates** ✅:
- Added `mask_filter_s` and `validity_bmp_s` accumulators to `telemetry.hpp`
- Updated `get_cpp_telemetry()` in `parquet_reader.pyx` to expose new fields

**Test Results**: Build succeeded, 14 parquet unit tests PASSED.

---

## Tier 2: SIMD Acceleration

**Impact**: ~10-25% on gather-heavy and filter-heavy workloads.

### 2A. SIMD dictionary gather (AVX2/NEON)

**New file**: `third_party/mabel/rugo/parquet/simd_gather.hpp`

**Implementation**:
- AVX2: Use `_mm256_i32gather_epi32` to gather 8 int32 dictionary values per instruction
- NEON: Provide scalar fallback (no native gather support)
- ARM advanced: Consider `_mm256_i64gather_epi64` for int64 dictionary keys
- Dispatch: Use existing `simd_dispatch.h` pattern from `decode_encodings.cpp`

**Apply to** (lines 514-528, 540-556, etc.):
```cpp
// Current pattern (scalar)
for (size_t i = 0; i < indices.size(); ++i) {
    result.int32_values.push_back(dict[indices[i]]);
}

// Target: SIMD gather
simd_dict_gather_int32(dict.data(), indices.data(), indices.size(), result.int32_values);
```

### 2B. SIMD row-mask compaction

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp` (lines 906-944)

**Implementation**:
- Replace per-element branch-and-push_back filter lambdas with SIMD stream compaction
- Use AVX2 shuffle tables + `_mm256_movemask_epi8` + popcount to compute output offsets
- Alternative: portable bit-packing/compress_store patterns (slower but broader hardware support)

**Example**:
```cpp
// Current: scalar filter
for (size_t i = 0; i < total; ++i) {
    if (row_mask[i]) output.push_back(values[i]);
}

// Target: SIMD compaction (AVX2)
simd_compress_int32(values.data(), row_mask.data(), total, output);
```

### 2C. SIMD type widening in Cython helpers

**File**: `third_party/mabel/rugo/parquet/parquet_reader.pyx` (lines 473, 576)

**New C++ functions** exposed via `.pxd`:
```cpp
void widen_int32_to_int64(const int32_t* src, int64_t* dst, size_t count);
// AVX2: _mm256_cvtepi32_epi64 processes 4 pairs per instruction

void widen_float32_to_float64(const float* src, double* dst, size_t count);
// AVX2: _mm256_cvtps_pd processes 4 floats per instruction
```

**Calls from**:
- `_make_int64_from_int32_vector` (line 473)
- `_make_float64_from_float32_vector` (line 576)

---

## Tier 3: Intra-Column Page Parallelism

**Impact**: ~20-50% on large column chunks with many pages. Most complex change.

### 3A. Page-parallel decode within DecodeColumnFromChunk

**File**: `third_party/mabel/rugo/parquet/decode_column.cpp`

**Strategy**: Split monolithic page loop (lines 328-887) into three phases:

1. **Pre-scan** (sequential, cheap): Walk cursor, parse page headers only. Record per-page metadata: offset, compressed_size, header_size, num_values, encoding. Compute per-page write offsets via prefix sum.

2. **Pre-allocate** (sequential): Resize all output vectors to exact total using pre-scan counts.

3. **Parallel decode** (concurrent): Each page task handles decompress + level decode + value decode, writing to its pre-allocated slice. Use BS::thread_pool for work-stealing.

**Example structure**:
```cpp
// Pre-scan phase
struct PageTask { size_t offset, size_t num_values, Encoding enc; ... };
std::vector<PageTask> pages;
size_t total_values = 0;
for (auto& page_hdr : ...) {
    pages.push_back({...});
    total_values += page_hdr.num_values;
}

// Pre-allocate
result.int32_values.reserve(total_values);
result.int32_values.resize(total_values);

// Parallel decode (only if pages.size() > 2)
if (pages.size() > 2) {
    PageDecodePool pool(num_threads);
    pool.decode_pages_parallel(pages, result);
} else {
    // Sequential fallback
}
```

**Complications**:
- Dictionary pages must be loaded first (sequential, already happens)
- Mixed dict/plain pages with interning (`unified_dict_map`) must be serial — but rare
- Each page task needs its own decompression buffer (currently shared/reused)
- Fallback to sequential when page count <= 2 (dispatch overhead)

### 3B. Use BS::thread_pool for inter-page parallelism (preferred)

**Files**:
- `third_party/mabel/rugo/parquet/decode_column.cpp`
- `third_party/mabel/rugo/parquet/thread_pool.hpp` (new)

**Implementation**:
```cpp
// thread_pool.hpp (new)
#include "BS_thread_pool.hpp"

class PageDecodePool {
    BS::thread_pool pool;  // work-stealing queue, lock-free
public:
    PageDecodePool(size_t num_threads) : pool(num_threads) {}

    void decode_pages_parallel(const std::vector<PageTask>& pages,
                               DecodeResult& result) {
        for (const auto& page : pages) {
            pool.push_task([&]() {
                decode_single_page(page, result);  // writes to pre-allocated slice
            });
        }
        pool.wait_for_tasks();  // blocks until all complete
    }
};
```

**Why BS::thread_pool over OpenMP**:
- True lock-free work-stealing (better cache locality on NUMA)
- Lower overhead: no pragma parsing, no compiler directive overhead
- Easier to reason about (explicit task submission vs implicit #pragma)
- Better interop with Cython nogil sections
- Smaller binary footprint (header-only, no runtime dependencies)
- Better control over thread count and scheduling

**Integration**:
- Instantiate at `DecodeColumnFromChunk` entry if `page_count > 2`
- Each page task is independent (no shared state beyond pre-allocated output buffers)
- Fallback to sequential if task submission overhead would exceed benefit

---

## Tier 4: Zero-Copy & IPC

### 4A. Zero-copy ext_* buffer path for non-nullable columns

**File**: `third_party/mabel/rugo/parquet/parquet_reader.pyx` (~line 1684)

**Current state**: `ext_int64`, `ext_float64`, `ext_int32`, `ext_float32` pointer API exists in `DecodeColumnFromChunk` but Cython code doesn't use it.

**Optimization**: For non-nullable columns (`max_definition_level == 0`):
1. Pre-allocate Draken vector buffer in Cython
2. Pass its raw pointer as ext_* parameter
3. C++ writes directly into Draken buffer — zero intermediate copies
4. Skip `_make_*_vector` conversion entirely

**Impact**: Eliminates one full memcpy per non-nullable fixed-width column.

### 4B. io_process_ring production hardening

**File**: `opteryx/connectors/parquet_io/io_process_ring.py`

**Current state**: Experimental, opt-in feature flag. Shared-memory queue with O(n) slot claiming overhead (~sub-millisecond, < 1% end-to-end impact).

**Production requirements**:
- Add worker health monitoring and automatic restart
- Add timeout handling for stuck slots
- Add graceful degradation to v2 scheduler on failure
- Replace busy-wait slot claiming with event-based signaling (optional: bitarray for O(1) free list)

---

## Part 2: Multi-CPU Parallelization for Bandwidth Saturation

### Core Problem

Current IO throughput utilizes only **~20% of available bandwidth**. The bottleneck is not single-threaded decoding, but rather **IO work cannot fan out across CPUs** to fully utilize parallel storage/network paths.

**Goals**:
1. Distribute IO reads across multiple CPUs (range read parallelism, page-level decoding)
2. Eliminate GIL contention in thread pool dispatch (replace ThreadPoolExecutor with C++-based pools)
3. Improve network concurrency (native async HTTP with libcurl)
4. Reduce IPC overhead for subprocess-based IO (optional: cpp-ipc)

**Expected outcome**: Move from ~20% → 80-95% bandwidth utilization for large bulk scans.

---

### Layer 1: HTTP Native Async with libcurl

**Goal**: Replace aiohttp/MinIO SDK with libcurl for unified async HTTP (S3, GCS, generic HTTP URLs)

**Files to create**:
- `opteryx/connectors/io_systems/http_filesystem.py` — OpteryxHttpFileSystem with async support
- `src/cpp/http_client.cpp` — libcurl wrapper (CURLM multi-handle)
- `src/cpp/http_client.h` — C++ API for `http_range_read(url, offset, length) → bytes`

**Implementation outline**:
- Wrap `CURLM` for multi-handle (event-driven async, no threads)
- HTTP Range headers for byte-range reads
- Connection pooling via CURLM handle
- Support sync and async paths via `curl_multi_socket_action()`
- Handle redirects, auth (Bearer tokens for GCS), timeouts

**Build integration**:
- Add `libcurl-dev` as optional dependency in `setup.py`
- Compile `http_client.cpp` conditionally (graceful fallback to aiohttp if curl unavailable)

**Advantages**:
- True async: eliminates thread overhead for network I/O
- Unified stack: S3, GCS, generic HTTP via single codepath
- Native connection pooling and keep-alive
- Better for high-concurrency (1000+ concurrent downloads)

---

### Layer 2: C++ Thread Pool for Parquet Range Reads

**Goal**: Replace ThreadPoolExecutor with BS::thread_pool for better cache locality and work-stealing

**Files to modify**:
- `third_party/mabel/parquet_io/reader.py` — Replace ThreadPoolExecutor dispatch
- `src/cpp/parquet_pool.cpp` (new) — BS::thread_pool wrapper
- `setup.py` — Add BS::thread_pool optional dependency

**Implementation outline**:
1. Integrate BS::thread_pool (header-only library)
2. Create `ThreadPool` wrapper in C++ (`src/cpp/thread_pool.h`)
3. Expose to Python via Cython/nanobind
4. Replace `_RANGE_POOL = ThreadPoolExecutor(max_workers=32)` with C++-based pool
5. All range reads dispatched from C++, no GIL release/acquire per task

**Benefits**:
- Work-stealing queue: better load balancing across cores
- Lock-free operations: lower contention
- NUMA-aware work distribution (if available)
- No Python object overhead per task

**Integration approach**:
- Move `_fetch_columns()` range read dispatch into C++
- Cython `nogil` sections for thread pool operations
- Preserve Python cancellation semantics (CancelledError)

---

### Layer 3: cpp-ipc for io_process_ring (Optional, defer for now)

**Current state**: Python multiprocessing.SharedMemory with O(n) linear slot scan.

**Evaluation**:
- cpp-ipc offers O(1) slot lookup via named shared memory
- Overhead: ring slot claiming is sub-millisecond, < 1% end-to-end (Parquet IO is 100ms+)
- Risk: Added complexity (C++ build, Boost dependency, ABI issues)

**Recommendation**: **DEFER** unless profiling shows ring saturation is a bottleneck.

**If pursuing**:
- Quick win: Add bitarray for O(1) free list in current Python impl (10 lines)
- Prototype: cpp-ipc ring with pybind11 bindings
- A/B test vs current, promote only if > 10% improvement

---

## Implementation Roadmap

| Phase | Tiers | Effort | Risk | Dependencies |
|-------|-------|--------|------|--------------|
| **Phase 1** (Done) | 1A-1E | 1 day | Very low | None |
| **Phase 2** | 2A-2C | 2-3 days | Low | simd_dispatch.h pattern, BS::thread_pool header |
| **Phase 3** | 3A-3B | 4-6 days | Medium | BS::thread_pool, understanding of page loop structure |
| **Phase 4** | 4A-4B | 2-3 days | Low-Medium | Draken vector API, io_process_ring architecture |
| **Phase 5** | Part 2 Layer 1 (libcurl HTTP) | 2-3 weeks | Medium | libcurl-dev, understanding of GCS/S3 auth |
| **Phase 6** | Part 2 Layer 2 (C++ thread pool) | 1-2 weeks | Medium | BS::thread_pool, Cython nogil sections |
| **Phase 7** | Part 2 Layer 3 (cpp-ipc, optional) | 3-4 weeks | Medium-high | Boost, pybind11, ring profiling data |

---

## Telemetry Extensions

**File**: `third_party/mabel/rugo/parquet/telemetry.hpp`

Add new thread_local accumulators:
```cpp
inline thread_local double page_prescan_s  = 0.0;   // Pre-scan phase (Tier 3A)
inline thread_local double simd_gather_s   = 0.0;   // Dictionary gather (Tier 2A)
inline thread_local double simd_compact_s  = 0.0;   // Row-mask compaction (Tier 2B)
inline thread_local int64_t pages_parallel = 0;     // Number of pages decoded in parallel
inline thread_local int64_t thread_spawns  = 0;     // Thread pool task spawns
```

Update `reset()` function to zero new fields.

**Python exposure**: Update `parquet_reader.pyx` `get_cpp_telemetry()` to return all fields.

---

## Verification Strategy

### Correctness
- Run existing test suite: `tests/unit/test_parquet_decoder_*.py` (14 tests currently PASS)
- Add new tests for:
  - Page-parallel decode matches sequential results
  - SIMD gather correctness (sanity test with small dict)
  - Zero-copy ext_* buffer path (memcmp against vector path)
  - HTTP range read correctness (checksum validation)

### Performance
- Use `bench_parquet_decoders_compare.py` and `get_cpp_telemetry()` to measure per-phase timing
- Measure before/after for:
  - Tier 1: Value expansion phase should drop ~15-30%
  - Tier 2: Dictionary gather + row-mask compact should add ~10-25%
  - Tier 3: Page parallelism on multi-page chunks should show near-linear scaling
  - Part 2: End-to-end bandwidth utilization should move from ~20% → 80-95%

### End-to-End
- Run ClickBench: `tests/performance/clickbench/clickbench.py` on full dataset
- Measure wall-clock time reduction across all query types
- Profile CPU/memory/network utilization via:
  - `perf stat` (CPU cycles, cache misses, branch misses)
  - `iotop` (IO utilization)
  - `nethogs` (network throughput)

### Safety & Fallbacks
- Verify fallback paths work when SIMD disabled via `OPTERYX_DISABLE_SIMD=1`
- Test on ARM hardware (NEON paths) if available
- Verify GIL release/acquire semantics in threading code
- Test io_process_ring graceful degradation on worker crash

---

## Critical Files Summary

| File | Tier(s) | Status |
|------|---------|--------|
| `third_party/mabel/rugo/parquet/decode_column.cpp` | 1-3 | 1A-1E done; 2A-2B, 3A-3B planned |
| `third_party/mabel/rugo/parquet/telemetry.hpp` | 1-3 | 1D-1E done; 3A extensions planned |
| `third_party/mabel/rugo/parquet/parquet_reader.pyx` | 1, 2C, 4A | 1D-1E done; 2C, 4A planned |
| `third_party/mabel/rugo/parquet/simd_gather.hpp` | 2A | Planned (new file) |
| `third_party/mabel/rugo/parquet/thread_pool.hpp` | 3B | Planned (new file) |
| `opteryx/connectors/parquet_io/io_process_ring.py` | 4B | Planned |
| `opteryx/connectors/io_systems/http_filesystem.py` | Part 2 Layer 1 | Planned (new file) |
| `src/cpp/http_client.cpp` | Part 2 Layer 1 | Planned (new file) |
| `src/cpp/http_client.h` | Part 2 Layer 1 | Planned (new file) |
| `src/cpp/parquet_pool.cpp` | Part 2 Layer 2 | Planned (new file) |
| `setup.py` | Part 2 | Planned (optional deps) |

---

## Success Criteria

✅ **Tier 1 (Complete)**:
- Build succeeds without warnings
- All parquet unit tests PASS
- `val_expand_s` telemetry shows ~15-30% reduction in value expansion phase

⏳ **Tier 2 (In Progress)**:
- Page-parallel decode preserves correctness (bitwise identical results)
- Multi-page chunks show near-linear scaling (90%+ parallel efficiency)
- Single-page chunks have zero overhead (fallback to sequential)

⏳ **Tier 3+ (Planning)**:
- End-to-end bandwidth utilization increases from ~20% to 80-95%
- ClickBench wall-clock time improves by 30-50% (measured on representative dataset)
- No regressions in cache hit rate or memory footprint

---

## References

- Design inspiration: [draken_hashing_optimizations.md](draken_hashing_optimizations.md) (SIMD dispatch patterns)
- Architecture: [parquet_reader.pyx](third_party/mabel/rugo/parquet/parquet_reader.pyx)
- Telemetry: [telemetry.hpp](third_party/mabel/rugo/parquet/telemetry.hpp)
- Similar work: [draken-native-engine-design.md](draken-native-engine-design.md), [draken-arrow-eradication-plan.md](draken-arrow-eradication-plan.md)
