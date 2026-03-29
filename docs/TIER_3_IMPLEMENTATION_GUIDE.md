# Tier 3A Implementation Guide: Parallel Page Decompression

## Current State

✅ Infrastructure complete:
- `PageTask` struct with row_mask skip detection
- `PageDecompressed` result structure
- `PreScanPages()` function with row_mask checking
- `PageDecodePool` thread pool (SimpleThreadPool alias)
- All tests passing

## Implementation Steps

### Step 1: Add Pre-Scan Call (5 minutes)

At line ~460 in `DecodeColumnFromChunk`, after loading dictionary, add:

```cpp
// Pre-scan all pages to collect metadata and detect row_mask skips
std::vector<PageTask> pages;
int32_t page_row_offset = 0;
{
  RUGO_TEL_START(_ps_t0);
  page_row_offset = PreScanPages(cursor, chunk_limit, row_mask, pages);
  RUGO_TEL_ACCUM(rugo_tel::prescan_s, _ps_t0);  // Add to telemetry
}

// Skip parallel decompression for small page counts (threshold: 2 pages)
bool use_parallel = pages.size() > 2;
```

### Step 2: Parallel Decompression Phase (10 minutes)

After pre-scan, before the main page loop, add:

```cpp
// Parallel decompression (Tier 3A)
std::vector<std::vector<uint8_t>> decompressed_pages;
decompressed_pages.resize(pages.size());

if (use_parallel && target_col->codec != 0) {
  PageDecodePool decomp_pool(4);  // Use 4 threads, or std::thread::hardware_concurrency()

  for (size_t page_idx = 0; page_idx < pages.size(); ++page_idx) {
    const PageTask& task = pages[page_idx];

    if (task.skip_page) {
      decompressed_pages[page_idx].clear();  // Mark as skipped
      continue;
    }

    // Submit decompression task
    decomp_pool.push_task([page_idx, &task, &decompressed_pages, target_col]() {
      try {
        auto codec = rugo::compression::CodecFromInt(target_col->codec);
        std::vector<uint8_t> buffer(task.uncompressed_size);
        rugo::compression::DecompressInto(
            task.compressed_data,
            task.compressed_size,
            task.uncompressed_size,
            codec,
            buffer);
        decompressed_pages[page_idx] = std::move(buffer);
      } catch (const std::exception& e) {
        // Mark decompression failure
        decompressed_pages[page_idx].clear();
      }
    });
  }

  decomp_pool.wait_for_tasks();  // Block until all decompression complete
}
```

### Step 3: Modify Main Loop to Use Pre-Decompressed Data (20 minutes)

Replace the current page loop (lines ~400-1100) with version that:

1. Iterates through pre-scanned `pages` vector instead of walking cursor
2. Skips decompression step (data already decompressed in parallel phase)
3. Uses `decompressed_pages[page_idx]` instead of `page_decompressed_data`

Key changes in pseudocode:

```cpp
for (size_t page_idx = 0; page_idx < pages.size(); ++page_idx) {
  const PageTask& page = pages[page_idx];

  // Skip pages with no selected rows (determined during pre-scan)
  if (page.skip_page) {
    ++result.pages_skipped;
    continue;
  }

  ++result.pages_decoded;

  // Use pre-decompressed data (from parallel phase or original)
  const uint8_t* data_ptr;
  size_t data_size;

  if (use_parallel && target_col->codec != 0) {
    // Use pre-decompressed buffer
    if (decompressed_pages[page_idx].empty()) {
      // Decompression failed
      break;
    }
    data_ptr = decompressed_pages[page_idx].data();
    data_size = decompressed_pages[page_idx].size();
  } else if (target_col->codec == 0) {
    // PLAIN encoding: no decompression needed
    data_ptr = page.compressed_data;
    data_size = page.compressed_size;
  } else {
    // Single-threaded decompression fallback (small page counts)
    // ... existing decompression code ...
  }

  // Rest of page decoding logic remains unchanged
  // (repetition levels, definition levels, value decoding)
  // All reuses existing code, just with pre-decompressed data
}
```

### Step 4: Add Telemetry (5 minutes)

In `telemetry.hpp`, add:

```cpp
double prescan_s = 0;
double decompress_parallel_s = 0;

// In functions for resetting/accumulating
```

This lets you measure parallelism effectiveness.

## Testing Strategy

1. **Correctness**: All existing tests should pass (no behavioral change, just parallelization)
2. **Performance**: Compare before/after on multi-page column chunks
   - Small chunks (1-2 pages): Should see negligible overhead
   - Medium chunks (5-10 pages): Should see 20-30% decompression speedup
   - Large chunks (20+ pages): Should see 50%+ decompression speedup (4x parallelism)
3. **Edge cases**:
   - Uncompressed data (codec 0) - should bypass parallel path
   - Single page - should use sequential path (overhead not worth it)
   - Failed decompression - should gracefully fallback
   - Row-mask filtering - should skip unnecessary pages

## Expected Impact

- **Decompression-heavy workloads** (compressed parquet): 20-50% improvement on decompression phase
- **Overall improvement** (on multi-page chunks): 10-20% wall-clock time reduction
- **No impact** on single-threaded paths or uncompressed data

## Future Extensions

After Step 1-4 work:

**Phase 2: Full Page Parallelization** (days 3-4):
- Extract value decoding logic into `decode_page_values()` helper
- Parallelize value decoding alongside decompression
- Use pre-allocated output buffers for lock-free writes
- Expected gain: Additional 20-30% on multi-page chunks

**Phase 3: Optimized Synchronization** (day 5, optional):
- Replace SimpleThreadPool with BS::thread_pool for work-stealing
- Add NUMA awareness
- Reduce mutex contention
- Expected gain: 5-10% additional improvement on large systems

## Debugging Tips

- Use telemetry (`get_cpp_telemetry()`) to verify parallel paths are active
- Add debug logging: `cerr << "Parallel decompression of " << pages.size() << " pages\n"`
- Validate decompressed sizes match uncompressed_size
- Check that row_mask skipping is correct (test with and without mask)
- Performance profile with Instruments.app to verify parallelism is working
