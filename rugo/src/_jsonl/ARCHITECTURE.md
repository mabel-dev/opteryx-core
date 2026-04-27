# JSONL Reader Architecture

## Live Pipeline

```
read_jsonl() [_jsonl_reader.pyx]
  ↓
JsonlReader::next_chunk() [jsonl_reader.cpp]
  ├─ read_next_chunk_from_source()  — loads 64MB chunks from file
  └─ process_buffer()
     ├─ scan_structural_markers() [structural_scan.cpp] ← NEON single-pass (2300 MB/s)
     ├─ interpret_jsonl() [interpreter.cpp] ← builds FieldSpans from markers (1700 MB/s)
     └─ Returns: buffer + FieldSpans
  ↓
_build_vectors_from_chunks() [_jsonl_reader.pyx]
  └─ For each column:
     ├─ extract_column() [column_builder.cpp] ← extracts raw bytes as StringColumnResult
     └─ _string_vector_from_result() ← builds StringVector (slow per-row loop ✗)
  ↓
Returns StringVectors
```

## Live Code Files

| File | Purpose | Status |
|------|---------|--------|
| `structural_scan.cpp` | NEON marker scanning | ✓ Optimized (2300 MB/s) |
| `interpreter.cpp` | Build FieldSpans from markers | ✓ Fast (1700 MB/s) |
| `field_span.cpp` | Predicate filtering | ✓ Live |
| `jsonl_reader.cpp` | Chunk reading, buffering | ✓ Live |
| `column_builder.cpp` | Extract columns as strings | ✓ Live (but StringVectors only) |
| `_jsonl_reader.pyx` | Python API | ✓ Live |

## Dead Code

Functions that are compiled but **never called**:
- `parse_int64()` — Type parsing (was for old implementation)
- `parse_float64()` — Type parsing (was for old implementation)  
- `parse_bool()` — Type parsing (was for old implementation)
- `extract_string()` — String extraction (was for old implementation)
- `fast_parse_int64()` — Fast parsing (was for old implementation)
- `fast_parse_float64()` — Fast parsing (was for old implementation)
- `ColumnResult` struct — Typed column results (replaced by StringColumnResult)
- `merge_column()` — Chunk merging (marked as "Legacy", never called)

**Note**: These are safe to ignore—they're not in the hot path. The real bottleneck is `_string_vector_from_result()` which does per-row Python iteration on StringVectors instead of direct type conversion.

## Performance Breakdown (TPCH Lineitem, 148 MB)

| Stage | Time | Speed | Notes |
|-------|------|-------|-------|
| Structural scan | 57 ms | **2500-3200 MB/s** | ✓ NEON unrolled 32-byte blocks |
| Document mapping | 88 ms | 1700 MB/s | ✓ Single-pass state machine |
| Vector construction | **1500+ ms** | **0.1 MB/s** | ✗ **BLOCKER** (per-row Python loop) |
| **Total** | **1650+ ms** | **90 MB/s** | 99% of time in vector construction |

### Scan Optimization: 32-byte Unroll
- Pre-load all 9 marker comparands to avoid repeated `vdupq_n_u8` calls
- Process 2 × 16-byte blocks per iteration
- Reduces setup overhead, amortizes nibble mask extraction cost
- Result: **+10-40% speedup** (2300 → 2500-3200 MB/s)

## Next Optimization

The bottleneck is **type parsing/conversion** at 0.1 MB/s. Current code:
1. ✓ Extracts values as raw strings (fast)
2. ✗ Builds StringVector row-by-row in Python loop (slow)
3. Not implemented: Direct parsing to Int64Vector/Float64Vector/BoolVector

**Solution**: Implement `_build_typed_vectors_from_spans()` that:
- Takes FieldSpans + inferred schema
- Parses directly to typed vectors in a single C++ loop (no Python iteration)
- Bypasses StringVector intermediate

Expected speedup: **10-100x** (dependent on type diversity in data).
