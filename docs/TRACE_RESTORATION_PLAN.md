# Trace Event Restoration Plan

## Problem Statement

The IO layer's trace system is not recording all expected events for the three main IO operations:
1. **Main reads** - column chunk downloads from storage
2. **Decodes** - decompression and parsing of column chunks
3. **Buffering** - the phase between download completion and decode start

While some trace events are present in the code, several critical event types and emission points are missing or incomplete.

## Current State

### What's Working
- `download_start` and `download_complete` events for column reads (in both `reader.py` and `io_process_ring.py`)
- `decode_start` and `decode_complete` events for individual column decodes
- `decode_start` and `decode_complete` events at rowgroup level (in `io_process_ring.py`)
- Basic event recording infrastructure (`record_event`, `flush_all`)

### What's Missing
1. **`file_discovered` events** - No trace events when files are initially discovered/identified as candidates
2. **Buffering phase visibility** - No explicit `buffer_start` or `buffer_complete` events to track the gap between download and decode
3. **Footer read tracing** - Footer reads (metadata) are not traced
4. **Main rowgroup-level download events** - In local serial path, individual column downloads are traced but not aggregated as "rowgroup download"
5. **Consistency** - Some code paths emit events while others don't

## Implementation Plan

### Phase 1: Add File Discovery Tracing

**Location**: `opteryx/connectors/filesystem_connector.py` and connector implementations

When files are discovered during manifest building, emit:
```python
record_event("file_discovered", file_id=path, size_bytes=size, connector=connector_name)
```

### Phase 2: Add Footer Fetch Tracing

**Location**: `opteryx/connectors/parquet_io/reader.py` - `_read_footer_payload()` function

Wrap footer reads with:
```python
if OPTERYX_TRACE:
    record_event("download_start", file_id=path, component="footer", connector=connector)

# ... fetch footer ...

if OPTERYX_TRACE:
    record_event("download_complete", file_id=path, component="footer", bytes_received=len(footer_bytes), connector=connector)
```

### Phase 3: Add Explicit Buffering Phase Tracing

**Location**: `opteryx/connectors/parquet_io/io_process_ring.py`

When a column is downloaded but queued for decode (not immediately decoded):
```python
if OPTERYX_TRACE:
    record_event("buffer_start", file_id=path, component="column", rg_idx=rg_idx, column=name, bytes=len(raw_bytes))
```

When buffered column is about to be decoded:
```python
if OPTERYX_TRACE:
    record_event("buffer_complete", file_id=path, component="column", rg_idx=rg_idx, column=name)
```

### Phase 4: Consistency Audit

**Location**: All IO code paths

Ensure all of these code paths emit consistent events:
1. ✓ `fetch_columns()` in `reader.py` - DONE
2. ✓ `_iter_row_groups_local_serial()` in `reader.py` - DONE (individual columns only, not rowgroup aggregate)
3. ✓ `iter_row_groups_io_process_v2()` in `io_process_ring.py` - DONE (with buffering phase missing)
4. ✓ `async_read_column_task()` in `async_io.py` - DONE

## Expected Trace Event Flow

### Complete Timeline for One File

```
Timeline:
├─ file_discovered
│
├─ FOOTER PHASE
│  ├─ download_start (component="footer")
│  └─ download_complete (component="footer")
│
├─ ROWGROUP PROCESSING
│  └─ For each row group:
│     ├─ [optional] rowgroup_start
│     │
│     ├─ COLUMN DOWNLOAD PHASE
│     │  └─ For each column or column batch:
│     │     ├─ download_start (component="columns"|"column")
│     │     └─ download_complete (component="columns"|"column", bytes_received=X)
│     │
│     ├─ BUFFERING PHASE (Optional - only if there's delay before decode)
│     │  └─ For each column:
│     │     ├─ buffer_start (when queued for decode)
│     │     └─ buffer_complete (when decode starts)
│     │
│     ├─ DECODE PHASE
│     │  └─ For each column:
│     │     ├─ decode_start (component="column")
│     │     └─ decode_complete (component="column", rows_decoded=X)
│     │
│     └─ [optional] rowgroup_complete
```

## Code Locations to Modify

### 1. `opteryx/connectors/parquet_io/reader.py`

**Function**: `_read_footer_payload()` (lines 109-192)
- Add `download_start` event before footer read
- Add `download_complete` event after footer read

**Function**: `_iter_row_groups_local_serial()` (lines 852-1197)
- Currently has good column-level tracing
- May need to add optional rowgroup-level buffer phase if delays occur

### 2. `opteryx/connectors/parquet_io/io_process_ring.py`

**Function**: `_read_column_task()` (lines 542-588)
- Currently has download events ✓

**Function**: `_decode_column_task()` (lines 591-654)
- Currently has decode events ✓
- Add optional buffer phase tracking

**Function**: `_io_worker()` (lines 825-1454)
- When appending to `decode_pending`, emit `buffer_start` event
- When dequeuing from `decode_pending`, emit `buffer_complete` event

### 3. `opteryx/connectors/filesystem_connector.py`

- Add `file_discovered` events when manifest is built

### 4. `opteryx/connectors/parquet_io/async_io.py`

- Already has download/decode events ✓

## Benefits of Restoration

1. **Complete visibility** into IO waterfall from file discovery through decode completion
2. **Buffer phase analysis** - identify bottlenecks in buffering strategy
3. **Footer impact measurement** - see how much time metadata reads consume
4. **Consistent tracing** across all code paths (serial, parallel, async)
5. **Better performance profiling** - can now pinpoint where time is spent

## Testing Strategy

1. Run simple query with `OPTERYX_TRACE=1`
2. Validate all expected event types appear in trace
3. Verify timestamps are monotonic within each file's timeline
4. Check that buffer phase (if present) shows reasonable duration
5. Validate waterfall visualization includes all phases

## Backwards Compatibility

- Trace format remains unchanged
- New events are additions, not modifications to existing events
- Tools that only look for specific event types will continue working
- Tools expecting comprehensive tracing will benefit from the additions