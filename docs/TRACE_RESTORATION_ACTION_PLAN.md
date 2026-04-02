# IO Stack Trace Restoration - Action Plan & Verification Guide

## Overview

This document provides a complete action plan for verifying and completing the IO stack trace restoration implementation. The trace system now captures comprehensive events for file discovery, buffering phases, and main read operations.

## Implementation Status

### ✅ COMPLETED (80%)

1. **io_process_ring.py** - Main IO scheduler
   - ✅ file_discovered events after footer parse (line ~1000)
   - ✅ buffer_start events when columns queued (line ~1380)
   - ✅ buffer_complete events before decode (line ~1250)

2. **reader.py - Local Serial Path** - Synchronous local storage fast path
   - ✅ file_discovered events after footer (line ~920)
   - ✅ buffer_start/complete for combined reads (lines ~1045-1055)
   - ✅ buffer_start/complete for individual reads (lines ~1100-1110)
   - ✅ buffer_complete in decode closure (line ~1165)

3. **async_io.py** - Async HTTP support
   - ✅ buffer_start after download_complete (line ~115)
   - ✅ buffer_complete after buffer_start (line ~130)

### ⏳ PENDING (20%)

1. **reader.py - fetch_columns()** - Direct column fetching
   - ⏳ buffer_start/complete events in _decode_one closure
   - Impact: Low (used mainly for cache hits and direct column operations)

2. **Connector Layer** - File discovery at manifest building
   - ⏳ file_discovered events at filesystem_connector level
   - Impact: Nice-to-have (footer parse already emits this)

## Verification Steps

### Step 1: Code Compilation Check

```bash
cd opteryx-core

# Verify no syntax errors
python -m py_compile opteryx/connectors/parquet_io/{reader,io_process_ring,async_io}.py

# Expected output: (no errors)
```

### Step 2: Import Verification

```bash
python -c "
from opteryx.connectors.parquet_io import reader, io_process_ring, async_io
from opteryx.tracing import record_event
from opteryx import config
print('✅ All imports successful')
print(f'OPTERYX_TRACE={config.OPTERYX_TRACE}')
print(f'OPTERYX_TRACE_SAMPLE_RATE={config.OPTERYX_TRACE_SAMPLE_RATE}')
"

# Expected output:
# ✅ All imports successful
# OPTERYX_TRACE=False
# OPTERYX_TRACE_SAMPLE_RATE=1.0
```

### Step 3: Quick Trace Test

```bash
# Run the test script that was created
python scratch/test_trace_events.py

# Expected output:
# ======================================================================
# IO Stack Trace Event Verification
# ======================================================================
# 
# 1. Creating session and executing query...
#    ✓ Query executed successfully
# 
# 2. Collecting trace events...
#    ✓ Collected N trace events
# 
# ... (detailed verification) ...
# 
# ✅ ALL TRACE EVENT TESTS PASSED
```

### Step 4: Manual Trace Inspection

```python
from opteryx import config
config.OPTERYX_TRACE = True

import opteryx
session = opteryx.session()

# Execute a simple query
results = session.execute_to_morsels(
    "SELECT COUNT(*) FROM 'testdata/generated/orders.parquet'"
)
for _ in results:
    pass

# Inspect events
traces = session.trace()
from collections import Counter, defaultdict

print(f"Total events: {len(traces)}")

# Event type breakdown
event_types = Counter(e.get("type") for e in traces)
print("\nEvent Types:")
for etype, count in sorted(event_types.items()):
    print(f"  {etype:25s}: {count:3d}")

# Per-file breakdown
files = defaultdict(list)
for event in traces:
    file_id = event.get("file_id")
    if file_id:
        files[file_id].append(event)

print(f"\nFiles Traced: {len(files)}")
for file_id in sorted(files.keys()):
    print(f"  {file_id}: {len(files[file_id])} events")

# Verify required events
required = {"file_discovered", "buffer_start", "buffer_complete"}
found = set(event_types.keys())
missing = required - found
if missing:
    print(f"\n❌ Missing: {missing}")
else:
    print(f"\n✅ All required event types present")
```

## Expected Event Distribution

### Single File Scan (typical case)
- file_discovered: 1
- download_start (footer): 1
- download_complete (footer): 1
- download_start (columns): varies
- download_complete (columns): varies
- buffer_start: ~2-10 (per column)
- buffer_complete: ~2-10 (per column)
- decode_start: ~2-10 (per column)
- decode_complete: ~2-10 (per column)

**Total: 30-50 events per file**

## Configuration Options

### Enable Tracing

```bash
# Via environment variable
export OPTERYX_TRACE=1
python your_query.py

# Via code
from opteryx import config
config.OPTERYX_TRACE = True
```

### Sampling (for reduced overhead)

```bash
# Sample 10% of files
export OPTERYX_TRACE=1
export OPTERYX_TRACE_SAMPLE_RATE=0.1
```

## Integration Tests

### Run Quick Test Suite

```bash
make c    # Quick recompile
make t    # Quick test suite
```

### Run Full Test Suite

```bash
make test  # Full regression suite
```

### Expected Results

- All tests should pass
- No new failures introduced
- Performance should be unchanged when OPTERYX_TRACE=False (default)

## Trace Event Validation Checklist

- [ ] **Chronological Ordering**: Events for each file are in time order
- [ ] **Phase Ordering**: download → buffer → decode
- [ ] **Pairing**: Each buffer_start has corresponding buffer_complete
- [ ] **Coverage**: All files have file_discovered events
- [ ] **Completeness**: All expected event types present
- [ ] **Field Presence**: Required fields in each event
- [ ] **No Duplicates**: Each event has unique timestamp + type + file_id combo

## Performance Validation

### Overhead Measurement

```python
import time
from opteryx import config

# Without tracing
config.OPTERYX_TRACE = False
start = time.perf_counter()
# Run query
duration_no_trace = time.perf_counter() - start

# With tracing
config.OPTERYX_TRACE = True
start = time.perf_counter()
# Run same query
duration_with_trace = time.perf_counter() - start

overhead_pct = ((duration_with_trace - duration_no_trace) / duration_no_trace) * 100
print(f"Overhead: {overhead_pct:.1f}%")

# Expected: <1% for typical queries
```

## Documentation Updates Needed

- [x] docs/TRACE_RESTORATION_PLAN.md - Created ✅
- [x] docs/TRACE_RESTORATION_SUMMARY.md - Created ✅
- [x] docs/TRACE_RESTORATION_COMPLETE.md - Created ✅
- [x] docs/TRACE_RESTORATION_ACTION_PLAN.md - This file ✅
- [ ] Update docs/io-waterfall-design/README.md with implementation status
- [ ] Add implementation notes to CHANGELOG

## Remaining Optional Work

### 1. Complete fetch_columns() Tracing

**File**: `opteryx/connectors/parquet_io/reader.py` lines 385-475

**What to add**:
```python
# In _decode_one closure, before decode_start:
if _cfg.OPTERYX_TRACE:
    _kwargs = {
        "file_id": path,
        "component": "column",
        "rg_idx": rg_idx,
        "column": col_name,
    }
    if connector:
        _kwargs["connector"] = connector
    record_event("buffer_complete", **_kwargs)
```

**And around the column read**:
```python
# After download_complete (before _decode_one calls)
if _cfg.OPTERYX_TRACE:
    for col_name in misses:
        kwargs = {
            "file_id": path,
            "component": "column",
            "rg_idx": rg_idx,
            "column": col_name,
        }
        if connector:
            kwargs["connector"] = connector
        record_event("buffer_start", **kwargs)
```

### 2. Add Connector-Level File Discovery

**File**: `opteryx/connectors/filesystem_connector.py`

**What to add**:
```python
# When building manifest, after identifying files:
if _cfg.OPTERYX_TRACE:
    for file_path in discovered_files:
        record_event("file_discovered", 
                    file_id=file_path, 
                    connector="local",
                    size_bytes=file_size_if_known)
```

## Deployment Checklist

- [ ] All Python files compile
- [ ] Quick test suite passes (make t)
- [ ] Full test suite passes (make test)
- [ ] Manual trace test shows all event types
- [ ] Waterfall visualization includes buffer phase
- [ ] Performance overhead <1%
- [ ] Documentation updated
- [ ] Optional enhancements completed (if desired)
- [ ] Code reviewed
- [ ] Merged to main branch

## Troubleshooting

### No trace events recorded

**Check**:
1. Is OPTERYX_TRACE=1?
2. Is session.trace() being called after execute_to_morsels()?
3. Are results being consumed (iterating through morsels)?

### Missing specific event types

**Check**:
1. Are you using the right scheduler? (io_process_ring vs local_serial)
2. Is the file large enough to have multiple columns?
3. Is OPTERYX_TRACE_SAMPLE_RATE filtering out the file?

### Events out of order

**Check**:
1. Timestamps should be monotonically increasing
2. Check system clock for skew
3. Verify local time vs UTC in timestamps

## Files Modified

| File | Lines | Status |
|------|-------|--------|
| opteryx/connectors/parquet_io/io_process_ring.py | ~50 | ✅ Done |
| opteryx/connectors/parquet_io/reader.py | ~80 | ✅ Done |
| opteryx/connectors/parquet_io/async_io.py | ~30 | ✅ Done |
| scratch/test_trace_events.py | New | ✅ Created |
| docs/TRACE_RESTORATION_PLAN.md | New | ✅ Created |
| docs/TRACE_RESTORATION_SUMMARY.md | New | ✅ Created |
| docs/TRACE_RESTORATION_COMPLETE.md | New | ✅ Created |

## Quick Reference Commands

```bash
# Verify compilation
python -m py_compile opteryx/connectors/parquet_io/{reader,io_process_ring,async_io}.py

# Test trace events
python scratch/test_trace_events.py

# Quick test
make c && make t

# Full test
make test

# Generate trace file and view waterfall
export OPTERYX_TRACE=1
python my_query.py > trace.jsonl
python -m io_waterfall trace trace.jsonl
```

## Success Criteria

✅ **Complete Success When**:
1. All trace event types present in output
2. Events properly ordered chronologically
3. Buffer phase visible between download and decode
4. No performance degradation
5. All tests passing
6. Backwards compatibility maintained

## Next Actions

1. **Immediate**:
   - Run `python scratch/test_trace_events.py`
   - Run `make c && make t`
   - Review trace output

2. **Short term**:
   - Complete optional fetch_columns() tracing
   - Run full test suite: `make test`
   - Validate waterfall visualization

3. **Medium term**:
   - Performance benchmarking
   - Documentation updates
   - Merge to production

## Support & References

- **Design Doc**: docs/io-waterfall-design/02-data-model.md
- **Quick Start**: docs/io-waterfall-design/QUICKSTART.md
- **Tracing Module**: opteryx/tracing/event_recorder.py
- **Config**: opteryx/config.py (OPTERYX_TRACE, OPTERYX_TRACE_SAMPLE_RATE)

---

**Status**: ✅ 80% Complete - Ready for testing and integration

All critical code changes implemented. Pending optional enhancements can be added independently.