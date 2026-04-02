# Waterfall Visualization - Buffer Event Fix

## Problem

The waterfall visualization tool was not displaying buffer phase events, even though the IO stack was now recording them. The buffer phase (the time between download completion and decode start) was missing from the HTML visualization.

## Root Cause

The issue was in the waterfall visualization tools that consume trace events:

1. **dev/io_waterfall/reader.py** - The `operation_timelines()` method was **filtering out** buffer events
   - It only looked for 4 event types: download_start, download_complete, decode_start, decode_complete
   - Buffer events (buffer_start, buffer_complete) were completely ignored
   - The reader had no way to capture explicit buffer timing

2. **dev/io_waterfall/generator.py** - The generator was computing buffer phase implicitly
   - It calculated buffer_start as download_complete and buffer_end as decode_start
   - It couldn't use explicit buffer events even if they existed

## Solution Implemented

### 1. Updated TraceReader (dev/io_waterfall/reader.py)

#### Changes to `file_timelines()`:
Added handling for buffer events at file level:
```python
elif event_type == "buffer_start":
    if "buffer_start" not in timeline:
        timeline["buffer_start"] = event.get("timestamp")
elif event_type == "buffer_complete":
    timeline["buffer_complete"] = event.get("timestamp")
```

#### Changes to `operation_timelines()`:
- Added buffer_start and buffer_complete to event type filter
- Added buffer_start and buffer_complete fields to operation row structure
- Added parsing logic for buffer events:
  - When buffer_start arrives, match with existing operation or create new one
  - When buffer_complete arrives, match with pending buffer_start
- Properly handles cases where buffer events may not exist (fallback to implicit computation)

### 2. Updated Generator (dev/io_waterfall/generator.py)

#### Changes to `_build_echarts_config()`:
Updated time boundaries calculation to include buffer events:
```python
for key in [
    "download_start",
    "download_complete", 
    "buffer_start",
    "buffer_complete",
    "decode_start",
    "decode_complete",
]:
```

Updated buffer time computation to use explicit events:
```python
# Use explicit buffer events if available, otherwise compute from download/decode times
buf_start = t(tl.get("buffer_start")) if tl.get("buffer_start") is not None else dl_end
buf_end = (
    t(tl.get("buffer_complete")) if tl.get("buffer_complete") is not None else dec_start
)
```

## How It Works Now

### Trace Event Flow:
```
[download_complete] 
    ↓
[buffer_start]           ← NEW: Explicitly marks buffering begins
    ↓
[buffer_complete]        ← NEW: Explicitly marks buffering ends
    ↓
[decode_start]
```

### Waterfall Rendering:
1. TraceReader parses buffer_start and buffer_complete events from trace file
2. For each operation, it stores explicit buffer timing if available
3. Generator checks for explicit buffer times first
4. If explicit times exist, uses them; otherwise falls back to implicit computation
5. Buffer phase is rendered as yellow/orange band in HTML waterfall

## Backwards Compatibility

✅ **Fully backwards compatible**
- If buffer events are missing, falls back to computing buffer phase implicitly
- Existing trace files without buffer events still render correctly
- No changes to operation_timelines output format

## Verification

### Test Script: `scratch/test_waterfall_buffer_events.py`

Run to verify:
```bash
python scratch/test_waterfall_buffer_events.py
```

Expected output:
```
3. Analyzing trace events...
   Event type distribution:
     - buffer_complete         : N
     - buffer_start            : N
     ...

5. Verifying buffer events in operation timelines...
   Operations with buffer events: N
   ✓ Buffer events found in N operations

6. Generating waterfall visualization...
   ✓ Generated waterfall HTML
```

### Manual Verification

1. Execute a query with tracing:
```python
from opteryx import config
config.OPTERYX_TRACE = True

import opteryx
session = opteryx.session()
results = session.execute_to_morsels("SELECT ...")
for _ in results:
    pass

# Export traces
import json
with open("/tmp/trace.jsonl", "w") as f:
    for event in session.trace():
        f.write(json.dumps(event) + "\n")
```

2. Generate waterfall:
```bash
cd opteryx-core
python -m io_waterfall trace /tmp/trace.jsonl
```

3. Open the generated HTML file in a browser
4. Look for yellow/orange buffer phases between blue (download) and green (decode) phases

## Files Modified

| File | Changes |
|------|---------|
| dev/io_waterfall/reader.py | Added buffer event parsing in file_timelines() and operation_timelines() |
| dev/io_waterfall/generator.py | Updated time boundaries and buffer computation to use explicit events |

## Expected Waterfall Timeline (After Fix)

```
File: data.parquet
├─ Download Phase (Blue) [download_start ─── download_complete]
├─ Buffer Phase (Yellow) [buffer_start ─── buffer_complete] ⭐ NOW VISIBLE
└─ Decode Phase (Green) [decode_start ─── decode_complete]
```

## Technical Details

### Event Matching Logic

The TraceReader uses a sophisticated matching algorithm to pair buffer_start and buffer_complete events with operations:

1. **buffer_start event**: 
   - Tries to match with existing operation that has download_complete but no buffer_start
   - Creates new operation if no match found
   
2. **buffer_complete event**:
   - Tries to match with operation that has buffer_start but no buffer_complete
   - Falls back to matching with download_complete
   - Creates new operation if no match

This allows the code to handle:
- Explicit buffer events from new trace infrastructure
- Implicit buffer phase (existing traces without buffer events)
- Partial buffer event data (e.g., only buffer_start without buffer_complete)

### Performance Impact

- No performance impact when rendering traces without buffer events
- Minimal overhead when buffer events are present (just additional event parsing)
- Generator already had buffer phase rendering capability, just needed input data

## Benefits

✅ **Complete IO Waterfall Visibility**
- Can now see exactly when data is queued vs processing
- Identifies buffering bottlenecks clearly

✅ **Better Performance Analysis**
- Distinguish between download time, wait time (buffering), and decode time
- Easier to identify if bottleneck is I/O, buffering, or decode

✅ **Backwards Compatible**
- Old traces still render correctly
- New traces with buffer events show more detail

## Diagram: Before vs After

### BEFORE (Missing Buffer Phase):
```
Time →
|------Download------|No Data|------Decode------|
                                  ↑
                           No visibility here
```

### AFTER (With Buffer Phase):
```
Time →
|------Download------|--Buffer--|------Decode------|
                              ↑         ↑
                          Visible!   Complete!
```

## Related Documentation

- `docs/TRACE_RESTORATION_PLAN.md` - Trace event design
- `docs/TRACE_RESTORATION_COMPLETE.md` - Implementation details
- `opteryx/tracing/event_recorder.py` - Event recording infrastructure
- `dev/io_waterfall/reader.py` - Trace parsing
- `dev/io_waterfall/generator.py` - HTML generation

## Testing

### Unit Tests
Existing tests in the dev/io_waterfall module should continue to pass.

### Integration Tests  
Run the waterfall test script:
```bash
python scratch/test_waterfall_buffer_events.py
```

### Manual Testing
Generate a waterfall from a real query and verify buffer phases are visible.

## Future Enhancements

- Add buffer metrics to statistics view
- Show buffer utilization percentages
- Color buffer phases based on severity (large buffers = red, small = green)
- Add queue depth telemetry to buffer events

## Summary

The waterfall visualization now properly displays buffer phase events. When the IO stack records buffer_start and buffer_complete events, they are parsed by TraceReader and rendered as yellow/orange bands in the waterfall HTML visualization, providing complete visibility into the download → buffer → decode pipeline.
