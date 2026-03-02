# IO Waterfall Tracing - Quick Start Guide

## Enable Tracing in Your Code

### Option 1: Via Environment Variables

```bash
export OPTERYX_TRACE=1
# enable tracing; events are kept in memory and accessed via
# ``session.trace()``
python your_query.py
```

### Option 2: Via Session Constructor

Tracing can also be enabled when you create a `Session`. You do **not** need to
provide a file path; events are kept in memory and can be accessed later via
`session.trace()`.

```python
from opteryx.query_session import Session

session = Session(sql="SELECT * FROM my_table")
# run your query
list(session.execute())

# grab the trace events and do what you like with them
events = list(session.trace())
with open("/tmp/trace.jsonl", "w", encoding="utf-8") as f:
    for ev in events:
        f.write(json.dumps(ev) + "\n")
# or pass `events` directly to a custom exporter
```

### Option 3: Direct Recording (Advanced)

```python
from opteryx.tracing.event_recorder import record_event, flush_all
from opteryx import config

config.OPTERYX_TRACE = True

record_event("my_event", file_id="test.parquet", duration_ms=100)
# events are stored in memory; ``flush_all()`` simply makes them
# available via ``session.trace()`` or to whatever exporter you choose
flush_all()
```

## Generate Visualization

Tracing no longer produces files automatically; if you want to use the
command‑line tools you must first export the events yourself (see Option 2
above).

### Generate HTML Waterfall Chart

```bash
python -m opteryx.tools.io_waterfall trace /tmp/trace.jsonl
# Chart saved to: /tmp/waterfall_<timestamp>.html
```
**With custom output:**
```bash
python -m opteryx.tools.io_waterfall trace /tmp/trace.jsonl --output /tmp/my_chart.html
```

### View Statistics Table

```bash
python -m opteryx.tools.io_waterfall stats /tmp/trace.jsonl
```

Output:
```
IO Trace Statistics
==================================================

  Total Files:             42
  Total Data:              1.2 GB
  Total Rows:              15,234

  Query Duration:          3.45 s
  Download Phase:          2.10 s
  Decode Phase:            1.35 s

  Avg Download/File:       50 ms
  Avg Decode/File:         32 ms
  Max Concurrent Downloads: 4
```

## Understanding the Visualization

### Color Legend
- **Blue bars:** Download phase (fetching data from storage)
- **Yellow overlay:** Buffer/parsing phase (time between download and decode)
- **Green completion:** Decode phase (decompression and parsing)

### Interactive Features
- **Hover:** Detailed metrics for each file
- **Zoom:** Click and drag to zoom into time range
- **Pan:** Use scrollbar to navigate timeline
- **Legend:** Click to toggle file visibility

## Performance Impact

When tracing is **enabled:**
- <0.1% overhead on typical queries
- Negligible impact on I/O-bound workloads
- ~90ns per event recorded

When tracing is **disabled** (default):
- **ZERO overhead** - comments remain inert
- No runtime cost checking
- Safe to leave `# TRACE:` comments in production code

## Trace File Format

Trace files are JSONLines (.jsonl) format - one JSON object per line:

```jsonl
{"type": "trace_session_start", "timestamp": 0, "session_id": "abc123", "query": "..."}
{"type": "file_discovered", "timestamp": 0.1, "file_id": "part0.parquet", "bytes_total": 1048576}
{"type": "download_start", "timestamp": 0.11, "file_id": "part0.parquet"}
{"type": "download_complete", "timestamp": 0.15, "file_id": "part0.parquet", "bytes_received": 1048576}
{"type": "decode_start", "timestamp": 0.15, "file_id": "part0.parquet"}
{"type": "decode_complete", "timestamp": 0.20, "file_id": "part0.parquet", "rows_decoded": 10000}
...
```

### Event Types

| Event | Fields | Meaning |
|-------|--------|---------|
| `trace_session_start` | session_id, query | Query started |
| `file_discovered` | file_id, bytes_total, connector | File identified |
| `download_start` | file_id, component, rg_idx, columns | Fetch begins (component may be footer/column-batch)
| `download_complete` | file_id, bytes_received | Fetch done |
| `decode_start` | file_id | Parse begins |
| `decode_complete` | file_id, rows_decoded | Parse done |

## Configuration Options

### Environment Variables

```bash
OPTERYX_TRACE=1                           # Enable/disable tracing (0=off, 1=on)
```

### Code Configuration

```python
from opteryx import config

config.OPTERYX_TRACE = True
```

### Advanced Tuning

Edit `opteryx/tracing/config.py` for advanced options:
- `buffer_size_per_thread`: Ring buffer size (default: 10,000 events)
- `flush_interval_ms`: Periodic flush interval (default: 5000ms)
- `writer_queue_max`: Max pending events (default: 100,000)
- `sample_rate`: Record N out of M events (default: 1.0 = all)

## Troubleshooting

### Trace File Not Created

- Check `OPTERYX_TRACE=1` is set
- tracing path variables were removed; use `session.trace()` instead
  only.  Prefer using `session.trace()` to grab events.
- If you relied on the old file output, call `session.trace()` and write the
  events yourself (see examples above).

### Chart Won't Open in Browser

- ECharts loaded from CDN - requires internet
- Check browser console for errors (F12 → Console tab)
- Some browsers block file:// access - use HTTP server instead:
  ```bash
  python -m http.server 8000 --directory /tmp
  # Open http://localhost:8000/waterfall.html
  ```

### Large Trace Files (MB+)

- Default buffer stores entire query trace in memory before flushing
- For very large queries, consider:
  - Reducing `buffer_size_per_thread`
  - Increasing `flush_interval_ms`
  - Using sample_rate < 1.0

## Example Workflows

### Quick Profile Session

```bash
# 1. Run with tracing
# (the file variable is legacy; tracing now lives in memory via session.trace())
OPTERYX_TRACE=1 python my_query.py

# 2. View stats
python -m opteryx.tools.io_waterfall stats /tmp/trace.jsonl

# 3. Generate chart
python -m opteryx.tools.io_waterfall trace /tmp/trace.jsonl

# 4. Open in browser
open /tmp/waterfall_<timestamp>.html
```

### Identify Slow Files

```bash
# View statistics to find bottlenecks
python -m opteryx.tools.io_waterfall stats /tmp/trace.jsonl

# Max Concurrent Downloads tells you parallelism
# Compare Download Phase vs Decode Phase times
# Look for files with large yellow (buffer) regions
```

### Compare Query Performance

```bash
# Run two queries with different configurations
OPTERYX_TRACE=1 python query.py
OPTERYX_TRACE=1 python query.py

# Compare stats
echo "=== Version 1 ==="
python -m opteryx.tools.io_waterfall stats /tmp/query_v1.jsonl

echo "=== Version 2 ==="
python -m opteryx.tools.io_waterfall stats /tmp/query_v2.jsonl

# Generate both charts and compare
python -m opteryx.tools.io_waterfall trace /tmp/query_v1.jsonl --output /tmp/v1.html
python -m opteryx.tools.io_waterfall trace /tmp/query_v2.jsonl --output /tmp/v2.html
```

## API Reference

### Event Recording

```python
from opteryx.tracing.event_recorder import record_event, flush_all, reset

# Record an event (when OPTERYX_TRACE=1)
record_event(
    event_type="download_complete",
    file_id="data.parquet",
    bytes_received=1024,
    duration_ms=100
)

# Flush events to disk (blocking, waits for write)
flush_all()

# Reset tracing system (testing only)
reset()
```

### TraceReader API

```python
from opteryx.tools.io_waterfall.reader import TraceReader

reader = TraceReader("/tmp/trace.jsonl")

# Iterate over events as they are parsed
for event in reader.events():
    print(event['type'], event['file_id'])

# Get session metadata
metadata = reader.metadata()
print(metadata['query'])

# Get file-level timelines
timelines = reader.file_timelines()
for file_id, timeline in timelines.items():
    print(f"{file_id}: {timeline['download_complete'] - timeline['download_start']}ms")

# Get computed statistics
stats = reader.statistics()
print(f"Total: {stats['total_files']} files, {stats['total_bytes']} bytes")
print(f"Concurrency: {stats['max_concurrent_downloads']}")
```

### Chart Generation

```python
from opteryx.tools.io_waterfall.generator import generate_waterfall_html

# Generate HTML chart
output_path = generate_waterfall_html(
    trace_file="/tmp/trace.jsonl",
    output_html="/tmp/chart.html"
)
print(f"Chart saved to {output_path}")
```

## Performance Tips

1. **Keep tracing enabled in development** - Zero cost when disabled (just comments)
2. **Disable for production** - Unless specifically profiling (set `OPTERYX_TRACE=0`)
3. **Use stats before charts** - Identifies issues faster
4. **Compare relative metrics** - Use statisticsfor trending, not absolute numbers
5. **Large queries** - Consider sampling (set `sample_rate=0.1` for 10% of events)

## Getting Help

- **Questions:** Check the design documentation in `/docs/io-waterfall-design/`
- **Issues:** Review test cases in `tests/unit/test_tracing/` and `tests/integration/`
- **Examples:** See integration tests for end-to-end usage patterns

---

**Last Updated:** December 2024
