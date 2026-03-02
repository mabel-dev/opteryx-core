# IO Waterfall Visualization System - Design Documentation

Complete design specification for tracking and visualizing IO layer operations with a Chrome DevTools-like waterfall chart.

## Overview

This system provides visibility into file I/O operations during Opteryx query execution. Each file's download, buffering, and decode phases are recorded in a trace file that can be visualized as an interactive waterfall chart, showing:

- **Download phase** (blue): Network request to data arrival
- **Buffer phase** (yellow): Data in memory, waiting to be processed  
- **Decode phase** (green): Parsing and decompression

By visualizing all files' timelines together, you can see concurrency patterns, identify bottlenecks, and understand performance characteristics.

## How to Use These Documents

### For Quick Understanding
Start with [00-quick-reference.md](00-quick-reference.md) - one-page overview of the entire system with key decisions and summary.

### For Design Review
Read in this order:
1. [01-overview.md](01-overview.md) - Architecture and goals
2. [02-data-model.md](02-data-model.md) - Event types and schema
3. [03-collection-strategy.md](03-collection-strategy.md) - How to capture with minimal overhead
4. [04-storage-format.md](04-storage-format.md) - JSONLines trace format
5. [05-visualization.md](05-visualization.md) - Chart generation and rendering
6. [06-implementation-roadmap.md](06-implementation-roadmap.md) - Phased implementation plan

### For Implementation
Start with [06-implementation-roadmap.md](06-implementation-roadmap.md) which details:
- What to build in each phase
- File structure and organization
- Test strategy
- Risk mitigation

## Key Design Principles

1. **Zero Query Impact** - Tracing is opt-in and adds <0.1% overhead
2. **Low Memory** - Thread-local circular buffers, no unbounded allocations
3. **Simple Format** - JSONLines for human readability and easy parsing
4. **Post-Query Visualization** - No real-time overhead, generate charts after query completes
5. **No Dependencies** - Core tracing uses only Python stdlib

## System Architecture

```
Query Engine
    ↓ (records events)
Thread-Local Ring Buffers
    ↓ (events retained in memory)
Python Visualization Tool  ← trace exported by client code
# (the engine does not automatically write a JSONLines file)
    ↓ (generate)
Interactive HTML Waterfall Chart
```

## Quick Facts

| Aspect | Detail |
|--------|--------|
| **Events Per File** | 5 (discovered, download_start, download_complete, decode_start, decode_complete) |
| **Memory Per Thread** | ~300KB (10,000-event ring buffer) |
| **Overhead Per Event** | ~90 nanoseconds |
| **Format** | JSONLines (one event per line) |
| **Visualization** | ECharts-based interactive HTML |
| **Configuration** | `OPTERYX_TRACE=1` enables tracing; file variable ignored (legacy) |
| **CLI** | `python -m opteryx.tools.io_waterfall <trace_file>` |

## Implementation Phases

| Phase | Duration | Components | Status |
|-------|----------|------------|--------|
| **1** | 1-2 weeks | Event recorder, ring buffer, trace writer, instrumentation | Design Only |
| **2** | 1-2 weeks | TraceReader, chart generator, HTML template, CLI | Design Only |
| **3** | 1 week | Documentation, performance testing, polish | Design Only |
| **4** | Optional | Advanced features (sampling, export formats, statistics) | Design Only |

## Usage Example (Post-Implementation)

```bash
# Enable tracing for a query (trace is kept in memory)
export OPTERYX_TRACE=1
opteryx query "SELECT * FROM large_table"

# If you want to visualize the trace you must export it yourself to a
# file and then run the CLI tool on that file:
#   python -m opteryx.tools.io_waterfall /path/to/exported_trace.jsonl
```
## Visualization Example

```
Timeline →
file.parquet.1   ███░░░░░░░███░░░░░░░░████████████
file.parquet.2      ███░░░░░░░███░░░░░░░███████████
file.parquet.3         ███░░░░░░░███░░░░████████████
file.parquet.4            ███░░░░░████████

Legend:
███ = Downloading (Blue)   | S3 → Buffer
░░░ = Buffering (Yellow)   | Buffer → Decoder
███ = Decoding (Green)     | Parsing/Decompression

You can see:
- Files 1-3 download in parallel
- File 4 only starts when memory available
- Decoding happens during other downloads
- Peak memory = max yellow+blue bars at any moment
```

## Design Trade-offs Explained

### Why Ring Buffers Instead of Dynamic Arrays?
Fixed-size ring buffers prevent unbounded memory growth and avoid allocations in the hot path. When full, new events overwrite oldest, but only latest events matter for analysis.

### Why JSONLines Instead of Binary?
Simpler to debug (human-readable), can use standard tools (grep, jq), and still achieves good compression with gzip. Binary would be ~2x smaller but adds parsing complexity.

### Why Post-Query Visualization Instead of Real-Time?
Real-time charting would require either:
- Network overhead (send events to server)
- Threading overhead (render while executing)

Post-query avoids both: generate chart after query finishes when performance doesn't matter.

### Why ECharts Instead of D3?
ECharts has built-in zoom/pan for large datasets (10K+ bars), good interactivity, and less boilerplate. D3 offers more control but requires more code.

## File Structure (Post-Implementation)

```
opteryx/
  tracing/                    (NEW - Phase 1)
    __init__.py
    event_recorder.py         Core recording API
    ring_buffer.py            Circular queue
    trace_writer.py           Async file writer
    config.py                 Configuration
    events.py                 Event definitions
  
  tools/
    io_waterfall/             (NEW - Phase 2)
      __init__.py
      __main__.py             CLI entry point
      reader.py               Parse trace files
      generator.py            Generate HTML
      templates/
        waterfall.html        Base template

docs/io-waterfall-design/     (THIS DIRECTORY)
  00-quick-reference.md
  01-overview.md
  02-data-model.md
  03-collection-strategy.md
  04-storage-format.md
  05-visualization.md
  06-implementation-roadmap.md
  README.md                   (this file)

tests/
  unit/test_tracing/          (NEW)
  integration/test_io_waterfall/  (NEW)
```

## Instrumentation Checklist

5 `record_event()` calls needed in these locations:

- [ ] Connector.discover_files() → `file_discovered`
- [ ] Connector.fetch_start() → `download_start`  
- [ ] Connector.fetch_end() → `download_complete`
- [ ] ParquetReader.decode_start() → `decode_start`
- [ ] ParquetReader.decode_end() → `decode_complete`

## Next Actions

1. **Get feedback on this design** - Any changes to event types, format, or approach?
2. **Finalize the data model** - Are 5 events sufficient? Any missing dimensions?
3. **Review performance assumptions** - Is <0.1% overhead acceptable?
4. **Start Phase 1 implementation** - Build the core recording infrastructure
5. **Iterate based on real data** - Adjust after first instrumentation

## Questions to Discuss

1. Should trace files be enabled by default (opt-out) or disabled (opt-in)?
2. Do we want per-connector traces or one trace file per query?
3. Should file paths be hashed for privacy in traces?
4. What's the retention policy for old trace files?
5. Should there be a server component for aggregate statistics across many queries?

## Related Tools/Concepts

This system is inspired by:
- **Chrome DevTools Network Tab** - Visual waterfall of requests
- **Flamegraphs** - Time-based visualization of overlapping operations
- **OpenTelemetry** - Standard event tracing framework
- **Linux trace-cmd** - System-level event recording

But simplified/optimized specifically for IO visibility in Opteryx.

---

**Status**: Design Phase Complete ✓  
**Next**: Implementation Phase 1 (Event Recorder & Ring Buffer)
