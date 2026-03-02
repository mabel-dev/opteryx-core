# IO Waterfall Design - Quick Reference

## One-Page Summary

### Problem
Need visibility into IO layer performance: which files are downloading, buffering, and being decoded, and how these phases overlap. Must have negligible performance impact.

### Solution: Waterfall Chart + Trace System

```
Event Recording (Hot Path - Minimal):
┌────────────────────────────┐
│ Query Execution            │
│  record_event() calls (5)  │  ← 5 events per file
└──────────┬─────────────────┘
           │
    ┌──────▼───────────┐
    │ Thread-Local     │
    │ Ring Buffers     │  ← No allocations, no locks
    │ (pre-allocated)  │
    └──────┬───────────┘
           │
    ┌──────▼───────────┐
    │ Background       │
    │ Writer Thread    │  ← Async, doesn't block query
    └──────┬───────────┘
           │
    ┌──────▼───────────┐
    │ Trace File       │
    │ (.jsonl format)  │  ← JSONLines for easy parsing
    └──────────────────┘

Post-Query Visualization (Separate):
┌──────────────────────┐
│ Python Tool          │
│ (no performance lock)│  ← Run after query finishes
└──────┬───────────────┘
       │
┌──────▼──────────────┐
│ Parse Trace         │
│ Generate Chart HTML │  ← ECharts interactive
└──────┬──────────────┘
       │
┌──────▼──────────────┐
│ View in Browser     │
│ Click/hover details │
└─────────────────────┘
```

### Key Design Decisions

| Aspect | Decision | Why |
|--------|----------|-----|
| **Overhead** | Thread-local ring buffers, no locks | Avoid contention, maintain query speed |
| **When** | Record events, flush async to file | No blocking on query execution |
| **Format** | JSONLines | Simple, standard, human-readable |
| **Visualization** | Post-query HTML generation | No runtime overhead on visualizer |
| **Library** | ECharts | Good performance, built-in interactivity |
| **Phases** | Download (blue), Buffer (yellow), Decode (green) | Clear visual distinction |

### Event Types (5 Total)

```
file_discovered       → File identified as needing fetch
download_start        → Network request begins
download_complete     → Data in memory/buffer
decode_start          → Parsing begins
decode_complete       → Arrow batches ready
```

### Implementation Phases

```
Phase 1 (1-2 weeks):   Core tracing infrastructure
                       - Ring buffer, event recorder, file writer
                       - 5 record_event() calls in existing code
                       
Phase 2 (1-2 weeks):   Visualization tool
                       - TraceReader, chart generator, CLI
                       - ECharts HTML template
                       
Phase 3 (1 week):      Documentation & polish
                       - User guide, perf testing, cleanup
                       
Phase 4 (Optional):    Advanced features (later)
                       - CSV/Parquet export, flamegraphs, etc.
```

### File Structure

```
docs/io-waterfall-design/
  01-overview.md              ← Start here
  02-data-model.md            ← Event schema
  03-collection-strategy.md   ← How to capture events
  04-storage-format.md        ← JSONLines format
  05-visualization.md         ← HTML generation
  06-implementation-roadmap.md  ← What to build when

opteryx/tracing/              ← Phase 1
  event_recorder.py
  ring_buffer.py
  trace_writer.py
  config.py

opteryx/tools/io_waterfall/   ← Phase 2
  reader.py
  generator.py
  __main__.py (CLI)
  templates/waterfall.html
```

### Configuration

```python
# Enable via environment variable
export OPTERYX_IO_TRACE_FILE=/tmp/trace.jsonl
opteryx query "SELECT ..."

# Or programmatically
session = QuerySession(io_trace_file="/tmp/trace.jsonl")
session.execute("SELECT ...")

# View chart
python -m opteryx.tools.io_waterfall /tmp/trace.jsonl
# Outputs: /tmp/trace.jsonl.html
# Open in browser
```

### Performance Impact

Expected overhead:
```
Per event: ~90 nanoseconds
Per file (5 events): ~450 nanoseconds
Per 1000 files: ~450 microseconds
Typical query (1-10s): <0.05% overhead
```

If too high, use sampling:
```python
OPTERYX_TRACE_SAMPLE_RATE=0.1  # Trace 10% of files (default 1.0 = 100%)
```

### Visual Output

```
File name                   Timeline with three phases
                           
file.parquet.1   ██░░░░░░░░██░░░░░░░░░██████████
                 ^download  ^buffer      ^decode
                 (blue)    (yellow)      (green)

file.parquet.2      ██░░░░░░░░██░░░░░░░██████████
file.parquet.3         ██░░░░░░░██░░░░░███████████

Statistics:
- Max concurrent downloads
- Max concurrent decodes
- Total time per phase
- Bandwidth/throughput stats
```

### Testing Strategy

```
Phase 1:   Unit tests (ring buffer, encoding, file I/O)
Phase 2:   Integration tests (full query with trace + HTML generation)
Phase 3:   Performance tests (overhead measurement)
           Acceptance tests (real queries, multiple connectors)
```

### No New Dependencies

Phase 1-2 use only Python stdlib:
```python
# Core tracing
import threading, json, dataclasses, time, pathlib

# Visualization
import json  # Already loaded

# Optional: Jinja2 for HTML templating
#   (can be avoided with f-strings)
```

Avoids bloat, keeps installation simple.

### Risk Mitigation Quick List

| Risk | Mitigation |
|------|-----------|
| Overhead too high | Sampling mode, selective tracing |
| Trace files huge | Compression (gzip), auto-cleanup (7 days) |
| Instrumentation incomplete | Code review checklist, unit tests |
| HTML renders slowly | Data zoom on 10K+, progressive loading |
| File corruption | Don't block query on write errors, log only |

### Next Steps

1. **Review this design** - Feedback on approach/trade-offs?
2. **Finalize data model** - Are these 5 events enough?
3. **Start Phase 1** - Build core tracing infrastructure
4. **Add instrumentation** - 5 record_event() calls
5. **Integration test** - Real query generates real trace
6. **Then Phase 2** - Visualization tool

---

## Design Questions To Answer

1. **File ID Format**: Use full S3 URL, or hash for privacy?
   - Current: Full URL (simpler, enables insights)
   - Alternative: Hash paths for sensitive data

2. **Precision**: Are float timestamps sufficient, or need nanoseconds?
   - Current: float (nanosecond precision via time.perf_counter())
   - Sufficient for 90µs granularity

3. **Connector Variants**: Trace into each connector separately?
   - Current: Single trace file for whole query
   - Alternative: Per-connector trace files

4. **Graph Library**: ECharts vs D3 vs Plotly?
   - Current: ECharts (good balance)
   - D3: More control, bigger learning curve
   - Plotly: Simpler, larger artifact

5. **Metadata File**: Include .meta.yml with statistics?
   - Current: Optional (computed post-query)
   - Helps with quick lookups but adds complexity

---

## Discussion Points

**Architectural Trade-offs Accepted**:
- ✅ Ring buffers over dynamic arrays (bounded memory, no live allocations)
- ✅ JSONLines over binary (human-readable, simpler parsing)
- ✅ Post-query viz over real-time (zero runtime overhead on query)
- ✅ ECharts over D3 (simplicity over control)
- ✅ No external dependencies in Phase 1 (keep lightweight)

**Alternative Approaches NOT Chosen**:
- ❌ Real-time streaming: Adds network overhead, complexity
- ❌ In-query visualization: Blocks query execution
- ❌ Using Datadog/APM system: Adds dependency, less control
- ❌ Profiler integration: Overkill, other tools do this
- ❌ Binary format first: Premature optimization

**Extensibility Built In**:
- Event schema versioning for future fields
- Configurable sampling and output location
- Modular event recorder (plug different backends)
- Trace reader uses graceful degradation (skip unknown fields)
