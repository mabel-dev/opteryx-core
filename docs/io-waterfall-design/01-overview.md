# IO Waterfall Visualization System - Design Overview

## Problem Statement

We need visibility into the IO layer's performance characteristics, specifically:
- Which files are being downloaded/fetched
- When downloads occur
- When files are buffered
- When files are being decoded
- How these phases overlap across multiple concurrent operations

This information should help identify:
- IO bottlenecks
- Buffer inefficiencies
- Decode-related slowdowns
- Concurrency patterns

## Goals

1. **Low Overhead**: Minimal impact on query execution performance
2. **Accurate Timing**: Precise measurement of phase transitions
3. **Visibility**: Clear understanding of concurrent IO operations
4. **Debuggability**: Easy to analyze and troubleshoot
5. **Optional**: Can be enabled/disabled without code changes

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Query Execution                         │
│     (Instrumented IO operations in Connectors)          │
└────────────────────┬────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
    ┌────▼─────┐          ┌─────▼──────┐
    │   Real   │          │  Trace     │
    │   Time   │          │  Recorder  │
    │ Execution│          │ (if enabled)
    │          │          │            │
    └──────────┘          └─────┬──────┘
                                │
                         ┌──────▼──────────┐
                         │  Trace Events   │
                         │  (Circular      │
                         │   In-Memory     │
                         │   Buffer)       │
                         └──────┬──────────┘
                                │
                         ┌──────▼──────────┐
                         │  Post-Query     │
                         │  Visualization  │
                         │  (Python Tool)  │
                         └─────────────────┘

(note: the engine no longer flushes to disk automatically; clients
are responsible for exporting events when a file is required)

```

## Key Design Constraints

1. **No Heap Allocations in Hot Path**: Use thread-local pre-allocated buffers
2. **Minimal Lock Contention**: Lock-free or minimal synchronization
3. **Bounded Memory**: Circular buffer prevents unbounded growth
4. **Async Flushing**: Don't block query execution on IO writes
5. **Opt-in**: Only activated when explicitly requested

## Core Components

1. **Event Recorder** - Captures timing events in-memory
2. **Trace Data Model** - Defines event schema and structure
3. **Event Writer** - Flushes trace data to disk asynchronously
4. **Visualization Tool** - Creates waterfall chart from trace file
5. **Configuration System** - Enables/disables tracing without code changes

## Usage Pattern

```bash
# Enable tracing for a query (environment variable is now ignored but
# shown here for historical reference)
OPTERYX_TRACE=1 opteryx query "SELECT ..."
```

```python
# In Python, simply enable tracing and later inspect the session
import opteryx
session = opteryx.QuerySession()
session.execute("SELECT ...")

# read events from memory and optionally write them yourself
events = list(session.trace())
# e.g.:
# with open("/tmp/trace.jsonl", "w") as f:
#     for ev in events:
#         f.write(json.dumps(ev) + "\n")
```

# Post-query: visualize using a trace file you produced yourself
python -m opteryx.tools.io_waterfall /tmp/io_trace.jsonl --output /tmp/waterfall.html

(note that the core engine does **not** create this file automatically)
## Next Documents

- **02-data-model.md** - Event schema and trace data structure
- **03-collection-strategy.md** - How to capture events with minimal overhead
- **04-storage-format.md** - On-disk format and serialization
- **05-visualization.md** - Chart generation and rendering
- **06-implementation-roadmap.md** - Phased rollout plan
