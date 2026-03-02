# IO Waterfall Design - Implementation Roadmap

## Phased Approach

Deliver incrementally to get value quickly while managing risk and scope.

## Phase 1: Foundation (1-2 weeks)

### Core Components
1. **Event Model** (01-overview.md, 02-data-model.md)
   - Define event types and schema
   - Implement event dataclasses/namedtuples
   - Version tracking

2. **Thread-Local Ring Buffer**
   - Circular queue for in-memory event storage
   - No allocations in hot paths
   - Basic drain() implementation

3. **JSONLines Writer**
   - Write events to JSONL format
   - Handle file rotation/naming
   - Implement async background thread

4. **Configuration System**
   - Environment variable parsing
   - Config dataclass
   - Enable/disable mechanism

### Instrumentation Points (Minimal)
Add these 5 recording calls to existing code:

```python
# In Connector.discover_files()
record_event("file_discovered", file_id=path, size_bytes=size)

# In Connector.fetch()
record_event("download_start", file_id=url)
# ... network call ...
record_event("download_complete", file_id=url, bytes_received=len(data))

# In ParquetReader.decode()
record_event("decode_start", file_id=self.path)
# ... parse ...
record_event("decode_complete", file_id=self.path, rows_decoded=rows, batches=batches)
```

### Deliverable
- [ ] `opteryx/tracing/event_recorder.py` - Core recording API
- [ ] `opteryx/tracing/ring_buffer.py` - Circular buffer implementation
- [ ] `opteryx/tracing/trace_writer.py` - JSONL writer + background flush
- [ ] `opteryx/tracing/config.py` - Configuration system
- [ ] Instrumentation in 3-5 key files
- [ ] Unit tests for ring buffer, writer
- [ ] Basic integration test (record events, verify output)

### Testing
```python
def test_event_recording():
    recorder = EventRecorder()
    recorder.record_event("test", value=123)
    
    events = recorder.drain()
    assert len(events) == 1
    assert events[0]['type'] == 'test'
    assert events[0]['value'] == 123

def test_trace_writer_write_to_file(tmp_path):
    writer = TraceWriter(tmp_path / "trace.jsonl")
    writer.write_event(EventType("test", timestamp=1.0))
    writer.flush()
    
    with open(tmp_path / "trace.jsonl") as f:
        event = json.loads(f.readline())
        assert event['type'] == 'test'
```

## Phase 2: Visualization (1-2 weeks)

### Python Tool
1. **TraceReader**
   - Parse JSONL files
   - Iterate events
   - Extract structured file timelines
   - Handle malformed events

2. **Chart Generator**
   - ECharts configuration builder
   - HTML template with embedded JavaScript
   - Summary statistics computation

3. **CLI Entry Point**
   - `python -m opteryx.tools.io_waterfall`
   - Subcommands: trace, stats
   - Output format selection (--format html/csv/json)

### Deliverable
- [ ] `opteryx/tools/io_waterfall/__init__.py`
- [ ] `opteryx/tools/io_waterfall/reader.py` - TraceReader
- [ ] `opteryx/tools/io_waterfall/generator.py` - Chart generation
- [ ] `opteryx/tools/io_waterfall/__main__.py` - CLI
- [ ] `opteryx/tools/io_waterfall/templates/waterfall.html` - Base template
- [ ] Unit tests (~80% coverage)
- [ ] Integration test with real trace file

### Testing
```python
def test_trace_reader_parses_jsonl(tmp_path):
    trace_file = tmp_path / "trace.jsonl"
    
    with open(trace_file, 'w') as f:
        f.write('{"type":"download_start","timestamp":1.0,"file_id":"a"}\n')
        f.write('{"type":"download_complete","timestamp":2.0,"file_id":"a"}\n')
    
    reader = TraceReader(trace_file)
    events = list(reader.events())
    
    assert len(events) == 2
    assert events[0]['type'] == 'download_start'

def test_chart_generation_produces_valid_html(tmp_path):
    trace_file = tmp_path / "trace.jsonl"
    output_file = tmp_path / "waterfall.html"
    
    # Create dummy trace with a few events
    with open(trace_file, 'w') as f:
        f.write('{"type":"trace_session_start","timestamp":1.0,"session_id":"abc"}\n')
        # ... more events
    
    generate_waterfall_html(trace_file, output_file)
    
    assert output_file.exists()
    html = output_file.read_text()
    assert 'echarts' in html.lower() or 'waterfall' in html.lower()
```

## Phase 3: Polish & Documentation (1 week)

### Documentation
- [ ] User guide with examples
- [ ] Architecture overview for developers
- [ ] Troubleshooting guide
- [ ] Performance impact analysis
- [ ] Update README

### Performance Testing
```bash
# Benchmark: measure actual overhead
python benchmark_tracing.py \
  --query "SELECT * FROM large_table" \
  --num_files 1000 \
  --with_tracing \
  --without_tracing

# Expected result: <0.1% overhead
```

### Polish
- [ ] Error handling for invalid trace files
- [ ] Progress indicators for long queries
- [ ] Proper logging throughout
- [ ] Code cleanup and refactoring
- [ ] Type hints on all functions

### Deliverable
- [ ] User documentation
- [ ] Developer documentation
- [ ] Performance report
- [ ] Examples and sample traces

## Phase 4: Advanced Features (Optional, Later)

### Statistics and Analysis
```python
class TraceAnalyzer:
    def critical_path(self) -> float:
        """Longest file end-to-end time"""
    
    def concurrency_timeline(self) -> List[int]:
        """Files being processed at each timestamp"""
    
    def bandwidths(self) -> List[float]:
        """Bandwidth for each download"""
    
    def decode_efficiency(self) -> float:
        """Rows/sec across all decodes"""
```

### Export Formats
- [ ] CSV export (for Excel analysis)
- [ ] Parquet export (for big data tools)
- [ ] PNG/SVG rendering (static images)
- [ ] PDF report generation

### Advanced Visualization
- [ ] Flamegraph of decode times
- [ ] Bandwidth heatmap over time
- [ ] Concurrency level timeline
- [ ] Anomaly detection (unusually slow files)

### Integration With UI
- [ ] Show trace link in query result metadata
- [ ] Embed inline waterfall in Jupyter notebooks
- [ ] Real-time chart updates during query execution

### Sampling Modes
- [ ] Adaptive sampling (automatic performance tuning)
- [ ] File-size-based sampling (trace only large files)
- [ ] Time-based windowing (first 5 min of 1hr query)

As deferrable:
```
Phase 4 is "nice to have". Phase 1-3 provides full core value.
```

## Implementation Order by File

### Phase 1 Files (Priority)
```
opteryx/
  tracing/                    (NEW)
    __init__.py
    event_recorder.py         (Main API)
    ring_buffer.py            (Core data structure)
    trace_writer.py           (Async file writer)
    config.py                 (Configuration)
    events.py                 (Event type definitions)
  
  # Instrumentation in:
  connectors/base.py          (fetch methods)
  connectors/s3.py            (if S3-specific)
  utils/parquet_reader.py     (decode methods)
```

### Phase 2 Files
```
opteryx/
  tools/
    io_waterfall/             (NEW)
      __init__.py
      __main__.py             (CLI entry)
      reader.py               (Parse traces)
      generator.py            (Generate HTML)
      templates/
        waterfall.html        (HTML + JS template)
      
tests/
  unit/
    test_tracing/             (NEW)
      test_event_recorder.py
      test_ring_buffer.py
      test_trace_writer.py
  
  integration/
    test_io_waterfall/        (NEW)
      test_end_to_end.py
```

## Dependency Considerations

### Phase 1 (No new dependencies!)
```
Core uses only Python stdlib:
  - threading
  - json
  - dataclasses
  - time
  - pathlib
```

Rationale: Tracing is infrastructure, shouldn't require external packages.

### Phase 2 (Minimal)
```
Python tool dependencies (optional, only for visualization):
  - jinja2 (for HTML templating)
          OR just use f-strings (simpler)
  
  - none for CLI itself (just subprocess/file I/O)
```

If visualization requires external dep in future:
```
extras_require = {
    'waterfall': ['jinja2>=3.0'],  # Install with: pip install opteryx[waterfall]
}
```

## Rollout Strategy

### Soft Launch (Phase 1-2)
- [ ] Merge to main branch but don't promote
- [ ] No CLI in entrypoints (quiet release)
- [x] Only enable via environment variable: `OPTERYX_IO_TRACE_FILE` (deprecated/removed)
- [ ] Users opt-in entirely

### Beta Period (2-4 weeks)
- [ ] Announce feature in release notes
- [ ] Request feedback from users
- [ ] Monitor for issues/overhead complaints
- [ ] Refine based on feedback

### GA Release (Phase 3)
- [ ] Add to CLI help: `opteryx --help | grep trace`
- [ ] Promote in documentation
- [ ] Consider enabling by default in debug mode?
- [ ] Release as stable feature

## Risk Mitigation

### Risk: Performance Impact Higher Than Expected
**Mitigation**:
1. Sampling mode: `OPTERYX_TRACE_SAMPLE_RATE=0.1` (10% of files)
2. Selective tracing: Only trace files > X MB
3. Different event encoder (binary instead of JSON) in hot path
4. Opt-out completely: `OPTERYX_TRACING_ENABLED=false`

### Risk: Trace Files Get Too Large
**Mitigation**:
1. Automatic cleanup: Delete traces > 7 days old
2. Compression: gzip by default
3. Aggregation: Combine similar files in bucket
4. Rotation: `trace.jsonl.1`, `trace.jsonl.2`, etc.

### Risk: Instrumentation Points Missed
**Mitigation**:
1. Code review points: All connector fetch methods
2. Search for TODO comments during review
3. Unit test verifies events are generated
4. Integration test creates real trace file

### Risk: Visualization Renders Slowly for Large Traces
**Mitigation**:
1. ECharts has built-in dataZoom for 10K+ items
2. Progressive rendering (show first 100 files, load more on demand)
3. Server-side aggregation (group files into buckets if >5K)

## Success Criteria

### Phase 1 Success
- [ ] Can capture IO events from real queries
- [ ] Overhead < 0.1% (measured)
- [ ] No memory leaks (monitored over 1h+ query)
- [ ] Events can be read back from file

### Phase 2 Success
- [ ] HTML renders for 1000-file trace in < 2 seconds
- [ ] Waterfall is visually clear (different phases distinct)
- [ ] Hover tooltips show detailed information
- [ ] Can identify bottlenecks from chart

### Phase 3 Success
- [ ] Full documentation with walkthrough example
- [ ] Zero GitHub issues about bugs in tracing
- [ ] ≥50% of contributor PRs include trace examples

## Timeline Estimate

```
Phase 1 (Foundation):        Week 1-2
Phase 2 (Visualization):     Week 3-4
Phase 3 (Polish):            Week 5
Soft Launch:                 End of Week 5
Beta Period (feedback loop): Week 6-9
GA Release:                  Week 10+
```

Total: ~10 weeks to full GA

Parallelization possible: Phase 2 could start before Phase 1 testing complete.

## Testing Strategy Summary

```
Unit Tests (Phase 1-2):
  - Ring buffer operations
  - Event parsing
  - Chart data format
  - File I/O
  
Integration Tests (Phase 2):
  - Full query execution with tracing
  - Trace file generation
  - Visualization from real trace
  
Performance Tests (Phase 3):
  - Overhead measurement
  - Memory growth checks
  - Rendering benchmark
  
User Acceptance Tests:
  - Real-world queries (1h+, 1000s files)
  - Different connectors (S3, Local, etc.)
  - Edge cases (errors, empty files, etc.)
```
