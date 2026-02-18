# IO Waterfall Design - Storage Format

## Primary Format: JSONLines

**Rationale**: 
- Simple to parse and debug
- One event per line (easy streaming)
- Human-readable for inspection
- Standard tools (grep, jq) work directly

### File Structure

```
# Trace file: io_trace_20260217_143022.jsonl

{"type":"trace_session_start","timestamp":1739815422.123456,"session_id":"uuid-abc123","query":"SELECT * FROM table","opteryx_version":"0.14.0","python_version":"3.13","hostname":"macbook-pro","cpu_count":8}
{"type":"file_discovered","timestamp":1739815422.134567,"file_id":"s3://bucket/year=2024/month=01/file.parquet.1","connector":"s3","size_bytes":1048576}
{"type":"file_discovered","timestamp":1739815422.145678,"file_id":"s3://bucket/year=2024/month=01/file.parquet.2","connector":"s3","size_bytes":2097152}
{"type":"download_start","timestamp":1739815422.156789,"file_id":"s3://bucket/year=2024/month=01/file.parquet.1","connector":"s3"}
{"type":"download_start","timestamp":1739815422.167890,"file_id":"s3://bucket/year=2024/month=01/file.parquet.2","connector":"s3"}
{"type":"download_complete","timestamp":1739815422.456789,"file_id":"s3://bucket/year=2024/month=01/file.parquet.1","bytes_received":1048576}
{"type":"download_complete","timestamp":1739815422.567890,"file_id":"s3://bucket/year=2024/month=01/file.parquet.2","bytes_received":2097152}
{"type":"decode_start","timestamp":1739815422.468900,"file_id":"s3://bucket/year=2024/month=01/file.parquet.1"}
{"type":"decode_start","timestamp":1739815422.579011,"file_id":"s3://bucket/year=2024/month=01/file.parquet.2"}
{"type":"decode_complete","timestamp":1739815422.678901,"file_id":"s3://bucket/year=2024/month=01/file.parquet.1","rows_decoded":12345,"batches":5}
{"type":"decode_complete","timestamp":1739815422.789012,"file_id":"s3://bucket/year=2024/month=01/file.parquet.2","rows_decoded":23456,"batches":8}
{"type":"trace_session_end","timestamp":1739815423.123456,"session_id":"uuid-abc123","total_files":2,"total_bytes":3145728,"total_time_seconds":1.0,"errors":0}
```

### File Naming Convention

```
io_trace_<YYYYMMDD>_<HHMMSS>_<session_id>.jsonl
```

Example:
```
io_trace_20260217_143022_abc123def456.jsonl
```

Options:
- Include session_id for uniqueness
- Include query hash for tracking same query across runs
- Numbered rotation: `io_trace.jsonl.1`, `io_trace.jsonl.2`

### Compression

Options for large trace files:

**On-disk compression** (GZIP):
```bash
# 1000 files, 5 events each = 5000 lines
# Typical uncompressed: ~500KB
# Compressed: ~50KB (10x reduction)

gzip -9 io_trace_20260217_143022.jsonl
```

JSONLines format maintains streaming capability even with compression.

**When to compress**:
- Traces > 10MB uncompressed
- Long-running queries (1+ hour)
- Production systems with many queries

**When NOT to compress**:
- Development/debugging (need human readability)
- Real-time analysis tools
- Frequent re-analysis of same trace

## Alternative Format: Parquet (Considered but Deferred)

**Advantages**:
- Efficient columnar storage
- Rich schema validation
- Built-in compression
- Good for statistical analysis

**Disadvantages**:
- Requires schema definition upfront
- Can't append incrementally (need full rewrite)
- Harder to parse in real-time
- Overkill for trace data

**Decision**: Start with JSONLines, migrate to Parquet if:
- Trace files exceed 1GB regularly
- Need advanced analytics (statistical queries)
- Storage costs become significant

## Binary Format Option (Advanced, Not Primary)

For ultra-low overhead scenarios (problematic for production):

```
Binary trace format:
[Header (32 bytes)]
[Event 1 (32 bytes)]
[Event 2 (32 bytes)]
[...]
[Footer (16 bytes)]

Header:
  - Version (4 bytes): 0x00000001
  - Magic (4 bytes): 0x4F505452 ("OPTR")
  - Flags (4 bytes): compression, version info
  - Session ID (16 bytes): UUID
  - Reserved (4 bytes)

Event (32 bytes per event):
  - Type (1 byte): 0=file_discovered, 1=download_start, etc.
  - Timestamp (8 bytes): uint64_t nanoseconds
  - File ID Hash (4 bytes): crc32 of file_id string
  - Metadata (19 bytes): size/data depending on type

Footer:
  - Event count (8 bytes)
  - Checksum (8 bytes)
```

**Evaluation**: Too much complexity for now. JSONLines is sufficient.

## Schema Versioning

Handle future format changes:

```json
{"type":"trace_session_start","version":"1.0","...":...}
```

**Version 1.0** features:
- Basic events (file_discovered, download_start, download_complete, decode_start, decode_complete)
- Timestamp float precision
- Text file_id (full path)

**Version 2.0** (potential future):
- Add network metrics (latency, retry count)
- Add memory metrics (peak buffer size)
- Add error details

Parsers should:
```python
def parse_trace_file(filepath):
    with open(filepath) as f:
        for line in f:
            event = json.loads(line)
            version = event.get('version', '1.0')
            
            if version == '1.0':
                yield parse_v1_event(event)
            elif version == '2.0':
                yield parse_v2_event(event)
            else:
                raise ValueError(f"Unknown version: {version}")
```

## Metadata File (Optional)

Companion metadata file for quick lookups:

```yaml
# io_trace_20260217_143022.meta.yml

version: 1.0
session_id: abc123def456
created_at: 2026-02-17T14:30:22Z
query: SELECT * FROM table
duration_seconds: 1.234
total_files: 42
total_bytes: 52428800
total_events: 250

files:
  count: 42
  min_size_bytes: 1024
  max_size_bytes: 10485760
  avg_size_bytes: 1248267

downloads:
  count: 42
  min_duration_ns: 100000000
  max_duration_ns: 5000000000
  avg_duration_ns: 2000000000
  min_bandwidth_mbps: 10
  max_bandwidth_mbps: 1000
  avg_bandwidth_mbps: 500

decodes:
  count: 42
  min_duration_ns: 50000000
  max_duration_ns: 1000000000
  avg_duration_ns: 300000000

concurrency:
  max_concurrent_downloads: 4
  max_concurrent_decodes: 4
  max_concurrent_both: 2
```

**Purpose**: 
- Quick statistics without parsing full trace
- Metadata for visualization tool
- Validation/sanity checks

**Generation**: Computed post-query during final flush

## Storage Location Strategy

### Development/Testing
```
$PROJECT_ROOT/traces/
  io_trace_20260217_143022.jsonl
  io_trace_20260217_143022.meta.yml
  io_trace_20260217_143545.jsonl
  ...
```

### Production
```
/var/log/opteryx/traces/
  io_trace_20260217_143022.jsonl.gz
  io_trace_20260217_143022.meta.yml
  ...
```

### Custom Location
```python
QuerySession(io_trace_file="/home/user/my_traces/query_trace.jsonl")
```

### Environment Variable
```bash
export OPTERYX_IO_TRACE_FILE=/home/user/traces/trace.jsonl
opteryx query "SELECT ..."
```

## Cleanup/Retention

Possible policies:

**Time-based**: Keep traces < 7 days
```python
# Cleanup function
import os, time
def cleanup_old_traces(directory, days=7):
    cutoff = time.time() - (days * 86400)
    for f in os.listdir(directory):
        if os.stat(f).st_mtime < cutoff:
            os.remove(f)
```

**Size-based**: Keep directory < 1GB
```python
def cleanup_by_size(directory, max_size_gb=1):
    total = sum(os.path.getsize(os.path.join(directory, f)) 
                for f in os.listdir(directory))
    
    if total > max_size_gb * 1e9:
        # Delete oldest files
```

**Manual cleanup**: User responsibility
```bash
rm -rf /var/log/opteryx/traces/*.jsonl.gz
```

Default: Keep all traces (users manage manually)

## Security Considerations

### File Permissions
```bash
# Trace files may contain sensitive info (S3 paths, credentials in queries)
chmod 600 io_trace_*.jsonl  # Owner read/write only
```

### Content Filtering Options
```python
@dataclass
class TraceConfig:
    enabled: bool = False
    output_file: str = None
    include_file_paths: bool = True  # False = hash paths
    include_query_text: bool = True  # False = omit query
    include_row_counts: bool = True  # False = omit row counts
```

If `include_file_paths=False`:
```json
{"type":"file_discovered","file_id":"hash_abc123","size_bytes":1048576}
```

Rather than:
```json
{"type":"file_discovered","file_id":"s3://secret-bucket/pii-data.parquet","size_bytes":1048576}
```

## Parsing Public Interface

Simple Python API for reading traces:

```python
from opteryx.tools.io_waterfall import TraceReader

with TraceReader("io_trace.jsonl") as reader:
    metadata = reader.metadata()
    
    for event in reader.events():
        print(f"{event['type']}: {event['file_id']}")
    
    # Or get structured data
    files = reader.get_file_timeline('s3://bucket/file.parquet.1')
    # Returns: 
    # {
    #   'discovered': timestamp,
    #   'download_start': timestamp,
    #   'download_end': timestamp,
    #   'decode_start': timestamp,
    #   'decode_end': timestamp,
    #   'size_bytes': 1048576,
    #   'rows': 12345
    # }
```

Handler for incomplete files/corrupted events:

```python
class TraceReader:
    def __init__(self, filepath, strict=False):
        self.strict = strict  # Raise on parse errors vs skip
    
    def events(self):
        with open(self.filepath) as f:
            for line_no, line in enumerate(f, 1):
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    if self.strict:
                        raise ValueError(f"Parse error at line {line_no}: {e}")
                    else:
                        logger.warning(f"Skipping invalid line {line_no}: {e}")
```
