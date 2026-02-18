# IO Waterfall Design - Data Model

## Event Types

The trace system records five types of events for each file:

### 1. File Discovery
```
{
  "type": "file_discovered",
  "timestamp": 1234567890.123456,
  "file_id": "s3://bucket/path/file.parquet.1",
  "connector": "s3",
  "size_bytes": 1048576
}
```

**Purpose**: Marks when a file becomes a candidate for download
**Captured At**: When Connector identifies files to fetch

### 2. Download Start
```
{
  "type": "download_start",
  "timestamp": 1234567890.234567,
  "file_id": "s3://bucket/path/file.parquet.1",
  "connector": "s3"
}
```

**Purpose**: Marks when download begins
**Captured At**: Before initiating network request

### 3. Download Complete
```
{
  "type": "download_complete",
  "timestamp": 1234567890.345678,
  "file_id": "s3://bucket/path/file.parquet.1",
  "bytes_received": 1048576
}
```

**Purpose**: Marks when download finishes (data in buffer/memory)
**Captured At**: After receiving all bytes

### 4. Decode Start
```
{
  "type": "decode_start",
  "timestamp": 1234567890.456789,
  "file_id": "s3://bucket/path/file.parquet.1"
}
```

**Purpose**: Marks when decoding/parsing begins
**Captured At**: Before decompression/parsing logic

### 5. Decode Complete
```
{
  "type": "decode_complete",
  "timestamp": 1234567890.567890,
  "file_id": "s3://bucket/path/file.parquet.1",
  "rows_decoded": 12345,
  "batches": 5
}
```

**Purpose**: Marks when file is fully decoded into Arrow batches
**Captured At**: After parse completion, before consumer access

## Trace Session Metadata

```
{
  "type": "trace_session_start",
  "timestamp": 1234567890.000000,
  "session_id": "uuid-string",
  "query": "SELECT * FROM table",
  "opteryx_version": "0.14.0",
  "python_version": "3.13",
  "hostname": "macbook-pro",
  "cpu_count": 8,
  "concurrent_file_limit": 4
}
```

```
{
  "type": "trace_session_end",
  "timestamp": 1234567891.999999,
  "session_id": "uuid-string",
  "total_files": 42,
  "total_bytes": 52428800,
  "total_time_seconds": 1.999999,
  "errors": 0
}
```

## Event Structure Details

### Common Fields
All events include:
- **type** (string): Event type identifier
- **timestamp** (float): High-resolution timestamp (seconds since epoch, nanosecond precision)
- **file_id** (string): Unique identifier for file (URL/path)

### Optional Fields per Event
- **size_bytes**: Known size of file
- **bytes_received**: Actual bytes received
- **rows_decoded**: Rows extracted from file
- **batches**: Number of Arrow batches produced
- **error**: Error message if applicable

## Timing Phases for Visualization

From the events above, we can derive three phases:

```
Timeline for file_id = "s3://bucket/path/file.parquet.1"

file_discovered
│
├─ DOWNLOAD PHASE (downloading color)
│  [download_start ─────────── download_complete]
│
├─ BUFFER PHASE (buffered color)
│  [download_complete ─ decode_start]
│
└─ DECODE PHASE (decoding color)
   [decode_start ─────────────── decode_complete]
```

### Color Scheme Proposal
- **Download**: Blue (#4A90E2) - data coming from network
- **Buffer**: Yellow (#F5D76E) - data waiting to be processed
- **Decode**: Green (#7ED321) - active processing

### Parallel Execution
By recording all files' timelines, we can show:
- How many files download simultaneously
- Whether decoding happens during other downloads
- Buffer size implications (if buffer phase is long, we're bottlenecked)
- Total system concurrency at any point in time

## Derived Metrics

From raw events, we can compute:

```
Per-File Metrics:
├─ download_duration = download_complete.timestamp - download_start.timestamp
├─ buffer_duration = decode_start.timestamp - download_complete.timestamp
├─ decode_duration = decode_complete.timestamp - decode_start.timestamp
├─ total_duration = decode_complete.timestamp - download_start.timestamp
├─ download_bandwidth = bytes_received / download_duration
├─ decode_throughput = rows_decoded / decode_duration
└─ decode_efficiency = batches / decode_duration

Session Metrics:
├─ max_concurrent_downloads = max files downloading at any point
├─ max_concurrent_decoding = max files decoding at any point
├─ buffer_peak_size = estimated peak memory used for buffering
└─ query_critical_path = longest end-to-end time for any file
```

## Schema Extensibility

The event format allows future fields without breaking parsers:

```
{
  "type": "download_complete",
  "timestamp": 1234567890.345678,
  "file_id": "s3://bucket/path/file.parquet.1",
  "bytes_received": 1048576,
  
  // Future fields
  "compression_codec": "snappy",
  "network_latency_ms": 45,
  "cache_hit": false
}
```

Visualization tools should:
1. Require only type, timestamp, file_id
2. Skip unknown fields gracefully
3. Log warnings for unexpected field values
