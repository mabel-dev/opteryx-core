# IO Waterfall Design - Event Collection Strategy

## Challenge: Minimal Overhead

We need to capture timing events without:
1. Allocating memory in hot code paths
2. Contending for locks
3. Blocking query execution
4. Adding significant CPU overhead

## Solution: Thread-Local Circular Ring Buffers

### Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                                                              │
│  Thread 1              Thread 2              Thread 3        │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐  │
│  │  Ring Buf   │      │  Ring Buf   │      │  Ring Buf   │  │
│  │  (pre-      │      │  (pre-      │      │  (pre-      │  │
│  │  allocated) │      │  allocated) │      │  allocated) │  │
│  │  Max: 10K   │      │  Max: 10K   │      │  Max: 10K   │  │
│  │  events     │      │  events     │      │  events     │  │
│  └──────┬──────┘      └──────┬──────┘      └──────┬──────┘  │
│         │                    │                    │         │
│         └────────────────────┼────────────────────┘         │
│                              │                              │
│                    ┌─────────▼──────────┐                   │
│                    │  Background Flush  │                   │
│                    │  Thread (condvar   │                   │
│                    │  triggered)        │                   │
│                    └─────────┬──────────┘                   │
│                              │                              │
│                    ┌─────────▼──────────┐                   │
│                    │  Write to Disk     │                   │
│                    │  (append, no lock  │                   │
│                    │  with query thread)│                   │
│                    └────────────────────┘                   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Key Principles

#### 1. Thread-Local Storage
- Each thread has its own ring buffer
- No synchronization for event recording
- CPU cache stays hot (working set fits in L1/L2)

```cpp
thread_local static RingBuffer trace_buffer(10000);  // C++ side
```

or in Python:

```python
import threading
_trace_buffer = threading.local()

def get_trace_buffer():
    if not hasattr(_trace_buffer, 'buffer'):
        _trace_buffer.buffer = RingBuffer(max_events=10000)
    return _trace_buffer.buffer
```

#### 2. Ring Buffer (Fixed Size, No Allocation)

```
Memory Layout (single thread):
┌─────────────────────────────────────────────────────┐
│  Event 1  │ Event 2 │ Event 3 │ ... │ Event 10000 │
└─────────────────────────────────────────────────────┘
 ^                                        ^
 head_ptr                           tail_ptr
 
When full, new events overwrite oldest:
Write new event → overwrite Event 1 → advance head_ptr
```

#### 3. Event Encoding (Minimal Memory)

Instead of storing full JSON in memory:
```
Binary format (32 bytes per event):
┌──────┬────────────────────┬──────────────┬───────────────┐
│Type  │ Timestamp (8 bytes)│ File ID Hash │ Size/Metadata │
│(1B)  │                    │ (8 bytes)    │ (variable)    │
├──────┼────────────────────┼──────────────┼───────────────┤
│  8   │ 1234567890.123456  │ 0xABCD1234   │ bytes_received│
└──────┴────────────────────┴──────────────┴───────────────┘
```

Lookup table for file_id → full path (in separate buffer):
```python
_file_id_map = {}  # hash -> full path
_file_id_counter = 0

def get_file_id_hash(filepath):
    global _file_id_counter
    h = hash(filepath) & 0xFFFFFFFF
    if h not in _file_id_map:
        _file_id_map[h] = filepath
    return h
```

#### 4. Circular Buffer Implementation Strategy

```python
class RingBuffer:
    def __init__(self, max_events=10000):
        self.max_events = max_events
        self.events = [None] * max_events
        self.head = 0
        self.tail = 0
        self.count = 0
    
    def push(self, event):
        """O(1) operation, no allocations"""
        self.events[self.tail] = event
        self.tail = (self.tail + 1) % self.max_events
        
        if self.count < self.max_events:
            self.count += 1
        else:
            self.head = (self.head + 1) % self.max_events
    
    def drain(self):
        """Called by flush thread"""
        result = []
        while self.head != self.tail:
            result.append(self.events[self.head])
            self.head = (self.head + 1) % self.max_events
            self.count -= 1
        return result
```

## Event Recording Points

### In Connector Layer

Where to add instrumentation (minimal code):

```python
# File discovery phase
def discover_files(self, path):
    files = []
    for f in list_files(path):
        files.append(f)
        record_event("file_discovered", file_id=f.path, size_bytes=f.size)
    return files
```

```python
# Download phase (already in Connector.fetch())
def fetch_data(self, urls):
    for url in urls:
        record_event("download_start", file_id=url)
        data = download(url)  # Network call
        record_event("download_complete", file_id=url, 
                     bytes_received=len(data))
    return data
```

```python
# Decode phase (in ParquetReader or similar)
def decode_batch(self, raw_bytes):
    record_event("decode_start", file_id=self.current_file)
    
    table = deserialize_parquet(raw_bytes)
    batches = table.to_batches()
    
    record_event("decode_complete", file_id=self.current_file,
                 rows_decoded=len(table), batches=len(batches))
    return batches
```

## Background Flush Strategy

### Timing-Based Flush
```python
class FlushThread(threading.Thread):
    def __init__(self, flush_interval_ms=100):
        self.flush_interval = flush_interval_ms / 1000.0
        self.running = True
    
    def run(self):
        while self.running:
            time.sleep(self.flush_interval)
            self.flush_all_buffers()
    
    def flush_all_buffers(self):
        """Collect from all thread-local buffers, write to disk"""
        for thread_id, buffer in get_all_buffers():
            events = buffer.drain()
            if events:
                write_to_disk(events)
```

### Query Completion Flush
```python
class QuerySession:
    def execute(self, query):
        try:
            return self.run_query(query)
        finally:
            self.flush_all_traces()  # Final flush on exit
```

## Lock-Free Writing to Disk

Use an atomic queue + dedicated writer thread to avoid locking the query thread:

```python
class TraceWriter:
    def __init__(self, filepath):
        self.filepath = filepath
        self.queue = queue.Queue(maxsize=100000)
        self.writer_thread = threading.Thread(target=self.writer_loop, daemon=True)
        self.writer_thread.start()
    
    def enqueue_events(self, events):
        """Query thread calls this (minimal contention)"""
        for event in events:
            self.queue.put_nowait(event)  # Non-blocking
    
    def writer_loop(self):
        """Background thread writes to disk"""
        with open(self.filepath, 'a') as f:
            while True:
                try:
                    event = self.queue.get(timeout=0.5)
                    f.write(json.dumps(event) + '\n')
                except queue.Empty:
                    f.flush()  # Periodic flush
```

## Memory Budget

Per query session:
```
Thread-local buffers (N threads):
  N threads × 10K events × 200 bytes = 20MB (worst case)
  
With 4 threads typical: 80KB per thread = 320KB

Global structures:
  File ID map: ~1K files × 256 bytes = 256KB
  
Total: ~600KB overhead for full tracing (acceptable)
```

## CPU Overhead Analysis

Per event recording:
```
Operation                      Cost
─────────────────────────────────────
Record timestamp               ~5-10 ns   (RDTSC or similar)
RingBuffer.push()              ~20-30 ns  (array write, modulo)
Acquire file_id hash           ~50 ns     (dict lookup)
─────────────────────────────────────
Total per event                ~80-90 ns
```

For a query processing 1000 files:
```
5 events per file × 1000 = 5000 events
5000 × 90ns = 450 µs total overhead
Typical query time: 1-10 seconds
Overhead: 0.0045-0.045% (negligible)
```

## Mitigation: Sampling

If even 0.045% is too much, implement optional sampling:

```python
TRACE_SAMPLE_RATE = 0.1  # Trace 10% of files

def record_event(event_type, file_id=None, **kwargs):
    if file_id and random.random() > TRACE_SAMPLE_RATE:
        return  # Skip this event
    
    # Record normally
    _recording_implementation(event_type, file_id, **kwargs)
```

Trade-off: Lose visibility into rare files, but very low overhead

## Configuration Options

```python
class TraceConfig:
    enabled: bool = False  # Master switch
    output_file: str = None  # Where to write
    flush_interval_ms: int = 100
    buffer_size_per_thread: int = 10000
    sample_rate: float = 1.0  # 1.0 = 100% tracing
    include_file_sizes: bool = True
    include_decode_stats: bool = True
```

Can be set via:
- Environment variables: `OPTERYX_TRACE_*`
- Query session config
- Global config file
