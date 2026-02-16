# Paged Memory Pool Design

## Problem Statement

The current `MemoryPool` implementation has two major issues:

**1. Per-Node Pool Creation:** Each `AsyncReadNode` creates its own `MemoryPool` instance, which is destroyed after the query completes. This wastes resources and prevents memory reuse across queries.

**2. Single Lock Serialization:** The `MemoryPool` uses a single `RLock` to protect all operations, creating a serialization bottleneck when multiple async workers attempt to commit data concurrently. With 8 async workers reading from S3/GCS, all workers serialize at the memory pool lock, causing:

- High `stalls_engine_waiting_on_data` counts (thousands of stalls for ~200 files)
- Workers blocked during memcpy operations (which hold the lock)
- Poor utilization of async I/O parallelism

**Root Cause:** The `commit()` method holds a global lock while:
1. Searching for free segments (fast)
2. Potentially compacting/defragmenting (medium)
3. Performing memcpy of data (slow - scales with blob size)

With 8 workers committing 5MB blobs, if each memcpy takes ~5ms, workers wait up to 40ms just to acquire the lock.

## Current Architecture

**Per-Node Pool Creation:**
```
Query 1 (AsyncReadNode)          Query 2 (AsyncReadNode)
   ┌─────────────────┐              ┌─────────────────┐
   │ MemoryPool (3GB)│              │ MemoryPool (3GB)│
   │  [Single RLock] │              │  [Single RLock] │
   │  [Segments]     │              │  [Segments]     │
   └─────────────────┘              └─────────────────┘
   Created → Destroyed              Created → Destroyed
```
- Each query creates its own pool (wasteful)
- No memory reuse across queries
- Pool destroyed after query completes

**Single Lock Serialization (per pool):**
```
┌─────────────────────────────────────┐
│         MemoryPool (3GB)            │
│  ┌────────────────────────────────┐ │
│  │      Single RLock              │ │
│  └────────────────────────────────┘ │
│                                     │
│  [Segment 1][Segment 2][Segment 3] │
│  [Segment 4][     Free Space     ] │
└─────────────────────────────────────┘
         ▲    ▲    ▲    ▲
         │    │    │    │
    [W1][W2][W3]...[W8]  ← All workers serialize here
```

## Proposed Solution: Shared Paged Memory Pool

**Two Key Changes:**

1. **Shared, Persistent Resource:** Single buffer pool shared across all queries (singleton pattern)
2. **Page-Level Locking:** Split pool into independent pages, each with its own lock

Workers select pages via **round-robin with timeout-based failover** to avoid deadlocks.

### Architecture

**Shared Singleton Pattern:**
```
All Queries → get_shared_read_buffer()
                      ↓
         ┌────────────────────────┐
         │ Singleton Instance     │
         │ (Created Once)         │
         │ PagedMemoryPool        │
         │ Persistent Across      │
         │ Query Lifecycles       │
         └────────────────────────┘
```

**Page-Level Locking:**
```
┌─────────────────────────────────────────────────────────────────┐
│       PagedMemoryPool (Python Wrapper) - Shared Singleton       │
│       Total: page_size × num_pages (e.g., 512MB × 8 = 4GB)     │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐│
│  │ MemoryPool       │  │ MemoryPool       │  │ MemoryPool    ││
│  │ (Cython)         │  │ (Cython)         │  │ (Cython)      ││
│  │ Page 0 (512MB)   │  │ Page 1 (512MB)   │  │ Page 2 (512MB)││
│  │  ┌────┐          │  │  ┌────┐          │  │  ┌────┐       ││
│  │  │RLock          │  │  │RLock          │  │  │RLock       ││
│  │  └────┘          │  │  └────┘          │  │  └────┘       ││
│  │  [segments]      │  │  [segments]      │  │  [segments]   ││
│  └──────────────────┘  └──────────────────┘  └───────────────┘│
└─────────────────────────────────────────────────────────────────┘
         ▲                   ▲                   ▲
         │                   │                   │
       [W1,W4,W7]        [W2,W5,W8]         [W3,W6]  ← Round-robin
```

### Key Design Decisions

**1. Implementation: Python Wrapper Around Cython Pools**
- Keep existing `MemoryPool` (Cython) unchanged - zero risk
- New `PagedMemoryPool` (Python) wraps multiple `MemoryPool` instances
- Python's `RLock.acquire(timeout=...)` provides clean timeout handling
- Negligible overhead vs lock contention savings

**2. Configuration**
- **Page Size:** 512MB (configurable via `OPTERYX_MEMORY_POOL_PAGE_SIZE`)
- **Number of Pages:** Default = CPU count (min 2)
- **Total Capacity:** `page_size × num_pages`
- **Lock Timeout:** 100ms (matches reader timeout, configurable via `OPTERYX_MEMORY_POOL_LOCK_TIMEOUT_MS`)

**3. Page Selection: Round-Robin with Timeout**
- Each worker attempts to acquire lock on next page in sequence
- If lock not acquired within 100ms, move to next page
- Prevents deadlock while maintaining balanced distribution

**4. Ref ID Encoding**
- Upper 16 bits: Page ID (supports 65,536 pages)
- Lower 48 bits: Local ref ID within page
- Format: `ref_id = (page_id << 48) | local_ref_id`

## Implementation Details

### Data Structures

**Python Wrapper (new file: `opteryx/compiled/structures/paged_memory_pool.py`)**

```python
import os
import threading
from typing import Optional
from opteryx.compiled.structures.memory_pool import MemoryPool

class PagedMemoryPool:
    """
    Paged memory pool wrapper that distributes allocations across multiple 
    Cython MemoryPool instances to reduce lock contention.
    
    Uses round-robin page selection with timeout-based failover.
    """
    
    def __init__(
        self, 
        page_size: int = 512_000_000,  # 512MB default
        num_pages: Optional[int] = None,
        name: str = "Paged Memory Pool",
        auto_resize: bool = False,
        alignment: int = 1,
        lock_timeout_ms: int = 100
    ):
        """
        Initialize paged memory pool.
        
        Args:
            page_size: Size of each page in bytes (default 512MB)
            num_pages: Number of pages (default: CPU count, min 2)
            name: Pool name for debugging
            auto_resize: Whether pages can auto-resize
            alignment: Memory alignment for allocations
            lock_timeout_ms: Timeout for lock acquisition in milliseconds
        """
        if num_pages is None:
            num_pages = max(2, os.cpu_count() or 2)
        
        self.page_size = page_size
        self.num_pages = num_pages
        self.name = name
        self.lock_timeout_ms = lock_timeout_ms
        self.lock_timeout_sec = lock_timeout_ms / 1000.0
        
        # Create individual pages (Cython MemoryPool instances)
        self.pages = [
            MemoryPool(
                size=page_size,
                name=f"{name}-Page{i}",
                auto_resize=auto_resize,
                alignment=alignment
            )
            for i in range(num_pages)
        ]
        
        # Round-robin counter (lightweight lock only for counter)
        self._next_page_index = 0
        self._selection_lock = threading.Lock()
        
        # Statistics
        self.lock_timeouts = 0
        self.page_full_retries = 0
    
    @property
    def size(self) -> int:
        """Total capacity across all pages."""
        return self.page_size * self.num_pages
    
    @property
    def used_size(self) -> int:
        """Total used size across all pages."""
        return sum(page.used_size for page in self.pages)
```

### Commit Algorithm with Timeout

```python
def commit(self, data) -> int:
    """
    Commit data to pool using round-robin page selection with timeout.
    
    Tries each page sequentially with 100ms lock timeout, moving to next
    page if lock cannot be acquired or page is full.
    
    Returns:
        ref_id: Encoded reference (page_id << 48) | local_ref
        -1: All pages exhausted or timed out
    """
    # Get starting page via round-robin
    with self._selection_lock:
        start_page = self._next_page_index
        self._next_page_index = (self._next_page_index + 1) % self.num_pages
    
    # Try up to num_pages pages
    for attempt in range(self.num_pages):
        page_idx = (start_page + attempt) % self.num_pages
        page = self.pages[page_idx]
        
        # Try to acquire lock with timeout (100ms default)
        acquired = page.lock.acquire(timeout=self.lock_timeout_sec)
        
        if acquired:
            try:
                # Call existing Cython MemoryPool.commit()
                local_ref = page.commit(data)
                
                if local_ref != -1:
                    # Success - encode ref_id with page index
                    ref_id = (page_idx << 48) | local_ref
                    return ref_id
                else:
                    # Page full, try next page
                    self.page_full_retries += 1
            finally:
                page.lock.release()
        else:
            # Lock timeout, try next page
            self.lock_timeouts += 1
    
    # All pages exhausted or timed out
    return -1
```

### Read Operation

```python
def read(self, ref_id: int, zero_copy: bool = False, latch: bool = False):
    """
    Read data from pool by decoding ref_id to find page.
    
    Delegates to the appropriate Cython MemoryPool.read().
    """
    # Decode ref_id
    page_idx = ref_id >> 48
    local_ref = ref_id & 0xFFFFFFFFFFFF
    
    if page_idx >= self.num_pages:
        raise ValueError(f"Invalid page index: {page_idx}")
    
    # Delegate to Cython MemoryPool
    return self.pages[page_idx].read(local_ref, zero_copy=zero_copy, latch=latch)
```

### Release Operation

```python
def release(self, ref_id: int):
    """
    Release data by decoding ref_id to find page.
    
    Delegates to the appropriate Cython MemoryPool.release().
    """
    page_idx = ref_id >> 48
    local_ref = ref_id & 0xFFFFFFFFFFFF
    
    if page_idx >= self.num_pages:
        raise ValueError(f"Invalid page index: {page_idx}")
    
    # Delegate to Cython MemoryPool
    self.pages[page_idx].release(local_ref)

def unlatch(self, ref_id: int):
    """
    Unlatch data by decoding ref_id to find page.
    
    Delegates to the appropriate Cython MemoryPool.unlatch().
    """
    page_idx = ref_id >> 48
    local_ref = ref_id & 0xFFFFFFFFFFFF
    
    if page_idx >= self.num_pages:
        raise ValueError(f"Invalid page index: {page_idx}")
    
    # Delegate to Cython MemoryPool
    self.pages[page_idx].unlatch(local_ref)
```

## Configuration

Configuration via environment variables or constructor parameters:

```python
# Environment variables (for read buffer memory pool)
READ_BUFFER_PAGE_SIZE = 512_000_000      # 512MB default
READ_BUFFER_NUM_PAGES = None             # CPU count (min 2) if None
READ_BUFFER_LOCK_TIMEOUT_MS = 100        # 100ms lock timeout

# Constructor usage (matches existing MemoryPool interface)
from opteryx.compiled.structures.paged_memory_pool import PagedMemoryPool

# Option 1: Specify page_size and num_pages
pool = PagedMemoryPool(
    page_size=512_000_000,  # 512MB per page
    num_pages=8,            # 8 pages = 4GB total
    name="ReadBuffer",
    auto_resize=False,
    alignment=1
)

# Option 2: Use environment variable defaults (recommended)
pool = PagedMemoryPool(
    # Uses READ_BUFFER_PAGE_SIZE, READ_BUFFER_NUM_PAGES, READ_BUFFER_LOCK_TIMEOUT_MS
    name="ReadBuffer"
)

# Total capacity = page_size × num_pages
# e.g., 512MB × 8 CPUs = 4GB total
```

**Note:** These settings are specifically for the read buffer memory pool, which is configured
via the legacy `READ_BUFFER_CAPACITY` setting. The paged pool divides this capacity into
pages for parallel access.

## API Compatibility

The Python wrapper provides the same interface as the Cython `MemoryPool`:

```python
# Both classes support the same methods:

# Existing Cython MemoryPool (unchanged)
pool = MemoryPool(size=3_000_000_000, name="Buffer")
ref = pool.commit(data)
data = pool.read(ref, zero_copy=True, latch=True)
pool.unlatch(ref)
pool.release(ref)

# New Python PagedMemoryPool (same interface)
pool = PagedMemoryPool(page_size=512_000_000, num_pages=6, name="Buffer")
ref = pool.commit(data)           # Same method
data = pool.read(ref, zero_copy=True, latch=True)  # Same method
pool.unlatch(ref)                 # Same method
pool.release(ref)                 # Same method

# Properties also match
total_size = pool.size            # Works on both
used_size = pool.used_size        # Works on both
```

**Key point:** Any code using `MemoryPool` can swap in `PagedMemoryPool` with just the constructor parameters changing. All method signatures remain identical.

## Migration Strategy

### Phase 1: Implementation
1. Create `opteryx/compiled/structures/paged_memory_pool.py` (Python wrapper)
2. Existing `MemoryPool` (Cython) remains completely unchanged
3. Add unit tests for `PagedMemoryPool`

### Phase 2: Integration

**Architectural Change:** Make the read buffer a **shared, persistent resource** instead of creating per-node instances.

1. Create module-level singleton in `async_read_node.py`:

```python
# In async_read_node.py (module level)
from opteryx.compiled.structures.paged_memory_pool import PagedMemoryPool

_shared_read_buffer = None
_buffer_lock = threading.Lock()

def get_shared_read_buffer():
    """
    Get or create the shared read buffer pool.
    
    The read buffer is a persistent, shared resource used across all
    queries and datasets. Using PagedMemoryPool allows concurrent commits
    from multiple async workers without serialization.
    """
    global _shared_read_buffer
    
    if _shared_read_buffer is None:
        with _buffer_lock:
            if _shared_read_buffer is None:
                _shared_read_buffer = PagedMemoryPool(
                    name="Shared Read Buffer"
                )
    
    return _shared_read_buffer
```

2. Update `AsyncReadNode.__init__` to use the shared pool:

```python
# Old: per-node pool creation
# self.pool = MemoryPool(MAX_READ_BUFFER_CAPACITY, f"ReadBuffer <{self.parameters['alias']}>")

# New: use shared pool
self.pool = get_shared_read_buffer()
```

**Benefits of Shared Pool:**
- Eliminates overhead of creating/destroying pools per query
- Enables memory reuse across queries
- Persistent resource matches traditional buffer pool semantics
- No need for per-node pool sizing - single configuration for all queries

### Phase 3: Validation
1. Run existing test suite to verify no regressions
2. Monitor `stalls_engine_waiting_on_data` metrics
3. Verify memory reuse across multiple queries
4. Validate with large-scale S3/GCS reads (200+ files)
5. Test concurrent queries sharing the same buffer pool

### Phase 4: Monitoring
1. Monitor production metrics for buffer pool utilization
2. Track `stalls_engine_waiting_on_data` reduction
3. Verify no memory leaks from persistent pool
4. Adjust page size/count if needed based on workload patterns

## Performance Expectations

### Current (Single Lock)
- 8 workers serialize completely at single RLock
- Effective parallelism: ~1x
- Lock wait time: O(n × blob_copy_time)
- With 5MB blobs @ 5ms each: 8 workers wait up to 40ms for lock

### Expected (Page-Level Locks)
With default configuration (512MB pages, CPU count = 8):
- 8 pages for 8 workers → minimal contention
- Each worker typically gets a page immediately
- 100ms timeout provides fast failover if needed
- Effective parallelism: ~7-8x (near-linear)
- Lock wait time: O((n/pages) × blob_copy_time) ≈ O(1) per worker

### Metrics Impact
- **`stalls_engine_waiting_on_data`:** Expected 80-90% reduction
  - Current: Thousands for 200 files
  - Expected: Hundreds (mostly network latency, not lock contention)
- **Throughput:** Expected 5-7x improvement on blob-heavy workloads
- **Memory overhead:** Minimal
  - 8 RLock objects vs 1: ~640 bytes
  - Python wrapper overhead: ~1KB
  - Total overhead: <0.01% of 4GB pool

### Scalability
- **Low worker count (1-2):** No regression, slight overhead acceptable
- **Medium worker count (4-8):** Major improvement, near-linear scaling
- **High worker count (16+):** May need more pages or larger pool

## Edge Cases & Considerations

### 1. Timeout Selection (100ms)
**Problem:** Short timeout → excessive page hopping; Long timeout → poor failover

**Solution:** 100ms balances well:
- Most commits complete in <20ms with lock held
- Fast enough to try all 8 pages in 800ms worst case
- Matches reader-side timeout convention
- Long enough to avoid spurious timeouts under normal load

### 2. Page Exhaustion
**Problem:** All pages could be full

**Solution:** 
- Return -1 (existing failure mode, already handled by callers)
- Caller's retry logic with 0.1s sleep already handles this
- Auto-resize per-page if enabled (existing MemoryPool feature)
- With 512MB × 8 pages = 4GB, exhaustion unlikely for typical workloads

### 3. Fragmentation Across Pages
**Problem:** Each page fragments independently, may waste space

**Solution:**
- Existing compaction/defragmentation works per-page (unchanged)
- Round-robin naturally balances load across pages
- No worse than current single-pool fragmentation
- Could add cross-page compaction later if needed (low priority)

### 4. Lock Fairness
**Problem:** Python's RLock doesn't guarantee fairness - starvation possible

**Solution:**
- Round-robin page selection provides fairness at page level
- Even if one page has unfair lock, other pages available
- Statistical fairness: worker probability of getting any page ≈ (num_pages - 1) / num_pages
- With 8 pages: 87.5% chance of getting A page even if one is contested

### 5. Statistics & Debugging
**Problem:** Per-page statistics need aggregation for debugging

**Solution:**
```python
def get_stats(self):
    """Aggregate statistics across all pages."""
    return {
        'total_commits': sum(p.commits for p in self.pages),
        'total_failed': sum(p.failed_commits for p in self.pages),
        'total_reads': sum(p.reads for p in self.pages),
        'total_releases': sum(p.releases for p in self.pages),
        'lock_timeouts': self.lock_timeouts,
        'page_full_retries': self.page_full_retries,
        'per_page': [
            {
                'name': p.name,
                'used_size': p.used_size,
                'free_size': p.size - p.used_size,
                'commits': p.commits
            }
            for p in self.pages
        ]
    }
```

### 6. Python Wrapper Performance
**Problem:** Python wrapper adds overhead vs pure Cython

**Analysis:**
- Wrapper overhead: ~1-2 µs for ref_id decode and delegation
- Lock acquisition: ~5-10ms (dominates the cost, same as before)
- Memcpy: Proportional to blob size (unchanged, done in Cython)
- **Verdict:** Wrapper overhead is <0.1% of total commit time, negligible

**Why Python wrapper is acceptable:**
- Timeout handling in Python is clean: `lock.acquire(timeout=0.1)`
- Cython's RLock doesn't expose timeout parameter easily
- The bottleneck is lock contention and memcpy, not wrapper overhead
- Simpler implementation and testing vs pure Cython

## Testing Plan

### Unit Tests
1. Single-threaded commit/read/release
2. Ref ID encoding/decoding correctness
3. Page selection round-robin behavior
4. Timeout handling

### Integration Tests
1. Multi-threaded concurrent commits (simulate 8 workers)
2. Mixed commit/read/release patterns
3. Page exhaustion scenarios
4. Lock timeout and failover

### Performance Tests
1. Benchmark vs single-lock pool with 1, 2, 4, 8 workers
2. Measure lock contention metrics
3. Profile commit latency distribution
4. Test with real S3/GCS workloads

## Alternative Approaches Considered

### 1. Lock-Free Allocation
**Pros:** Maximum parallelism
**Cons:** Complex implementation, harder to debug
**Decision:** Rejected - page-level locking is simpler and sufficient

### 2. Per-Worker Pools
**Pros:** Zero contention
**Cons:** Unbalanced memory usage, complex inter-pool ops
**Decision:** Rejected - doesn't decouple pool size from worker count

### 3. Moving memcpy Outside Lock
**Pros:** Minimal code change
**Cons:** Race conditions with compaction, complex state management
**Decision:** Rejected - page-level locking is cleaner architecture

## Open Questions

1. **Optimal page count for different workloads?**
   - Start with CPU count (adaptive to hardware)
   - Monitor lock_timeouts metric - if high, increase num_pages
   - If mostly idle, could reduce num_pages to save memory

2. **Adaptive timeout?**
   - Could track average commit time per page
   - Adjust timeout dynamically if commits consistently take longer
   - Low priority - 100ms is conservative enough

3. **Cross-page rebalancing?**
   - If load becomes imbalanced, migrate segments between pages
   - Complex and likely unnecessary given round-robin
   - Defer unless telemetry shows need

4. **Should we make this the default everywhere MemoryPool is used?**
   - AsyncReadNode: Yes (primary use case)
   - Other single-threaded uses: No benefit, slight overhead
   - Decision: Feature flag per use case

## Summary

**Problem:** Single RLock in MemoryPool serializes 8 async workers, causing high `stalls_engine_waiting_on_data`

**Solution:** Python wrapper (`PagedMemoryPool`) managing multiple Cython `MemoryPool` instances with:
- 512MB pages (configurable)
- CPU count pages by default (e.g., 8 pages = 4GB total)
- Round-robin selection with 100ms timeout failover
- Ref ID encoding: `(page_id << 48) | local_ref`

**Implementation:**
- New file: `opteryx/compiled/structures/paged_memory_pool.py`
- Existing `memory_pool.pyx` unchanged (zero risk)
- Drop-in API compatible with MemoryPool
- Feature flag for gradual rollout

**Expected Impact:**
- 80-90% reduction in `stalls_engine_waiting_on_data`
- 5-7x throughput improvement on blob-heavy workloads
- Near-linear scaling with worker count
- Negligible memory/CPU overhead

**Why Python wrapper vs pure Cython:**
- Clean timeout handling: `lock.acquire(timeout=0.1)`
- Simpler implementation and testing
- Wrapper overhead (<1µs) is negligible vs lock/memcpy (ms)
- Allows reusing battle-tested Cython MemoryPool unchanged

## References

- Current bottleneck analysis: See conversation history
- Lock contention telemetry: `stalls_engine_waiting_on_data` metric
- Similar patterns: PostgreSQL buffer pools, MySQL InnoDB page-level locks
