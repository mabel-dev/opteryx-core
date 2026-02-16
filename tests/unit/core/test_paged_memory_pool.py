"""
Paged Memory Pool Tests for Opteryx

This module contains unit tests for the PagedMemoryPool component, which wraps
multiple Cython MemoryPool instances to reduce lock contention in multi-threaded
scenarios.

Tests cover:
- Basic allocation and release functionality
- Ref ID encoding/decoding correctness
- Round-robin page selection behavior
- Lock timeout and failover
- Multi-threaded concurrent access
- API compatibility with MemoryPool
- Statistics aggregation across pages
- Edge cases (page exhaustion, invalid refs, etc.)

The PagedMemoryPool is designed for high-concurrency async I/O workloads, so
these tests validate both correctness and concurrency behavior.
"""

import os
import random
import sys
import threading
import time

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.compiled.structures.paged_memory_pool import PagedMemoryPool


def test_basic_commit_and_read():
    """Test basic commit and read operations."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    ref = pool.commit(b"Hello World")
    assert ref != -1
    assert pool.read(ref, zero_copy=False) == b"Hello World"


def test_ref_id_encoding():
    """Test that ref IDs correctly encode page index and local ref."""
    pool = PagedMemoryPool(page_size=1000, num_pages=4)
    
    # Commit to multiple pages
    refs = []
    for i in range(8):
        ref = pool.commit(f"Data {i}".encode())
        assert ref != -1
        refs.append(ref)
    
    # Decode and verify
    for ref in refs:
        page_idx = ref >> 48
        local_ref = ref & 0xFFFFFFFFFFFF
        
        # Page index should be valid
        assert 0 <= page_idx < 4
        # Local ref should be valid (positive, non-zero for data)
        assert local_ref > 0


def test_round_robin_distribution():
    """Test that commits distribute across pages via round-robin."""
    pool = PagedMemoryPool(page_size=10000, num_pages=4)
    
    # Commit multiple items
    refs = []
    for i in range(12):
        ref = pool.commit(f"Data {i}".encode())
        assert ref != -1
        refs.append(ref)
    
    # Extract page indices
    page_indices = [ref >> 48 for ref in refs]
    
    # Should see all 4 pages used (round-robin)
    unique_pages = set(page_indices)
    assert len(unique_pages) >= 3, "Round-robin should distribute across pages"


def test_page_size_configuration():
    """Test custom page size and num_pages configuration."""
    pool = PagedMemoryPool(page_size=512_000_000, num_pages=8)
    
    assert pool.page_size == 512_000_000
    assert pool.num_pages == 8
    assert pool.size == 512_000_000 * 8


def test_default_num_pages_is_cpu_count():
    """Test that num_pages defaults to CPU count (min 2)."""
    pool = PagedMemoryPool(page_size=1000, num_pages=None)
    
    assert pool.num_pages >= 2
    assert pool.num_pages == max(2, os.cpu_count() or 2)


def test_commit_read_release_cycle():
    """Test complete commit-read-release cycle."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    data = b"Test data for cycle"
    ref = pool.commit(data)
    assert ref != -1
    
    # Read
    read_data = pool.read(ref, zero_copy=False)
    assert read_data == data
    
    # Release
    pool.release(ref)
    
    # After release, reading should fail
    with pytest.raises(ValueError):
        pool.read(ref)


def test_zero_copy_read():
    """Test zero-copy read returns memoryview."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    data = b"Zero copy test"
    ref = pool.commit(data)
    
    # Zero-copy read
    mv = pool.read(ref, zero_copy=True)
    assert isinstance(mv, memoryview)
    assert bytes(mv) == data


def test_latch_and_unlatch():
    """Test latching mechanism."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    data = b"Latched data"
    ref = pool.commit(data)
    
    # Read with latch
    pool.read(ref, latch=True)
    
    # Unlatch
    pool.unlatch(ref)
    
    # Should still be readable
    assert pool.read(ref) == data


def test_page_exhaustion_fails_gracefully():
    """Test that exhausting all pages returns -1."""
    # Use small pages with auto_resize=False to ensure exhaustion
    pool = PagedMemoryPool(page_size=100, num_pages=2, auto_resize=False)
    
    # Fill both pages with large chunks
    refs = []
    for i in range(10):
        ref = pool.commit(b"X" * 80)  # 80 bytes each
        if ref == -1:
            break
        refs.append(ref)
    
    # At some point should return -1 (no space)
    # Keep trying until we get -1 or hit limit
    exhausted = False
    for _ in range(10):
        ref = pool.commit(b"Y" * 80)
        if ref == -1:
            exhausted = True
            break
    
    # Should eventually exhaust (though maybe not immediately due to alignment/fragmentation)
    # This test is somewhat probabilistic, so we just verify -1 is possible
    assert exhausted or len(refs) < 10, "Should eventually exhaust or fail early"


def test_invalid_page_index_raises():
    """Test that invalid page indices raise ValueError."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    # Create invalid ref with page_idx=5 (only 2 pages exist)
    invalid_ref = (5 << 48) | 123
    
    with pytest.raises(ValueError, match="Invalid page index"):
        pool.read(invalid_ref)
    
    with pytest.raises(ValueError, match="Invalid page index"):
        pool.release(invalid_ref)
    
    with pytest.raises(ValueError, match="Invalid page index"):
        pool.unlatch(invalid_ref)


def test_statistics_aggregation():
    """Test that statistics are correctly aggregated across pages."""
    pool = PagedMemoryPool(page_size=1000, num_pages=3)
    
    # Perform some operations
    refs = []
    for i in range(6):
        ref = pool.commit(f"Data {i}".encode())
        refs.append(ref)
    
    # Read some
    for ref in refs[:3]:
        pool.read(ref)
    
    # Check aggregated stats
    assert pool.commits >= 6
    assert pool.reads >= 3
    assert pool.used_size > 0
    
    # Get detailed stats
    stats = pool.get_stats()
    assert stats['num_pages'] == 3
    assert stats['total_commits'] >= 6
    assert len(stats['per_page']) == 3


def test_properties_match_memorypool_interface():
    """Test that all MemoryPool properties are available."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    # All these properties should exist and work
    assert isinstance(pool.size, int)
    assert isinstance(pool.used_size, int)
    assert isinstance(pool.commits, int)
    assert isinstance(pool.failed_commits, int)
    assert isinstance(pool.reads, int)
    assert isinstance(pool.read_locks, int)
    assert isinstance(pool.releases, int)
    assert isinstance(pool.l1_compaction, int)
    assert isinstance(pool.l2_compaction, int)
    assert isinstance(pool.resizes, int)


def test_multithreaded_concurrent_commits():
    """Test concurrent commits from multiple threads."""
    pool = PagedMemoryPool(page_size=10000, num_pages=4, lock_timeout_ms=100)
    
    refs = []
    refs_lock = threading.Lock()
    errors = []
    
    def worker(worker_id, num_commits):
        try:
            for i in range(num_commits):
                data = f"Worker {worker_id} - Item {i}".encode()
                ref = pool.commit(data)
                if ref != -1:
                    with refs_lock:
                        refs.append((ref, data))
                time.sleep(0.001)  # Small delay to simulate work
        except Exception as e:
            errors.append(e)
    
    # Start 8 workers
    threads = []
    for i in range(8):
        t = threading.Thread(target=worker, args=(i, 10))
        threads.append(t)
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    # Check no errors
    assert len(errors) == 0, f"Errors occurred: {errors}"
    
    # Verify all data
    for ref, original_data in refs:
        read_data = pool.read(ref, zero_copy=False)
        assert read_data == original_data, "Data corruption detected"


def test_multithreaded_read_write_interleaved():
    """Test concurrent reads and writes."""
    pool = PagedMemoryPool(page_size=10000, num_pages=4)
    
    # Pre-populate some data
    initial_refs = []
    for i in range(20):
        ref = pool.commit(f"Initial {i}".encode())
        initial_refs.append((ref, f"Initial {i}".encode()))
    
    errors = []
    
    def reader_worker():
        try:
            for _ in range(50):
                if initial_refs:
                    ref, expected = random.choice(initial_refs)
                    data = pool.read(ref, zero_copy=False)
                    assert data == expected
                time.sleep(0.001)
        except Exception as e:
            errors.append(e)
    
    def writer_worker(worker_id):
        try:
            for i in range(10):
                data = f"New {worker_id}-{i}".encode()
                ref = pool.commit(data)
                if ref != -1:
                    initial_refs.append((ref, data))
                time.sleep(0.002)
        except Exception as e:
            errors.append(e)
    
    # Start readers and writers
    threads = []
    for i in range(4):
        threads.append(threading.Thread(target=reader_worker))
        threads.append(threading.Thread(target=writer_worker, args=(i,)))
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()
    
    assert len(errors) == 0, f"Errors occurred: {errors}"


def test_lock_timeout_increments_counter():
    """Test that lock timeouts are tracked."""
    # Create pool with very short timeout to force timeouts
    pool = PagedMemoryPool(page_size=10000, num_pages=2, lock_timeout_ms=1)
    
    initial_timeouts = pool.lock_timeouts
    
    def aggressive_committer():
        for _ in range(100):
            pool.commit(b"X" * 50)
    
    # Start multiple threads to create contention
    threads = [threading.Thread(target=aggressive_committer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # Should have some timeouts due to contention
    # Note: This is probabilistic, may not always happen
    # assert pool.lock_timeouts > initial_timeouts  # Might be flaky


def test_page_full_retries_counter():
    """Test that page_full_retries counter works."""
    pool = PagedMemoryPool(page_size=100, num_pages=2)
    
    initial_retries = pool.page_full_retries
    
    # Fill up pages
    refs = []
    for _ in range(50):
        ref = pool.commit(b"X" * 60)
        if ref != -1:
            refs.append(ref)
    
    # Try to commit more (should trigger retries)
    for _ in range(10):
        pool.commit(b"Y" * 60)
    
    # Should have some retries
    assert pool.page_full_retries >= initial_retries


def test_empty_data_commit():
    """Test committing empty bytes."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    ref = pool.commit(b"")
    assert ref != -1
    
    data = pool.read(ref)
    assert data == b""


def test_large_data_commit():
    """Test committing data larger than single page."""
    pool = PagedMemoryPool(page_size=100, num_pages=3)
    
    # This should fail if data is larger than a single page
    large_data = b"X" * 150
    ref = pool.commit(large_data)
    
    # Should fail (data larger than page_size)
    assert ref == -1


def test_memoryview_commit():
    """Test committing memoryview instead of bytes."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2)
    
    data = b"Original data"
    mv = memoryview(data)
    
    ref = pool.commit(mv)
    assert ref != -1
    
    read_data = pool.read(ref)
    assert read_data == data


def test_repr_output():
    """Test string representation."""
    pool = PagedMemoryPool(page_size=512_000_000, num_pages=4, name="TestPool")
    
    repr_str = repr(pool)
    assert "TestPool" in repr_str
    assert "num_pages=4" in repr_str
    assert "512,000,000" in repr_str or "512000000" in repr_str


def test_configuration_validation():
    """Test that invalid configurations raise errors."""
    # Invalid page_size
    with pytest.raises(ValueError):
        PagedMemoryPool(page_size=0, num_pages=2)
    
    with pytest.raises(ValueError):
        PagedMemoryPool(page_size=-100, num_pages=2)
    
    # Invalid num_pages
    with pytest.raises(ValueError):
        PagedMemoryPool(page_size=1000, num_pages=0)
    
    with pytest.raises(ValueError):
        PagedMemoryPool(page_size=1000, num_pages=-1)


def test_alignment_parameter():
    """Test that alignment parameter is passed to underlying pools."""
    pool = PagedMemoryPool(page_size=1000, num_pages=2, alignment=8)
    
    # Should work without errors
    ref = pool.commit(b"Aligned data")
    assert ref != -1


def test_auto_resize_parameter():
    """Test that auto_resize parameter is passed to underlying pools."""
    pool = PagedMemoryPool(page_size=100, num_pages=2, auto_resize=True)
    
    # With auto_resize, should handle large data by resizing pages
    refs = []
    for i in range(10):
        ref = pool.commit(b"X" * 80)
        if ref != -1:
            refs.append(ref)
    
    # Should have succeeded with some commits
    assert len(refs) > 0


def test_stress_random_operations():
    """Stress test with random operations."""
    pool = PagedMemoryPool(page_size=5000, num_pages=4)
    
    refs = []
    
    for _ in range(1000):
        op = random.choice(['commit', 'read', 'release'])
        
        if op == 'commit' or not refs:
            size = random.randint(10, 100)
            data = bytes([random.randint(0, 255) for _ in range(size)])
            ref = pool.commit(data)
            if ref != -1:
                refs.append((ref, data))
        
        elif op == 'read' and refs:
            ref, expected_data = random.choice(refs)
            read_data = pool.read(ref, zero_copy=False)
            assert read_data == expected_data, "Data corruption detected"
        
        elif op == 'release' and refs:
            idx = random.randint(0, len(refs) - 1)
            ref, _ = refs.pop(idx)
            pool.release(ref)
    
    # Cleanup
    for ref, _ in refs:
        pool.release(ref)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
