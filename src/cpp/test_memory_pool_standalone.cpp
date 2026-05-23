#include "memory_pool.hpp"
#include <cassert>
#include <cstring>
#include <cstdio>

using namespace opteryx;

// Pool size used by most tests — large enough that a 10-byte commit always
// leaves a remainder >= kMinSplitRemainder so segments split as expected.
static const int64_t TEST_POOL = 2048;

void test_basic_commit_read_release() {
    MemoryPool pool(TEST_POOL);

    unsigned char data[10];
    for (int i = 0; i < 10; i++) {
        data[i] = 'A';
    }

    int64_t ref = pool.commit(data, 10);
    assert(ref >= 1);

    ReadResult result = pool.read(ref, false);
    assert(result.ptr != nullptr);
    assert(result.length == 10);
    assert(std::memcmp(result.ptr, data, 10) == 0);

    PoolStats stats = pool.get_stats();
    assert(stats.commits == 1);
    assert(stats.used_size == 10);

    pool.release(ref);
    stats = pool.get_stats();
    assert(stats.used_size == 0);

    printf("test_basic_commit_read_release: PASS\n");
}

void test_invalid_ref_throws() {
    MemoryPool pool(TEST_POOL);

    unsigned char data[10] = {'X'};
    int64_t ref = pool.commit(data, 10);
    pool.release(ref);

    bool caught_read = false;
    try {
        pool.read(ref, false);
    } catch (const std::invalid_argument&) {
        caught_read = true;
    }
    assert(caught_read);

    bool caught_release = false;
    try {
        pool.release(ref);
    } catch (const std::invalid_argument&) {
        caught_release = true;
    }
    assert(caught_release);

    bool caught_unlatch = false;
    try {
        pool.unlatch(ref);
    } catch (const std::invalid_argument&) {
        caught_unlatch = true;
    }
    assert(caught_unlatch);

    printf("test_invalid_ref_throws: PASS\n");
}

void test_zero_length_commit() {
    MemoryPool pool(TEST_POOL);

    int64_t ref = pool.commit(nullptr, 0);
    assert(ref >= 1);

    ReadResult result = pool.read(ref, false);
    assert(result.ptr == nullptr);
    assert(result.length == 0);

    bool caught_unlatch = false;
    try {
        pool.unlatch(ref);
    } catch (const std::runtime_error&) {
        caught_unlatch = true;
    }
    assert(!caught_unlatch);

    pool.release(ref);

    printf("test_zero_length_commit: PASS\n");
}

void test_min_split_threshold() {
    // A remainder below kMinSplitRemainder (256) absorbs into the allocation
    // rather than creating a tiny free fragment.
    MemoryPool pool(TEST_POOL);

    // Commit TEST_POOL - 100 bytes: leaves a 100-byte remainder, below threshold.
    // The full TEST_POOL bytes should be given; no free fragment created.
    int64_t large_size = TEST_POOL - 100;
    unsigned char* data = new unsigned char[large_size];
    std::memset(data, 'X', large_size);

    int64_t ref = pool.commit(data, large_size);
    assert(ref >= 1);

    PoolStats stats = pool.get_stats();
    assert(stats.used_size == TEST_POOL);   // whole pool absorbed
    assert(stats.free_blocks == 0);

    pool.release(ref);
    delete[] data;

    printf("test_min_split_threshold: PASS\n");
}

void test_coalesce_on_release() {
    // Adjacent frees coalesce immediately during release, without explicit compaction.
    MemoryPool pool(TEST_POOL);

    unsigned char data_a[10];
    unsigned char data_b[10];
    unsigned char data_c[10];
    std::memset(data_a, 'A', 10);
    std::memset(data_b, 'B', 10);
    std::memset(data_c, 'C', 10);

    int64_t r1 = pool.commit(data_a, 10);
    int64_t r2 = pool.commit(data_b, 10);
    int64_t r3 = pool.commit(data_c, 10);

    pool.release(r1);
    // r2 still in use — no coalesce yet, 2 free blocks (r1's slot + trailing)
    assert(pool.get_stats().free_blocks == 2);

    pool.release(r2);
    // r1+r2 coalesce immediately: still 2 free blocks (merged r1+r2, plus trailing)
    assert(pool.get_stats().free_blocks == 2);

    pool.release(r3);

    printf("test_coalesce_on_release: PASS\n");
}

void test_compaction_moves_unlatched() {
    MemoryPool pool(TEST_POOL);

    unsigned char data_a[10];
    unsigned char data_b[10];
    unsigned char data_c[10];
    std::memset(data_a, 'A', 10);
    std::memset(data_b, 'B', 10);
    std::memset(data_c, 'C', 10);

    int64_t r1 = pool.commit(data_a, 10);
    int64_t r2 = pool.commit(data_b, 10);
    int64_t r3 = pool.commit(data_c, 10);

    pool.release(r1);
    pool.release(r3);

    pool.compaction();
    PoolStats stats = pool.get_stats();
    assert(stats.compactions >= 1);

    ReadResult result = pool.read(r2, false);
    assert(result.length == 10);
    assert(std::memcmp(result.ptr, data_b, 10) == 0);

    pool.release(r2);

    printf("test_compaction_moves_unlatched: PASS\n");
}

void test_latched_segment_not_moved_by_compaction() {
    MemoryPool pool(TEST_POOL);

    unsigned char data_a[10];
    unsigned char data_b[10];
    unsigned char data_c[10];
    std::memset(data_a, 'A', 10);
    std::memset(data_b, 'B', 10);
    std::memset(data_c, 'C', 10);

    int64_t r1 = pool.commit(data_a, 10);
    int64_t r2 = pool.commit(data_b, 10);
    int64_t r3 = pool.commit(data_c, 10);

    pool.latch(r2);

    ReadResult before = pool.read(r2, false);
    const void* ptr_before = before.ptr;

    pool.release(r1);
    pool.release(r3);

    pool.compaction();

    ReadResult after = pool.read(r2, false);
    const void* ptr_after = after.ptr;

    assert(ptr_after == ptr_before);
    assert(after.length == 10);
    assert(std::memcmp(after.ptr, data_b, 10) == 0);

    pool.unlatch(r2);
    pool.release(r2);

    printf("test_latched_segment_not_moved_by_compaction: PASS\n");
}

void test_reserve_and_finalize() {
    MemoryPool pool(TEST_POOL);

    ReserveResult reserve = pool.reserve_for_write(20);
    assert(reserve.ref_id >= 1);
    assert(reserve.ptr != nullptr);
    assert(reserve.capacity >= 20);

    unsigned char* ptr = static_cast<unsigned char*>(reserve.ptr);
    for (int i = 0; i < 15; i++) {
        ptr[i] = 'X';
    }

    pool.finalize_commit(reserve.ref_id, 15);

    ReadResult result = pool.read(reserve.ref_id, false);
    assert(result.length == 15);
    assert(std::memcmp(result.ptr, ptr, 15) == 0);

    pool.release(reserve.ref_id);

    printf("test_reserve_and_finalize: PASS\n");
}

void test_clear_resets_pool() {
    MemoryPool pool(TEST_POOL);

    unsigned char data_x[10] = {'X'};
    unsigned char data_y[15] = {'Y'};

    pool.commit(data_x, 10);
    pool.commit(data_y, 15);

    pool.clear();

    PoolStats stats = pool.get_stats();
    assert(stats.used_size == 0);
    assert(stats.commits == 0);
    assert(stats.free_blocks == 1);
    assert(stats.largest_free_block == TEST_POOL);

    printf("test_clear_resets_pool: PASS\n");
}

void test_release_clears_latches() {
    MemoryPool pool(TEST_POOL);

    unsigned char data[10] = {'X'};
    int64_t ref = pool.commit(data, 10);

    pool.latch(ref);
    pool.release(ref);

    bool caught = false;
    try {
        pool.unlatch(ref);
    } catch (const std::invalid_argument&) {
        caught = true;
    }
    assert(caught);

    printf("test_release_clears_latches: PASS\n");
}

void test_auto_resize() {
    MemoryPool pool(20, "Test Pool", true, 1);

    unsigned char data_a[15];
    unsigned char data_b[15];
    std::memset(data_a, 'A', 15);
    std::memset(data_b, 'B', 15);

    int64_t r1 = pool.commit(data_a, 15);
    assert(r1 >= 1);

    int64_t r2 = pool.commit(data_b, 15);
    assert(r2 >= 1);

    ReadResult result1 = pool.read(r1, false);
    assert(std::memcmp(result1.ptr, data_a, 15) == 0);

    ReadResult result2 = pool.read(r2, false);
    assert(std::memcmp(result2.ptr, data_b, 15) == 0);

    PoolStats stats = pool.get_stats();
    assert(stats.resizes >= 1);

    pool.release(r1);
    pool.release(r2);

    printf("test_auto_resize: PASS\n");
}

int main() {
    test_basic_commit_read_release();
    test_invalid_ref_throws();
    test_zero_length_commit();
    test_min_split_threshold();
    test_coalesce_on_release();
    test_compaction_moves_unlatched();
    test_latched_segment_not_moved_by_compaction();
    test_reserve_and_finalize();
    test_clear_resets_pool();
    test_release_clears_latches();
    test_auto_resize();

    printf("All tests passed.\n");
    return 0;
}
