#include "memory_pool.hpp"
#include <cassert>
#include <cstring>
#include <cstdio>

using namespace opteryx;

void test_basic_commit_read_release() {
    MemoryPool pool(100);

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
    MemoryPool pool(100);

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
    MemoryPool pool(100);

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

void test_l1_compaction() {
    MemoryPool pool(100);

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
    pool.release(r2);

    PoolStats before = pool.get_stats();
    pool.level1_compaction();
    PoolStats after = pool.get_stats();

    assert(after.l1_compactions >= 1);
    assert(after.free_blocks < before.free_blocks || after.free_blocks == 1);

    pool.release(r3);

    printf("test_l1_compaction: PASS\n");
}

void test_l2_compaction_moves_unlatched() {
    MemoryPool pool(50);

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

    pool.level2_compaction();
    PoolStats stats = pool.get_stats();
    assert(stats.l2_compactions >= 1);

    ReadResult result = pool.read(r2, false);
    assert(result.length == 10);
    assert(std::memcmp(result.ptr, data_b, 10) == 0);

    pool.release(r2);

    printf("test_l2_compaction_moves_unlatched: PASS\n");
}

void test_latched_segment_not_moved_by_l2() {
    MemoryPool pool(50);

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

    pool.level2_compaction();

    ReadResult after = pool.read(r2, false);
    const void* ptr_after = after.ptr;

    assert(ptr_after == ptr_before);
    assert(after.length == 10);
    assert(std::memcmp(after.ptr, data_b, 10) == 0);

    pool.unlatch(r2);
    pool.release(r2);

    printf("test_latched_segment_not_moved_by_l2: PASS\n");
}

void test_reserve_and_finalize() {
    MemoryPool pool(100);

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
    MemoryPool pool(100);

    unsigned char data_x[10] = {'X'};
    unsigned char data_y[15] = {'Y'};

    pool.commit(data_x, 10);
    pool.commit(data_y, 15);

    pool.clear();

    PoolStats stats = pool.get_stats();
    assert(stats.used_size == 0);
    assert(stats.commits == 0);
    assert(stats.free_blocks == 1);
    assert(stats.largest_free_block == 100);

    printf("test_clear_resets_pool: PASS\n");
}

void test_release_clears_latches() {
    MemoryPool pool(100);

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
    test_l1_compaction();
    test_l2_compaction_moves_unlatched();
    test_latched_segment_not_moved_by_l2();
    test_reserve_and_finalize();
    test_clear_resets_pool();
    test_release_clears_latches();
    test_auto_resize();

    printf("All tests passed.\n");
    return 0;
}
