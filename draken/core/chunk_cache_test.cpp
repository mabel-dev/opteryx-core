// Policy tests for the chunk cache (chunk_cache.cpp): S3-FIFO admission, the
// remembered probation evictions, and cost-weighted CLOCK. Built and run by
// `make chunk-cache-test` against chunk_cache.cpp + mem_account.cpp only.
#include <cstdio>
#include <cstdlib>
#include <string>

#include "chunk_cache.h"
#include "mem_account.h"

static int g_failures = 0;
#define CHECK(cond, ...)                                     \
    do {                                                     \
        if (!(cond)) {                                       \
            std::fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
            std::fprintf(stderr, __VA_ARGS__);               \
            std::fprintf(stderr, "\n");                      \
            ++g_failures;                                    \
        }                                                    \
    } while (0)

constexpr int64_t kChunk = 100;
constexpr int64_t kCheapNs = 1;                  // weight 0: plain CLOCK
constexpr int64_t kDearNs = kChunk * 1000;       // 1000 ns/byte: weight 7

static std::string path_for(const char* kind, int i) {
    return std::string("/data/") + kind + "_" + std::to_string(i) + ".parquet";
}

static int insert(const std::string& p, int64_t cost_ns) {
    uint8_t* buf = draken_cc_fill_alloc(kChunk);
    const int64_t index[3] = {0, 0, kChunk};
    return draken_cc_insert(p.data(), p.size(), 0, buf, kChunk, index, 1, cost_ns);
}

static bool present(const std::string& p) {
    int fill_ok = 0;
    const DrakenChunkEntry* e = draken_cc_lookup(p.data(), p.size(), 0, 0, &fill_ok);
    if (e == nullptr) return false;
    draken_cc_unpin(e);
    return true;
}

// A cache of `chunks` chunks, empty.
static void reset(int64_t chunks) {
    draken_mem_configure(int64_t(1) << 40, 0, chunks * kChunk);
    draken_cc_flush();
}

static void empty_cache_fills_completely() {
    reset(50);
    const DrakenChunkCacheStats before = draken_cc_stats();
    for (int i = 0; i < 50; ++i) CHECK(insert(path_for("fill", i), kCheapNs) == 1, "fill %d refused", i);
    const DrakenChunkCacheStats s = draken_cc_stats();
    CHECK(s.bytes == 50 * kChunk, "cache holds %lld bytes, want %lld", (long long)s.bytes,
          (long long)(50 * kChunk));
    CHECK(s.evictions == before.evictions, "filling an empty cache evicted");
}

static void one_hit_chunks_leave_first_and_are_remembered() {
    reset(50);
    for (int i = 0; i < 50; ++i) insert(path_for("hot", i), kCheapNs);
    for (int i = 0; i < 50; ++i) present(path_for("hot", i));   // all used again
    const DrakenChunkCacheStats before = draken_cc_stats();
    // 50 one-hit chunks stream through: they must not displace the hot set.
    for (int i = 0; i < 50; ++i) insert(path_for("once", i), kCheapNs);
    const DrakenChunkCacheStats s = draken_cc_stats();
    CHECK(s.promotions > before.promotions, "no hot chunk was promoted off probation");
    int hot = 0;
    for (int i = 0; i < 50; ++i) hot += present(path_for("hot", i)) ? 1 : 0;
    int once = 0;
    for (int i = 0; i < 50; ++i) once += present(path_for("once", i)) ? 1 : 0;
    CHECK(hot > once, "one-hit chunks displaced the hot set: hot %d survived, one-hit %d", hot, once);
    // A one-hit chunk evicted from probation and filled again skips probation.
    int first_gone = -1;
    for (int i = 0; i < 50 && first_gone < 0; ++i)
        if (!present(path_for("once", i))) first_gone = i;
    CHECK(first_gone >= 0, "no one-hit chunk was evicted");
    if (first_gone >= 0) {
        const DrakenChunkCacheStats b2 = draken_cc_stats();
        insert(path_for("once", first_gone), kCheapNs);
        const DrakenChunkCacheStats s2 = draken_cc_stats();
        CHECK(s2.remembered_hits == b2.remembered_hits + 1, "a remembered chunk went through probation");
    }
}

static void expensive_chunks_outlive_cheap_ones() {
    reset(320);
    for (int i = 0; i < 160; ++i) {
        insert(path_for("dear", i), kDearNs);
        insert(path_for("cheap", i), kCheapNs);
    }
    for (int i = 0; i < 160; ++i) {
        present(path_for("dear", i));
        present(path_for("cheap", i));
    }
    for (int i = 0; i < 160; ++i) insert(path_for("new", i), kCheapNs);
    int dear = 0, cheap = 0;
    for (int i = 0; i < 160; ++i) {
        dear += present(path_for("dear", i)) ? 1 : 0;
        cheap += present(path_for("cheap", i)) ? 1 : 0;
    }
    CHECK(cheap < 160, "nothing cheap was evicted (the test made no pressure)");
    CHECK(dear > cheap, "expensive chunks did not outlive cheap ones: dear %d, cheap %d", dear, cheap);
}

static void flush_forgets_everything() {
    reset(10);
    for (int i = 0; i < 20; ++i) insert(path_for("gone", i), kCheapNs);   // half evicted, remembered
    draken_cc_flush();
    const DrakenChunkCacheStats b = draken_cc_stats();
    CHECK(b.bytes == 0 && b.probation_bytes == 0, "flush left %lld bytes (%lld on probation)",
          (long long)b.bytes, (long long)b.probation_bytes);
    for (int i = 0; i < 20; ++i) insert(path_for("gone", i), kCheapNs);
    const DrakenChunkCacheStats s = draken_cc_stats();
    CHECK(s.remembered_hits == b.remembered_hits, "flush kept remembered evictions");
}

int main() {
    empty_cache_fills_completely();
    one_hit_chunks_leave_first_and_are_remembered();
    expensive_chunks_outlive_cheap_ones();
    flush_forgets_everything();
    draken_cc_flush();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d chunk cache policy check(s) failed\n", g_failures);
        return 1;
    }
    std::printf("chunk cache policy: all checks passed\n");
    return 0;
}
