// Policy tests for the chunk cache (chunk_cache.cpp): S3-FIFO admission, the
// remembered probation evictions, cost-weighted CLOCK, and COMPRESSED -> IMAGE swaps. Built and run by
// `make chunk-cache-test` against chunk_cache.cpp + mem_account.cpp only.
#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>

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

// §30: a COMPRESSED entry goes on probation; a swap puts its IMAGE in MAIN while a
// reader still holding the COMPRESSED pin keeps its bytes; a swap of an entry that
// left the cache is refused and stays gone.
static void compressed_entry_swaps_to_its_image() {
    reset(50);
    const std::string p = path_for("swap", 0);
    uint8_t* raw = draken_cc_fill_alloc(kChunk / 4);
    for (int64_t i = 0; i < kChunk / 4; ++i) raw[i] = 0xAB;
    const DrakenChunkCacheStats b = draken_cc_stats();
    CHECK(draken_cc_insert_compressed(p.data(), p.size(), 0, raw, kChunk / 4, kDearNs) == 1,
          "compressed insert refused");
    DrakenChunkCacheStats s = draken_cc_stats();
    CHECK(s.compressed_bytes == b.compressed_bytes + kChunk / 4, "compressed bytes not counted");
    CHECK(s.probation_bytes == b.probation_bytes + kChunk / 4, "compressed entry not on probation");

    int fill_ok = 1;
    const DrakenChunkEntry* old = draken_cc_lookup(p.data(), p.size(), 0, 0, &fill_ok);
    CHECK(old != nullptr && draken_cc_entry_compressed(old) == 1, "lookup did not find the compressed entry");
    CHECK(draken_cc_stats().compressed_hits == s.compressed_hits + 1, "compressed hit not counted");
    const uint8_t* page = nullptr;
    int64_t page_len = 0;
    CHECK(draken_cc_page(old, 0, &page, &page_len) == 0, "a compressed entry indexed a page");

    uint8_t* img = draken_cc_fill_alloc(kChunk);
    const int64_t index[3] = {0, 0, kChunk};
    CHECK(draken_cc_replace(p.data(), p.size(), 0, old, img, kChunk, index, 1, kDearNs) == 1,
          "swap refused");
    s = draken_cc_stats();
    CHECK(s.swaps == b.swaps + 1, "swap not counted");
    CHECK(s.compressed_bytes == b.compressed_bytes, "swapped-out bytes still counted compressed");
    CHECK(s.probation_bytes == b.probation_bytes, "image went to probation, not MAIN");
    CHECK(s.bytes == b.bytes + kChunk, "cache holds %lld bytes after the swap", (long long)s.bytes);
    CHECK(s.inserts == b.inserts + 1, "a swap counted as an insert");
    const uint8_t* old_bytes = nullptr;
    int64_t old_len = 0;
    draken_cc_entry_bytes(old, &old_bytes, &old_len);
    CHECK(old_len == kChunk / 4 && old_bytes[0] == 0xAB && old_bytes[old_len - 1] == 0xAB,
          "the pinned compressed bytes did not survive the swap");
    const DrakenChunkEntry* now = draken_cc_lookup(p.data(), p.size(), 0, 0, &fill_ok);
    CHECK(now != nullptr && now != old && draken_cc_entry_compressed(now) == 0,
          "the key does not hold the image after the swap");
    if (now != nullptr) draken_cc_unpin(now);
    draken_cc_unpin(old);   // the swapped-out entry is freed here

    // A swap of an entry that left the cache meanwhile is refused.
    const std::string q = path_for("swap", 1);
    draken_cc_insert_compressed(q.data(), q.size(), 0, draken_cc_fill_alloc(kChunk / 4), kChunk / 4, kDearNs);
    const DrakenChunkEntry* gone = draken_cc_lookup(q.data(), q.size(), 0, 0, &fill_ok);
    CHECK(gone != nullptr, "second compressed entry missing");
    draken_cc_flush();
    const DrakenChunkCacheStats f = draken_cc_stats();
    CHECK(draken_cc_replace(q.data(), q.size(), 0, gone, draken_cc_fill_alloc(kChunk), kChunk, index, 1,
                            kDearNs) == 0,
          "a swap of an evicted entry was admitted");
    CHECK(draken_cc_stats().bytes == f.bytes, "a refused swap changed the cache");
    if (gone != nullptr) draken_cc_unpin(gone);
}

// draken::MemCharge (mem_account.h): a held charge makes a full cache give way
// exactly as an allocator charge does, and set / move / adopt / destruction
// leave the account where it started - a leak here would starve the cache.
static void mem_charge_gives_way_and_balances() {
    draken_mem_configure(20 * kChunk, 0, 10 * kChunk);   // room for 10 chunks
    draken_cc_flush();
    const int64_t before = draken_mem_charged();
    for (int i = 0; i < 10; ++i) insert(path_for("held", i), kCheapNs);
    CHECK(draken_mem_cache_bytes() == 10 * kChunk, "cache did not fill: %lld bytes",
          (long long)draken_mem_cache_bytes());
    {
        draken::MemCharge a;
        a.set(15 * kChunk);   // leaves the cache room for 5 chunks
        CHECK(draken_mem_charged() == before + 15 * kChunk, "set charged %lld",
              (long long)(draken_mem_charged() - before));
        CHECK(draken_mem_cache_bytes() <= 5 * kChunk, "cache did not give way: %lld bytes",
              (long long)draken_mem_cache_bytes());
        draken::MemCharge b = std::move(a);
        CHECK(a.bytes() == 0 && b.bytes() == 15 * kChunk, "move did not transfer the charge");
        CHECK(draken_mem_charged() == before + 15 * kChunk, "move changed the account");
        draken::MemCharge c;
        c.set(kChunk);
        b.adopt(c);
        CHECK(c.bytes() == 0 && b.bytes() == 16 * kChunk, "adopt did not transfer the charge");
        CHECK(draken_mem_charged() == before + 16 * kChunk, "adopt changed the account");
        b.set(2 * kChunk);
        CHECK(draken_mem_charged() == before + 2 * kChunk, "set down did not uncharge");
    }
    CHECK(draken_mem_charged() == before, "charges leaked: %lld bytes",
          (long long)(draken_mem_charged() - before));
}

int main() {
    empty_cache_fills_completely();
    one_hit_chunks_leave_first_and_are_remembered();
    expensive_chunks_outlive_cheap_ones();
    flush_forgets_everything();
    compressed_entry_swaps_to_its_image();
    mem_charge_gives_way_and_balances();
    draken_cc_flush();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d chunk cache policy check(s) failed\n", g_failures);
        return 1;
    }
    std::printf("chunk cache policy: all checks passed\n");
    return 0;
}
