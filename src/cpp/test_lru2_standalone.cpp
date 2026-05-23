#include "lru2.hpp"
#include <cassert>
#include <cstdio>
#include <string>

using namespace opteryx;

static int passed = 0;
static int failed = 0;

#define ASSERT(cond, msg) do { \
    if (!(cond)) { printf("FAIL  %s\n", msg); failed++; } \
    else         { printf("pass  %s\n", msg); passed++; } \
} while(0)

static LRU2 make(int64_t max_size) { return LRU2(max_size, 0); }

static void set(LRU2& c, const char* k, const char* v, bool evict = false) {
    c.set(k, (int64_t)strlen(k), v, (int64_t)strlen(v), evict);
}

static const std::string* get(LRU2& c, const char* k) {
    return c.get(k, (int64_t)strlen(k));
}

static std::pair<std::string,std::string> evict(LRU2& c, bool need_value = true) {
    return c.evict_one(need_value);
}

// ── tests ─────────────────────────────────────────────────────────────────────

void test_basic_get_set() {
    LRU2 c = make(10);
    set(c, "a", "1");
    auto* v = get(c, "a");
    ASSERT(v != nullptr,    "basic get: found");
    ASSERT(*v == "1",       "basic get: value correct");
    ASSERT(get(c, "z") == nullptr, "miss returns nullptr");
}

void test_overwrite_key() {
    LRU2 c = make(10);
    set(c, "a", "1");
    set(c, "a", "2");
    ASSERT(c.size() == 1,   "overwrite: size stays 1");
    ASSERT(*get(c, "a") == "2", "overwrite: new value visible");
}

void test_evict_young_before_mature() {
    // a gets 1 access (young), b gets 2 accesses (mature).
    // Eviction must choose a (young) even though b was accessed longer ago.
    LRU2 c = make(0);
    set(c, "b", "B");  // access 1 for b (clock=1)
    set(c, "b", "B");  // access 2 for b — now mature (clock=2)
    set(c, "a", "A");  // access 1 for a — young (clock=3)

    auto res = evict(c, true);
    ASSERT(res.first == "a",  "young evicted before mature");
    ASSERT(res.second == "A", "evicted value correct");
    ASSERT(c.size() == 1,     "size after eviction");
}

void test_evict_mature_oldest_kth() {
    // Both entries are mature.  The one with the oldest 2nd-most-recent access
    // should be evicted.
    LRU2 c = make(0);
    set(c, "a", "A"); // clock=1 (young)
    set(c, "a", "A"); // clock=2 → a mature, prev=1
    set(c, "b", "B"); // clock=3 (young)
    set(c, "b", "B"); // clock=4 → b mature, prev=3

    // a.prev_access=1, b.prev_access=3 → a should be evicted (older K-th)
    auto res = evict(c, true);
    ASSERT(res.first == "a", "mature: oldest K-th access evicted");
}

void test_access_rejuvenates_entry() {
    // a: clocks 1,2 → mature, prev=1.  b: clocks 3,4 → mature, prev=3.
    // Without further access, a would be evicted (prev=1 < prev=3).
    // Two gets on a at clocks 5,6 → a.prev=5 > b.prev=3.
    // Now b should be evicted (its K-th access is older).
    LRU2 c = make(0);
    set(c, "a", "A");  // 1
    set(c, "a", "A");  // 2 → a mature, prev=1
    set(c, "b", "B");  // 3
    set(c, "b", "B");  // 4 → b mature, prev=3
    get(c, "a");        // 5 → a.prev=2, last=5
    get(c, "a");        // 6 → a.prev=5, last=6; now a.prev=5 > b.prev=3

    auto res = evict(c, true);
    ASSERT(res.first == "b", "rejuvenation: b evicted (a.prev surpassed b.prev)");
}

void test_size_limit_eviction() {
    LRU2 c = make(3);
    set(c, "a", "A");
    set(c, "b", "B");
    set(c, "c", "C");
    ASSERT(c.size() == 3, "size at limit");
    set(c, "d", "D", true);  // should trigger eviction
    ASSERT(c.size() == 3, "size held after auto-evict");
}

void test_erase() {
    LRU2 c = make(10);
    set(c, "a", "A");
    bool removed = c.erase("a", 1);
    ASSERT(removed, "erase returns true");
    ASSERT(get(c, "a") == nullptr, "erased key not found");
    ASSERT(c.size() == 0, "size after erase");
}

void test_clear() {
    LRU2 c = make(10);
    set(c, "a", "A");
    set(c, "b", "B");
    c.clear(true);
    ASSERT(c.size() == 0, "clear: size 0");
    ASSERT(get(c, "a") == nullptr, "clear: no items");
    ASSERT(c.hits() == 0, "clear resets stats");
}

void test_hits_misses_stats() {
    LRU2 c = make(10);
    set(c, "a", "A");
    get(c, "a");
    get(c, "z");
    ASSERT(c.hits()   == 1, "stats: 1 hit");
    ASSERT(c.misses() == 1, "stats: 1 miss");
}

void test_evict_no_value() {
    LRU2 c = make(0);
    set(c, "a", "A");
    auto res = c.evict_one(false);
    ASSERT(res.first == "a",  "no-value evict: key correct");
    ASSERT(res.second.empty(), "no-value evict: value empty");
}

void test_empty_evict() {
    LRU2 c = make(0);
    auto res = evict(c, true);
    ASSERT(res.first.empty(), "empty evict: no key");
}

void test_contains() {
    LRU2 c = make(10);
    set(c, "a", "A");
    ASSERT(c.contains("a", 1),  "contains: present");
    ASSERT(!c.contains("z", 1), "contains: absent");
}

void test_lru_policy_fifo_young() {
    // Among young entries, evict the one first accessed (FIFO).
    LRU2 c = make(0);
    set(c, "a", "A");  // clock=1
    set(c, "b", "B");  // clock=2
    set(c, "c", "C");  // clock=3
    // All young, a was first
    auto r1 = evict(c, true);
    ASSERT(r1.first == "a", "young FIFO: a evicted first");
    auto r2 = evict(c, true);
    ASSERT(r2.first == "b", "young FIFO: b evicted second");
}

// ── main ─────────────────────────────────────────────────────────────────────

int main() {
    test_basic_get_set();
    test_overwrite_key();
    test_evict_young_before_mature();
    test_evict_mature_oldest_kth();
    test_access_rejuvenates_entry();
    test_size_limit_eviction();
    test_erase();
    test_clear();
    test_hits_misses_stats();
    test_evict_no_value();
    test_empty_evict();
    test_contains();
    test_lru_policy_fifo_young();

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
