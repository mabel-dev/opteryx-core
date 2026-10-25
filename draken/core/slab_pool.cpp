// Process-wide slab pool - see slab_pool.h. Compiled into draken_native ONLY
// (build_common.py); standalone native binaries compile their own copy because
// they have no draken_native to resolve against.
//
// MEASURED 2026-10-24 (ClickBench, 5 interleaved rounds, process per query, vs the
// pre-pool tree; i5-8500 THP=madvise / M-series Mac): recycling earns 7-12% of the
// hot sum on x86 (budget 0 = 1.120), huge pages ~1.3% (no MADV_HUGEPAGE = 0.996
// with dTLB-load-misses 1.35x), and touching every slot at acquire LOST (x86 0.977
// without vs 0.980 with; Mac 0.964 vs 0.983 and peak RSS 1.06 vs 1.16): the first
// user write faults a page just as well, and the scan pool's 256 MiB reservation
// is mostly never written. So a slot is faulted by its first write, except for
// the boot prefault, which exists to move that cost to boot.
#include "slab_pool.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <cerrno>

#include <sys/mman.h>
#include <unistd.h>

#include "mem_account.h"

extern "C" {
uintptr_t draken_slab_base = 0;
uintptr_t draken_slab_end = 0;
}

namespace {

constexpr size_t   kSlot      = DRAKEN_SLAB_SLOT;
constexpr int      kSlotLog2  = 21;
constexpr size_t   kReserve   = size_t(256) << 30;      // address space, never committed
constexpr uint32_t kMaxSlots  = uint32_t(kReserve / kSlot);
constexpr uint32_t kGrowSlots = 16;                      // mprotect frontier growth: 32 MiB
constexpr int      kClasses   = 5;                       // 64 KiB .. 1 MiB
constexpr uint32_t kNone      = UINT32_MAX;
constexpr int64_t  kDefaultBudget = int64_t(4) << 30;   // == config SLAB_POOL_BUDGET_BYTES default (opteryx/config.py)

static_assert((DRAKEN_SLAB_MIN << (kClasses - 1)) * 2 == kSlot, "classes must end one below the slot");

enum Kind : uint8_t { kFree = 0, kClassSlab = 1, kRunStart = 2, kRunBody = 3 };

// Per-slot state. Class-slab fields are atomics so the block fast path is
// lock-free; the rest is written under the pool or class mutex.
struct SlotMeta {
    std::atomic<uint32_t> free_mask{0};   // class slab: bit set = block free
    std::atomic<uint32_t> live{0};        // class slab: blocks taken or being taken
    std::atomic<uint32_t> gen{0};         // bumped when the slot is carved or reclaimed (ABA guard)
    uint32_t run_len = 0;                 // run start: slots in the run
    uint32_t prev = kNone, next = kNone;  // class partial list links
    uint8_t  kind = kFree;
    uint8_t  cls = 0;
    uint8_t  faulted = 0;                 // pages present (charged); pool mutex
    uint8_t  current = 0;                 // some thread's current slab; class mutex
    uint8_t  in_partial = 0;              // on the class partial list; class mutex
};

struct Pool {
    std::mutex mtx;                              // frontier, free runs, faulted, budget
    uint8_t*   base = nullptr;
    uint32_t   frontier = 0;                     // slots [0, frontier) are writable
    std::map<uint32_t, uint32_t> free_runs;      // start -> len, coalesced
    int64_t    faulted_bytes = 0;
    int64_t    free_faulted_bytes = 0;
    int64_t    in_use_bytes = 0;
    int64_t    class_slabs = 0;
    int64_t    acquires = 0, faults = 0, give_backs = 0;
    std::atomic<int64_t> budget{kDefaultBudget};
    std::atomic<int64_t> prefault{0};
    bool       huge = false;
    size_t     page = 4096;

    struct Class {
        std::mutex mtx;
        uint32_t partial_head = kNone;           // slabs with at least one free block
    };
    Class classes[kClasses];
    SlotMeta* meta = nullptr;                    // kMaxSlots entries
};

Pool& pool() {
    static Pool* p = new Pool();   // leaked on purpose: worker threads release at exit
    return *p;
}

[[noreturn]] void die(const char* what) {
    std::fprintf(stderr, "draken slab pool: %s (errno %d)\n", what, errno);
    std::fflush(stderr);
    std::abort();
}

// One-time reservation. Called under pool().mtx.
void init_locked(Pool& P) {
    if (P.base != nullptr) return;
    P.page = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    void* m = mmap(nullptr, kReserve + kSlot, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (m == MAP_FAILED) die("cannot reserve the slab address space");
    uintptr_t a = (reinterpret_cast<uintptr_t>(m) + kSlot - 1) & ~(uintptr_t)(kSlot - 1);
    P.base = reinterpret_cast<uint8_t*>(a);
    P.meta = new SlotMeta[kMaxSlots];
    draken_slab_base = a;
    draken_slab_end = a + kReserve;
}

inline uint32_t slot_of(const Pool& P, const void* p) {
    return static_cast<uint32_t>((reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(P.base)) >> kSlotLog2);
}
inline uint8_t* slot_ptr(const Pool& P, uint32_t s) { return P.base + (size_t(s) << kSlotLog2); }
inline size_t class_bytes(int c) { return DRAKEN_SLAB_MIN << c; }
inline uint32_t class_blocks(int c) { return uint32_t(kSlot / class_bytes(c)); }
inline uint32_t class_full_mask(int c) {
    const uint32_t n = class_blocks(c);
    return n == 32 ? 0xFFFFFFFFu : ((1u << n) - 1u);
}

// Make slots [0, want) writable, growing the frontier in kGrowSlots steps. Under mtx.
bool grow_locked(Pool& P, uint32_t want) {
    if (want <= P.frontier) return true;
    if (want > kMaxSlots) return false;
    uint32_t to = (want + kGrowSlots - 1) / kGrowSlots * kGrowSlots;
    if (to > kMaxSlots) to = kMaxSlots;
    uint8_t* from_p = slot_ptr(P, P.frontier);
    const size_t len = size_t(to - P.frontier) << kSlotLog2;
    if (mprotect(from_p, len, PROT_READ | PROT_WRITE) != 0) die("mprotect of the slab frontier failed");
#if defined(__linux__)
    // A kernel built without THP answers EINVAL: recorded (stats.huge_pages == 0),
    // not hidden - the pool still recycles; it just has no huge pages to recycle.
    if (madvise(from_p, len, MADV_HUGEPAGE) == 0) P.huge = true;
    else if (errno != EINVAL) die("MADV_HUGEPAGE on the slab frontier failed");
#endif
    P.frontier = to;
    return true;
}

// Fault the pages of slots [s, s+n) in by writing one byte per page (the boot
// prefault only - see the note at the top). The bytes are already zero (fresh or
// given back), so the writes change nothing. Under THP the first write maps the
// huge page and the rest are plain stores.
void touch(Pool& P, uint32_t s, uint32_t n) {
    uint8_t* p = slot_ptr(P, s);
    uint8_t* end = p + (size_t(n) << kSlotLog2);
    for (; p < end; p += P.page) *p = 0;
}

// macOS: pages given back with MADV_FREE_REUSABLE are claimed again before use.
// Called by the run's new owner OUTSIDE the pool mutex (the run is already out of
// the free runs, so nobody else can touch it); on pages that were never given
// back the call is a cheap no-op. Linux: nothing to do (MADV_DONTNEED pages
// refault on touch).
void reuse(Pool& P, uint32_t s, uint32_t n) {
#if defined(__APPLE__)
    if (madvise(slot_ptr(P, s), size_t(n) << kSlotLog2, MADV_FREE_REUSE) != 0)
        die("MADV_FREE_REUSE on a slab run failed");
#else
    (void)P; (void)s; (void)n;
#endif
}

// Account slots [s, s+n) as faulted (charged) and return how many were not. Under mtx.
uint32_t mark_faulted_locked(Pool& P, uint32_t s, uint32_t n) {
    uint32_t fresh = 0;
    for (uint32_t i = s; i < s + n; ++i) {
        if (!P.meta[i].faulted) { P.meta[i].faulted = 1; ++fresh; }
    }
    if (fresh != 0) {
        const int64_t b = int64_t(fresh) << kSlotLog2;
        P.faulted_bytes += b;
        P.faults += fresh;
        draken_mem_charge(b);
    }
    return fresh;
}

// Take a run of n slots: first fit over the free runs, else the frontier
// (extending a free tail run over it rather than leaving a gap). Returns the
// start slot, or kNone. Slots that were given back are claimed again outside the
// lock (macOS); their pages then come back on the first write.
uint32_t acquire_run(Pool& P, uint32_t n) {
    uint32_t start = kNone;
    bool need_reuse = false;
    {
        std::lock_guard<std::mutex> lk(P.mtx);
        init_locked(P);
        for (auto it = P.free_runs.begin(); it != P.free_runs.end(); ++it) {
            if (it->second >= n) {
                start = it->first;
                const uint32_t rest = it->second - n;
                P.free_runs.erase(it);
                if (rest != 0) P.free_runs.emplace(start + n, rest);
                break;
            }
        }
        if (start == kNone) {
            start = P.frontier;
            uint32_t have = 0;
            if (!P.free_runs.empty()) {
                auto last = std::prev(P.free_runs.end());
                if (last->first + last->second == P.frontier) {
                    start = last->first;
                    have = last->second;
                    P.free_runs.erase(last);
                }
            }
            if (!grow_locked(P, start + n)) {
                if (have != 0) P.free_runs.emplace(start, have);
                return kNone;
            }
        }
        for (uint32_t i = start; i < start + n; ++i)
            if (P.meta[i].faulted) P.free_faulted_bytes -= int64_t(kSlot);
        need_reuse = mark_faulted_locked(P, start, n) != 0;
        P.in_use_bytes += int64_t(n) << kSlotLog2;
        P.acquires += 1;
        for (uint32_t i = start; i < start + n; ++i) P.meta[i].kind = kRunBody;
        P.meta[start].kind = kRunStart;
        P.meta[start].run_len = n;
    }
    if (need_reuse) reuse(P, start, n);
    return start;
}

// Insert [s, s+len) into the free runs, coalescing with its neighbours. Under mtx.
void free_runs_insert_locked(Pool& P, uint32_t s, uint32_t len) {
    auto right = P.free_runs.find(s + len);
    if (right != P.free_runs.end()) { len += right->second; P.free_runs.erase(right); }
    auto left = P.free_runs.lower_bound(s);
    if (left != P.free_runs.begin()) {
        --left;
        if (left->first + left->second == s) { s = left->first; len += left->second; P.free_runs.erase(left); }
    }
    P.free_runs.emplace(s, len);
}

// Return a run of n slots at `start`. Faulted slots stay faulted up to the budget
// and go straight back to the free runs; the excess is given back to the OS
// OUTSIDE the pool mutex (MEASURED: madvise of gigabytes under the lock serialised
// every worker behind one release - ClickBench Q33 +35% on the Mac), held aside
// meanwhile so nobody can be handed a slot whose pages are being dropped.
struct GiveBack { uint32_t s, n; };

void release_run(Pool& P, uint32_t start, uint32_t n) {
    GiveBack gb[64];
    int ngb = 0;
    uint32_t overflow_from = kNone;   // ranges past gb[]'s capacity: given back on a later pass
    {
        std::lock_guard<std::mutex> lk(P.mtx);
        P.in_use_bytes -= int64_t(n) << kSlotLog2;
        for (uint32_t i = start; i < start + n; ++i) { P.meta[i].kind = kFree; P.meta[i].run_len = 0; }
        int64_t budget = P.budget.load(std::memory_order_relaxed);
        const int64_t pre = P.prefault.load(std::memory_order_relaxed);
        if (pre > budget) budget = pre;
        int64_t room = budget - P.free_faulted_bytes;
        if (room < 0) room = 0;
        uint32_t keep = static_cast<uint32_t>(room / int64_t(kSlot));
        // Walk the run: unfaulted slots and the first `keep` faulted slots are free
        // now; the remaining faulted slots are gathered for the give-back.
        uint32_t i = start;
        uint32_t free_from = start;   // start of the pending "free now" range
        while (i < start + n) {
            const bool faulted = P.meta[i].faulted != 0;
            if (!faulted || keep != 0) {
                if (faulted) { --keep; P.free_faulted_bytes += int64_t(kSlot); }
                ++i;
                continue;
            }
            // A give-back range of faulted slots.
            if (i > free_from) free_runs_insert_locked(P, free_from, i - free_from);
            uint32_t j = i;
            while (j < start + n && P.meta[j].faulted) ++j;
            if (ngb < 64) gb[ngb++] = {i, j - i};
            else if (overflow_from == kNone) overflow_from = i;
            if (ngb >= 64 && overflow_from != kNone) {
                // Give the rest back to the free runs faulted (retained past the
                // budget until the next release trims it) rather than lose track.
                for (uint32_t k = i; k < start + n; ++k)
                    if (P.meta[k].faulted) P.free_faulted_bytes += int64_t(kSlot);
                free_runs_insert_locked(P, i, start + n - i);
                free_from = start + n;
                i = start + n;
                break;
            }
            i = j;
            free_from = j;
        }
        if (i > free_from) free_runs_insert_locked(P, free_from, i - free_from);
    }
    if (ngb == 0) return;
    int64_t given_b = 0;
    for (int k = 0; k < ngb; ++k) {
        uint8_t* p = slot_ptr(P, gb[k].s);
        const size_t len = size_t(gb[k].n) << kSlotLog2;
#if defined(__APPLE__)
        if (madvise(p, len, MADV_FREE_REUSABLE) != 0) die("MADV_FREE_REUSABLE on a slab run failed");
#else
        if (madvise(p, len, MADV_DONTNEED) != 0) die("MADV_DONTNEED on a slab run failed");
#endif
        given_b += int64_t(len);
    }
    {
        std::lock_guard<std::mutex> lk(P.mtx);
        for (int k = 0; k < ngb; ++k) {
            for (uint32_t i = gb[k].s; i < gb[k].s + gb[k].n; ++i) P.meta[i].faulted = 0;
            P.give_backs += gb[k].n;
            free_runs_insert_locked(P, gb[k].s, gb[k].n);
        }
        P.faulted_bytes -= given_b;
    }
    draken_mem_uncharge(given_b);
}

// ---- class slabs ------------------------------------------------------------------

struct ThreadCur {
    uint32_t cur[kClasses];
    ThreadCur() { for (int c = 0; c < kClasses; ++c) cur[c] = kNone; }
    ~ThreadCur();
};
ThreadCur& tcur() {
    static thread_local ThreadCur t;
    return t;
}

// Under the class mutex: put slab s on the partial list.
void partial_push_locked(Pool& P, int c, uint32_t s) {
    SlotMeta& m = P.meta[s];
    if (m.in_partial) return;
    m.prev = kNone;
    m.next = P.classes[c].partial_head;
    if (m.next != kNone) P.meta[m.next].prev = s;
    P.classes[c].partial_head = s;
    m.in_partial = 1;
}
void partial_remove_locked(Pool& P, int c, uint32_t s) {
    SlotMeta& m = P.meta[s];
    if (!m.in_partial) return;
    if (m.prev != kNone) P.meta[m.prev].next = m.next; else P.classes[c].partial_head = m.next;
    if (m.next != kNone) P.meta[m.next].prev = m.prev;
    m.prev = m.next = kNone;
    m.in_partial = 0;
}

// Under the class mutex: a slab nobody holds a block of and nobody has as current
// is taken out of the class here; the caller returns its slot to the pool AFTER
// dropping the class mutex (the give-back may madvise). Returns true when it did.
bool try_reclaim_locked(Pool& P, int c, uint32_t s) {
    SlotMeta& m = P.meta[s];
    if (m.current || m.live.load(std::memory_order_acquire) != 0) return false;
    if (m.free_mask.load(std::memory_order_acquire) != class_full_mask(c)) return false;
    partial_remove_locked(P, c, s);
    m.kind = kFree;
    m.gen.fetch_add(1, std::memory_order_acq_rel);
    return true;
}

void reclaim_slot(Pool& P, uint32_t s) {
    {
        std::lock_guard<std::mutex> lk(P.mtx);
        P.class_slabs -= 1;
    }
    release_run(P, s, 1);
}

// A thread lets go of its current slab for class c (it is full, or the thread exits).
void drop_current(Pool& P, int c, uint32_t s) {
    bool reclaim;
    {
        std::lock_guard<std::mutex> lk(P.classes[c].mtx);
        SlotMeta& m = P.meta[s];
        m.current = 0;
        if (m.free_mask.load(std::memory_order_acquire) != 0) partial_push_locked(P, c, s);
        reclaim = try_reclaim_locked(P, c, s);
    }
    if (reclaim) reclaim_slot(P, s);
}

ThreadCur::~ThreadCur() {
    Pool& P = pool();
    for (int c = 0; c < kClasses; ++c)
        if (cur[c] != kNone) drop_current(P, c, cur[c]);
}

void* class_alloc(Pool& P, int c) {
    ThreadCur& t = tcur();
    const size_t bytes = class_bytes(c);
    for (;;) {
        uint32_t s = t.cur[c];
        if (s != kNone) {
            SlotMeta& m = P.meta[s];
            m.live.fetch_add(1, std::memory_order_acq_rel);
            uint32_t mask = m.free_mask.load(std::memory_order_acquire);
            while (mask != 0) {
                const uint32_t bit = mask & (0u - mask);
                if (m.free_mask.compare_exchange_weak(mask, mask & ~bit, std::memory_order_acq_rel)) {
                    const uint32_t idx = static_cast<uint32_t>(__builtin_ctz(bit));
                    return slot_ptr(P, s) + size_t(idx) * bytes;
                }
            }
            m.live.fetch_sub(1, std::memory_order_acq_rel);
            t.cur[c] = kNone;
            drop_current(P, c, s);
        }
        // A partial slab, else a fresh one.
        {
            std::lock_guard<std::mutex> lk(P.classes[c].mtx);
            uint32_t h = P.classes[c].partial_head;
            if (h != kNone) {
                partial_remove_locked(P, c, h);
                P.meta[h].current = 1;
                t.cur[c] = h;
                continue;
            }
        }
        const uint32_t ns = acquire_run(P, 1);
        if (ns == kNone) return nullptr;
        SlotMeta& m = P.meta[ns];
        {
            std::lock_guard<std::mutex> lk(P.mtx);
            m.kind = kClassSlab;
            m.run_len = 0;
            m.cls = static_cast<uint8_t>(c);
            m.gen.fetch_add(1, std::memory_order_acq_rel);
            P.class_slabs += 1;
        }
        m.live.store(0, std::memory_order_relaxed);
        m.free_mask.store(class_full_mask(c), std::memory_order_release);
        {
            std::lock_guard<std::mutex> lk(P.classes[c].mtx);
            m.current = 1;
        }
        t.cur[c] = ns;
    }
}

void class_free(Pool& P, uint32_t s, void* p) {
    SlotMeta& m = P.meta[s];
    const int c = m.cls;
    const uint32_t g = m.gen.load(std::memory_order_acquire);
    const size_t off = static_cast<size_t>(static_cast<uint8_t*>(p) - slot_ptr(P, s));
    if (off % class_bytes(c) != 0) die("free of a pointer inside a class block");
    const uint32_t bit = 1u << (off / class_bytes(c));
    const uint32_t old = m.free_mask.fetch_or(bit, std::memory_order_acq_rel);
    if (old & bit) die("double free of a class block");
    const uint32_t live = m.live.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if (live == 0 || old == 0) {
        bool reclaim;
        {
            std::lock_guard<std::mutex> lk(P.classes[c].mtx);
            // Reclaimed (and maybe re-carved) by another thread between the bit
            // and the lock: the slab this block belonged to is gone.
            if (m.kind != kClassSlab || m.gen.load(std::memory_order_acquire) != g) return;
            if (!m.current && m.free_mask.load(std::memory_order_acquire) != 0) partial_push_locked(P, c, s);
            reclaim = try_reclaim_locked(P, c, s);
        }
        if (reclaim) reclaim_slot(P, s);
    }
}

inline int class_of(size_t bytes) {
    // bytes in [64 KiB, 1 MiB]: smallest c with (64 KiB << c) >= bytes
    int c = 0;
    while (class_bytes(c) < bytes) ++c;
    return c;
}
inline uint32_t slots_for(size_t bytes) { return static_cast<uint32_t>((bytes + kSlot - 1) >> kSlotLog2); }

}  // namespace

extern "C" {

void* draken_slab_alloc(size_t bytes) {
    Pool& P = pool();
    if (bytes < DRAKEN_SLAB_MIN) die("draken_slab_alloc below DRAKEN_SLAB_MIN");
    if (bytes <= class_bytes(kClasses - 1)) return class_alloc(P, class_of(bytes));
    const uint32_t n = slots_for(bytes);
    const uint32_t s = acquire_run(P, n);
    return s == kNone ? nullptr : slot_ptr(P, s);
}

void draken_slab_free(void* p) {
    if (p == nullptr) return;
    Pool& P = pool();
    const uint32_t s = slot_of(P, p);
    SlotMeta& m = P.meta[s];
    if (m.kind == kClassSlab) { class_free(P, s, p); return; }
    if (m.kind != kRunStart || p != slot_ptr(P, s)) die("free of a pointer the pool did not hand out");
    release_run(P, s, m.run_len);
}

size_t draken_slab_usable_size(const void* p) {
    Pool& P = pool();
    const uint32_t s = slot_of(P, p);
    const SlotMeta& m = P.meta[s];
    if (m.kind == kClassSlab) return class_bytes(m.cls);
    if (m.kind == kRunStart && p == slot_ptr(P, s)) return size_t(m.run_len) << kSlotLog2;
    die("usable_size of a pointer the pool did not hand out");
}

void* draken_slab_realloc(void* p, size_t bytes) {
    Pool& P = pool();
    if (p == nullptr) return draken_slab_alloc(bytes);
    if (bytes < DRAKEN_SLAB_MIN) die("draken_slab_realloc below DRAKEN_SLAB_MIN");
    const size_t old = draken_slab_usable_size(p);
    if (bytes <= old) {
        const uint32_t s = slot_of(P, p);
        SlotMeta& m = P.meta[s];
        if (m.kind == kRunStart) {
            // A run shrinks in place: its tail slots go back.
            const uint32_t keep = slots_for(bytes);
            if (keep < m.run_len) {
                uint32_t drop;
                {
                    std::lock_guard<std::mutex> lk(P.mtx);
                    drop = m.run_len - keep;
                    m.run_len = keep;
                }
                release_run(P, s + keep, drop);
            }
        }
        return p;
    }
    const uint32_t s = slot_of(P, p);
    SlotMeta& m = P.meta[s];
    if (m.kind == kRunStart) {
        // Grow in place when the slots after the run are free.
        const uint32_t want = slots_for(bytes);
        const uint32_t more = want - m.run_len;
        bool grown = false, need_reuse = false;
        {
            std::lock_guard<std::mutex> lk(P.mtx);
            const uint32_t next = s + m.run_len;
            auto it = P.free_runs.find(next);
            if (it != P.free_runs.end() && it->second >= more) {
                const uint32_t rest = it->second - more;
                P.free_runs.erase(it);
                if (rest != 0) P.free_runs.emplace(next + more, rest);
                for (uint32_t i = next; i < next + more; ++i)
                    if (P.meta[i].faulted) P.free_faulted_bytes -= int64_t(kSlot);
                grown = true;
            } else if (next == P.frontier || (it != P.free_runs.end() && next + it->second == P.frontier)) {
                // The run ends at the frontier (possibly through a free tail run): extend it.
                uint32_t have = 0;
                if (it != P.free_runs.end()) { have = it->second; P.free_runs.erase(it); }
                if (grow_locked(P, next + more)) {
                    for (uint32_t i = next; i < next + have; ++i)
                        if (P.meta[i].faulted) P.free_faulted_bytes -= int64_t(kSlot);
                    grown = true;
                } else if (have != 0) {
                    P.free_runs.emplace(next, have);
                }
            }
            if (grown) {
                need_reuse = mark_faulted_locked(P, next, more) != 0;
                P.in_use_bytes += int64_t(more) << kSlotLog2;
                for (uint32_t i = next; i < next + more; ++i) P.meta[i].kind = kRunBody;
                m.run_len = want;
            }
        }
        if (grown) {
            if (need_reuse) reuse(P, s + want - more, more);
            return p;
        }
    }
    void* q = draken_slab_alloc(bytes);
    if (q == nullptr) return nullptr;
    std::memcpy(q, p, old);
    draken_slab_free(p);
    return q;
}

void draken_slab_configure(int64_t budget_bytes, int64_t prefault_bytes) {
    Pool& P = pool();
    if (budget_bytes < 0 || prefault_bytes < 0) die("negative slab pool budget or prefault");
    P.budget.store(budget_bytes, std::memory_order_relaxed);
    P.prefault.store(prefault_bytes, std::memory_order_relaxed);
    if (prefault_bytes == 0) return;
    // Fault the requested bytes into the free runs now (the slots stay faulted:
    // the retained set is at least the prefault).
    const uint32_t n = slots_for(static_cast<size_t>(prefault_bytes));
    const uint32_t s = acquire_run(P, n);
    if (s == kNone) die("slab pool prefault exceeds the reservation");
    touch(P, s, n);
    release_run(P, s, n);
}

int64_t draken_slab_budget_bytes(void) { return pool().budget.load(std::memory_order_relaxed); }
int64_t draken_slab_prefault_bytes(void) { return pool().prefault.load(std::memory_order_relaxed); }

DrakenSlabStats draken_slab_stats(void) {
    Pool& P = pool();
    std::lock_guard<std::mutex> lk(P.mtx);
    DrakenSlabStats s;
    s.reserved_bytes = P.base == nullptr ? 0 : int64_t(kReserve);
    s.mapped_bytes = int64_t(P.frontier) << kSlotLog2;
    s.faulted_bytes = P.faulted_bytes;
    s.in_use_bytes = P.in_use_bytes;
    s.free_faulted_bytes = P.free_faulted_bytes;
    s.class_slabs = P.class_slabs;
    s.acquires = P.acquires;
    s.faults = P.faults;
    s.give_backs = P.give_backs;
    s.huge_pages = P.huge ? 1 : 0;
    return s;
}

}  // extern "C"
