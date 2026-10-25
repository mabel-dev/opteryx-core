#pragma once
// src/cpp/engine/gb_block_store.hpp — GROUP BY per-group storage: a recycled block
// pool, a segmented array over it, and a chunked string arena
// (docs/GROUPBY_REDESIGN_2026_10_12.md, R1).
//
// WHY. A GROUP BY partition's per-group columns (hashes, the COUNT(*) lane, the
// aggregate lanes, the key store) were std::vectors / AppendBuffers. Growing one
// reallocated and copied it, value-initialised the new slots, and — for the large
// merge-side columns — took fresh pages from the OS that the first write then faulted
// in. Measured on ClickBench Q32 (100M groups, 2026-10-12): 40% of samples were vector
// growth, zero-fill and realloc copies; the 2026-10-06 lanes-on-AppendBuffer revert had
// already shown the residue is first-touch faulting, not avoidable copying. Here:
//   * a column is a chain of SEGMENTS that never move, so growth never copies;
//   * segments come from a process-wide pool and go back to it, so a merge bucket
//     re-uses the pages of the bucket before it, and a hot query those of the last;
//   * only the rows a column gains are initialised (zeroed by `resize`, left for the
//     caller by `resize_uninit`/`append`).
//
// GEOMETRY: segment 0 holds 2^b rows; segment k >= 1 holds 2^(b+k-1) rows and starts at
// row 2^(b+k-1). Growth never copies, slack is no worse than a doubling std::vector, and
// a column has at most ~35 segments, held inline. A caller that knows a column's final
// size up front (a merge) passes it to `hint` before the first append, and the column
// is then ONE segment. (Fixed-size segments were the doc's first proposal; MEASURED
// 2026-10-12 no faster and +19-22% peak RSS — ruled out by the architect.)
// Every column of one partition is given the same hint, so a row range that is
// contiguous in one column is contiguous in all of them; where a range is NOT
// contiguous (an unhinted partition that grew), readers copy it out (`copy_out`) —
// correctness never depends on the geometry.

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "core/alloc.h"   // draken_malloc / draken_free — charged to the memory account


namespace opteryx::engine {

// First-segment rows (log2) of an UNHINTED doubling column: the first flush window,
// and every partition of a low-cardinality query (which never flushes). 2,048 rows of
// an 8-byte lane is 16 KiB — one macOS ARM page — so every such lane came back
// page-aligned and the hot first rows of every lane of every partition aliased in L1
// (MEASURED 2026-10-13, M5: H2O g1 1.69x / g9 1.57x pre-redesign; 512 rows: g1 0.86,
// g9 1.03; x86 neutral). Hinted windows keep 2,048 (kGBSegHintMaxLog2).
constexpr int kGBSegFirstLog2 = 9;
// Largest first segment the flush-window hint may ask for. A local partition holds
// ~1,024 groups per window (65,536 groups over 64 partitions), so 2,048 rows hold a
// whole window on the first-segment fast path (at()). Capping it at the unhinted 512
// instead MEASURED g10 +15% (2.2x pool allocations, 2026-10-13).
constexpr int kGBSegHintMaxLog2 = 11;
// Smallest first segment a HINT may ask for (64 rows). A flush window's partition is
// hinted from the size its predecessor reached, and a low-cardinality partition holds
// tens of groups: 2,048 empty rows per lane per partition per window cost a fresh page
// per lane per window (MEASURED, ClickBench Q09 2026-10-12: 2.1x peak RSS, +10% time).
constexpr int kGBSegMinLog2 = 6;
// Largest first segment a hint may ask for (2^40 rows: never reached in practice;
// the bound only keeps the shift arithmetic in range).
constexpr int kGBSegMaxLog2 = 40;

// ---- telemetry ---------------------------------------------------------------------
// Process-wide, relaxed, per-block (never per-row). Read through groupby_tel.
namespace gb_pool_tel {
inline std::atomic<long long> hits{0};       // blocks served from the pool
inline std::atomic<long long> misses{0};     // blocks allocated from the system
inline std::atomic<long long> os_bytes{0};   // bytes allocated from the system
inline std::atomic<long long> freed_bytes{0};// bytes returned to the system (over the cap / oversize)
inline void reset() {
    hits.store(0, std::memory_order_relaxed);
    misses.store(0, std::memory_order_relaxed);
    os_bytes.store(0, std::memory_order_relaxed);
    freed_bytes.store(0, std::memory_order_relaxed);
}
}  // namespace gb_pool_tel

// ---- block pool --------------------------------------------------------------------
// Power-of-two size classes. Classes from kSlabClass (64 KiB) up are the draken slab
// pool's (draken/core/slab_pool.h): draken_malloc serves them from recycled, prefaulted
// slots with its own per-thread lists, so this pool neither caches nor counts them -
// two layers of free lists over the same blocks would retain twice. Below that, the
// system allocator's small blocks are cached here. Blocks the pool holds stay charged
// to the memory account — they are memory the process holds — and the pool holds at
// most kRetainBytes; anything returned beyond that is freed.
//
// Each thread keeps a small cache per class and trades with the shared lists in
// batches: one shared list per class behind one mutex was MEASURED as the regression
// (ClickBench Q32, 16 workers, 2026-10-12: __psynch_mutexwait was the top frame). A
// thread's cache goes back to the shared lists when the thread exits (finalize's
// fork_join threads are short-lived), so nothing is stranded.
class GBBlockPool {
  public:
    static constexpr int kMinClass = 8;    // 256 bytes
    static constexpr int kSlabClass = 16;  // 64 KiB: this class and up are the slab pool's
    static constexpr int kMaxClass = 30;   // 1 GiB
    static_assert((size_t(1) << kSlabClass) == DRAKEN_SLAB_MIN, "kSlabClass is the slab threshold");
    // Retained across queries. UNMEASURED — the redesign doc lists the pool's size and
    // release policy as open (§8.2); this is the first value, not a ruling.
    static constexpr int64_t kRetainBytes = int64_t(1) << 30;
    // Per-thread cache: at most this many blocks per class, traded in half-batches.
    static constexpr size_t kLocalBlocks = 32;
    // ...and never more than this many bytes of one class (big classes cache few).
    static constexpr size_t kLocalBytes = size_t(8) << 20;

    // Leaked on purpose: blocks may be released by worker threads during process exit,
    // after a function-local static would have been destroyed.
    static GBBlockPool& instance() {
        static GBBlockPool* p = new GBBlockPool();
        return *p;
    }

    static int class_of(size_t bytes) {
        const int c = bytes <= 1 ? 0 : static_cast<int>(std::bit_width(bytes - 1));
        return c < kMinClass ? kMinClass : c;
    }

    // A block of at least `bytes`. Contents are unspecified (a recycled block holds
    // whatever its last user left). Throws std::bad_alloc on failure.
    void* acquire(size_t bytes) {
        const int c = class_of(bytes);
        if (c >= kSlabClass) return from_slab(c > kMaxClass ? bytes : size_t(1) << c);
        std::vector<void*>& mine = local().cls[static_cast<size_t>(c)];
        if (mine.empty()) refill(c, mine);
        if (!mine.empty()) {
            void* p = mine.back();
            mine.pop_back();
            retained_.fetch_sub(int64_t(1) << c, std::memory_order_relaxed);
            gb_pool_tel::hits.fetch_add(1, std::memory_order_relaxed);
            return p;
        }
        return from_system(size_t(1) << c);
    }

    // Return a block obtained from acquire(bytes) with the same `bytes`.
    void release(void* p, size_t bytes) noexcept {
        if (p == nullptr) return;
        const int c = class_of(bytes);
        if (c >= kSlabClass) {
            draken_free(p);   // back to the slab pool's lists
            return;
        }
        const int64_t sz = int64_t(1) << c;
        if (retained_.fetch_add(sz, std::memory_order_relaxed) + sz > kRetainBytes) {
            retained_.fetch_sub(sz, std::memory_order_relaxed);
            gb_pool_tel::freed_bytes.fetch_add(sz, std::memory_order_relaxed);
            charge_small(p, false);
            draken_free(p);
            return;
        }
        std::vector<void*>& mine = local().cls[static_cast<size_t>(c)];
        mine.push_back(p);
        if (mine.size() > local_cap(c)) spill(c, mine, mine.size() / 2);
    }

    int64_t retained_bytes() const { return retained_.load(std::memory_order_relaxed); }

  private:
    GBBlockPool() = default;

    static size_t local_cap(int c) {
        const size_t by_bytes = kLocalBytes >> c;
        const size_t cap = by_bytes < kLocalBlocks ? by_bytes : kLocalBlocks;
        return cap < 1 ? 1 : cap;
    }

    // This thread's cache. Its destructor hands every cached block back to the shared
    // lists, so a thread that exits strands nothing.
    struct Local {
        std::array<std::vector<void*>, kMaxClass + 1> cls;
        ~Local() {
            GBBlockPool& pool = instance();
            for (int c = 0; c <= kMaxClass; ++c)
                if (!cls[static_cast<size_t>(c)].empty())
                    pool.spill(c, cls[static_cast<size_t>(c)], cls[static_cast<size_t>(c)].size());
        }
    };
    static Local& local() {
        static thread_local Local l;
        return l;
    }

    // Take up to half a local cache's worth from the shared list of class c. An empty
    // shared list is seen without the lock: while a query is only sinking, nothing is
    // being freed, and every miss used to queue on the class mutex to find that out
    // (MEASURED, Q32 2026-10-12: refill was the top __psynch_mutexwait caller).
    void refill(int c, std::vector<void*>& mine) {
        Class& k = classes_[static_cast<size_t>(c)];
        if (k.count.load(std::memory_order_relaxed) == 0) return;
        std::lock_guard<std::mutex> lk(k.mtx);
        const size_t want = (local_cap(c) + 1) / 2;
        const size_t n = k.free.size() < want ? k.free.size() : want;
        mine.insert(mine.end(), k.free.end() - static_cast<std::ptrdiff_t>(n), k.free.end());
        k.free.resize(k.free.size() - n);
        k.count.store(k.free.size(), std::memory_order_relaxed);
    }

    // Move the newest `n` blocks of a local cache to the shared list of class c.
    void spill(int c, std::vector<void*>& mine, size_t n) noexcept {
        Class& k = classes_[static_cast<size_t>(c)];
        std::lock_guard<std::mutex> lk(k.mtx);
        k.free.insert(k.free.end(), mine.end() - static_cast<std::ptrdiff_t>(n), mine.end());
        mine.resize(mine.size() - n);
        k.count.store(k.free.size(), std::memory_order_relaxed);
    }

    // draken_malloc charges a block only from DRAKEN_MEM_ACCOUNT_MIN usable bytes,
    // and this pool's blocks are mostly smaller: H2O g10 (~100M groups over many
    // small partitions) held ~6 GiB of group state the cache's give-way could not
    // see. So the pool charges exactly the complement - usable size below the
    // threshold - on the way in from and out to the system. A block the pool
    // retains stays charged, as a large one already does.
    static void charge_small(void* p, bool charge) noexcept {
        const int64_t n = static_cast<int64_t>(draken_usable_size(p));
        if (n <= 0 || n >= DRAKEN_MEM_ACCOUNT_MIN) return;
        if (charge) draken_mem_charge(n);
        else draken_mem_uncharge(n);
    }

    static void* from_system(size_t bytes) {
        void* p = draken_malloc(bytes);
        if (p == nullptr) throw std::bad_alloc();
        charge_small(p, true);
        gb_pool_tel::misses.fetch_add(1, std::memory_order_relaxed);
        gb_pool_tel::os_bytes.fetch_add(static_cast<long long>(bytes), std::memory_order_relaxed);
        return p;
    }

    // A slab-class block: the slab pool charges it by the slot it lives in. Counted
    // as a miss here (not served by THIS pool's lists), as it always was - but NOT in
    // os_bytes: release() never counts it back into freed_bytes, so os_bytes less
    // freed_bytes is the memory THIS pool holds charged (small blocks only), and a
    // slab-class block would make that figure grow by every block, every query.
    static void* from_slab(size_t bytes) {
        void* p = draken_malloc(bytes);
        if (p == nullptr) throw std::bad_alloc();
        gb_pool_tel::misses.fetch_add(1, std::memory_order_relaxed);
        return p;
    }

    struct Class {
        std::mutex mtx;
        std::vector<void*> free;
        std::atomic<size_t> count{0};   // free.size(), readable without mtx (a hint only)
    };
    std::array<Class, kMaxClass + 1> classes_;
    // Blocks held by the pool anywhere: shared lists and every thread's cache.
    std::atomic<int64_t> retained_{0};
};

// Cython-surface accessors declared in groupby_tel.hpp.
namespace groupby_tel {
inline long long gb_pool_hits_count() { return gb_pool_tel::hits.load(std::memory_order_relaxed); }
inline long long gb_pool_misses_count() { return gb_pool_tel::misses.load(std::memory_order_relaxed); }
inline long long gb_pool_os_bytes_count() { return gb_pool_tel::os_bytes.load(std::memory_order_relaxed); }
inline long long gb_pool_freed_bytes_count() { return gb_pool_tel::freed_bytes.load(std::memory_order_relaxed); }
inline long long gb_pool_retained_bytes() { return GBBlockPool::instance().retained_bytes(); }
inline void gb_pool_tel_reset() { gb_pool_tel::reset(); }
}  // namespace groupby_tel

// ---- segmented array ---------------------------------------------------------------
// kStride > 0: element size fixed at compile time (the typed lanes). kStride == 0:
// element size set once at runtime with set_stride (the key store's raw column, whose
// width is the key type's). Move-only; destruction returns every segment to the pool.
template <size_t kStride>
class GBSegArray {
  public:
    GBSegArray() = default;
    ~GBSegArray() { release_all(); }
    GBSegArray(const GBSegArray&) = delete;
    GBSegArray& operator=(const GBSegArray&) = delete;
    GBSegArray(GBSegArray&& o) noexcept { take(o); }
    GBSegArray& operator=(GBSegArray&& o) noexcept {
        if (this != &o) {
            release_all();
            take(o);
        }
        return *this;
    }

    // Runtime element size (kStride == 0 only). Must precede the first growth.
    void set_stride(size_t s) {
        static_assert(kStride == 0, "set_stride is for runtime-stride arrays only");
        stride_ = s;
    }
    size_t stride() const noexcept {
        if constexpr (kStride != 0) return kStride;
        else return stride_;
    }

    // First-segment size from the column's known final row count. Only effective
    // before the first segment exists, and only in the doubling geometry.
    void hint(size_t rows) noexcept {
        if (nseg() != 0 || rows == 0) return;
        int b = static_cast<int>(std::bit_width(rows - 1));
        if (b < kGBSegMinLog2) b = kGBSegMinLog2;
        if (b > kGBSegMaxLog2) b = kGBSegMaxLog2;
        b_ = static_cast<uint8_t>(b);
    }

    size_t size() const noexcept { return size_; }
    bool empty() const noexcept { return size_ == 0; }

    // Pointer to element i (i < size, or < capacity for a slot being filled). Rows of
    // the first segment — all of a hinted column, most of a small one — skip the
    // segment arithmetic behind one predictable compare.
    inline uint8_t* at(size_t i) const noexcept {
        if ((i >> b_) == 0) return seg_ptr(0) + i * stride();
        size_t k, off;
        locate(i, k, off);
        return seg_ptr(k) + off * stride();
    }

    // Grow by one element and return its (uninitialised) slot. A bump of the tail
    // cursor while the current segment has room — the append cost of a flat buffer.
    inline uint8_t* append() {
        if (tail_left_ == 0) retail_for_append();
        uint8_t* p = tail_;
        tail_ += stride();
        --tail_left_;
        ++size_;
        return p;
    }

    // Size to n. Rows past the old size are ZEROED (std::vector::resize's value-
    // initialisation for the trivially-copyable lanes); shrinking keeps the segments.
    void resize(size_t n) {
        if (n <= size_) { size_ = n; retail(); return; }
        if (n > cap_) grow_to(n);
        for_each_run(size_, n - size_, [this](uint8_t* p, size_t rows) {
            std::memset(p, 0, rows * stride());
        });
        size_ = n;
        retail();
    }

    // Size to n. Rows past the old size are UNINITIALISED, for the caller to write.
    void resize_uninit(size_t n) {
        if (n > cap_) grow_to(n);
        size_ = n;
        retail();
    }

    // Release every segment to the pool and become empty, geometry reset.
    void clear() noexcept {
        release_all();
        size_ = 0;
        cap_ = 0;
        b_ = kDefaultLog2;
        tail_ = nullptr;
        tail_left_ = 0;
    }

    // [start, start+n) as one pointer when it lies in a single segment, else nullptr.
    uint8_t* run_ptr(size_t start, size_t n) const noexcept {
        if (n == 0) return nullptr;
        size_t k0, o0, k1, o1;
        locate(start, k0, o0);
        locate(start + n - 1, k1, o1);
        return k0 == k1 ? seg_ptr(k0) + o0 * stride() : nullptr;
    }

    // f(p, rows) over the maximal single-segment runs of [start, start+n), in order.
    template <class F>
    void for_each_run(size_t start, size_t n, F&& f) const {
        while (n > 0) {
            size_t k, off;
            locate(start, k, off);
            const size_t room = seg_rows(k) - off;
            const size_t take = room < n ? room : n;
            f(seg_ptr(k) + off * stride(), take);
            start += take;
            n -= take;
        }
    }

    // Copy [start, start+n) into dst (n * stride bytes).
    void copy_out(size_t start, size_t n, void* dst) const {
        uint8_t* d = static_cast<uint8_t*>(dst);
        for_each_run(start, n, [&d, this](const uint8_t* p, size_t rows) {
            std::memcpy(d, p, rows * stride());
            d += rows * stride();
        });
    }

  private:
    static constexpr uint8_t kDefaultLog2 = kGBSegFirstLog2;

    // Segment k and the row within it holding element i.
    inline void locate(size_t i, size_t& k, size_t& off) const noexcept {
        const size_t q = i >> b_;
        k = static_cast<size_t>(std::bit_width(q));
        off = i - (((size_t(1) << k) >> 1) << b_);
    }
    size_t seg_rows(size_t k) const noexcept {
        return (size_t(1) << b_) << (k == 0 ? 0 : k - 1);
    }
    size_t seg_bytes(size_t k) const noexcept { return seg_rows(k) * stride(); }

    // A column never exceeds 2^kGBSegMaxLog2 rows and its first segment is at least
    // 2^kGBSegMinLog2, so this many segments always suffice (grow_to checks).
    static constexpr size_t kMaxSegs = kGBSegMaxLog2 - kGBSegMinLog2 + 2;
    uint8_t* seg_ptr(size_t k) const noexcept { return segs_[k]; }
    size_t nseg() const noexcept { return nseg_; }
    void push_seg(uint8_t* p) { segs_[nseg_++] = p; }

    // Point the tail cursor at row size_ (the next append) and count the room left in
    // its segment; 0 when the column is full.
    void retail() noexcept {
        if (size_ >= cap_) { tail_ = nullptr; tail_left_ = 0; return; }
        size_t k, off;
        locate(size_, k, off);
        tail_ = seg_ptr(k) + off * stride();
        tail_left_ = seg_rows(k) - off;
    }

    [[gnu::noinline]] void retail_for_append() {
        if (size_ == cap_) grow_to(size_ + 1);
        retail();
    }

    [[gnu::noinline]] void grow_to(size_t need) {
        if (stride() == 0) throw std::logic_error("GBSegArray: stride not set before growth");
        while (cap_ < need) {
            const size_t k = nseg();
            if (k == kMaxSegs)
                throw std::length_error("GBSegArray: column exceeds 2^kGBSegMaxLog2 rows");
            push_seg(static_cast<uint8_t*>(GBBlockPool::instance().acquire(seg_bytes(k))));
            cap_ += seg_rows(k);
        }
    }

    // No cache colouring: an offset into each block costs either a next-class block
    // (2x pool bytes) or a non-power-of-two malloc size, and on macOS any such size
    // raises peak RSS (MEASURED 2026-10-13: Q32 12.4 -> 15.1 GB with a pad and a ZERO
    // offset). The aliasing it targeted is fixed by the first-segment size instead.
    void release_all() noexcept {
        const size_t n = nseg();
        for (size_t k = 0; k < n; ++k)
            GBBlockPool::instance().release(seg_ptr(k), seg_bytes(k));
        nseg_ = 0;
    }

    void take(GBSegArray& o) noexcept {
        nseg_ = o.nseg_;
        for (size_t k = 0; k < nseg_; ++k) segs_[k] = o.segs_[k];
        o.nseg_ = 0;
        size_ = o.size_;
        cap_ = o.cap_;
        b_ = o.b_;
        tail_ = o.tail_;
        tail_left_ = o.tail_left_;
        if constexpr (kStride == 0) stride_ = o.stride_;
        o.size_ = 0;
        o.cap_ = 0;
        o.b_ = kDefaultLog2;
        o.tail_ = nullptr;
        o.tail_left_ = 0;
    }

    uint8_t* segs_[kMaxSegs] = {};
    size_t nseg_ = 0;
    size_t size_ = 0;
    size_t cap_ = 0;
    uint8_t* tail_ = nullptr;   // slot of row size_ (the next append), when tail_left_ > 0
    size_t tail_left_ = 0;      // rows left in tail_'s segment
    size_t stride_ = kStride;   // only read when kStride == 0
    uint8_t b_ = kDefaultLog2;
};

// Typed view of a segmented array: the per-group lanes (hashes, grows, sums, …).
// std::vector's vocabulary for what the GROUP BY code uses — operator[], size, empty,
// push_back, resize (zero-filling) — and deliberately NO data(): the storage is not
// contiguous, so a caller that needs a pointer asks for a run (run_ptr / copy_out).
template <typename T>
class GBSegVec {
    static_assert(std::is_trivially_copyable_v<T>, "GBSegVec holds trivially copyable lanes");

  public:
    GBSegVec() = default;
    GBSegVec(GBSegVec&&) noexcept = default;
    GBSegVec& operator=(GBSegVec&&) noexcept = default;

    inline T& operator[](size_t i) noexcept { return *reinterpret_cast<T*>(a_.at(i)); }
    inline const T& operator[](size_t i) const noexcept {
        return *reinterpret_cast<const T*>(a_.at(i));
    }
    inline void push_back(const T& v) { std::memcpy(a_.append(), &v, sizeof(T)); }
    size_t size() const noexcept { return a_.size(); }
    bool empty() const noexcept { return a_.empty(); }
    void resize(size_t n) { a_.resize(n); }
    void resize_uninit(size_t n) { a_.resize_uninit(n); }
    void hint(size_t rows) noexcept { a_.hint(rows); }
    void clear() noexcept { a_.clear(); }
    T* run_ptr(size_t start, size_t n) const noexcept {
        return reinterpret_cast<T*>(a_.run_ptr(start, n));
    }
    template <class F>
    void for_each_run(size_t start, size_t n, F&& f) const {
        a_.for_each_run(start, n, [&f](uint8_t* p, size_t rows) {
            f(reinterpret_cast<T*>(p), rows);
        });
    }
    void copy_out(size_t start, size_t n, T* dst) const { a_.copy_out(start, n, dst); }

    // [start, start+n) as one pointer: the run itself when contiguous, else a copy in
    // `scratch` (which must outlive the pointer's use).
    const T* span(size_t start, size_t n, std::vector<T>& scratch) const {
        if (n == 0) return nullptr;
        if (const T* p = run_ptr(start, n)) return p;
        scratch.resize(n);
        copy_out(start, n, scratch.data());
        return scratch.data();
    }

  private:
    GBSegArray<sizeof(T)> a_;
};

// ---- chunked string arena ----------------------------------------------------------
// The key store's long-string bytes: one linear address space whose chunks never move,
// so the 16-byte string slot's 32-bit arena_offset stays valid for the arena's life and
// a range of offsets can be copied out chunk by chunk with relative positions kept.
//
// Chunks DOUBLE from 4 KiB to 1 MiB, then stay at 1 MiB. A sink-side partition holds
// one flush window — tens of KB of strings — and a fixed 1 MiB first chunk per key
// column per partition per window was MEASURED as the dominant allocation (ClickBench
// Q18, 2026-10-12: 55 GB requested from the system per query, peak RSS 2.1x the
// pre-redesign build).
//
// A string is placed whole in one chunk. One too long for the next chunk skips it —
// the skipped chunks stay unallocated (null) and hold nothing, so for_each_run hands
// them out as nullptr — and a string longer than 1 MiB gets a run of consecutive
// 1 MiB chunk indices backed by one block.
class GBArena {
  public:
    static constexpr int kFirstLog2 = 12;                    // 4 KiB first chunk
    static constexpr int kChunkLog2 = 20;                    // 1 MiB steady-state chunk
    static constexpr size_t kChunk = size_t(1) << kChunkLog2;
    static constexpr size_t kGrowChunks = kChunkLog2 - kFirstLog2 + 1;   // 4 KiB .. 1 MiB
    // Bytes of address space the doubling chunks cover: 4 KiB * (2^kGrowChunks - 1).
    static constexpr uint64_t kGrowBytes = ((uint64_t(1) << kGrowChunks) - 1) << kFirstLog2;

    GBArena() = default;
    ~GBArena() { release_all(); }
    GBArena(const GBArena&) = delete;
    GBArena& operator=(const GBArena&) = delete;
    GBArena(GBArena&& o) noexcept
        : chunks_(std::move(o.chunks_)), owner_bytes_(std::move(o.owner_bytes_)), used_(o.used_) {
        o.chunks_.clear();
        o.owner_bytes_.clear();
        o.used_ = 0;
    }
    GBArena& operator=(GBArena&& o) noexcept {
        if (this != &o) {
            release_all();
            chunks_ = std::move(o.chunks_);
            owner_bytes_ = std::move(o.owner_bytes_);
            used_ = o.used_;
            o.chunks_.clear();
            o.owner_bytes_.clear();
            o.used_ = 0;
        }
        return *this;
    }

    bool empty() const noexcept { return used_ == 0; }
    // One past the highest offset handed out.
    uint64_t used() const noexcept { return used_; }

    // Chunk k's first offset and size.
    static uint64_t chunk_start(size_t k) noexcept {
        if (k < kGrowChunks) return ((uint64_t(1) << k) - 1) << kFirstLog2;
        return kGrowBytes + (uint64_t(k - kGrowChunks) << kChunkLog2);
    }
    static uint64_t chunk_size(size_t k) noexcept {
        return k < kGrowChunks ? uint64_t(1) << (kFirstLog2 + k) : uint64_t(kChunk);
    }
    // The chunk holding offset `off`.
    static size_t chunk_of(uint64_t off) noexcept {
        if (off < kGrowBytes)
            return static_cast<size_t>(std::bit_width((off >> kFirstLog2) + 1)) - 1;
        return kGrowChunks + static_cast<size_t>((off - kGrowBytes) >> kChunkLog2);
    }

    // Copy len bytes in; returns their offset. Throws std::bad_alloc on failure.
    uint64_t put(const uint8_t* p, size_t len) {
        if (len == 0) return used_;
        const bool fits = !chunks_.empty()
            && used_ + len <= chunk_start(chunks_.size() - 1) + chunk_size(chunks_.size() - 1);
        if (!fits) {
            // Skip (unallocated) chunks too small for this string; past the doubling
            // chunks every chunk is 1 MiB and a longer string spans several.
            size_t k = chunks_.size();
            while (k < kGrowChunks && chunk_size(k) < len) {
                chunks_.push_back(nullptr);
                owner_bytes_.push_back(0);
                ++k;
            }
            const size_t span = len <= chunk_size(k)
                ? 1 : static_cast<size_t>((len + kChunk - 1) >> kChunkLog2);
            const size_t bytes = span == 1 ? static_cast<size_t>(chunk_size(k)) : span * kChunk;
            auto* blk = static_cast<uint8_t*>(GBBlockPool::instance().acquire(bytes));
            for (size_t j = 0; j < span; ++j) {
                chunks_.push_back(blk + j * kChunk);
                owner_bytes_.push_back(j == 0 ? bytes : 0);
            }
            used_ = chunk_start(k);
        }
        const uint64_t off = used_;
        std::memcpy(resolve_mut(off), p, len);
        used_ = off + len;
        return off;
    }

    inline const uint8_t* resolve(uint64_t off) const noexcept { return resolve_mut(off); }

    // f(p, bytes) over [lo, hi) in address order, one call per chunk touched. p is
    // nullptr for a skipped chunk (no string lives there, so nothing points into it).
    template <class F>
    void for_each_run(uint64_t lo, uint64_t hi, F&& f) const {
        while (lo < hi) {
            const size_t k = chunk_of(lo);
            const uint64_t end_k = chunk_start(k) + chunk_size(k);
            const uint64_t end = end_k < hi ? end_k : hi;
            f(chunks_[k] == nullptr ? nullptr : chunks_[k] + (lo - chunk_start(k)),
              static_cast<size_t>(end - lo));
            lo = end;
        }
    }

    void clear() noexcept {
        release_all();
        used_ = 0;
    }

  private:
    uint8_t* resolve_mut(uint64_t off) const noexcept {
        const size_t k = chunk_of(off);
        return chunks_[k] + (off - chunk_start(k));
    }

    void release_all() noexcept {
        for (size_t j = 0; j < chunks_.size(); ++j)
            if (owner_bytes_[j] != 0) GBBlockPool::instance().release(chunks_[j], owner_bytes_[j]);
        chunks_.clear();
        owner_bytes_.clear();
    }

    std::vector<uint8_t*> chunks_;      // nullptr = a chunk skipped by a long string
    std::vector<size_t> owner_bytes_;   // block size on the chunk that owns a block, else 0
    uint64_t used_ = 0;
};

}  // namespace opteryx::engine
