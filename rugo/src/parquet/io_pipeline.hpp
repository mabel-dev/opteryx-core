/**
 * Lock-free parquet IO pipeline using BS::thread_pool and moodycamel queue.
 *
 * Pure C++ IO:
 * - Local files: POSIX pread()
 * - HTTP / HTTPS: HttpClient::get() with Range header
 * - GCS gs://: rewritten to https://storage.googleapis.com/... then HTTP range
 *
 * Worker threads read + decode + IPC-serialize without the GIL.
 * Results dequeued via lock-free moodycamel queue.
 */

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <deque>
#include <exception>
#include <cstdint>
#include <utility>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <map>

#include "BS_thread_pool.hpp"
#include "http_client.hpp"
#include "decode.hpp"
#include "ipc_serialize.hpp"
#include "metadata.hpp"

namespace rugo {

// C-ABI sink for handing decoded columns to the opteryx side. Pure C types only
// — rugo must not depend on opteryx/draken; the opteryx adapter fills these in
// (opteryx/compiled/structures/pool_sink_adapter.hpp).
//   reserve(ctx, size, &ptr) -> ref_id : reserve `size` MemoryPool bytes, set
//       *ptr to the writable region; returns ref_id (>=0) or -1 on exhaustion.
//   finalize(ctx, ref_id, actual_len)  : commit the reserved segment.
//   draken_alloc / draken_free         : the Draken allocator (WP-6b direct
//       path). Non-nullable fixed-width columns are serialized as raw Draken
//       buffers the consumer wraps into a Vector with zero copy; abandoned
//       buffers are freed via draken_free in MorselRef's destructor.
struct PoolSink {
    void*   ctx = nullptr;
    int64_t (*reserve)(void* ctx, int64_t size, void** out_ptr) = nullptr;
    void    (*finalize)(void* ctx, int64_t ref_id, int64_t actual_len) = nullptr;
    void*   (*draken_alloc)(size_t n) = nullptr;
    void    (*draken_free)(void* p) = nullptr;
};

// rugo-local discriminant for a column's handoff form. Kept as a plain int in
// ColumnOut so the .pxd sees a stable layout.
//   0           : pool path — IPC blob in MemoryPool at ref_id (WP-6a).
//   1/2/3       : direct path — `data` is a Draken-owned positional buffer of
//                 `length` rows that the consumer wraps via draken_vector_own_raw.
enum DirectKind { DK_POOL = 0, DK_INT64 = 1, DK_FLOAT32 = 2, DK_FLOAT64 = 3 };

struct ColumnOut {
    int      direct_kind = DK_POOL;  // DirectKind
    void*    data = nullptr;         // direct path: draken_alloc'd, owned until taken
    uint32_t length = 0;             // direct path: row count
    int64_t  ref_id = -1;            // pool path: MemoryPool ref
};

// Owns the direct-path Draken buffers it carries: any ColumnOut.data not "taken"
// by the consumer (via morsel_take_direct) is draken_free'd on destruction —
// covering decode-error, LIMIT early-exit, and shutdown-drain paths. Move-only;
// std::vector move leaves the source's columns empty so a moved-from MorselRef
// frees nothing.
struct MorselRef {
    std::string path;
    int rg_idx = -1;
    std::vector<std::string> column_names;
    std::vector<ColumnOut> columns;
    void (*free_fn)(void* p) = nullptr;   // = PoolSink.draken_free
    int64_t bytes_fetched = 0;
    uint64_t read_ns = 0;
    uint64_t decode_ns = 0;
    std::string error;
    bool success = false;

    MorselRef() = default;
    MorselRef(const MorselRef&) = delete;
    MorselRef& operator=(const MorselRef&) = delete;
    MorselRef(MorselRef&&) = default;
    MorselRef& operator=(MorselRef&&) = default;
    ~MorselRef() {
        if (free_fn) {
            for (auto& c : columns)
                if (c.data) free_fn(c.data);
        }
    }
};

// Take ownership of column i's direct buffer: returns the pointer and nulls the
// slot so MorselRef's destructor won't free it (the consumer's Vector now owns
// it). Returns nullptr for a pool-path column.
static inline void* morsel_take_direct(MorselRef& m, size_t i) {
    void* p = m.columns[i].data;
    m.columns[i].data = nullptr;
    return p;
}

// WP-6b: a column qualifies for the direct (zero-copy-into-Draken) path only
// when it is a plain, fixed-width numeric column with NO ACTUAL NULLS, no
// dict/RLE encoding, and not a list. The decoder stores value streams COMPACT
// (parquet omits null rows), so `values.size() == num_rows` holds iff zero rows
// were omitted — i.e. there are no nulls and the buffer is already positional.
// That single equality is the null guard: any null makes the array shorter than
// num_rows and disqualifies the column (it falls to the scatter-capable IPC
// path). A present-but-all-valid validity bitmap (parquet OPTIONAL column, no
// nulls) is fine — the consumer wraps with validity = NULL. These are exactly
// the cases needing only a literal memcpy (or int32→int64 widen), no scatter.
static inline DirectKind direct_kind_for(const DecodedColumn& d) {
    if (!d.rep_levels.empty()) return DK_POOL;           // list
    if (!d.dict_indices.empty() || !d.dict_codes_array.empty()) return DK_POOL;
    if (!d.rle_int64_values.empty() || !d.rle_float64_values.empty() ||
        !d.rle_str_lens.empty()) return DK_POOL;         // RLE skip-dense
    const std::string& t = d.type;
    const uint32_t n = static_cast<uint32_t>(d.num_rows);
    if (t == "int64"   && d.int64_values.size()   == n) return DK_INT64;
    if (t == "int32"   && d.int32_values.size()   == n) return DK_INT64;  // widened
    if (t == "float32" && d.float32_values.size() == n) return DK_FLOAT32;
    if (t == "float64" && d.float64_values.size() == n) return DK_FLOAT64;
    return DK_POOL;
}

class ParquetIOPipeline {
 private:
    struct WorkItem {
        std::string path;
        int rg_idx;
        std::vector<std::string> column_names;
        std::vector<ColumnStats> column_stats;  // absolute file offsets
        std::vector<uint8_t> row_mask;           // empty = no mask (decode all rows)
    };

    std::shared_ptr<BS::light_thread_pool> decode_pool_;
    // Multi-producer (4 decode workers) / single-consumer (Python-side caller)
    // queue. Lock contention is negligible vs the IO/decode cost per item.
    std::deque<MorselRef> result_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    size_t queue_capacity_;

    // Thread-local HTTP client: each BS worker thread owns its own HttpClient
    // and thus its own CURLSH connection cache. Eliminates CURL_LOCK_DATA_CONNECT
    // mutex contention when N threads simultaneously issue GCS range reads.
    static HttpClient& tl_http_client() {
        thread_local HttpClient client;
        return client;
    }

    std::atomic<int> pending_work_{0};
    std::atomic<bool> shutdown_{false};

    // Destination pool for serialized columns. Set once before any submit via
    // set_pool_sink(); workers reserve+serialize+finalize through it.
    PoolSink pool_sink_;

    // Diagnostic counters for queue-contention investigation.
    std::atomic<uint64_t> spin_iterations_{0};
    std::atomic<uint64_t> enqueue_count_{0};
    std::atomic<size_t>   queue_high_watermark_{0};

    // IO/handoff observability counters (all relaxed atomics; aggregated at
    // diagnostics time, zero coordination cost in the hot path).
    //
    // http_request_count_: total individual byte ranges requested from remote
    // storage (one per column chunk), whether issued singly or in a batch.
    // http_lat_buckets_: histogram of fetch *operations* — one entry per
    // read_range() single GET or per get_many() batch — bucketed by wall time
    // (upper bounds in ms below, last bucket is overflow). Request count and
    // operation count differ once batching is in play: a 4-column batch is
    // 4 requests but 1 operation.
    // worker_blocked_ns_: time workers spend blocked on the back-pressure CV
    // waiting for the consumer to drain — the consumer-bound signal.
    // ipc_bytes_serialized_: bytes written by serialize_decoded_column — the
    // first of the handoff copies (serialize → pool commit → deserialize).
    static constexpr int kHttpLatBuckets = 9;
    static constexpr uint64_t kHttpLatBoundsMs[kHttpLatBuckets - 1] =
        {1, 10, 50, 100, 250, 500, 1000, 5000};
    std::atomic<uint64_t> http_request_count_{0};
    std::atomic<uint64_t> http_fetch_ops_{0};
    std::atomic<uint64_t> http_lat_buckets_[kHttpLatBuckets] = {};
    std::atomic<uint64_t> worker_blocked_ns_{0};
    std::atomic<uint64_t> ipc_bytes_serialized_{0};

    // Record one fetch operation covering n_requests byte ranges that took
    // elapsed_ns wall time. n_requests=1 for a single GET, N for a batch.
    void record_http_fetch(uint64_t elapsed_ns, uint64_t n_requests) {
        http_request_count_.fetch_add(n_requests, std::memory_order_relaxed);
        http_fetch_ops_.fetch_add(1, std::memory_order_relaxed);
        const uint64_t ms = elapsed_ns / 1000000ULL;
        int b = 0;
        while (b < kHttpLatBuckets - 1 && ms >= kHttpLatBoundsMs[b]) ++b;
        http_lat_buckets_[b].fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * Convert gs://bucket/path to https://storage.googleapis.com/bucket/path.
     */
    static std::string gcs_to_https(const std::string& path) {
        // gs://bucket/object  →  https://storage.googleapis.com/bucket/object
        return "https://storage.googleapis.com/" + path.substr(5);
    }

    /**
     * Read a byte range from any supported path type.
     * Returns (bytes, elapsed_ns).
     */
    std::pair<std::vector<uint8_t>, uint64_t> read_range(
            const std::string& path, int64_t offset, int64_t size) {

        auto t0 = std::chrono::steady_clock::now();
        std::vector<uint8_t> bytes;
        bool is_remote = false;

        if (path.substr(0, 5) == "gs://") {
            is_remote = true;
            std::string url = gcs_to_https(path);
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = tl_http_client().get(url, {{"Range", range_hdr}});

        } else if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
            is_remote = true;
            std::string range_hdr = "bytes=" + std::to_string(offset) +
                                    "-" + std::to_string(offset + size - 1);
            bytes = tl_http_client().get(path, {{"Range", range_hdr}});

        } else {
            // Local file: POSIX pread
            bytes.resize(size);
            int fd = open(path.c_str(), O_RDONLY);
            if (fd < 0) {
                throw std::runtime_error("Cannot open file: " + path);
            }
            ssize_t n = pread(fd, bytes.data(), size, offset);
            close(fd);
            if (n < 0) {
                throw std::runtime_error("Read error: " + path);
            }
            if (static_cast<size_t>(n) != static_cast<size_t>(size)) {
                throw std::runtime_error("Short read: " + path +
                    " (expected " + std::to_string(size) +
                    ", got " + std::to_string(n) + ")");
            }
        }

        uint64_t elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t0).count();
        if (is_remote) {
            record_http_fetch(elapsed, 1);
        }
        return {std::move(bytes), elapsed};
    }

    /**
     * Build the fetch URL for a path: gs:// is rewritten to the GCS HTTPS
     * endpoint; http(s):// is used verbatim. Mirrors read_range()'s dispatch.
     */
    static std::string fetch_url_for(const std::string& path) {
        if (path.substr(0, 5) == "gs://") return gcs_to_https(path);
        return path;
    }

    void decode_row_group(const WorkItem& item) {
        MorselRef result;
        result.path = item.path;
        result.rg_idx = item.rg_idx;
        result.column_names = item.column_names;
        result.free_fn = pool_sink_.draken_free;   // owns abandoned direct buffers
        result.success = true;

        uint64_t total_read_ns = 0;
        uint64_t total_decode_ns = 0;

        // For local files, mmap the full column-chunk extent of the row group
        // once rather than open/pread/close per column.  Eliminates per-column
        // heap allocation and gives the kernel a sequential-prefetch hint via
        // MADV_SEQUENTIAL.  Falls back to read_range() for HTTP/GCS.
        bool is_local = item.path.rfind("gs://",   0) != 0 &&
                        item.path.rfind("http://",  0) != 0 &&
                        item.path.rfind("https://", 0) != 0;

        void*   mmap_base   = MAP_FAILED;
        size_t  mmap_len    = 0;
        int64_t mmap_offset = 0;  // page-aligned file offset of the mapping

        if (is_local && !item.column_stats.empty()) {
            int64_t span_min = INT64_MAX, span_max = 0;
            for (const auto& cs : item.column_stats) {
                int64_t base = cs.data_page_offset;
                if (cs.dictionary_page_offset >= 0 && cs.dictionary_page_offset < base)
                    base = cs.dictionary_page_offset;
                int64_t end = base + cs.total_compressed_size;
                if (base < span_min) span_min = base;
                if (end   > span_max) span_max = end;
            }
            long page_size  = sysconf(_SC_PAGESIZE);
            mmap_offset     = (span_min / page_size) * page_size;
            mmap_len        = static_cast<size_t>(span_max - mmap_offset);

            auto t_map = std::chrono::steady_clock::now();
            int fd = open(item.path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd >= 0) {
                mmap_base = mmap(nullptr, mmap_len, PROT_READ, MAP_PRIVATE, fd, mmap_offset);
                close(fd);
                // No madvise: let the OS manage readahead.
                if (mmap_base == MAP_FAILED)
                    mmap_base = MAP_FAILED;
            }
            total_read_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - t_map).count();
        }

        // Precompute mask pointer once — shared across all columns in this row group.
        const uint8_t* mask_ptr = item.row_mask.empty() ? nullptr : item.row_mask.data();

        // Per-column base offset (dictionary page if it precedes the data page,
        // else the data page). Computed once here and reused for both the
        // remote batch request and the in-loop chunk slicing.
        std::vector<int64_t> base_offsets(item.column_stats.size());
        for (size_t i = 0; i < item.column_stats.size(); ++i) {
            int64_t base = item.column_stats[i].data_page_offset;
            if (item.column_stats[i].dictionary_page_offset >= 0 &&
                item.column_stats[i].dictionary_page_offset < base) {
                base = item.column_stats[i].dictionary_page_offset;
            }
            base_offsets[i] = base;
        }

        // Remote batch prefetch: for HTTP/GCS, fetch every column chunk for
        // this row group concurrently in a single get_many() call rather than
        // one blocking GET per column (which serialized C round-trips per row
        // group). Local files use mmap (above) or per-column pread (in-loop).
        // The path is already a signed/self-authenticating URL when needed, so
        // no auth header is attached here.
        const bool remote = !is_local;
        std::vector<std::vector<uint8_t>> remote_buffers;

        try {
            if (remote && !item.column_stats.empty()) {
                const std::string url = fetch_url_for(item.path);
                std::vector<std::pair<std::string, std::map<std::string, std::string>>> reqs;
                reqs.reserve(item.column_stats.size());
                for (size_t i = 0; i < item.column_stats.size(); ++i) {
                    int64_t chunk_size = item.column_stats[i].total_compressed_size;
                    std::string range_hdr = "bytes=" + std::to_string(base_offsets[i]) +
                        "-" + std::to_string(base_offsets[i] + chunk_size - 1);
                    reqs.emplace_back(url, std::map<std::string, std::string>{{"Range", range_hdr}});
                }
                auto t_fetch = std::chrono::steady_clock::now();
                remote_buffers = tl_http_client().get_many(reqs);
                uint64_t batch_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - t_fetch).count();
                total_read_ns += batch_ns;
                // One fetch operation covering reqs.size() concurrent ranges.
                record_http_fetch(batch_ns, reqs.size());
            }

            for (size_t i = 0; i < item.column_stats.size(); ++i) {
                const auto& col_stats = item.column_stats[i];

                int64_t base_offset = base_offsets[i];
                int64_t chunk_size = col_stats.total_compressed_size;

                ColumnStats adjusted = col_stats;
                adjusted.data_page_offset -= base_offset;
                if (adjusted.dictionary_page_offset >= 0)
                    adjusted.dictionary_page_offset -= base_offset;

                DecodedColumn decoded;
                if (mmap_base != MAP_FAILED) {
                    // Zero-copy: slice directly into the mmap — no heap allocation.
                    const uint8_t* chunk_ptr =
                        static_cast<const uint8_t*>(mmap_base) + (base_offset - mmap_offset);
                    auto t_dec = std::chrono::steady_clock::now();
                    decoded = DecodeColumnFromChunk(
                        chunk_ptr, static_cast<size_t>(chunk_size), &adjusted, mask_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                    result.bytes_fetched += chunk_size;
                } else if (remote) {
                    // Batch-prefetched above: decode straight from the buffer.
                    const std::vector<uint8_t>& raw = remote_buffers[i];
                    result.bytes_fetched += chunk_size;
                    auto t_dec = std::chrono::steady_clock::now();
                    decoded = DecodeColumnFromChunk(
                        raw.data(), raw.size(), &adjusted, mask_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                } else {
                    // Local file whose mmap failed: per-column pread fallback.
                    auto [raw_bytes, read_ns] = read_range(item.path, base_offset, chunk_size);
                    result.bytes_fetched += chunk_size;
                    total_read_ns += read_ns;
                    auto t_dec = std::chrono::steady_clock::now();
                    decoded = DecodeColumnFromChunk(
                        raw_bytes.data(), raw_bytes.size(), &adjusted, mask_ptr);
                    total_decode_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now() - t_dec).count();
                }

                if (!decoded.success) {
                    result.success = false;
                    result.error = "Decode failed for column: " + col_stats.name;
                    break;
                }

                ColumnOut cout;
                // Direct path only for columns whose LOGICAL type is a plain
                // numeric (logical_type == the physical name, e.g. "int64").
                // DATE / TIMESTAMP / DECIMAL are physically int32/int64 but carry
                // a distinct logical_type ("date32[day]", "timestamp[ms,UTC]",
                // "decimal(p,s)") and get reinterpreted downstream
                // (_coerce_logical_types) from the pool/deserialize
                // representation — so they stay on the IPC path.
                const std::string& lt = col_stats.logical_type;
                const bool plain_logical = lt.empty() || lt == "int64" ||
                    lt == "int32" || lt == "float64" || lt == "float32";
                DirectKind dk = (pool_sink_.draken_alloc && plain_logical)
                    ? direct_kind_for(decoded) : DK_POOL;

                if (dk != DK_POOL) {
                    // Direct path: non-nullable fixed-width → a Draken-owned
                    // positional buffer the consumer wraps with zero copy.
                    const uint32_t n = static_cast<uint32_t>(decoded.num_rows);
                    void* buf = nullptr;
                    if (dk == DK_INT64 && decoded.type == "int32") {
                        buf = pool_sink_.draken_alloc(static_cast<size_t>(n) * 8);
                        if (buf) {
                            int64_t* d64 = static_cast<int64_t*>(buf);
                            for (uint32_t r = 0; r < n; ++r)
                                d64[r] = static_cast<int64_t>(decoded.int32_values[r]);
                        }
                    } else {
                        const void* src; size_t bytes;
                        if (dk == DK_INT64)        { src = decoded.int64_values.data();   bytes = static_cast<size_t>(n) * 8; }
                        else if (dk == DK_FLOAT32) { src = decoded.float32_values.data(); bytes = static_cast<size_t>(n) * 4; }
                        else                       { src = decoded.float64_values.data(); bytes = static_cast<size_t>(n) * 8; }
                        buf = pool_sink_.draken_alloc(bytes);
                        if (buf && bytes) std::memcpy(buf, src, bytes);
                    }
                    if (!buf) {
                        result.success = false;
                        result.error = "draken_alloc failed for column: " + col_stats.name;
                        break;
                    }
                    cout.direct_kind = dk;
                    cout.data = buf;
                    cout.length = n;
                    // Direct path emits no IPC bytes — ipc_bytes_serialized only
                    // accrues for pool-path columns, so its drop is the WP-6b signal.
                } else {
                    // Pool path (WP-6a): serialize straight into a MemoryPool
                    // region — no heap buffer, no consumer-side commit() copy.
                    // Parse precision/scale from the logical_type string
                    // (e.g. "decimal(15,2)") for DECIMAL128 columns.
                    uint8_t dec_precision = 38, dec_scale = 0;
                    if (!decoded.int128_values.empty()) {
                        const std::string& lt = col_stats.logical_type;
                        size_t lp = lt.find('(');
                        size_t cm = lt.find(',', lp);
                        size_t rp = lt.find(')', cm);
                        if (lp != std::string::npos && cm != std::string::npos && rp != std::string::npos) {
                            dec_precision = static_cast<uint8_t>(std::stoi(lt.substr(lp + 1, cm - lp - 1)));
                            dec_scale     = static_cast<uint8_t>(std::stoi(lt.substr(cm + 1, rp - cm - 1)));
                        }
                    }
                    // Exact size first (count pass), then one write pass into the
                    // reserved bytes; the two cannot disagree (same code path).
                    size_t sz = rugo::serialized_size(decoded, dec_precision, dec_scale);
                    void* dst = nullptr;
                    int64_t ref_id = pool_sink_.reserve
                        ? pool_sink_.reserve(pool_sink_.ctx, static_cast<int64_t>(sz), &dst)
                        : -1;
                    if (ref_id < 0 || dst == nullptr) {
                        result.success = false;
                        result.error = "MemoryPool exhausted serializing column: " + col_stats.name;
                        break;
                    }
                    size_t written = rugo::serialize_decoded_column_into(
                        decoded, static_cast<uint8_t*>(dst), dec_precision, dec_scale);
                    pool_sink_.finalize(pool_sink_.ctx, ref_id, static_cast<int64_t>(written));
                    ipc_bytes_serialized_.fetch_add(written, std::memory_order_relaxed);
                    cout.direct_kind = DK_POOL;
                    cout.ref_id = ref_id;
                }
                result.columns.push_back(cout);
            }
        } catch (const std::exception& e) {
            result.success = false;
            result.error = e.what();
        }

        if (mmap_base != MAP_FAILED)
            munmap(mmap_base, mmap_len);

        result.read_ns = total_read_ns;
        result.decode_ns = total_decode_ns;
        // Apply soft back-pressure: if the consumer is far behind, block
        // on the condition variable until it drains rather than spin-yielding.
        {
            auto t_bp = std::chrono::steady_clock::now();
            std::unique_lock<std::mutex> lk(queue_mutex_);
            queue_cv_.wait(lk, [this]() {
                return result_queue_.size() < queue_capacity_ || shutdown_.load(std::memory_order_relaxed);
            });
            worker_blocked_ns_.fetch_add(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - t_bp).count(),
                std::memory_order_relaxed);
            if (!shutdown_.load(std::memory_order_relaxed)) {
                result_queue_.push_back(std::move(result));
                size_t sz = result_queue_.size();
                enqueue_count_.fetch_add(1, std::memory_order_relaxed);
                size_t prev = queue_high_watermark_.load(std::memory_order_relaxed);
                while (sz > prev &&
                       !queue_high_watermark_.compare_exchange_weak(
                           prev, sz, std::memory_order_relaxed)) {}
            }
        }
        pending_work_--;
        queue_cv_.notify_one();
    }

 public:
    ParquetIOPipeline(int decode_workers = 4,
                      size_t result_queue_capacity = 256)
        : decode_pool_(std::make_shared<BS::light_thread_pool>(decode_workers)),
          queue_capacity_(result_queue_capacity) {}

    ~ParquetIOPipeline() {
        wait_shutdown();
    }

    // Wire the destination MemoryPool. Must be called before any submit; the
    // workers serialize decoded columns directly into pool-reserved regions.
    void set_pool_sink(PoolSink sink) {
        pool_sink_ = sink;
    }

    /**
     * Submit a row group for read + decode + serialize.
     * column_stats carry absolute file offsets — worker adjusts to buffer-relative.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats) {
        if (shutdown_) return;

        pending_work_++;

        WorkItem item;
        item.path = path;
        item.rg_idx = rg_idx;
        item.column_names = column_names;
        item.column_stats = column_stats;

        decode_pool_->detach_task([this, item = std::move(item)]() {
            decode_row_group(item);
        });
    }

    /**
     * Submit a row group with a per-row mask (1=keep, 0=skip).
     * Workers apply the mask during decode so only surviving rows are serialized.
     * Default-empty mask in the base overload means existing callers are unaffected.
     */
    void submit_row_group(const std::string& path, int rg_idx,
                          const std::vector<std::string>& column_names,
                          const std::vector<ColumnStats>& column_stats,
                          const std::vector<uint8_t>& row_mask) {
        if (shutdown_) return;

        pending_work_++;

        WorkItem item;
        item.path = path;
        item.rg_idx = rg_idx;
        item.column_names = column_names;
        item.column_stats = column_stats;
        item.row_mask = row_mask;

        decode_pool_->detach_task([this, item = std::move(item)]() {
            decode_row_group(item);
        });
    }

    bool try_get_result(MorselRef& out) {
        std::lock_guard<std::mutex> lk(queue_mutex_);
        if (result_queue_.empty()) return false;
        out = std::move(result_queue_.front());
        result_queue_.pop_front();
        queue_cv_.notify_one();  // wake a blocked producer if queue was full
        return true;
    }

    /**
     * Block until a result is available or the pipeline is fully drained.
     * Returns true and populates `out` when a result is ready.
     * Returns false when the pipeline is shut down and the queue is empty.
     */
    bool wait_and_get_result(MorselRef& out) {
        std::unique_lock<std::mutex> lk(queue_mutex_);
        queue_cv_.wait(lk, [this]() {
            return !result_queue_.empty() || shutdown_.load(std::memory_order_relaxed);
        });
        if (result_queue_.empty()) {
            return false;  // shutdown and nothing left
        }
        out = std::move(result_queue_.front());
        result_queue_.pop_front();
        queue_cv_.notify_one();  // wake a blocked producer if queue was full
        return true;
    }

    void wait_shutdown() {
        shutdown_ = true;
        queue_cv_.notify_all();
        if (decode_pool_) {
            decode_pool_->wait();
        }
    }

    int pending_work_count() const {
        return pending_work_.load(std::memory_order_relaxed);
    }

    uint64_t spin_iterations() const {
        return spin_iterations_.load(std::memory_order_relaxed);
    }
    uint64_t enqueue_count() const {
        return enqueue_count_.load(std::memory_order_relaxed);
    }
    size_t queue_high_watermark() const {
        return queue_high_watermark_.load(std::memory_order_relaxed);
    }

    uint64_t http_request_count() const {
        return http_request_count_.load(std::memory_order_relaxed);
    }
    uint64_t http_fetch_ops() const {
        return http_fetch_ops_.load(std::memory_order_relaxed);
    }
    int http_latency_bucket_count() const {
        return kHttpLatBuckets;
    }
    // Upper bound (ms) of bucket i; the final bucket is overflow and returns 0.
    uint64_t http_latency_bucket_bound_ms(int i) const {
        return (i >= 0 && i < kHttpLatBuckets - 1) ? kHttpLatBoundsMs[i] : 0;
    }
    uint64_t http_latency_bucket(int i) const {
        return (i >= 0 && i < kHttpLatBuckets)
            ? http_lat_buckets_[i].load(std::memory_order_relaxed) : 0;
    }
    uint64_t worker_blocked_ns() const {
        return worker_blocked_ns_.load(std::memory_order_relaxed);
    }
    uint64_t ipc_bytes_serialized() const {
        return ipc_bytes_serialized_.load(std::memory_order_relaxed);
    }
};

}  // namespace rugo
