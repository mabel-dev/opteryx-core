#pragma once
// src/cpp/engine/native_jsonl_scan_source.hpp — a native (zero-Python) streaming
// JSONL scan Source: READ_JSONL and JSONL dataset scans. There is no other JSONL
// read path.
//
// Sibling of native_postgres_scan_source.hpp. Planning (Python) resolves the file
// list, the projection, the bind-time schema pinned onto every chunk and the pushed
// predicates, and builds rugo's ParseContext ONCE through the same code read_jsonl
// uses (rugo.rugo_native.prepare_jsonl_context). This Source borrows that plan in a
// JsonlScanSpec and runs it; nothing here touches Python.
//
// Decode: the Source owns its OWN decode pool (architect ruling 2026-10-01, like the
// parquet reader's — not the engine's DOP budget). Each pool worker claims one whole
// newline-aligned chunk — (file, start, end), cut at the first newline at or after
// `chunk_size` so no record is split — and decodes it SERIALLY through rugo's C++
// JSONL path (interpret_jsonl_threaded / parse_all_columns with max_threads = 1:
// the parallelism is across chunks, not inside one). A finished chunk becomes one
// morsel on a ready queue that the engine's workers drain, so decode overlaps
// execution. At most `decode_workers + 2` chunks are claimed-but-unconsumed at any
// time, which bounds resident memory instead of holding the whole decoded dataset.
//
// Files: a local file is memory-mapped; a remote one (http(s):// URLs, and public
// gs:// / s3:// objects whose credential-free URLs planning resolved) is fetched
// WHOLE with one GET through the native HttpClient — no credentials, ever
// (architect ruling 2026-10-01: authenticated remote JSONL is not supported). A
// worker loads a file only when no loaded file has bytes left to cut, and loads run
// outside the cursor lock, so several files load in parallel and a slow GET never
// stalls chunks of files already loaded. A file's bytes are released when its last
// chunk is decoded (decoded columns own their bytes — nothing points back in).
//
// Compressed files (gzip / zstd / lz4, detected by magic bytes —
// rugo/src/compression/stream_decompress.hpp) cannot be cut at arbitrary offsets,
// so each one is a sequential stream: one worker at a time decompresses its next
// newline-aligned chunk (same cut rule as a plain file) into an owned buffer,
// outside the cursor lock, while other workers decode chunks already produced or
// stream other files. Resident memory stays bounded by the in-flight window — the
// decompressed file is never held whole. An unsupported codec, a mislabelled
// extension, or a corrupt/truncated stream fails the query naming the file.
//
// Order: chunk/morsel order is NOT guaranteed (architect ruling 2026-10-01).
//
// Errors fail fast (architect ruling 2026-10-01): a declared-type mismatch, an
// invalid JSON record (fail_on_error) or column drift in ANY chunk ends the query
// mid-execution through ErrCtx with kErrCodeReadError (-> DatasetReadError), with
// the same message text the compile-time materialized path raised.
//
// Per-chunk contract, unchanged from opteryx/operators/jsonl_read/jsonl_read.pyx:
//   - every projected column is DECLARED (explicit_schema) and parsed strictly as
//     its bind-time type; a value that does not fit fails loud;
//   - a projected column whose key appears in NO record of a chunk is column drift
//     and fails loud naming the file — EXCEPT a nested `key->>'sub'` / `key->'sub'`
//     column (`drift_exempt`), which a chunk may legitimately lack;
//   - a chunk whose rows are all filtered out by the pushed predicates contributes
//     nothing (skipped, not an error);
//   - a zero-column projection (COUNT(*)) emits zero-column morsels whose row count
//     rides on `zero_col_rows`. Only the record count is needed, so no column is
//     decoded at all.
//
// Vectors: rugo's ParsedColumn is a carrier of plain draken_malloc'd buffers, so
// columns are built straight into VectorOwner/CxxColumn exactly as the parquet
// Source builds them (draken_vector_from_dense + emit_dense_string_column) — no
// PyObject, no box.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "operator.hpp"
#include "morsels/cxx_morsel.h"
#include "core/alloc.h"                    // draken_free
#include "core/vector_alloc.h"             // draken_vector_from_dense
#include "core/vector_owner.h"             // VectorOwner, OwnedBuffer
#include "logical_type.h"                  // LogicalType / logical_type_intern
#include "native_varchar_pool_decode.hpp"  // emit_dense_string_column, string_arena_block
#include "disk_io.h"                       // read_all_mmap / unmap_memory_c
#include "http_client.hpp"                 // HttpClient — remote files, whole-object GET

// rugo's JSONL core (rugo/src/jsonl/core) — pure C++.
#include "parse_context.hpp"
#include "interpreter.hpp"       // check_predicate_literals, RecordSet
#include "field_span.hpp"        // interpret_jsonl_threaded, OrdinalPredictor
#include "column_builder.hpp"    // parse_all_columns, ParsedColumn
#include "jsonl_reader.hpp"      // maybe_prefilter, malformed_error_message, py_str_repr
#include "compression/stream_decompress.hpp"   // resolve_codec, LineChunker

namespace opteryx::engine {

// The plan-time description of one native JSONL scan. Built by the Cython
// JsonlScanPlan (opteryx/operators/_operators.pyx) and BORROWED by
// NativeJsonlScanSource for the driver's lifetime.
struct JsonlScanSpec {
    std::vector<std::string> files;          // as resolved at bind time; named in messages
    std::vector<std::string> urls;           // parallel: "" = local path, else the URL to
                                             // GET with NO credentials
    rugo::_jsonl::ParseContext context;      // projection, predicates, pinned schema, options

    // Per DECODED column (parallel vectors): each physical (pre-alias) name once.
    std::vector<std::string> decode_names;
    std::vector<uint8_t>     drift_exempt;   // 1 = nested column: may be absent from a chunk

    // Per EMITTED column (parallel vectors, emit order). One physical column can feed
    // several identities (`commit->>'collection'` bound in SELECT and in WHERE is one
    // read emitted under both), so emission indexes into the decoded set.
    std::vector<std::string> out_identities; // plan identities the morsel names carry
    std::vector<uint32_t>    emit_index;     // index into decode_names

    bool     zero_columns = false;  // COUNT(*)-shaped scan: zero-column morsels, row count only
    uint64_t chunk_size = 0;        // target chunk bytes (cut forward to the next newline)
    int      decode_workers = 1;    // width of this Source's own decode pool

    // Written by the Source (once, when its global state is torn down) and read from
    // Python after the driver finishes. -1 = never ran. `mutable` because the Source
    // borrows the spec as const and these are its only writes.
    mutable int64_t rows_read   = -1;   // rows emitted
    mutable int64_t bytes_read  = -1;   // chunk bytes decoded
    mutable int64_t chunks_read = -1;   // chunks decoded
};

// Copy the ParseContext prepare_jsonl_context built (owned by a PyCapsule the
// planner holds) into the spec. A plain copy: the spec must outlive nothing else.
inline void jsonl_scan_spec_set_context(JsonlScanSpec* spec, const void* context) {
    spec->context = *static_cast<const rugo::_jsonl::ParseContext*>(context);
}

namespace jsonl_detail {

// One loaded file: a mapping (local) or a fetched body (remote). Shared by every
// chunk cut from it; released when the last chunk holding it is decoded. For a
// compressed file it is the COMPRESSED bytes, and each decompressed chunk is its
// own Mapping owning `chunk`.
struct Mapping {
    uint8_t* ptr = nullptr;
    size_t   len = 0;
    bool     mapped = false;          // ptr is an mmap; otherwise it points into `body`/`chunk`
    std::vector<uint8_t> body;
    rugo::compression::ByteBuffer chunk;
    ~Mapping() { if (mapped && ptr != nullptr) unmap_memory_c(ptr, len); }
};

enum class FileStatus : uint8_t { PENDING, LOADING, READY, DONE };

struct FileState {
    FileStatus status = FileStatus::PENDING;
    std::shared_ptr<Mapping> data;
    size_t offset = 0;
    // Compressed file: its decompressing stream (which reads from `data`), and whether
    // a worker is producing its next chunk right now (one at a time — a stream is
    // sequential).
    std::unique_ptr<rugo::compression::LineChunker> stream;
    bool busy = false;
};

struct Chunk {
    std::shared_ptr<Mapping> map;
    size_t start = 0;
    size_t end = 0;
    size_t file = 0;
};

// ParsedColumn -> CxxColumn, taking ownership of every buffer it carries. Mirrors
// rugo's wrap_column (column_builder.cpp) branch for branch, minus the Python box.
inline void build_column(rugo::_jsonl::ParsedColumn& pc, CxxColumn& out) {
    if (pc.type == DRAKEN_ARRAY) {
        std::unique_ptr<VectorOwner> child;
        if (pc.array_child_slots != nullptr) {
            // String-family child (draken_vector_own_array): header + slots block, the
            // child arena borrowed into arena_buf, element validity on the header too.
            DrakenStringArena* sa = nullptr;
            uint8_t* block = string_arena_block(pc.array_child_slots, pc.array_child_length,
                                                pc.array_child_arena, pc.array_child_arena_len,
                                                pc.array_child_type, &sa);
            draken_free(pc.array_child_slots);
            sa->null_bitmap = pc.array_child_validity;
            DrakenVector cv = draken_vector_from_dense(sa, pc.array_child_length,
                                                       pc.array_child_type,
                                                       pc.array_child_validity);
            child = std::make_unique<VectorOwner>(cv, OwnedBuffer<void>(block),
                                                  OwnedBuffer<uint8_t>(pc.array_child_validity),
                                                  OwnedBuffer<void>(nullptr),
                                                  OwnedBuffer<uint8_t>(pc.array_child_arena));
        } else {
            // Fixed-width child (draken_vector_own_array_numeric).
            DrakenVector cv = draken_vector_from_dense(pc.array_child_data, pc.array_child_length,
                                                       pc.array_child_type,
                                                       pc.array_child_validity);
            child = std::make_unique<VectorOwner>(cv, OwnedBuffer<void>(pc.array_child_data),
                                                  OwnedBuffer<uint8_t>(pc.array_child_validity));
        }
        DrakenVector pv = draken_vector_from_dense(pc.array_parent_offsets, pc.length,
                                                   DRAKEN_ARRAY, pc.validity);
        out.own = std::make_shared<VectorOwner>(pv, OwnedBuffer<void>(pc.array_parent_offsets),
                                                OwnedBuffer<uint8_t>(pc.validity));
        out.own->child_owner = std::move(child);
        out.view = out.own->vec;
        return;
    }
    if (pc.is_string) {
        emit_dense_string_column(pc.slots, pc.length, pc.arena, pc.arena_len, pc.validity,
                                 pc.type, out);
        return;
    }
    DrakenVector v = draken_vector_from_dense(pc.data, pc.length, pc.type, pc.validity);
    out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(pc.data),
                                            OwnedBuffer<uint8_t>(pc.validity));
    // A declared IPV4/TIMESTAMP/DECIMAL column carries its logical-type descriptor on
    // the owner (draken_vector_own_raw_logical); every inferred column has kind NONE.
    if (pc.logical_kind != 0) {
        LogicalType lt;
        lt.kind = static_cast<LogicalKind>(pc.logical_kind);
        lt.unit = static_cast<TimestampUnit>(pc.unit);
        lt.offset_minutes = pc.offset_minutes;
        lt.precision = pc.precision;
        lt.scale = pc.scale;
        lt.dimension = 0;
        out.own->logical_type = logical_type_intern(lt);
    }
    out.view = out.own->vec;
}

}  // namespace jsonl_detail

struct NativeJsonlScanGlobal : GlobalSourceState {
    const JsonlScanSpec* spec = nullptr;

    // Ready queue + flow control (all under `mtx`).
    std::mutex mtx;
    std::condition_variable cv_ready;   // engine workers wait for a morsel / the end
    std::condition_variable cv_room;    // decode workers wait for in-flight room
    std::deque<MorselPtr> ready;
    size_t in_flight = 0;               // claimed chunks not yet handed to the engine
    size_t window = 1;
    int live_workers = 0;
    bool stop = false;
    bool failed = false;
    std::string err;                    // ErrCtx::msg points here

    // Chunk cursor (under `claim_mtx`): per-file load state. `first_live` skips the
    // files already DONE; `next_pending` is the next file nobody has started loading.
    std::mutex claim_mtx;
    std::condition_variable cv_loaded;  // a file finished loading, or the scan ended
    std::vector<jsonl_detail::FileState> files;
    size_t first_live = 0;
    size_t next_pending = 0;
    int loading = 0;
    std::atomic<bool> abort{false};     // stop / failure: claimers stop waiting

    std::unique_ptr<HttpClient> http;   // created only when some file is remote

    std::atomic<int64_t> rows{0};
    std::atomic<int64_t> bytes{0};
    std::atomic<int64_t> chunks{0};

    std::vector<std::thread> workers;

    ~NativeJsonlScanGlobal() override {
        {
            std::lock_guard<std::mutex> lock(mtx);
            stop = true;
        }
        abort.store(true);
        {
            std::lock_guard<std::mutex> lock(claim_mtx);
        }
        cv_room.notify_all();
        cv_loaded.notify_all();
        for (auto& t : workers) t.join();
        spec->rows_read = rows.load();
        spec->bytes_read = bytes.load();
        spec->chunks_read = chunks.load();
    }
};

class NativeJsonlScanSource : public Source {
public:
    explicit NativeJsonlScanSource(const JsonlScanSpec* spec) : spec_(spec) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        auto g = std::make_unique<NativeJsonlScanGlobal>();
        g->spec = spec_;
        const int n = spec_->decode_workers > 0 ? spec_->decode_workers : 1;
        g->window = static_cast<size_t>(n) + 2;
        g->live_workers = n;
        g->files.resize(spec_->files.size());
        for (const auto& url : spec_->urls) {
            if (!url.empty()) {
                g->http = std::make_unique<HttpClient>();
                break;
            }
        }
        NativeJsonlScanGlobal* gp = g.get();
        g->workers.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i)
            g->workers.emplace_back([this, gp]() { decode_loop(*gp); });
        return g;
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativeJsonlScanGlobal&>(gs);
        std::unique_lock<std::mutex> lock(g.mtx);
        g.cv_ready.wait(lock, [&]() {
            return g.failed || !g.ready.empty() || g.live_workers == 0;
        });
        if (g.failed) {
            err.code = kErrCodeReadError;
            err.msg = g.err.c_str();
            return SourceResult::FINISHED;
        }
        if (g.ready.empty()) return SourceResult::FINISHED;
        out = std::move(g.ready.front());
        g.ready.pop_front();
        g.in_flight -= 1;
        lock.unlock();
        g.cv_room.notify_one();
        // rows_out/bytes_out are charged by the driver around this call (executor.hpp).
        return SourceResult::HAVE_MORE;
    }

private:
    // First failure wins; every waiter is released.
    static void fail(NativeJsonlScanGlobal& g, std::string message) {
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            if (!g.failed) {
                g.failed = true;
                g.err = std::move(message);
            }
        }
        g.abort.store(true);
        {
            std::lock_guard<std::mutex> lock(g.claim_mtx);
        }
        g.cv_ready.notify_all();
        g.cv_room.notify_all();
        g.cv_loaded.notify_all();
    }

    // Load file `i` — map it, or GET it whole with no credentials — and, when its
    // bytes are compressed, open its decompressing stream. Called WITHOUT the cursor
    // lock. Empty on success, else the query-ending message.
    std::string load(NativeJsonlScanGlobal& g, size_t i,
                     std::shared_ptr<jsonl_detail::Mapping>& out,
                     std::unique_ptr<rugo::compression::LineChunker>& stream) {
        const std::string& path = spec_->files[i];
        auto m = std::make_shared<jsonl_detail::Mapping>();
        if (spec_->urls[i].empty()) {
            const int rc = read_all_mmap(path.c_str(), &m->ptr, &m->len);
            if (rc != 0)
                return "READ_JSONL('" + path + "'): the file could not be read: " +
                       std::strerror(-rc);
            m->mapped = m->ptr != nullptr;
        } else {
            try {
                m->body = g.http->get(spec_->urls[i]);
            } catch (const std::exception& e) {
                return "READ_JSONL('" + path + "'): the file could not be read: " + e.what();
            }
            m->ptr = m->body.data();
            m->len = m->body.size();
        }
        try {
            const auto codec = rugo::compression::resolve_codec(path, m->ptr, m->len);
            if (codec != rugo::compression::Codec::NONE)
                stream = std::make_unique<rugo::compression::LineChunker>(
                    rugo::compression::make_decoder(codec, m->ptr, m->len));
        } catch (const std::exception& e) {
            return "READ_JSONL('" + path + "'): " + e.what();
        }
        out = std::move(m);
        return std::string();
    }

    // Decompress file `i`'s next chunk. Called WITHOUT the cursor lock, by the one
    // worker that marked the stream busy. Empty on success (`out` null at end of
    // stream), else the query-ending message.
    std::string next_compressed_chunk(rugo::compression::LineChunker& stream, size_t i,
                                      std::shared_ptr<jsonl_detail::Mapping>& out) {
        auto m = std::make_shared<jsonl_detail::Mapping>();
        try {
            if (!stream.next(static_cast<size_t>(spec_->chunk_size), m->chunk)) return std::string();
        } catch (const std::exception& e) {
            return "READ_JSONL('" + spec_->files[i] + "'): the compressed file could not be "
                   "decompressed: " + e.what();
        }
        m->ptr = m->chunk.data.get();
        m->len = m->chunk.len;
        out = std::move(m);
        return std::string();
    }

    // Cut the next newline-aligned chunk. Prefers bytes of files already loaded;
    // only when none remain does it start loading the next file (outside the lock),
    // and when every file is loaded-or-loading it waits for a load to land. False at
    // the end of the last file, or with `error` set when a file cannot be loaded.
    bool claim(NativeJsonlScanGlobal& g, jsonl_detail::Chunk& chunk, std::string& error) {
        using jsonl_detail::FileStatus;
        std::unique_lock<std::mutex> lock(g.claim_mtx);
        const size_t n = g.files.size();
        for (;;) {
            if (g.abort.load()) return false;
            for (size_t i = g.first_live; i < n; ++i) {
                jsonl_detail::FileState& f = g.files[i];
                if (f.status != FileStatus::READY) continue;
                if (f.stream) {
                    if (f.busy) continue;
                    // Produce this stream's next chunk outside the lock; while busy it
                    // counts as loading, so waiters neither give up nor spin on it.
                    f.busy = true;
                    g.loading += 1;
                    lock.unlock();
                    std::shared_ptr<jsonl_detail::Mapping> data;
                    error = next_compressed_chunk(*f.stream, i, data);
                    lock.lock();
                    g.loading -= 1;
                    f.busy = false;
                    g.cv_loaded.notify_all();
                    if (!error.empty()) return false;
                    if (data == nullptr) {
                        f.status = FileStatus::DONE;
                        f.stream.reset();
                        f.data.reset();
                        continue;
                    }
                    chunk.map = std::move(data);
                    chunk.start = 0;
                    chunk.end = chunk.map->len;
                    chunk.file = i;
                    return true;
                }
                const size_t len = f.data->len;
                if (f.offset >= len) {
                    f.status = FileStatus::DONE;
                    f.data.reset();
                    continue;
                }
                size_t end = f.offset + static_cast<size_t>(spec_->chunk_size);
                if (end >= len) {
                    end = len;
                } else {
                    // Push the boundary forward to the next newline so no record is
                    // split; a file whose last line has no newline ends at EOF.
                    const void* nl = std::memchr(f.data->ptr + end, '\n', len - end);
                    end = nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - f.data->ptr) + 1
                             : len;
                }
                chunk.map = f.data;
                chunk.start = f.offset;
                chunk.end = end;
                chunk.file = i;
                f.offset = end;
                return true;
            }
            while (g.first_live < n && g.files[g.first_live].status == FileStatus::DONE)
                g.first_live += 1;
            if (g.next_pending < n) {
                const size_t i = g.next_pending++;
                g.files[i].status = FileStatus::LOADING;
                g.loading += 1;
                lock.unlock();
                std::shared_ptr<jsonl_detail::Mapping> data;
                std::unique_ptr<rugo::compression::LineChunker> stream;
                error = load(g, i, data, stream);
                lock.lock();
                g.loading -= 1;
                if (!error.empty()) return false;
                g.files[i].data = std::move(data);
                g.files[i].stream = std::move(stream);
                g.files[i].status = FileStatus::READY;
                g.cv_loaded.notify_all();
                continue;
            }
            if (g.loading == 0) return false;
            g.cv_loaded.wait(lock, [&]() { return g.abort.load() || g.loading == 0 ||
                                                  any_ready(g); });
        }
    }

    static bool any_ready(const NativeJsonlScanGlobal& g) {
        for (size_t i = g.first_live; i < g.files.size(); ++i)
            if (g.files[i].status == jsonl_detail::FileStatus::READY && !g.files[i].busy)
                return true;
        return false;
    }

    void decode_loop(NativeJsonlScanGlobal& g) {
        for (;;) {
            {
                std::unique_lock<std::mutex> lock(g.mtx);
                g.cv_room.wait(lock, [&]() {
                    return g.stop || g.failed || g.in_flight < g.window;
                });
                if (g.stop || g.failed) break;
                g.in_flight += 1;
            }
            jsonl_detail::Chunk chunk;
            std::string error;
            if (!claim(g, chunk, error)) {
                {
                    std::lock_guard<std::mutex> lock(g.mtx);
                    g.in_flight -= 1;
                }
                if (!error.empty()) fail(g, std::move(error));
                break;
            }
            MorselPtr morsel;
            try {
                decode(g, chunk, morsel, error);
            } catch (const std::exception& e) {
                error = "READ_JSONL('" + spec_->files[chunk.file] + "'): " + e.what();
            }
            chunk.map.reset();
            if (!error.empty()) {
                fail(g, std::move(error));
                break;
            }
            const bool produced = morsel != nullptr;
            {
                std::lock_guard<std::mutex> lock(g.mtx);
                if (produced) g.ready.push_back(std::move(morsel));
                else g.in_flight -= 1;   // zero-row chunk: its slot frees immediately
            }
            if (produced) g.cv_ready.notify_one();
            else g.cv_room.notify_one();
        }
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.live_workers -= 1;
        }
        g.cv_ready.notify_all();
    }

    // Decode one chunk. `out` stays null when no row survives (a legitimate zero-row
    // chunk); `error` is set to the query-ending message otherwise.
    void decode(NativeJsonlScanGlobal& g, const jsonl_detail::Chunk& chunk, MorselPtr& out,
                std::string& error) {
        namespace rj = rugo::_jsonl;
        const std::string& path = spec_->files[chunk.file];
        const rj::ParseContext& ctx = spec_->context;
        const uint8_t* buf = chunk.map->ptr + chunk.start;
        size_t len = chunk.end - chunk.start;
        g.bytes.fetch_add(static_cast<int64_t>(len), std::memory_order_relaxed);
        g.chunks.fetch_add(1, std::memory_order_relaxed);

        // A decode failure in a projected scan is reported against the bind-time
        // schema, exactly as jsonl_read.pyx wrapped every ValueError rugo raised; a
        // zero-column scan reported rugo's message bare.
        auto decode_error = [&](const std::string& what) {
            if (spec_->zero_columns) return what;
            return "READ_JSONL('" + path + "'): a value does not fit the schema resolved at "
                   "bind time (from the first file in this glob's matched-file set). " + what;
        };

        try {
            // Predicate literals are checked against their columns BEFORE any row is
            // filtered — a literal of the wrong type raises instead of answering "no rows".
            if (!ctx.predicates.empty()) rj::check_predicate_literals(buf, len, ctx);
        } catch (const std::invalid_argument& e) {
            error = decode_error(e.what());
            return;
        }

        std::vector<uint8_t> survivors;
        if (len > 0 && rj::maybe_prefilter(buf, len, ctx, survivors)) {
            buf = survivors.data();
            len = survivors.size();
        }
        if (len == 0) return;

        rj::OrdinalPredictor predictor;
        rj::InterpreterResult ir;
        try {
            ir = rj::interpret_jsonl_threaded(buf, len, ctx, predictor, 1);
        } catch (const std::invalid_argument& e) {
            error = decode_error(e.what());
            return;
        }
        if (ctx.fail_on_error && ir.all_records.malformed) {
            error = decode_error(rj::malformed_error_message(buf, len, ir.all_records.malformed_pos));
            return;
        }
        if (ir.all_records.num_records() == 0 || ir.num_records_passed == 0) return;
        const uint32_t rows = static_cast<uint32_t>(ir.num_records_passed);

        auto morsel = std::make_shared<CxxMorsel>();
        if (spec_->zero_columns) {
            morsel->zero_col_rows = rows;
        } else {
            const bool may_escape = std::memchr(buf, '\\', len) != nullptr;
            std::vector<rj::ParsedColumn> parsed;
            try {
                parsed = rj::parse_all_columns(buf, ir.all_records, spec_->decode_names, 1,
                                               may_escape, ctx);
            } catch (const std::invalid_argument& e) {
                error = decode_error(e.what());
                return;
            }
            // Ownership moves into owners BEFORE the drift check, so a failing chunk's
            // buffers are released with it rather than leaked.
            const size_t ncols = parsed.size();
            std::vector<CxxColumn> decoded(ncols);
            for (size_t i = 0; i < ncols; ++i)
                jsonl_detail::build_column(parsed[i], decoded[i]);
            std::vector<std::string> absent;
            for (size_t i = 0; i < ncols; ++i)
                if (parsed[i].key_absent && spec_->drift_exempt[i] == 0)
                    absent.push_back(spec_->decode_names[i]);
            if (!absent.empty()) {
                // Column drift: a bound column whose key appears in NO record of this
                // chunk — a file in the glob that does not have the column at all.
                std::sort(absent.begin(), absent.end());
                std::string listed = "[";
                for (size_t i = 0; i < absent.size(); ++i) {
                    if (i) listed += ", ";
                    listed += rj::py_str_repr(reinterpret_cast<const uint8_t*>(absent[i].data()),
                                              absent[i].size());
                }
                listed += "]";
                error = "READ_JSONL('" + path + "'): the expected columns " + listed +
                        " (from the bind-time schema, resolved from the first file in this "
                        "glob's matched-file set) are absent from every record in a chunk "
                        "of this file.";
                return;
            }
            // A column emitted under several identities shares one owner.
            morsel->columns.reserve(spec_->emit_index.size());
            for (const uint32_t d : spec_->emit_index) morsel->columns.push_back(decoded[d]);
            morsel->names = spec_->out_identities;
        }
        g.rows.fetch_add(rows, std::memory_order_relaxed);
        out = std::move(morsel);
    }

    const JsonlScanSpec* spec_;
};

}  // namespace opteryx::engine
