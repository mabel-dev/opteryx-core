#pragma once
// src/cpp/engine/native_avro_scan_source.hpp — a native (zero-Python) Avro scan
// Source: READ_AVRO. There is no other Avro read path in the engine.
//
// Sibling of native_jsonl_scan_source.hpp. Planning (Python) resolves the file list,
// the projection (the file's top-level field names), the reader schema every file is
// read with (the first file's writer schema — so a file whose schema evolved resolves
// onto the bound relation, docs/AVRO_READER_DESIGN.md §19.3), and where each file is
// read from. This Source borrows that plan in an AvroScanSpec and runs it; nothing
// here touches Python.
//
// Decode: the Source owns its own decode pool (as the JSONL and parquet readers do).
// The unit of work is a BATCH — a run of whole Avro blocks, at most 65,536 rows — so
// one large file spreads across the whole pool (Avro blocks are independent). A file
// is OPENED once, by the first worker that needs it and outside the cursor lock:
// loaded (a local file is memory-mapped; a remote one is fetched whole with one GET,
// no credentials — the READ_JSONL rules), compiled (rugo::avro::AvroFile: header,
// schemas, decode program) and framed (its block headers walked into batches, no
// decompression). Every worker then claims that file's batches and decodes each
// through its own AvroFile::Worker (decoder state + decompressor). A finished batch is
// one morsel on a ready queue the engine's workers drain, so decode overlaps
// execution. At most `decode_workers + 2` batches are claimed-but-unconsumed at any
// time, which bounds resident memory; a file is released when its last batch is done.
//
// Order: morsel order is NOT guaranteed, within a file or across files.
//
// A zero-column projection (COUNT(*)) decodes nothing: each batch's row count comes
// from the block headers and the Source emits zero-column morsels carrying
// `zero_col_rows`.
//
// Errors fail fast through ErrCtx with kErrCodeReadError (-> DatasetReadError),
// naming the file: a corrupt file, a refused construct, or a file whose schema does
// not resolve against the reader schema.
//
// Vectors: rugo's AvroColumn is a carrier of plain draken_malloc'd buffers, so
// columns are built straight into VectorOwner/CxxColumn as the JSONL Source builds
// them — no PyObject. A reader-only field is a constant (one value, positions all 0 —
// CLAUDE.md §11); a TIMESTAMP / TIME / DECIMAL one carries its logical type here,
// which the Python edge cannot (docs §19.4).

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "operator.hpp"
#include "morsels/cxx_morsel.h"
#include "core/alloc.h"                    // draken_free
#include "core/vector_alloc.h"             // draken_vector_from_dense / _from_dict
#include "core/vector_owner.h"             // VectorOwner, OwnedBuffer
#include "logical_type.h"                  // LogicalType / logical_type_intern
#include "native_varchar_pool_decode.hpp"  // emit_dense_string_column, string_arena_block
#include "disk_io.h"                       // read_all_mmap / unmap_memory_c
#include "http_client.hpp"                 // HttpClient — remote files, whole-object GET

// rugo's Avro reader (rugo/src/avro) — pure C++.
#include "avro/avro_reader.hpp"

namespace opteryx::engine {

// The plan-time description of one native Avro scan. Built by the Cython
// AvroScanPlan (opteryx/operators/_operators.pyx) and BORROWED by
// NativeAvroScanSource for the driver's lifetime.
struct AvroScanSpec {
    std::vector<std::string> files;          // as resolved at bind time; named in messages
    std::vector<std::string> urls;           // parallel: "" = local path, else the URL to
                                             // GET with NO credentials
    std::string reader_schema;               // every file is read as this schema

    // Per DECODED column: each top-level field name once.
    std::vector<std::string> decode_names;
    // Per EMITTED column (parallel vectors, emit order): one decoded column can feed
    // several identities.
    std::vector<std::string> out_identities;
    std::vector<uint32_t>    emit_index;     // index into decode_names

    bool zero_columns = false;  // COUNT(*)-shaped scan: zero-column morsels, row count only
    int  decode_workers = 1;    // width of this Source's own decode pool

    // Written by the Source when its global state is torn down; read from Python after
    // the driver finishes. -1 = never ran.
    mutable int64_t rows_read  = -1;
    mutable int64_t bytes_read = -1;   // file bytes loaded
    mutable int64_t files_read = -1;
    mutable int64_t batches_read = -1;
};

namespace avro_detail {

// One loaded file: a mapping (local) or a fetched body (remote).
struct Loaded {
    uint8_t* ptr = nullptr;
    size_t   len = 0;
    bool     mapped = false;
    std::vector<uint8_t> body;
    ~Loaded() {
        if (mapped && ptr != nullptr) unmap_memory_c(ptr, len);
    }
};

// An opened file: its bytes and the compiled, framed AvroFile over them. Shared by
// every batch claimed from it; released when the last of them is decoded. `bytes` is
// declared first so it outlives `avro` (members are destroyed in reverse order).
struct OpenFile {
    Loaded bytes;
    std::unique_ptr<rugo::avro::AvroFile> avro;
    size_t index = 0;
};

enum class FileStatus : uint8_t { PENDING, OPENING, READY, DONE };

struct FileState {
    FileStatus status = FileStatus::PENDING;
    std::shared_ptr<OpenFile> open;
    size_t next_batch = 0;
};

struct Unit {
    std::shared_ptr<OpenFile> file;
    size_t batch = 0;
};

inline const LogicalType* logical_for(DrakenType type, uint8_t precision, uint8_t scale) {
    LogicalType lt;
    if (type == DRAKEN_TIMESTAMP64) {
        lt.kind = LogicalKind::TIMESTAMP;
        lt.unit = TimestampUnit::MICROSECONDS;
    } else if (type == DRAKEN_TIME64) {
        lt.kind = LogicalKind::TIME;
        lt.unit = TimestampUnit::MICROSECONDS;
    } else if (type == DRAKEN_DECIMAL || type == DRAKEN_DECIMAL128) {
        lt.kind = LogicalKind::DECIMAL;
        lt.precision = precision;
        lt.scale = scale;
    } else {
        return nullptr;
    }
    return logical_type_intern(lt);
}

// AvroColumn -> CxxColumn, taking ownership of every buffer it carries. Mirrors
// rugo's wrap_avro_column (_avro_column_wrap.cpp) branch for branch, minus the
// Python box.
inline void build_column(rugo::avro::AvroColumn& c, CxxColumn& out) {
    using rugo::avro::OutKind;
    switch (c.kind) {
        case OutKind::String:
            emit_dense_string_column(c.slots, c.length, c.arena, c.arena_len, c.validity, c.type, out);
            break;
        case OutKind::Dict:
            emit_dict_string_column(c.slots, c.dict_len, c.arena, c.arena_len, c.codes, c.length,
                                    c.validity, c.type, out);
            break;
        case OutKind::ConstString:
            emit_dict_string_column(c.slots, 1, c.arena, c.arena_len, c.codes, c.length,
                                    c.validity, c.type, out);
            break;
        case OutKind::ConstRaw: {
            DrakenVector v = draken_vector_from_dict(c.data, 1, c.codes, c.length, c.type, c.validity);
            out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(c.data),
                                                    OwnedBuffer<uint8_t>(c.validity),
                                                    OwnedBuffer<void>(static_cast<void*>(c.codes)));
            out.own->logical_type = logical_for(c.type, c.precision, c.scale);
            out.view = out.own->vec;
            break;
        }
        case OutKind::Array: {
            std::unique_ptr<VectorOwner> child;
            if (c.child_type == DRAKEN_VARCHAR || c.child_type == DRAKEN_VARBINARY) {
                DrakenStringArena* sa = nullptr;
                uint8_t* block = string_arena_block(c.slots, c.child_length, c.arena, c.arena_len,
                                                    c.child_type, &sa);
                draken_free(c.slots);
                sa->null_bitmap = c.child_validity;
                DrakenVector cv = draken_vector_from_dense(sa, c.child_length, c.child_type, c.child_validity);
                child = std::make_unique<VectorOwner>(cv, OwnedBuffer<void>(block),
                                                      OwnedBuffer<uint8_t>(c.child_validity),
                                                      OwnedBuffer<void>(nullptr),
                                                      OwnedBuffer<uint8_t>(c.arena));
            } else {
                DrakenVector cv = draken_vector_from_dense(c.data, c.child_length, c.child_type, c.child_validity);
                child = std::make_unique<VectorOwner>(cv, OwnedBuffer<void>(c.data),
                                                      OwnedBuffer<uint8_t>(c.child_validity));
            }
            DrakenVector pv = draken_vector_from_dense(c.offsets, c.length, DRAKEN_ARRAY, c.validity);
            out.own = std::make_shared<VectorOwner>(pv, OwnedBuffer<void>(c.offsets),
                                                    OwnedBuffer<uint8_t>(c.validity));
            out.own->child_owner = std::move(child);
            out.view = out.own->vec;
            break;
        }
        default: {
            // Raw, Decimal64/128, Timestamp, Time64: fixed-width data.
            DrakenVector v = draken_vector_from_dense(c.data, c.length, c.type, c.validity);
            out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(c.data),
                                                    OwnedBuffer<uint8_t>(c.validity));
            out.own->logical_type = logical_for(c.type, c.precision, c.scale);
            out.view = out.own->vec;
            break;
        }
    }
    c.disown();
}

}  // namespace avro_detail

struct NativeAvroScanGlobal : GlobalSourceState {
    const AvroScanSpec* spec = nullptr;

    // Ready queue + flow control (all under `mtx`).
    std::mutex mtx;
    std::condition_variable cv_ready;   // engine workers wait for a morsel / the end
    std::condition_variable cv_room;    // decode workers wait for in-flight room
    std::deque<MorselPtr> ready;
    size_t in_flight = 0;               // decoded morsels not yet handed to the engine
    size_t window = 1;
    int live_workers = 0;
    bool stop = false;
    bool failed = false;
    std::string err;                    // ErrCtx::msg points here

    // Batch cursor (under `claim_mtx`): per-file open state. `first_live` skips files
    // already DONE; `next_pending` is the next file nobody has started opening.
    std::mutex claim_mtx;
    std::condition_variable cv_opened;  // a file finished opening, or the scan ended
    std::vector<avro_detail::FileState> states;
    size_t first_live = 0;
    size_t next_pending = 0;
    int opening = 0;
    std::atomic<bool> abort{false};     // stop / failure: claimers stop waiting

    std::unique_ptr<HttpClient> http;   // created only when some file is remote

    std::atomic<int64_t> rows{0};
    std::atomic<int64_t> bytes{0};
    std::atomic<int64_t> files{0};
    std::atomic<int64_t> batches{0};

    std::vector<std::thread> workers;

    ~NativeAvroScanGlobal() override {
        {
            std::lock_guard<std::mutex> lock(mtx);
            stop = true;
        }
        abort.store(true);
        {
            std::lock_guard<std::mutex> lock(claim_mtx);
        }
        cv_room.notify_all();
        cv_opened.notify_all();
        for (auto& t : workers) t.join();
        spec->rows_read = rows.load();
        spec->bytes_read = bytes.load();
        spec->files_read = files.load();
        spec->batches_read = batches.load();
    }
};

class NativeAvroScanSource : public Source {
public:
    explicit NativeAvroScanSource(const AvroScanSpec* spec) : spec_(spec) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        auto g = std::make_unique<NativeAvroScanGlobal>();
        g->spec = spec_;
        const int n = spec_->decode_workers > 0 ? spec_->decode_workers : 1;
        g->window = static_cast<size_t>(n) + 2;
        g->live_workers = n;
        g->states.resize(spec_->files.size());
        for (const auto& url : spec_->urls) {
            if (!url.empty()) {
                g->http = std::make_unique<HttpClient>();
                break;
            }
        }
        NativeAvroScanGlobal* gp = g.get();
        g->workers.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i)
            g->workers.emplace_back([this, gp]() { decode_worker(*gp); });
        return g;
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativeAvroScanGlobal&>(gs);
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
        return SourceResult::HAVE_MORE;
    }

private:
    // First failure wins; every waiter is released.
    static void fail(NativeAvroScanGlobal& g, std::string message) {
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
        g.cv_opened.notify_all();
    }

    // Load file `i`: map it, or GET it whole with no credentials. Empty on success,
    // else the query-ending message.
    std::string load(NativeAvroScanGlobal& g, size_t i, avro_detail::Loaded& out) {
        const std::string& path = spec_->files[i];
        if (spec_->urls[i].empty()) {
            const int rc = read_all_mmap(path.c_str(), &out.ptr, &out.len);
            if (rc != 0)
                return "READ_AVRO('" + path + "'): the file could not be read: " + std::strerror(-rc);
            out.mapped = out.ptr != nullptr;
            return std::string();
        }
        try {
            out.body = g.http->get(spec_->urls[i]);
        } catch (const std::exception& e) {
            return "READ_AVRO('" + path + "'): the file could not be read: " + e.what();
        }
        out.ptr = out.body.data();
        out.len = out.body.size();
        return std::string();
    }

    // A decode thread's body: anything escaping decode_loop goes through fail(), the
    // scan's one error channel, and the worker retires as on a normal exit.
    void decode_worker(NativeAvroScanGlobal& g) {
        try {
            decode_loop(g);
            return;
        } catch (const std::exception& e) {
            fail(g, std::string("READ_AVRO: unhandled C++ exception in a decode worker: ") + e.what());
        } catch (...) {
            fail(g, "READ_AVRO: unhandled non-standard C++ exception in a decode worker");
        }
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.live_workers -= 1;
        }
        g.cv_ready.notify_all();
    }

    // Wait for in-flight room; false when the scan is stopping or has failed.
    static bool reserve_slot(NativeAvroScanGlobal& g) {
        std::unique_lock<std::mutex> lock(g.mtx);
        g.cv_room.wait(lock, [&]() { return g.stop || g.failed || g.in_flight < g.window; });
        if (g.stop || g.failed) return false;
        g.in_flight += 1;
        return true;
    }

    static void release_slot(NativeAvroScanGlobal& g) {
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.in_flight -= 1;
        }
        g.cv_room.notify_one();
    }

    // Open file `i`: load it, compile it against the reader schema and frame it into
    // batches. Called WITHOUT the cursor lock. Empty on success, else the query-ending
    // message naming the file.
    std::string open_file(NativeAvroScanGlobal& g, size_t i, std::shared_ptr<avro_detail::OpenFile>& out) {
        auto f = std::make_shared<avro_detail::OpenFile>();
        f->index = i;
        std::string error = load(g, i, f->bytes);
        if (!error.empty()) return error;
        try {
            f->avro = std::make_unique<rugo::avro::AvroFile>(f->bytes.ptr, f->bytes.len, spec_->decode_names,
                                                             false, spec_->reader_schema);
            f->avro->batches();
        } catch (const std::exception& e) {
            return "READ_AVRO('" + spec_->files[i] + "'): " + e.what();
        }
        g.bytes.fetch_add(static_cast<int64_t>(f->bytes.len), std::memory_order_relaxed);
        g.files.fetch_add(1, std::memory_order_relaxed);
        out = std::move(f);
        return std::string();
    }

    // Claim the next batch. Prefers batches of files already open; only when none
    // remain does it open the next file (outside the lock), and when every file is
    // open-or-opening it waits for an open to land. False at the end, or with `error`
    // set when a file cannot be opened.
    bool claim(NativeAvroScanGlobal& g, avro_detail::Unit& unit, std::string& error) {
        using avro_detail::FileStatus;
        std::unique_lock<std::mutex> lock(g.claim_mtx);
        const size_t n = g.states.size();
        for (;;) {
            if (g.abort.load()) return false;
            for (size_t i = g.first_live; i < n; ++i) {
                avro_detail::FileState& f = g.states[i];
                if (f.status != FileStatus::READY) continue;
                if (f.next_batch >= f.open->avro->batches().size()) {
                    f.status = FileStatus::DONE;
                    f.open.reset();   // the file is released once its claimed batches finish
                    continue;
                }
                unit.file = f.open;
                unit.batch = f.next_batch++;
                return true;
            }
            while (g.first_live < n && g.states[g.first_live].status == FileStatus::DONE)
                g.first_live += 1;
            if (g.next_pending < n) {
                const size_t i = g.next_pending++;
                g.states[i].status = FileStatus::OPENING;
                g.opening += 1;
                lock.unlock();
                std::shared_ptr<avro_detail::OpenFile> opened;
                error = open_file(g, i, opened);
                lock.lock();
                g.opening -= 1;
                if (!error.empty()) return false;
                g.states[i].open = std::move(opened);
                g.states[i].status = FileStatus::READY;
                g.cv_opened.notify_all();
                continue;
            }
            if (g.opening == 0) return false;
            g.cv_opened.wait(lock, [&]() { return g.abort.load() || g.opening == 0 || any_ready(g); });
        }
    }

    static bool any_ready(const NativeAvroScanGlobal& g) {
        for (size_t i = g.first_live; i < g.states.size(); ++i)
            if (g.states[i].status == avro_detail::FileStatus::READY) return true;
        return false;
    }

    void decode_loop(NativeAvroScanGlobal& g) {
        // The Worker for the file this thread last decoded from: reused while the thread
        // keeps claiming batches of the same file.
        std::shared_ptr<avro_detail::OpenFile> current;
        std::unique_ptr<rugo::avro::AvroFile::Worker> worker;
        for (;;) {
            if (!reserve_slot(g)) break;
            avro_detail::Unit unit;
            std::string error;
            if (!claim(g, unit, error)) {
                release_slot(g);
                if (!error.empty()) fail(g, std::move(error));
                break;
            }
            MorselPtr morsel;
            try {
                if (unit.file != current) {
                    worker.reset();
                    current = unit.file;
                    worker = std::make_unique<rugo::avro::AvroFile::Worker>(*current->avro);
                }
                morsel = decode(g, *current, *worker, unit.batch);
            } catch (const std::exception& e) {
                error = "READ_AVRO('" + spec_->files[unit.file->index] + "'): " + e.what();
            }
            unit.file.reset();
            if (!error.empty()) {
                release_slot(g);
                fail(g, std::move(error));
                break;
            }
            {
                std::lock_guard<std::mutex> lock(g.mtx);
                g.ready.push_back(std::move(morsel));   // the slot now belongs to the morsel
            }
            g.cv_ready.notify_one();
        }
        worker.reset();
        current.reset();
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.live_workers -= 1;
        }
        g.cv_ready.notify_all();
    }

    // Decode one batch into a morsel.
    MorselPtr decode(NativeAvroScanGlobal& g, const avro_detail::OpenFile& file,
                     rugo::avro::AvroFile::Worker& worker, size_t batch_index) {
        const rugo::avro::AvroBatchRange& range = file.avro->batches()[batch_index];
        rugo::avro::AvroBatch batch = worker.decode(range);
        auto morsel = std::make_shared<CxxMorsel>();
        if (spec_->zero_columns) {
            morsel->zero_col_rows = batch.rows;
        } else {
            std::vector<CxxColumn> decoded(batch.columns.size());
            for (size_t c = 0; c < batch.columns.size(); ++c)
                avro_detail::build_column(batch.columns[c], decoded[c]);
            morsel->columns.reserve(spec_->emit_index.size());
            for (const uint32_t d : spec_->emit_index) morsel->columns.push_back(decoded[d]);
            morsel->names = spec_->out_identities;
        }
        g.rows.fetch_add(batch.rows, std::memory_order_relaxed);
        g.batches.fetch_add(1, std::memory_order_relaxed);
        return morsel;
    }

    const AvroScanSpec* spec_;
};

}  // namespace opteryx::engine
