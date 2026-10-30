#pragma once
// src/cpp/engine/native_orc_scan_source.hpp — a native (zero-Python) ORC scan Source:
// an Iceberg table's ORC data files and a folder of .orc files.
// docs/ORC_READER_DESIGN.md.
//
// Sibling of native_avro_scan_source.hpp, and built the same way: planning (Python)
// fixes the file list, the reader columns every file is read as, and which of them to
// decode; this Source borrows that plan in an OrcScanSpec and runs it; nothing here
// touches Python. It reuses the Avro Source's file mapping and its column carrier ->
// CxxColumn conversion (rugo's ORC reader produces the same AvroColumn carrier).
//
// Decode: the Source owns its decode pool. The unit of work is a STRIPE (an ORC
// stripe is the unit its streams are independent in; design doc §6.3). A file is OPENED
// once, by the first worker that needs it and outside the cursor lock: memory-mapped
// (local files only) and bound to the reader columns (rugo::orc::OrcFile: tail, footer,
// column matching). Every worker then claims that file's stripes and decodes each a
// range of at most kOrcMorselRows rows at a time (rugo::orc::StripeCursor); each range
// is one morsel on a ready queue the engine's workers drain, so decode overlaps
// execution. At most `decode_workers + 2` morsels are decoded-but-unconsumed, which
// bounds resident memory; a file is released when its last stripe is done.
//
// Order: morsel order is NOT guaranteed, within a file or across files.
//
// A zero-column projection (COUNT(*)) decodes nothing: each stripe's row count comes
// from the footer and the Source emits zero-column morsels carrying `zero_col_rows`.
//
// Errors fail fast through ErrCtx with kErrCodeReadError (-> DatasetReadError), naming
// the file: a corrupt file, a refused construct, a file whose columns do not match the
// reader's.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "native_avro_scan_source.hpp"  // avro_detail::Loaded / build_column (the shared carrier)
#include "native_scan_prefilter.hpp"     // add_prefilter_row_column / run_prefilter / prefilter_survivors
#include "topn_keep.hpp"                 // topn_keep_mask — the top-N cut
#include "morsels/cxx_morsel_c.h"        // cxx_take_c / cxx_morsel_delete

// rugo's ORC reader (rugo/src/orc) — pure C++.
#include "orc/orc_reader.hpp"

namespace opteryx::engine {

// The most rows one morsel of an ORC scan carries (design doc §6.3).
constexpr uint32_t kOrcMorselRows = 65536;

// The plan-time description of one native ORC scan. Built by the Cython OrcScanPlan
// (opteryx/operators/_operators.pyx) and BORROWED by NativeOrcScanSource for the
// driver's lifetime.
struct OrcScanSpec {
    std::vector<std::string> files;          // as resolved at bind time; named in messages
    std::vector<std::string> urls;           // parallel: "" = local path, else the URL to GET
    bool deny_private_addresses = false;     // refuse non-public HTTP peers (READ_* of an
                                             // http(s) URL with ALLOW_LOCAL_READS off)
    // The `Authorization` value every remote GET carries; empty when the URLs are signed.
    // Resolved at compile time by opteryx.connectors.io_systems.native_locations.
    std::string auth_header;

    // The reader schema every file is read as: parallel vectors, one entry per column.
    std::vector<std::string> reader_names;
    std::vector<int64_t>     reader_field_ids;   // Iceberg field id, or -1
    std::vector<int32_t>     reader_types;       // DrakenType ordinals
    std::vector<uint32_t>    reader_precision;
    std::vector<uint32_t>    reader_scale;
    std::vector<int32_t>     reader_child_types;  // an ARRAY's innermost element DrakenType ordinal; else unused
    std::vector<uint32_t>    reader_array_depths; // an ARRAY's levels of list nesting; else unused
    // A nested column of a table: the shape the table declares, verified against each file; else empty.
    std::vector<std::vector<rugo::DeclaredNode>> reader_declared;
    std::vector<uint8_t>     reader_nullable;

    // Per DECODED column: the reader column's name, once.
    std::vector<std::string> decode_names;
    // Per EMITTED column (parallel vectors, emit order): one decoded column can feed
    // several identities.
    std::vector<std::string> out_identities;
    std::vector<uint32_t>    emit_index;     // index into decode_names

    // The WHERE's conjuncts the statistics can test (design doc §6.7), parallel vectors: a
    // decoded column (index into decode_names), the rugo::orc::PruneOp, its values. Set only
    // with the prefilter.
    std::vector<uint32_t> prune_columns;
    std::vector<uint8_t> prune_ops;
    std::vector<std::vector<int64_t>> prune_values;

    // Top-N mode (docs/ORC_TOPN_LATE_MATERIALISATION_DESIGN.md): the HeapSort directly
    // above the prefiltered scan keeps `topn_limit` rows ordered by these keys, each an
    // EMITTED column (index into out_identities) decoded in pass 1. 0 = off; only with
    // the prefilter.
    int64_t topn_limit = 0;
    std::vector<uint32_t> topn_key_emit;
    std::vector<uint8_t>  topn_ascending;
    std::vector<uint8_t>  topn_nulls_first;

    // Fill the chunk cache on a miss (docs/ORC_CHUNK_CACHE_DESIGN.md); hits are used either
    // way. False under `SET chunk_cache_admit = false`.
    bool chunk_cache_admit = true;

    bool zero_columns = false;   // COUNT(*)-shaped scan: zero-column morsels, row count only
    // Match columns to the file's by `iceberg.id` (an Iceberg table) rather than by name.
    bool match_by_id = false;
    int  decode_workers = 1;     // width of this Source's own decode pool

    // Written by the Source when its global state is torn down; read from Python after
    // the driver finishes. -1 = never ran.
    mutable int64_t rows_read  = -1;
    mutable int64_t bytes_read = -1;   // file bytes mapped
    mutable int64_t files_read = -1;
    mutable int64_t stripes_read = -1;
    mutable int64_t pass2_rows = -1;   // prefilter only: rows its second pass decoded
    mutable int64_t stripes_pruned = -1;      // prune terms only: stripes their statistics skipped
    mutable int64_t row_groups_pruned = -1;   // ... and row groups, in the stripes read
    mutable int64_t topn_kept_rows = -1;      // top-N only: rows the cut kept for pass 2
};

namespace orc_detail {

// An opened file: its bytes and the bound OrcFile over them. Shared by every stripe
// claimed from it; released when the last of them is decoded. `bytes` is declared first
// so it outlives `orc` (members are destroyed in reverse order).
struct OpenFile {
    avro_detail::Loaded bytes;
    std::unique_ptr<rugo::orc::OrcFile> orc;
    size_t index = 0;
};

enum class FileStatus : uint8_t { PENDING, OPENING, READY, DONE };

struct FileState {
    FileStatus status = FileStatus::PENDING;
    std::shared_ptr<OpenFile> open;
    size_t next_stripe = 0;
};

struct Unit {
    std::shared_ptr<OpenFile> file;
    size_t stripe = 0;
};

// Top-N mode: one range's pass-1 survivors — the emitted pass-1 columns plus the row
// number in the range (kPrefilterRowColumn) — and where the range is.
struct TopNCandidates {
    size_t file = 0;
    size_t stripe = 0;
    uint32_t first = 0;   // the range's first row in the stripe
    uint32_t n = 0;       // its rows
    MorselPtr kept;
};

// A decode worker's candidates, cut back to its own best n (plus ties) as they grow.
struct TopNBuffer {
    std::vector<TopNCandidates> candidates;
    size_t rows = 0;
};

// Pass-2 work after the cut: one range's kept rows.
struct TopNItem {
    TopNCandidates where;              // `kept` holds exactly the kept rows
    std::vector<uint32_t> stripe_rows; // each kept row's row in the stripe, in `kept` order
};

}  // namespace orc_detail

struct NativeOrcScanGlobal : GlobalSourceState {
    const OrcScanSpec* spec = nullptr;

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
    int err_code = kErrCodeReadError;   // a read error unless the prefilter's predicate failed

    // Stripe cursor (under `claim_mtx`): per-file open state.
    std::mutex claim_mtx;
    std::condition_variable cv_opened;  // a file finished opening, or the scan ended
    std::vector<orc_detail::FileState> states;
    size_t first_live = 0;
    size_t next_pending = 0;
    int opening = 0;
    std::atomic<bool> abort{false};     // stop / failure: claimers stop waiting

    std::unique_ptr<HttpClient> http;   // created only when some file is remote

    std::atomic<int64_t> rows{0};
    std::atomic<int64_t> bytes{0};
    std::atomic<int64_t> files{0};
    std::atomic<int64_t> stripes{0};
    std::atomic<int64_t> pass2_rows{0};
    std::atomic<int64_t> stripes_pruned{0};
    std::atomic<int64_t> row_groups_pruned{0};
    bool prefiltered = false;
    bool pruned = false;

    // Top-N mode (under `mtx`): the pass-1 barrier, then the pass-2 work list.
    std::vector<orc_detail::TopNBuffer> topn_buffers;   // one per decode worker
    int topn_pending = 0;               // workers not yet at the barrier
    bool topn_ready = false;            // the cut is made; `topn_items` is final
    std::condition_variable cv_topn;
    std::vector<orc_detail::TopNItem> topn_items;
    std::atomic<size_t> topn_next{0};
    std::atomic<int64_t> topn_kept{0};
    bool topn = false;

    std::vector<std::thread> workers;

    ~NativeOrcScanGlobal() override {
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
        cv_topn.notify_all();
        for (auto& t : workers) t.join();
        spec->rows_read = rows.load();
        spec->bytes_read = bytes.load();
        spec->files_read = files.load();
        spec->stripes_read = stripes.load();
        if (prefiltered) spec->pass2_rows = pass2_rows.load();
        if (pruned) {
            spec->stripes_pruned = stripes_pruned.load();
            spec->row_groups_pruned = row_groups_pruned.load();
        }
        if (topn) spec->topn_kept_rows = topn_kept.load();
    }
};

class NativeOrcScanSource : public Source {
public:
    explicit NativeOrcScanSource(const OrcScanSpec* spec) : spec_(spec) {}

    // Arm the prefilter (design doc §6.6): the WHERE directly above the scan runs here, and
    // the Filter is not compiled. `pass1` flags, per decode_names entry, the columns the
    // predicate reads. `pass1_prog` is the predicate resolved against the pass-1 morsel
    // (every emitted column whose decoded column is pass 1, emit order, then the row number
    // kPrefilterRowColumn, INT32). The scan always runs two-pass, so the single-pass program
    // the Avro scan also takes is not used here.
    void set_prefilter(ExprProgram pass1_prog, ExprProgram /*full_prog*/, ExprFilterFn fn,
                       std::vector<uint8_t> pass1) {
        if (pass1.size() != spec_->decode_names.size())
            throw std::runtime_error("set_prefilter: one pass-1 flag per decoded column");
        pass1_prog_ = std::move(pass1_prog);
        prefilter_fn_ = fn;
        pass1_cols_.clear();
        pass2_cols_.clear();
        decode_pos_.assign(pass1.size(), 0);
        for (uint32_t d = 0; d < pass1.size(); ++d) {
            std::vector<uint32_t>& into = pass1[d] ? pass1_cols_ : pass2_cols_;
            decode_pos_[d] = static_cast<uint32_t>(into.size());
            into.push_back(d);
        }
        if (pass1_cols_.empty()) throw std::runtime_error("set_prefilter: the predicate reads no column");
        pass1_ = std::move(pass1);
        pass1_emit_.clear();
        for (size_t k = 0; k < spec_->emit_index.size(); ++k)
            if (pass1_[spec_->emit_index[k]]) pass1_emit_.push_back(static_cast<uint32_t>(k));
    }
    static constexpr const char* kPrefilterRowColumn = "$orc_row";

    std::unique_ptr<GlobalSourceState> make_global() override {
        auto g = std::make_unique<NativeOrcScanGlobal>();
        g->spec = spec_;
        g->prefiltered = prefilter_fn_ != nullptr;
        prune_terms_.clear();
        const size_t terms = spec_->prune_columns.size();
        if (spec_->prune_ops.size() != terms || spec_->prune_values.size() != terms)
            throw std::runtime_error("NativeOrcScanSource: prune terms are not parallel");
        if (terms != 0 && prefilter_fn_ == nullptr)
            throw std::runtime_error("NativeOrcScanSource: prune terms without the prefilter they come from");
        for (size_t t = 0; t < terms; ++t) {
            if (spec_->prune_columns[t] >= spec_->decode_names.size() || spec_->prune_ops[t] > 6 ||
                spec_->prune_values[t].empty())
                throw std::runtime_error("NativeOrcScanSource: a prune term is malformed");
            rugo::orc::PruneTerm term;
            term.column = spec_->prune_columns[t];
            term.op = static_cast<rugo::orc::PruneOp>(spec_->prune_ops[t]);
            term.values = spec_->prune_values[t];
            prune_terms_.push_back(std::move(term));
        }
        g->pruned = !prune_terms_.empty();
        arm_topn();
        g->topn = topn_limit_ != 0;
        const int n = spec_->decode_workers > 0 ? spec_->decode_workers : 1;
        if (g->topn) {
            g->topn_buffers.resize(static_cast<size_t>(n));
            g->topn_pending = n;
        }
        g->window = static_cast<size_t>(n) + 2;
        g->live_workers = n;
        g->states.resize(spec_->files.size());
        for (const auto& url : spec_->urls) {
            if (!url.empty()) {
                g->http = std::make_unique<HttpClient>(128, 60000, spec_->deny_private_addresses);
                break;
            }
        }
        NativeOrcScanGlobal* gp = g.get();
        g->workers.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i)
            g->workers.emplace_back([this, gp, i]() { decode_worker(*gp, static_cast<size_t>(i)); });
        return g;
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativeOrcScanGlobal&>(gs);
        std::unique_lock<std::mutex> lock(g.mtx);
        g.cv_ready.wait(lock, [&]() {
            return g.failed || !g.ready.empty() || g.live_workers == 0;
        });
        if (g.failed) {
            err.code = g.err_code;
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
    // First failure wins; every waiter is released. `code` is the ErrCtx code: a read
    // error unless the prefilter's predicate failed (decode_filtered).
    static void fail(NativeOrcScanGlobal& g, std::string message, int code = kErrCodeReadError) {
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            if (!g.failed) {
                g.failed = true;
                g.err = std::move(message);
                g.err_code = code;
            }
        }
        g.abort.store(true);
        {
            std::lock_guard<std::mutex> lock(g.claim_mtx);
        }
        g.cv_ready.notify_all();
        g.cv_room.notify_all();
        g.cv_opened.notify_all();
        g.cv_topn.notify_all();
    }

    // A decode thread's body: anything escaping decode_loop goes through fail(), the
    // scan's one error channel, and the worker retires as on a normal exit.
    void decode_worker(NativeOrcScanGlobal& g, size_t worker) {
        try {
            decode_loop(g, worker);
            return;
        } catch (const std::exception& e) {
            fail(g, std::string("ORC: unhandled C++ exception in a decode worker: ") + e.what());
        } catch (...) {
            fail(g, "ORC: unhandled non-standard C++ exception in a decode worker");
        }
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.live_workers -= 1;
        }
        g.cv_ready.notify_all();
    }

    // Wait for in-flight room; false when the scan is stopping or has failed.
    static bool reserve_slot(NativeOrcScanGlobal& g) {
        std::unique_lock<std::mutex> lock(g.mtx);
        g.cv_room.wait(lock, [&]() { return g.stop || g.failed || g.in_flight < g.window; });
        if (g.stop || g.failed) return false;
        g.in_flight += 1;
        return true;
    }

    static void release_slot(NativeOrcScanGlobal& g) {
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.in_flight -= 1;
        }
        g.cv_room.notify_one();
    }

    // The reader columns, as rugo's reader takes them.
    std::vector<rugo::orc::ReaderColumn> reader_columns() const {
        std::vector<rugo::orc::ReaderColumn> out(spec_->reader_names.size());
        for (size_t i = 0; i < out.size(); ++i) {
            out[i].name = spec_->reader_names[i];
            out[i].field_id = spec_->reader_field_ids[i];
            out[i].type = static_cast<DrakenType>(spec_->reader_types[i]);
            out[i].precision = static_cast<uint8_t>(spec_->reader_precision[i]);
            out[i].scale = static_cast<uint8_t>(spec_->reader_scale[i]);
            out[i].child_type = static_cast<DrakenType>(spec_->reader_child_types[i]);
            out[i].array_depth = static_cast<uint8_t>(spec_->reader_array_depths[i]);
            out[i].declared = spec_->reader_declared[i];
            out[i].nullable = spec_->reader_nullable[i] != 0;
        }
        return out;
    }

    // Open file `i`: map it and bind it to the reader columns. Called WITHOUT the cursor
    // lock. Empty on success, else the query-ending message naming the file.
    std::string open_file(NativeOrcScanGlobal& g, size_t i, std::shared_ptr<orc_detail::OpenFile>& out) {
        auto f = std::make_shared<orc_detail::OpenFile>();
        f->index = i;
        const std::string& path = spec_->files[i];
        if (spec_->urls[i].empty()) {
            const int rc = read_all_mmap(path.c_str(), &f->bytes.ptr, &f->bytes.len);
            if (rc != 0) return "ORC('" + path + "'): the file could not be read: " + std::strerror(-rc);
            f->bytes.mapped = f->bytes.ptr != nullptr;
        } else {
            try {
                f->bytes.body = avro_detail::fetch_whole(*g.http, spec_->urls[i], spec_->auth_header);
            } catch (const std::exception& e) {
                return "ORC('" + path + "'): the file could not be read: " + e.what();
            }
            f->bytes.ptr = f->bytes.body.data();
            f->bytes.len = f->bytes.body.size();
        }
        try {
            f->orc = std::make_unique<rugo::orc::OrcFile>(f->bytes.ptr, f->bytes.len);
            f->orc->bind(reader_columns(), spec_->decode_names, spec_->match_by_id);
            f->orc->use_chunk_cache(path, spec_->chunk_cache_admit);
            if (!prune_terms_.empty()) f->orc->load_stripe_statistics();
        } catch (const std::exception& e) {
            return "ORC('" + path + "'): " + e.what();
        }
        g.bytes.fetch_add(static_cast<int64_t>(f->bytes.len), std::memory_order_relaxed);
        g.files.fetch_add(1, std::memory_order_relaxed);
        out = std::move(f);
        return std::string();
    }

    // Claim the next stripe. Prefers stripes of files already open; only when none
    // remain does it open the next file (outside the lock), and when every file is
    // open-or-opening it waits for an open to land. False at the end, or with `error`
    // set when a file cannot be opened.
    bool claim(NativeOrcScanGlobal& g, orc_detail::Unit& unit, std::string& error) {
        using orc_detail::FileStatus;
        std::unique_lock<std::mutex> lock(g.claim_mtx);
        const size_t n = g.states.size();
        for (;;) {
            if (g.abort.load()) return false;
            for (size_t i = g.first_live; i < n; ++i) {
                orc_detail::FileState& f = g.states[i];
                if (f.status != FileStatus::READY) continue;
                if (f.next_stripe >= f.open->orc->stripe_count()) {
                    f.status = FileStatus::DONE;
                    f.open.reset();   // the file is released once its claimed stripes finish
                    continue;
                }
                unit.file = f.open;
                unit.stripe = f.next_stripe++;
                return true;
            }
            while (g.first_live < n && g.states[g.first_live].status == FileStatus::DONE)
                g.first_live += 1;
            if (g.next_pending < n) {
                const size_t i = g.next_pending++;
                g.states[i].status = FileStatus::OPENING;
                g.opening += 1;
                lock.unlock();
                std::shared_ptr<orc_detail::OpenFile> opened;
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

    static bool any_ready(const NativeOrcScanGlobal& g) {
        for (size_t i = g.first_live; i < g.states.size(); ++i)
            if (g.states[i].status == orc_detail::FileStatus::READY) return true;
        return false;
    }

    void decode_loop(NativeOrcScanGlobal& g, size_t worker) {
        orc_detail::TopNBuffer* topn = g.topn ? &g.topn_buffers[worker] : nullptr;
        for (;;) {
            if (!reserve_slot(g)) break;
            orc_detail::Unit unit;
            std::string error;
            if (!claim(g, unit, error)) {
                release_slot(g);
                if (!error.empty()) fail(g, std::move(error));
                break;
            }
            // The slot reserved above carries the stripe's first morsel; each later one
            // reserves its own. False when the scan stopped or failed meanwhile.
            bool have_slot = true;
            bool stopped = false;
            int code = kErrCodeReadError;
            try {
                stopped = !(prefilter_fn_ != nullptr
                                ? decode_filtered(g, *unit.file, unit.stripe, have_slot, error, code, topn)
                                : decode(g, *unit.file, unit.stripe, have_slot));
            } catch (const std::exception& e) {
                error = "ORC('" + spec_->files[unit.file->index] + "'): " + e.what();
                code = kErrCodeReadError;
            }
            unit.file.reset();
            if (have_slot) release_slot(g);   // an empty stripe, or a failure
            if (!error.empty()) {
                fail(g, std::move(error), code);
                break;
            }
            if (stopped) break;
        }
        // Top-N: every worker reaches the barrier, whatever ended its pass 1, so none
        // waits for one that left; then the pass-2 work is shared out.
        if (topn != nullptr && topn_barrier(g)) topn_pass2(g);
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.live_workers -= 1;
        }
        g.cv_ready.notify_all();
    }

    void push(NativeOrcScanGlobal& g, MorselPtr morsel) {
        {
            std::lock_guard<std::mutex> lock(g.mtx);
            g.ready.push_back(std::move(morsel));   // the slot now belongs to the morsel
        }
        g.cv_ready.notify_one();
    }

    // Decode one stripe into morsels of at most kOrcMorselRows rows, each pushed as it is
    // decoded. `have_slot` says whether the caller holds a reserved slot; false when the
    // scan stopped or failed before the stripe was done.
    bool decode(NativeOrcScanGlobal& g, const orc_detail::OpenFile& file, size_t stripe, bool& have_slot) {
        const uint32_t stripe_rows = file.orc->stripe_rows(stripe);
        if (stripe_rows == 0) return true;   // an empty stripe has nothing to emit
        g.stripes.fetch_add(1, std::memory_order_relaxed);
        if (spec_->zero_columns) {
            auto morsel = std::make_shared<CxxMorsel>();
            morsel->zero_col_rows = stripe_rows;
            g.rows.fetch_add(stripe_rows, std::memory_order_relaxed);
            push(g, std::move(morsel));
            have_slot = false;
            return true;
        }
        rugo::orc::StripeCursor cursor(*file.orc, stripe);
        while (cursor.remaining() != 0) {
            if (!have_slot) {
                if (!reserve_slot(g)) return false;
                have_slot = true;
            }
            const uint32_t n = std::min(cursor.remaining(), kOrcMorselRows);
            rugo::avro::AvroBatch batch = cursor.next(n);
            std::vector<CxxColumn> decoded(batch.columns.size());
            for (size_t c = 0; c < batch.columns.size(); ++c)
                avro_detail::build_column(batch.columns[c], decoded[c]);
            auto morsel = std::make_shared<CxxMorsel>();
            morsel->columns.reserve(spec_->emit_index.size());
            for (const uint32_t d : spec_->emit_index) morsel->columns.push_back(decoded[d]);
            morsel->names = spec_->out_identities;
            g.rows.fetch_add(n, std::memory_order_relaxed);
            push(g, std::move(morsel));
            have_slot = false;
        }
        return true;
    }

    // A batch's columns as the engine's carrier.
    static std::vector<CxxColumn> build_columns(rugo::avro::AvroBatch& batch) {
        std::vector<CxxColumn> out(batch.columns.size());
        for (size_t c = 0; c < batch.columns.size(); ++c) avro_detail::build_column(batch.columns[c], out[c]);
        return out;
    }

    // A range of a stripe the prefilter reads: rows [first, first + n), in a run of rows
    // read in order that ends at run_end.
    struct Range {
        uint32_t first;
        uint32_t n;
        uint32_t run_end;
    };

    // The ranges of a stripe: with row groups, whole groups (at most kOrcMorselRows rows),
    // leaving out the ones the prune terms rule out (design doc §6.7); else every
    // kOrcMorselRows rows.
    std::vector<Range> plan_ranges(NativeOrcScanGlobal& g, rugo::orc::StripeCursor& first, uint32_t rows,
                                   bool groups) {
        std::vector<Range> ranges;
        if (!groups) {
            for (uint32_t at = 0; at < rows; at += kOrcMorselRows)
                ranges.push_back({at, std::min(kOrcMorselRows, rows - at), rows});
            return ranges;
        }
        const uint32_t stride = first.stride();
        const uint32_t span = stride * (kOrcMorselRows / stride);
        const size_t count = (static_cast<size_t>(rows) + stride - 1) / stride;
        std::vector<uint8_t> keep(count, 1);
        if (!prune_terms_.empty()) keep = first.groups_may_match(prune_terms_);
        int64_t dropped = 0;
        for (size_t g0 = 0; g0 < count;) {
            if (!keep[g0]) {
                ++dropped;
                ++g0;
                continue;
            }
            size_t g1 = g0;
            while (g1 < count && keep[g1]) ++g1;
            const uint32_t run_first = static_cast<uint32_t>(g0 * stride);
            const uint32_t run_end = static_cast<uint32_t>(std::min<size_t>(g1 * stride, rows));
            for (uint32_t at = run_first; at < run_end; at += span)
                ranges.push_back({at, std::min(span, run_end - at), run_end});
            g0 = g1;
        }
        if (dropped != 0) g.row_groups_pruned.fetch_add(dropped, std::memory_order_relaxed);
        return ranges;
    }

    // Decode one stripe two-pass (design doc §6.6), a range at a time, each surviving
    // range pushed as a morsel: the predicate's columns for every row of the range, the
    // predicate, then the other columns for only the rows from the first survivor's row
    // group to the last one's (with a row index; without one, the whole range), of which
    // the survivors are taken. A range with no survivor decodes no other column. False
    // when the scan stopped, or when the predicate failed (`error` / `code` set).
    //
    // Top-N mode (`topn` non-null): nothing is pushed. A range's survivors go to the
    // worker's buffer for the cut at the barrier, and the other columns are not decoded.
    bool decode_filtered(NativeOrcScanGlobal& g, const orc_detail::OpenFile& file, size_t stripe,
                         bool& have_slot, std::string& error, int& code, orc_detail::TopNBuffer* topn) {
        const uint32_t stripe_rows = file.orc->stripe_rows(stripe);
        if (stripe_rows == 0) return true;
        if (!prune_terms_.empty() && !file.orc->stripe_may_match(stripe, prune_terms_)) {
            g.stripes_pruned.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        g.stripes.fetch_add(1, std::memory_order_relaxed);
        g.rows.fetch_add(stripe_rows, std::memory_order_relaxed);
        rugo::orc::StripeCursor first(*file.orc, stripe, &pass1_cols_);
        std::unique_ptr<rugo::orc::StripeCursor> rest;
        if (!pass2_cols_.empty() && topn == nullptr)
            rest = std::make_unique<rugo::orc::StripeCursor>(*file.orc, stripe, &pass2_cols_);
        // With a row index the ranges are whole row groups: a range's survivors seek, and row
        // groups the statistics rule out are not read at all.
        const uint32_t stride = first.stride();
        const bool groups = (rest || !prune_terms_.empty()) && stride != 0 && stride <= kOrcMorselRows &&
                            first.indexed() && (!rest || rest->indexed());
        const std::vector<Range> ranges = plan_ranges(g, first, stripe_rows, groups);
        for (const Range& r : ranges) {
            if (!have_slot) {
                if (!reserve_slot(g)) return false;
                have_slot = true;
            }
            const uint32_t at = r.first;
            const uint32_t n = r.n;
            rugo::avro::AvroBatch one = first.read(at, n, r.run_end);
            std::vector<CxxColumn> decoded1 = build_columns(one);
            CxxMorsel probe;
            probe.columns.reserve(pass1_emit_.size() + 1);
            for (const uint32_t k : pass1_emit_) {
                probe.columns.push_back(decoded1[decode_pos_[spec_->emit_index[k]]]);
                probe.names.push_back(spec_->out_identities[k]);
            }
            add_prefilter_row_column(probe, n, kPrefilterRowColumn);
            std::unique_ptr<CxxMorsel> kept = run_prefilter(prefilter_fn_, pass1_prog_, probe, error, code);
            if (!error.empty()) return false;
            if (kept == nullptr) continue;   // no survivor: the other columns are not decoded
            if (topn != nullptr) {
                topn_add(g, *topn, {file.index, stripe, at, n, MorselPtr(std::move(kept))});
                if (g.abort.load()) return false;   // the cut failed (fail() set it) or the scan stopped
                continue;
            }

            std::unique_ptr<CxxMorsel, void (*)(CxxMorsel*)> taken(nullptr, cxx_morsel_delete);
            if (rest) {
                std::vector<uint32_t> rows = prefilter_survivors(*kept);
                uint32_t lo = rows[0], hi = rows[0];
                for (const uint32_t r : rows) {
                    lo = std::min(lo, r);
                    hi = std::max(hi, r);
                }
                // Without row groups the other columns read on in order to the stripe's end.
                uint32_t from = at, to = at + n, run_end = stripe_rows;
                if (groups) {
                    from = at + lo / stride * stride;
                    to = std::min(at + (hi / stride + 1) * stride, at + n);
                    run_end = to;
                }
                rugo::avro::AvroBatch two = rest->read(from, to - from, run_end);
                g.pass2_rows.fetch_add(to - from, std::memory_order_relaxed);
                CxxMorsel second;
                second.columns = build_columns(two);
                second.names.assign(second.columns.size(), std::string());
                std::vector<int32_t> idx(rows.size());
                for (size_t i = 0; i < rows.size(); ++i) idx[i] = static_cast<int32_t>(at + rows[i] - from);
                taken.reset(cxx_take_c(&second, idx.data(), static_cast<uint32_t>(idx.size())));
                if (taken == nullptr) throw std::runtime_error("gathering the surviving rows failed");
            }
            push(g, assemble(*kept, taken.get()));
            have_slot = false;
        }
        return true;
    }

    // The emitted morsel: pass-1 columns from `first` (emit order, then the row number,
    // which is dropped), the others from `rest` (pass-2 order; null when there are none).
    MorselPtr assemble(const CxxMorsel& first, const CxxMorsel* rest) const {
        auto morsel = std::make_shared<CxxMorsel>();
        morsel->columns.reserve(spec_->emit_index.size());
        size_t at1 = 0;
        for (size_t k = 0; k < spec_->emit_index.size(); ++k) {
            const uint32_t d = spec_->emit_index[k];
            if (pass1_[d]) morsel->columns.push_back(first.columns[at1++]);
            else morsel->columns.push_back(rest->columns[decode_pos_[d]]);
        }
        morsel->names = spec_->out_identities;
        return morsel;
    }

    // ── Top-N mode (docs/ORC_TOPN_LATE_MATERIALISATION_DESIGN.md) ────────────────────

    // The spec's top-N as sort keys over a pass-1 survivor morsel (emitted pass-1
    // columns in emit order, then the row number). Every key must be decoded in pass 1.
    void arm_topn() {
        topn_limit_ = 0;
        topn_spec_.clear();
        if (spec_->topn_limit == 0) return;
        const size_t keys = spec_->topn_key_emit.size();
        if (spec_->topn_limit < 0 || keys == 0 || spec_->topn_ascending.size() != keys ||
            spec_->topn_nulls_first.size() != keys)
            throw std::runtime_error("NativeOrcScanSource: a malformed top-N spec");
        if (prefilter_fn_ == nullptr)
            throw std::runtime_error("NativeOrcScanSource: top-N without the prefilter it rides on");
        for (size_t k = 0; k < keys; ++k) {
            size_t at = pass1_emit_.size();
            for (size_t i = 0; i < pass1_emit_.size(); ++i)
                if (pass1_emit_[i] == spec_->topn_key_emit[k]) at = i;
            if (at == pass1_emit_.size())
                throw std::runtime_error("NativeOrcScanSource: a top-N key is not decoded in pass 1");
            topn_spec_.emplace_back(at, spec_->topn_ascending[k] != 0, spec_->topn_nulls_first[k] != 0);
        }
        topn_limit_ = static_cast<size_t>(spec_->topn_limit);
    }

    // Keep the rows of `cands` not strictly worse than the n-th best (topn_keep.hpp),
    // dropping candidates left with none. False with the scan failed on an error.
    bool topn_cut(NativeOrcScanGlobal& g, std::vector<orc_detail::TopNCandidates>& cands, size_t& rows) {
        std::vector<MorselPtr> ms;
        ms.reserve(cands.size());
        for (const orc_detail::TopNCandidates& c : cands) ms.push_back(c.kept);
        std::vector<uint8_t> keep;
        ErrCtx err;
        if (!topn_keep_mask(ms, topn_spec_, topn_limit_, keep, err)) {
            fail(g, std::string("ORC top-N: ") + (err.msg != nullptr ? err.msg : "the sort keys could not be built"),
                 err.code != 0 ? err.code : kErrCodeReadError);
            return false;
        }
        std::vector<orc_detail::TopNCandidates> out;
        rows = 0;
        size_t base = 0;
        for (orc_detail::TopNCandidates& c : cands) {
            const uint32_t count = c.kept->num_rows();
            std::vector<int32_t> idx;
            for (uint32_t r = 0; r < count; ++r)
                if (keep[base + r]) idx.push_back(static_cast<int32_t>(r));
            base += count;
            if (idx.empty()) continue;
            if (idx.size() != count) {
                CxxMorsel* taken = cxx_take_c(c.kept.get(), idx.data(), static_cast<uint32_t>(idx.size()));
                if (taken == nullptr) throw std::runtime_error("ORC top-N: gathering the kept rows failed");
                c.kept = MorselPtr(taken, cxx_morsel_delete);
            }
            rows += idx.size();
            out.push_back(std::move(c));
        }
        cands = std::move(out);
        return true;
    }

    // Add a range's survivors to a worker's buffer, cutting it back to its own best n
    // (plus ties) when it passes max(4n, kOrcMorselRows) rows — the TopNSink's rule.
    void topn_add(NativeOrcScanGlobal& g, orc_detail::TopNBuffer& buf, orc_detail::TopNCandidates c) {
        buf.rows += c.kept->num_rows();
        buf.candidates.push_back(std::move(c));
        if (buf.rows > std::max<size_t>(4 * topn_limit_, kOrcMorselRows)) topn_cut(g, buf.candidates, buf.rows);
    }

    // The barrier: the last worker to arrive makes the cut over every worker's buffer and
    // lists the pass-2 work. True when pass 2 is to run; false when the scan failed or
    // was stopped.
    bool topn_barrier(NativeOrcScanGlobal& g) {
        std::unique_lock<std::mutex> lock(g.mtx);
        g.topn_pending -= 1;
        if (g.topn_pending == 0 && !g.failed && !g.stop) {
            lock.unlock();
            std::vector<orc_detail::TopNCandidates> all;
            for (orc_detail::TopNBuffer& b : g.topn_buffers)
                for (orc_detail::TopNCandidates& c : b.candidates) all.push_back(std::move(c));
            size_t rows = 0;
            std::vector<orc_detail::TopNItem> items;
            bool ok = true;
            try {
                ok = topn_cut(g, all, rows);
                for (orc_detail::TopNCandidates& c : all) {
                    orc_detail::TopNItem item;
                    const DrakenVector& rv = c.kept->columns.back().view;   // the row number
                    const auto* rd = static_cast<const int32_t*>(rv.data);
                    for (uint32_t i = 0; i < rv.length; ++i)
                        item.stripe_rows.push_back(c.first + static_cast<uint32_t>(rd[rv.selection[i]]));
                    item.where = std::move(c);
                    items.push_back(std::move(item));
                }
            } catch (const std::exception& e) {
                fail(g, std::string("ORC top-N: ") + e.what());
                ok = false;
            }
            g.topn_kept.store(static_cast<int64_t>(rows), std::memory_order_relaxed);
            lock.lock();
            g.topn_items = std::move(items);
            g.topn_ready = ok;
            g.cv_topn.notify_all();
        } else {
            g.cv_topn.wait(lock, [&]() { return g.topn_ready || g.failed || g.stop; });
        }
        return g.topn_ready && !g.failed && !g.stop;
    }

    // Pass 2 of top-N: claim items until none is left. For each, open its file again,
    // decode the other columns for the row group of every kept row (seeking by the row
    // index; without one, the item's range), take the kept rows, and push one morsel per
    // row group read.
    void topn_pass2(NativeOrcScanGlobal& g) {
        for (;;) {
            const size_t i = g.topn_next.fetch_add(1, std::memory_order_relaxed);
            if (i >= g.topn_items.size()) return;
            const orc_detail::TopNItem& item = g.topn_items[i];
            std::string error;
            try {
                if (!topn_pass2_item(g, item, error)) {
                    if (!error.empty()) fail(g, std::move(error));
                    return;
                }
            } catch (const std::exception& e) {
                fail(g, "ORC('" + spec_->files[item.where.file] + "'): " + e.what());
                return;
            }
        }
    }

    bool topn_pass2_item(NativeOrcScanGlobal& g, const orc_detail::TopNItem& item, std::string& error) {
        const orc_detail::TopNCandidates& c = item.where;
        if (pass2_cols_.empty()) {   // every emitted column is in pass 1
            if (!reserve_slot(g)) return false;
            push(g, assemble(*c.kept, nullptr));
            return true;
        }
        std::shared_ptr<orc_detail::OpenFile> file;
        error = open_file(g, c.file, file);
        if (!error.empty()) return false;
        rugo::orc::StripeCursor rest(*file->orc, c.stripe, &pass2_cols_);
        const uint32_t stripe_rows = rest.rows();
        const uint32_t stride = rest.stride();
        const bool groups = stride != 0 && rest.indexed();
        // The kept rows' positions in `c.kept`, by the row group (or, without one, the
        // range) each sits in, in row order.
        std::vector<uint32_t> order(item.stripe_rows.size());
        for (uint32_t k = 0; k < order.size(); ++k) order[k] = k;
        std::sort(order.begin(), order.end(),
                  [&](uint32_t a, uint32_t b) { return item.stripe_rows[a] < item.stripe_rows[b]; });
        size_t k = 0;
        while (k < order.size()) {
            uint32_t from = c.first, to = c.first + c.n, run_end = stripe_rows;
            if (groups) {
                from = item.stripe_rows[order[k]] / stride * stride;
                to = std::min(from + stride, stripe_rows);
                run_end = to;
            }
            std::vector<int32_t> p1_idx, p2_idx;
            while (k < order.size() && item.stripe_rows[order[k]] < to) {
                p1_idx.push_back(static_cast<int32_t>(order[k]));
                p2_idx.push_back(static_cast<int32_t>(item.stripe_rows[order[k]] - from));
                ++k;
            }
            if (!reserve_slot(g)) return false;
            rugo::avro::AvroBatch two = rest.read(from, to - from, run_end);
            g.pass2_rows.fetch_add(to - from, std::memory_order_relaxed);
            CxxMorsel second;
            second.columns = build_columns(two);
            second.names.assign(second.columns.size(), std::string());
            std::unique_ptr<CxxMorsel, void (*)(CxxMorsel*)> taken(
                cxx_take_c(&second, p2_idx.data(), static_cast<uint32_t>(p2_idx.size())), cxx_morsel_delete);
            std::unique_ptr<CxxMorsel, void (*)(CxxMorsel*)> first(
                cxx_take_c(c.kept.get(), p1_idx.data(), static_cast<uint32_t>(p1_idx.size())), cxx_morsel_delete);
            if (taken == nullptr || first == nullptr) {
                release_slot(g);
                throw std::runtime_error("ORC top-N: gathering the kept rows failed");
            }
            push(g, assemble(*first, taken.get()));
        }
        return true;
    }

    const OrcScanSpec* spec_;
    // Prefilter (set_prefilter); unarmed when prefilter_fn_ is null.
    ExprProgram pass1_prog_;
    ExprFilterFn prefilter_fn_ = nullptr;
    std::vector<uint8_t> pass1_;          // per decode_names: decoded in pass 1
    std::vector<uint32_t> pass1_cols_;    // decode_names indices decoded in pass 1, in order
    std::vector<uint32_t> pass2_cols_;    // ... and in pass 2
    std::vector<uint32_t> decode_pos_;    // per decode_names: position in its pass's batch
    std::vector<uint32_t> pass1_emit_;    // emitted columns (emit order) decoded in pass 1
    std::vector<rugo::orc::PruneTerm> prune_terms_;   // from the spec, at make_global
    size_t topn_limit_ = 0;                           // top-N (arm_topn); 0 = off
    std::vector<SortKeySpec> topn_spec_;              // its keys over a pass-1 survivor morsel
};

}  // namespace opteryx::engine
