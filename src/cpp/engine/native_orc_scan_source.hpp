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
    bool prefiltered = false;

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
        for (auto& t : workers) t.join();
        spec->rows_read = rows.load();
        spec->bytes_read = bytes.load();
        spec->files_read = files.load();
        spec->stripes_read = stripes.load();
        if (prefiltered) spec->pass2_rows = pass2_rows.load();
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
        NativeOrcScanGlobal* gp = g.get();
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
    }

    // A decode thread's body: anything escaping decode_loop goes through fail(), the
    // scan's one error channel, and the worker retires as on a normal exit.
    void decode_worker(NativeOrcScanGlobal& g) {
        try {
            decode_loop(g);
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

    void decode_loop(NativeOrcScanGlobal& g) {
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
                stopped = !(prefilter_fn_ != nullptr ? decode_filtered(g, *unit.file, unit.stripe, have_slot, error, code)
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

    // Decode one stripe two-pass (design doc §6.6), a range at a time, each surviving
    // range pushed as a morsel: the predicate's columns for every row of the range, the
    // predicate, then the other columns for only the rows from the first survivor's row
    // group to the last one's (with a row index; without one, the whole range), of which
    // the survivors are taken. A range with no survivor decodes no other column. False
    // when the scan stopped, or when the predicate failed (`error` / `code` set).
    bool decode_filtered(NativeOrcScanGlobal& g, const orc_detail::OpenFile& file, size_t stripe,
                         bool& have_slot, std::string& error, int& code) {
        const uint32_t stripe_rows = file.orc->stripe_rows(stripe);
        if (stripe_rows == 0) return true;
        g.stripes.fetch_add(1, std::memory_order_relaxed);
        g.rows.fetch_add(stripe_rows, std::memory_order_relaxed);
        rugo::orc::StripeCursor first(*file.orc, stripe, &pass1_cols_);
        std::unique_ptr<rugo::orc::StripeCursor> rest;
        if (!pass2_cols_.empty()) rest = std::make_unique<rugo::orc::StripeCursor>(*file.orc, stripe, &pass2_cols_);
        // With a row index the ranges are whole row groups, so a range's survivors seek.
        const uint32_t stride = rest ? rest->stride() : 0;
        const bool groups = rest && stride != 0 && stride <= kOrcMorselRows && rest->indexed();
        const uint32_t range = groups ? stride * (kOrcMorselRows / stride) : kOrcMorselRows;
        for (uint32_t at = 0; at < stripe_rows; at += range) {
            if (!have_slot) {
                if (!reserve_slot(g)) return false;
                have_slot = true;
            }
            const uint32_t n = std::min(range, stripe_rows - at);
            rugo::avro::AvroBatch one = first.next(n);
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
            auto morsel = std::make_shared<CxxMorsel>();
            morsel->columns.reserve(spec_->emit_index.size());
            size_t at1 = 0;
            for (size_t k = 0; k < spec_->emit_index.size(); ++k) {
                const uint32_t d = spec_->emit_index[k];
                if (pass1_[d]) morsel->columns.push_back(kept->columns[at1++]);
                else morsel->columns.push_back(taken->columns[decode_pos_[d]]);
            }
            morsel->names = spec_->out_identities;
            push(g, std::move(morsel));
            have_slot = false;
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
};

}  // namespace opteryx::engine
