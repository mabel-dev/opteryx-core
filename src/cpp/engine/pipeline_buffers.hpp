#pragma once
// src/cpp/engine/pipeline_buffers.hpp — breaker -> dependent-pipeline hand-off,
// with budgeted spill behind the contract.
//
// A breaker's sink materializes its result morsels into a MorselBuffer; the
// dependent pipeline's BufferSource hands them out to workers by atomic claim.
// Split out of engine.hpp so breaker sinks (native_sort.hpp) can write into
// buffers without a circular include.
//
// THE CONTRACT (docs/MORSEL_SPILL_DESIGN.md): operators append morsels and
// read claims. Whether anything reaches disk, in what format, through what
// store — none of it is visible across this interface. Callers must not know.
//
// Residency policy (architect, 2026-08-27): accumulate in memory; once the
// shared pile holds kSpillFlushBytes (512MB) AND a spill root is configured,
// flush the pile — all of it — to ONE .skene file (spill profile + zstd-1)
// and release the memory. After the first flush the buffer is a sequence of
// on-disk units and read-back decodes row groups on the claiming worker;
// there is deliberately NO mixed resident/spilled read path (one path, one
// chance to get ordering and fidelity wrong). With no spill root configured,
// behaviour is exactly the pre-spill engine: unbounded resident accumulation,
// no threshold, no error.
//
// kSpillCeilingBytes (1GB) is enforced as BACKPRESSURE, not as an error:
// while one worker's flush is in flight, other workers keep appending into a
// fresh pile; a worker that finds the pile at the ceiling BLOCKS until the
// in-flight flush completes and then flushes the pile itself. Outstanding
// memory is therefore bounded at roughly ceiling + one in-flight pile + the
// workers' 32MB local batches — and a query whose input outruns the disk is
// throttled to disk speed rather than killed. Loud errors remain what they
// were: encode failure, disk failure, disk exhaustion.
//
// Fidelity is a correctness obligation, not a quality goal: LogicalType,
// DrakenVector.flags and the dict selection round-trip through skene
// restored, never re-derived. Columns are written under synthetic positional
// names (c0, c1, ...) — engine morsel names may repeat, differ across UNION
// legs, or be empty, and skene's schema check compares names — and each row
// group's REAL names are kept in the unit record and restored on read, so a
// morsel comes back identical in shape, order and identity.

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "operator.hpp"
#include "spill_budgets.hpp"
#include "spill_store.hpp"

#include "skene/reader.h"
#include "skene/writer.h"

namespace opteryx::engine {

// Engine-owned spill environment: one per Engine, shared by every buffer.
// The SpillStore (and its q<pid>-<seq> directory) is created lazily on the
// FIRST flush anywhere in the query — a query that never spills never touches
// the filesystem. `root` empty means spill is not configured.
struct SpillEnv {
    std::string root;
    std::mutex mtx;
    std::unique_ptr<SpillStore> store;

    // Returns the store, creating it on first use. nullptr + err on failure.
    SpillStore* get(std::string& err) {
        std::lock_guard<std::mutex> lk(mtx);
        if (!store) {
            auto s = std::make_unique<SpillStore>(root);
            if (!s->ok()) {
                err = s->error();
                return nullptr;
            }
            store = std::move(s);
        }
        return store.get();
    }
};

struct MorselBuffer {
    // ---- configuration (driver thread, plan build) ------------------------
    void configure(SpillEnv* env) { env_ = env; }
    void set_label(const std::string& label) { if (label_.empty()) label_ = label; }

    // ---- write side -------------------------------------------------------
    // Single-threaded append (breaker finalize, the driver's plan-time
    // materialization, TopN's internal scratch). Returns false with the error
    // latched; callers surface it through error().
    bool append(const MorselPtr& m) {
        if (m == nullptr || m->num_rows() == 0)
            return !failed_.load(std::memory_order_acquire);
        std::vector<MorselPtr> one{m};
        return splice_(one, cxx_morsel_nbytes(m.get()));
    }

    // Per-worker append handle: lock-free local batch, spliced into the shared
    // pile once it holds kSpillSpliceBytes. `commit()` splices the remainder —
    // the existing combine() point. A Writer must not outlive its buffer.
    class Writer {
      public:
        Writer() = default;
        explicit Writer(MorselBuffer* b) : b_(b) {}
        bool append(const MorselPtr& m) {
            if (b_ == nullptr) return false;
            if (m == nullptr || m->num_rows() == 0) return true;
            local_.push_back(m);
            local_bytes_ += cxx_morsel_nbytes(m.get());
            if (static_cast<int64_t>(local_bytes_) >= spill_budgets::kSpillSpliceBytes)
                return commit();
            return true;
        }
        bool commit() {
            if (b_ == nullptr) return false;
            if (local_.empty()) return !b_->failed();
            const bool ok = b_->splice_(local_, local_bytes_);
            local_.clear();
            local_bytes_ = 0;
            return ok;
        }
      private:
        MorselBuffer* b_ = nullptr;
        std::vector<MorselPtr> local_;
        size_t local_bytes_ = 0;
    };
    Writer writer() { return Writer(this); }

    // ---- read side --------------------------------------------------------
    // Idempotent; the first reader seals. Once spilled, the residue (whatever
    // accumulated since the last flush) is flushed too, so the read path is
    // uniform. Pipelines execute sequentially, so every writer pipeline has
    // completed before the first consumer claim arrives.
    bool seal() {
        std::unique_lock<std::mutex> lk(mtx_);
        if (sealed_) return !failed_.load(std::memory_order_acquire);
        cv_.wait(lk, [this] { return !flush_active_; });
        sealed_ = true;
        if (!units_.empty() && !resident_.empty()) {
            std::vector<MorselPtr> pile;
            pile.swap(resident_);
            const size_t nb = resident_bytes_;
            resident_bytes_ = 0;
            if (!flush_pile_(lk, std::move(pile), nb)) return false;
        }
        claims_ = units_.empty() ? resident_.size() : 0;
        for (auto& u : units_) { u.claim_base = claims_; claims_ += u.rg_rows.size(); }
        maps_.assign(units_.size(), {});
        return !failed_.load(std::memory_order_acquire);
    }

    size_t claim_count() {
        seal();
        std::lock_guard<std::mutex> lk(mtx_);
        return claims_;
    }

    // Claim `idx` (in [0, claim_count())). Resident: a shared_ptr copy.
    // Spilled: the CLAIMING worker maps the unit file (a shared mapping,
    // weak-cached so at most the units currently being decoded stay mapped)
    // and decodes ONE row group — a claim is (file, row group), never a whole
    // file, so read-back parallelism matches the resident path.
    bool get(size_t idx, MorselPtr& out) {
        if (failed_.load(std::memory_order_acquire)) return false;
        if (units_.empty()) {
            if (idx >= resident_.size()) return fail_("claim out of range");
            out = resident_[idx];
            return true;
        }
        size_t u = units_.size();
        for (size_t i = units_.size(); i-- > 0;) {
            if (units_[i].claim_base <= idx) { u = i; break; }
        }
        if (u >= units_.size() || idx - units_[u].claim_base >= units_[u].rg_rows.size())
            return fail_("claim out of range");
        const SpillUnit& unit = units_[u];
        const uint32_t rg = static_cast<uint32_t>(idx - unit.claim_base);

        std::shared_ptr<SpillFileMapping> map;
        {
            std::lock_guard<std::mutex> lk(map_mtx_);
            map = maps_[u].lock();
            if (!map) {
                map = std::make_shared<SpillFileMapping>(unit.path);
                if (!map->ok())
                    return fail_("cannot map spill unit '" + unit.path + "'");
                maps_[u] = map;
            }
        }
        auto morsel = std::make_shared<CxxMorsel>();
        skene::Status status = skene::read_morsel(map->data(), map->size(), rg,
                                                  skene::ReadOptions(), morsel.get());
        if (!status.is_ok())
            return fail_("unit '" + unit.path + "' row group " + std::to_string(rg) +
                         ": " + status.message());
        if (!restore_identity_(*morsel, unit, rg)) return false;
        spill_tel::bytes_read().fetch_add(
            static_cast<int64_t>(cxx_morsel_nbytes(morsel.get())),
            std::memory_order_relaxed);
        out = std::move(morsel);
        return true;
    }

    // The engine's last-consumer release: drop resident morsels AND delete the
    // on-disk units — a spill unit's lifetime is its buffer's lifetime, never
    // longer. (The store directory itself dies with the Engine; the startup
    // sweep backstops a killed process.)
    void release() {
        std::lock_guard<std::mutex> lk(mtx_);
        resident_.clear();
        resident_.shrink_to_fit();
        resident_bytes_ = 0;
        if (env_ != nullptr && env_->store != nullptr)
            for (const auto& u : units_) env_->store->remove_unit(u.path);
        units_.clear();
        {
            std::lock_guard<std::mutex> mlk(map_mtx_);
            maps_.clear();
        }
        claims_ = 0;
    }

    // Escape hatch for UNCONFIGURED scratch buffers only (TopN's compact()):
    // moves the resident pile out. A buffer that has spilled has no resident
    // pile to take; that is a caller bug and fails loud.
    std::vector<MorselPtr> take_resident() {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!units_.empty()) {
            fail_locked_("take_resident() on a spilled buffer");
            return {};
        }
        resident_bytes_ = 0;
        return std::move(resident_);
    }

    // ---- fixpoint-loop support (docs/RECURSIVE_CTE_DESIGN.md) -------------
    // Driver-thread-only, between pipelines, on UNCONFIGURED scratch buffers
    // (the loop's WORKING/DELTA — Engine::new_scratch_buffer — which can never
    // spill). Replaces the contents with `pile` and reopens the buffer for
    // append and a fresh seal(); the next seal recomputes the claim set, which
    // is what lets one BufferSource pipeline re-read the buffer every
    // iteration. An empty pile is the DELTA reset.
    bool reset_with(std::vector<MorselPtr> pile) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (!units_.empty()) return fail_locked_("reset_with() on a spilled buffer");
        if (failed_.load(std::memory_order_acquire)) return false;
        resident_ = std::move(pile);
        resident_bytes_ = 0;
        for (const auto& m : resident_) resident_bytes_ += cxx_morsel_nbytes(m.get());
        sealed_ = false;
        claims_ = 0;
        return true;
    }

    bool spilled() const { return spilled_.load(std::memory_order_relaxed); }
    bool failed() const { return failed_.load(std::memory_order_acquire); }
    // The error is latched exactly once; the lock acquisition synchronizes the
    // read, and the string never mutates after the latch, so ErrCtx may hold
    // its c_str() for the rest of the run.
    const std::string& error() {
        std::lock_guard<std::mutex> lk(mtx_);
        return err_;
    }

  private:
    struct SpillUnit {
        std::string path;
        std::vector<uint32_t> rg_rows;                    // rows per row group
        std::vector<std::vector<std::string>> rg_names;   // REAL names per row group
        size_t claim_base = 0;
    };

    bool fail_(const std::string& msg) {
        std::lock_guard<std::mutex> lk(mtx_);
        return fail_locked_(msg);
    }
    // mtx_ held. err_ is written before failed_ flips (release), so a reader
    // that observed failed_ sees the complete message even without the lock.
    bool fail_locked_(const std::string& msg) {
        if (!failed_.load(std::memory_order_relaxed)) {
            err_ = "spill (" + (label_.empty() ? std::string("buffer") : label_) +
                   "): " + msg;
            failed_.store(true, std::memory_order_release);
        }
        return false;
    }

    // Move `pile` into the shared pile; flush when the trigger is reached.
    bool splice_(std::vector<MorselPtr>& pile, size_t pile_bytes) {
        std::unique_lock<std::mutex> lk(mtx_);
        if (failed_.load(std::memory_order_relaxed)) return false;
        if (sealed_) return fail_locked_("append after seal");
        for (MorselPtr& m : pile) resident_.push_back(std::move(m));
        pile.clear();
        resident_bytes_ += pile_bytes;
        if (env_ == nullptr || env_->root.empty())
            return true;                       // spill unconfigured: legacy unbounded
        if (static_cast<int64_t>(resident_bytes_) < spill_budgets::kSpillFlushBytes)
            return true;
        if (flush_active_) {
            // A flush is already writing. Let the pile grow toward the ceiling;
            // at the ceiling, WAIT (backpressure) and then flush it ourselves.
            if (static_cast<int64_t>(resident_bytes_) <
                spill_budgets::kSpillCeilingBytes)
                return true;
            cv_.wait(lk, [this] { return !flush_active_; });
            if (failed_.load(std::memory_order_relaxed)) return false;
            if (static_cast<int64_t>(resident_bytes_) <
                spill_budgets::kSpillFlushBytes)
                return true;                   // the finishing flush took the pile
        }
        std::vector<MorselPtr> take;
        take.swap(resident_);
        const size_t nb = resident_bytes_;
        resident_bytes_ = 0;
        return flush_pile_(lk, std::move(take), nb);
    }

    // mtx_ HELD on entry and exit; RELEASED around the encode so other workers
    // keep appending while this worker serializes and writes. flush_active_
    // serializes flushes (unit order is flush order, and two concurrent
    // encodes would double the encode working set).
    bool flush_pile_(std::unique_lock<std::mutex>& lk, std::vector<MorselPtr> pile,
                     size_t pile_bytes) {
        (void)pile_bytes;
        flush_active_ = true;
        lk.unlock();

        SpillUnit unit;
        std::string msg;
        const bool ok = encode_and_store_(pile, unit, msg);

        lk.lock();
        flush_active_ = false;
        cv_.notify_all();
        if (!ok) return fail_locked_(msg);
        units_.push_back(std::move(unit));
        spilled_.store(true, std::memory_order_relaxed);
        return true;
    }

    // No buffer locks held. Serializes `pile` to one .skene file in the store.
    bool encode_and_store_(std::vector<MorselPtr>& pile, SpillUnit& unit,
                           std::string& msg) {
        SpillStore* store = env_->get(msg);
        if (store == nullptr) return false;

        skene::WriteOptions options = skene::WriteOptions::for_spill();
        options.codec = skene::SectionCodec::kZstd;
        options.zstd_level = 1;   // bakeoff: 0.30x in 58ms; decode is level-flat

        std::vector<uint8_t> bytes;
        skene::FileWriter writer;
        skene::Status status = writer.begin(options, &bytes);
        if (!status.is_ok()) { msg = status.message(); return false; }
        int64_t rows = 0;
        for (MorselPtr& m : pile) {
            // Shallow renamed copy: synthetic positional names, so schema
            // equality across the pile is by TYPE alone and duplicate/empty
            // engine names cannot fail the write. Views and owners are shared,
            // not copied.
            CxxMorsel renamed;
            renamed.columns = m->columns;
            renamed.zero_col_rows = m->zero_col_rows;
            renamed.state = m->state;
            renamed.names.reserve(m->columns.size());
            for (size_t c = 0; c < m->columns.size(); ++c)
                renamed.names.push_back("c" + std::to_string(c));
            status = writer.add_row_group(renamed);
            if (!status.is_ok()) { msg = status.message(); return false; }
            unit.rg_names.push_back(m->names);
            unit.rg_rows.push_back(m->num_rows());
            rows += m->num_rows();
            m.reset();   // release each morsel as soon as it is encoded
        }
        status = writer.finish();
        if (!status.is_ok()) { msg = status.message(); return false; }
        pile.clear();

        unit.path = store->write_unit(bytes, msg);
        if (unit.path.empty()) return false;
        spill_tel::rows_spilled().fetch_add(rows, std::memory_order_relaxed);
        return true;
    }

    // Reorder decoded columns back to position (by synthetic name) and restore
    // the row group's REAL names. Column order is not part of the reader's
    // contract; POSITION is part of this buffer's.
    bool restore_identity_(CxxMorsel& m, const SpillUnit& unit, uint32_t rg) {
        const std::vector<std::string>& names = unit.rg_names[rg];
        if (m.columns.size() != names.size() || m.names.size() != names.size())
            return fail_("read-back column count mismatch");
        std::vector<CxxColumn> ordered(m.columns.size());
        std::vector<bool> seen(m.columns.size(), false);
        for (size_t i = 0; i < m.names.size(); ++i) {
            const std::string& n = m.names[i];
            if (n.size() < 2 || n[0] != 'c')
                return fail_("read-back: unexpected column name '" + n + "'");
            const size_t pos =
                static_cast<size_t>(std::strtoul(n.c_str() + 1, nullptr, 10));
            if (pos >= ordered.size() || seen[pos])
                return fail_("read-back: column position out of range");
            ordered[pos] = std::move(m.columns[i]);
            seen[pos] = true;
        }
        m.columns = std::move(ordered);
        m.names = names;
        return true;
    }

    SpillEnv* env_ = nullptr;
    std::string label_;

    std::mutex mtx_;                       // resident_, units_, seal/fail state
    std::condition_variable cv_;           // flush completion (backpressure)
    std::mutex map_mtx_;                   // maps_
    std::vector<MorselPtr> resident_;
    size_t resident_bytes_ = 0;
    std::vector<SpillUnit> units_;
    std::vector<std::weak_ptr<SpillFileMapping>> maps_;
    size_t claims_ = 0;
    bool sealed_ = false;
    bool flush_active_ = false;
    std::atomic<bool> spilled_{false};
    std::atomic<bool> failed_{false};
    std::string err_;
};

// Append-through sink: streams every morsel into a (possibly SHARED) MorselBuffer.
// UNION ALL is exactly this — each leg's pipeline appends into one buffer, the
// dependent pipeline reads the union. Local accumulate is a lock-free Writer
// batch (one buffer-mutex touch per kSpillSpliceBytes); commit() at combine
// splices the remainder. Order across legs is unspecified (UNION ALL
// semantics; an ORDER BY above restores determinism).
struct BufferAppendSink : Sink {
    MorselBuffer* out;
    explicit BufferAppendSink(MorselBuffer* b) : out(b) {}
    struct Local : LocalSinkState { MorselBuffer::Writer w; };
    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<GlobalSinkState>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        auto l = std::make_unique<Local>();
        l->w = out->writer();
        return l;
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        if (!static_cast<Local&>(ls).w.append(in)) {
            err.code = 1;
            err.msg = out->error().c_str();
        }
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState&, LocalSinkState& ls, ErrCtx& err) override {
        if (!static_cast<Local&>(ls).w.commit()) {
            err.code = 1;
            err.msg = out->error().c_str();
        }
    }
    void finalize(GlobalSinkState&, ErrCtx&) override {}
};

struct BufferSource : Source {
    MorselBuffer* buf;
    // Per-RUN claim cursor: one pipeline run makes one Global, so a buffer read
    // by several consumer pipelines (a shared CTE's result) hands the full
    // claim set to each of them; workers within one run still claim by atomic.
    struct Global : GlobalSourceState {
        std::atomic<size_t> cursor{0};
        size_t claims = 0;
        bool seal_ok = true;
    };
    explicit BufferSource(MorselBuffer* b) : buf(b) {}
    std::unique_ptr<GlobalSourceState> make_global() override {
        auto g = std::make_unique<Global>();
        g->seal_ok = buf->seal();
        g->claims = g->seal_ok ? buf->claim_count() : 0;
        return g;
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }
    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<Global&>(gs);
        if (!g.seal_ok) {
            err.code = 1;
            err.msg = buf->error().c_str();
            return SourceResult::FINISHED;
        }
        size_t idx = g.cursor.fetch_add(1);
        if (idx >= g.claims) return SourceResult::FINISHED;
        if (!buf->get(idx, out)) {
            err.code = 1;
            err.msg = buf->error().c_str();
            return SourceResult::FINISHED;
        }
        return SourceResult::HAVE_MORE;
    }
};

}  // namespace opteryx::engine
