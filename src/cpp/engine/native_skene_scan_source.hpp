#pragma once
// src/cpp/engine/native_skene_scan_source.hpp — a genuinely native (zero-Python)
// skene scan Source for the morsel-driven engine.
//
// Sibling of native_parquet_scan_source.hpp, and far smaller, because skene's
// decode is a PURE FUNCTION over a byte buffer: skene::read_morsel(bytes, opts,
// &out) touches no shared state, allocates only what the output morsel owns,
// and returns a Status rather than throwing. There is no IO pipeline to submit
// into and no in-flight window to manage — a worker claims a ROW GROUP, decodes
// it, and emits it. Parallelism is therefore the trivial kind: N workers, one
// atomic counter over a flat list of (file, row group) pairs.
//
// The claim unit is the row group and NOT the file. A .skene file holds up to 16
// of them, so claiming files would divide the available parallelism by 16 — see
// SkeneClaim below for the measurements.
//
// This replaces the compile-time materialized path for skene scans, which
// decoded EVERY file on the driver thread before execution began. That cost
// both latency (serial decode, nothing overlapped) and memory (the whole read
// set resident at once — ~880 MB for a full-width TPC-H SF1 lineitem scan).
// Here the scan is pipelined and its memory is O(morsels in flight).
//
// Why the decoded morsel outlives the mapping it was decoded from: the format
// COPIES buffers verbatim and rebuilds absolute pointers from stored offsets
// (skene/include/skene/reader.h). A decoded CxxMorsel therefore shares nothing
// with the source bytes. Mappings are nonetheless held for the whole scan, in
// the global state, because a packed file is now decoded once per row group and
// remapping it each time would be up to 16 map/unmap pairs per file. If the
// reader ever becomes zero-copy/borrowing, that contract is what this rests on.
//
// Scope (first landing — fail loud, not silently, outside it):
//   - Local files only (mmap). Remote/ranged reads via footer_extent are the
//     next stage; a non-local path fails loud rather than silently falling back.
//   - Whole-file mappings. The mapping is whole-file; the DECODE is per row
//     group and touches only that row group's extents, so a packed file is not
//     read 16 times over. Ranged reads (fetching only a surviving row group's
//     bytes from object storage) are what the file footer's row group directory
//     exists to make possible, but there is no remote caller yet and an unused
//     seek path is an untested one.
//   - No predicates, no zone-map/bloom skipping. FILE-level pruning already
//     happened at plan time (manifest bounds from the footer), and row-level
//     filtering runs as a parallel engine Filter above this scan. Reader-side
//     predicates belong here eventually — this Source can filter without
//     serializing, which the materialized path could not — but that is a
//     separate, measurable change, and it is still open.
//     What DOES now evaluate a predicate during a skene scan is the two-pass
//     late-materialization sibling, native_skene_latmat_scan_source.hpp. That is
//     not the same thing and does not reopen this: it uses the predicate to avoid
//     DECODING columns nobody reads, not to save the Filter's work, and the
//     Filter stays in the plan above it. The shared decode contract
//     (skene_map_decoded_columns below) is the one thing the two hold in common.
//   - Projection IS pushed: only the requested columns are materialized.
//   - No schema evolution: every projected column must exist in every file,
//     with the type the plan bound. A divergent file fails loud, naming itself.
//     The single exception is a scan-declared INT64→TIMESTAMP64 retag, which the
//     plan requests explicitly per column (see `retag_units_`) — an allowlist of
//     one, not a loosening of the guard.

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "operator.hpp"

#include "logical_type.h"  // LogicalType / logical_type_intern (TIMESTAMP64 descriptor)
#include "morsels/cxx_morsel.h"
#include "skene/reader.h"
#include "skene/status.h"

namespace opteryx::engine {

// RAII read-only mapping of a whole file. Fails loud (fd < 0 / addr == nullptr)
// rather than throwing — this runs on worker threads in a no-exception context,
// matching skene's own status-code posture.
class SkeneFileMapping {
  public:
    explicit SkeneFileMapping(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return;
        struct stat st {};
        if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
            ::close(fd);
            return;
        }
        void* addr = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ,
                            MAP_PRIVATE, fd, 0);
        // The mapping keeps its own reference to the file; the descriptor is not
        // needed once mmap succeeds, and holding one per in-flight file would
        // burn descriptors on a wide scan.
        ::close(fd);
        if (addr == MAP_FAILED) return;
        data_ = addr;
        size_ = static_cast<size_t>(st.st_size);
    }

    ~SkeneFileMapping() {
        if (data_ != nullptr) ::munmap(data_, size_);
    }

    SkeneFileMapping(const SkeneFileMapping&) = delete;
    SkeneFileMapping& operator=(const SkeneFileMapping&) = delete;

    bool ok() const noexcept { return data_ != nullptr; }
    const void* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }

  private:
    void* data_ = nullptr;
    size_t size_ = 0;
};

// Retag an INT64-decoded column to TIMESTAMP64 in place. Payload-preserving by
// construction: only the type tag and the owner's logical descriptor change —
// `data`, `selection`, `validity`, `length` and `data_length` are untouched, so
// the column keeps its shape (dense/constant/dict) and every row survives.
// Mirrors the parquet Source's build_temporal_column LC_TIMESTAMP branch, which
// attaches the same interned descriptor.
inline bool skene_retag_as_timestamp64(CxxColumn& column, int unit,
                                       const std::string& path, ErrCtx& err,
                                       std::string& err_buf) {
    // draken treats a TIMESTAMP64 vector with a nullptr descriptor as a hard
    // error, and the descriptor hangs off the owner — so a borrowed/unowned
    // column cannot be retagged. skene's decode always owns its buffers; if that
    // ever changes, fail loud rather than emit an undescribed timestamp.
    if (!column.own) {
        err.code = 1;
        err_buf = "skene scan: '" + path +
                  "': cannot retag an unowned column to TIMESTAMP64";
        err.msg = err_buf.c_str();
        return false;
    }
    LogicalType lt;
    lt.kind = LogicalKind::TIMESTAMP;
    lt.unit = static_cast<TimestampUnit>(unit);
    lt.offset_minutes = 0;
    column.own->logical_type = logical_type_intern(lt);
    column.own->vec.type = DRAKEN_TIMESTAMP64;
    column.view.type = DRAKEN_TIMESTAMP64;
    return true;
}

// Validate a freshly decoded skene morsel against the bound schema and report,
// per DECODED column position, which PLAN column it is (`out_map[i]` = index into
// `column_names`). Name-keyed, never positional: the reader's column order is not
// part of the contract, and a positional match would silently mislabel columns if
// it ever changed.
//
// This is the ONE place the type contract for a skene read lives — both the
// single-pass Source and the two-pass late-materialization Source go through it,
// so the sanctioned-retag allowlist below cannot drift between them.
inline bool skene_map_decoded_columns(CxxMorsel& morsel,
                                      const std::vector<std::string>& column_names,
                                      const std::vector<int>& column_types,
                                      const std::vector<int>& retag_units,
                                      const std::string& path,
                                      std::vector<size_t>& out_map,
                                      ErrCtx& err, std::string& err_buf) {
    if (morsel.names.size() != column_names.size()) {
        err.code = 1;
        err_buf = "skene scan: '" + path + "': decoded " +
                  std::to_string(morsel.names.size()) + " columns, projected " +
                  std::to_string(column_names.size());
        err.msg = err_buf.c_str();
        return false;
    }
    out_map.assign(morsel.names.size(), 0);
    for (size_t i = 0; i < morsel.names.size(); ++i) {
        // Small N (a projection), so a linear probe beats building a map per
        // morsel — and the common case is that position i already matches.
        size_t want = column_names.size();
        if (morsel.names[i] == column_names[i]) {
            want = i;
        } else {
            for (size_t j = 0; j < column_names.size(); ++j) {
                if (morsel.names[i] == column_names[j]) {
                    want = j;
                    break;
                }
            }
        }
        if (want == column_names.size()) {
            err.code = 1;
            err_buf = "skene scan: '" + path + "': decoded unexpected column '" +
                      morsel.names[i] + "'";
            err.msg = err_buf.c_str();
            return false;
        }
        const int file_type = static_cast<int>(morsel.columns[i].view.type);
        const int bound_type = column_types[want];
        if (file_type != bound_type) {
            // The ONE permitted divergence: the plan declares TIMESTAMP64 for a
            // column this file stores as INT64. That is not schema drift — it is
            // TimestampCastSinkStrategy having sunk a `col::TIMESTAMP[unit]` into
            // the scan, so the temporal-ness comes from SQL rather than the footer.
            // INT64 and TIMESTAMP64 share the same 8-byte payload and these units
            // keep the integer verbatim, so this is a pure retag: no rescale, no
            // reallocation, no row touched. `retag_units[want] >= 0` is the plan
            // SAYING so — the compiler sets it only for a declared-TIMESTAMP64
            // column.
            //
            // Deliberately a closed allowlist of one, not a "types are close
            // enough" relaxation: every other mismatch is a file that does not
            // share the dataset's schema, and must still fail loud.
            if (!(bound_type == DRAKEN_TIMESTAMP64 && file_type == DRAKEN_INT64 &&
                  retag_units[want] >= 0)) {
                err.code = 1;
                err_buf = "skene scan: '" + path + "': column '" + morsel.names[i] +
                          "' is type " + std::to_string(file_type) +
                          " in this file but " + std::to_string(bound_type) +
                          " at bind time — this dataset's files do not share one schema";
                err.msg = err_buf.c_str();
                return false;
            }
            if (!skene_retag_as_timestamp64(morsel.columns[i], retag_units[want], path,
                                            err, err_buf))
                return false;
        }
        out_map[i] = want;
    }
    return true;
}

// ─── The claim unit ─────────────────────────────────────────────────────────
//
// ONE ROW GROUP OF ONE FILE — never a whole file. A .skene file holds up to 16
// row groups (skene/FORMAT.md), and claiming files would coarsen the scan's
// parallelism by that factor.
//
// This is the load-bearing half of packing row groups into files. Measured on
// 16M rows with the claim unit tied to row group size, total scan time was flat
// from 64k to 256k rows per claim (340/311/326ms) and then collapsed — 750ms at
// 1M, 1809ms at 4M. Packing 16 row groups per file without making row groups
// independently claimable reproduces the bottom of that table.
struct SkeneClaim {
    uint32_t file_idx;
    uint32_t row_group;
};

// Every file mapped, and every (file, row group) pair flattened into one list a
// single atomic counter hands out.
//
// The row group counts come from each file's FILE FOOTER, which is the cheap
// half of skene's metadata: schema plus the row group directory plus per-row-
// group statistics, with no row group footer and no section directory parsed.
// This costs one open+mmap+footer-parse per FILE, which is strictly fewer than
// the one-per-row-group the unpacked layout paid.
//
// Mappings are held for the whole scan rather than remapped per row group.
// They are lazy and file-backed, so the kernel evicts what is not in use; the
// alternative is to mmap and munmap the same file up to 16 times.
class SkeneClaimSet {
  public:
    bool build(const std::vector<std::string>& files, std::string& err_buf) {
        mappings_.reserve(files.size());
        for (size_t i = 0; i < files.size(); ++i) {
            auto mapping = std::make_unique<SkeneFileMapping>(files[i]);
            if (!mapping->ok()) {
                err_buf = "NativeSkeneScanSource: cannot map file '" + files[i] + "'";
                return false;
            }
            skene::FileMetadata metadata;
            skene::Status status =
                skene::read_metadata(mapping->data(), mapping->size(), &metadata);
            if (!status.is_ok()) {
                err_buf = "NativeSkeneScanSource: '" + files[i] + "': " +
                          status.message();
                return false;
            }
            // A file with no row groups cannot be produced by the writer and is
            // rejected by the reader, so reaching here would mean the two
            // disagree — fail rather than silently scan nothing.
            if (metadata.row_groups.empty()) {
                err_buf = "NativeSkeneScanSource: '" + files[i] +
                          "' declares no row groups";
                return false;
            }
            for (uint32_t g = 0; g < metadata.row_groups.size(); ++g) {
                // A row group with no rows is skipped at CLAIM time rather than
                // decoded and dropped: the work item would cost a footer parse
                // and a worker's turn to produce nothing.
                if (metadata.row_groups[g].row_count == 0) continue;
                claims_.push_back(SkeneClaim{static_cast<uint32_t>(i), g});
            }
            mappings_.push_back(std::move(mapping));
        }
        return true;
    }

    const std::vector<SkeneClaim>& claims() const { return claims_; }
    const SkeneFileMapping& mapping(uint32_t file_idx) const {
        return *mappings_[file_idx];
    }

  private:
    std::vector<std::unique_ptr<SkeneFileMapping>> mappings_;
    std::vector<SkeneClaim>                        claims_;
};

// Global state: the claim counter over (file, row group) pairs. Work items are
// handed out one at a time rather than block-partitioned up front, so a worker
// that draws a large row group does not hold the tail of the scan behind it.
struct NativeSkeneScanGlobal : GlobalSourceState {
    std::once_flag      init;
    bool                init_ok = false;
    std::string         init_err;   // stable once `init` has run; err.msg borrows it
    SkeneClaimSet       work;
    std::atomic<size_t> next_claim{0};
};

class NativeSkeneScanSource : public Source {
  public:
    // Every pointer is BORROWED from the plan (NativePlan holds the owning
    // Python objects alive for the driver's lifetime), matching how
    // NativeParquetScanSource borrows from its NativeScanPlan.
    NativeSkeneScanSource(const std::vector<std::string>* files,
                          const std::vector<std::string>* column_names,
                          const std::vector<std::string>* out_identities,
                          const std::vector<int>* column_types,
                          const std::vector<int>* retag_units)
        : files_(files),
          column_names_(column_names),
          out_identities_(out_identities),
          column_types_(column_types),
          retag_units_(retag_units) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<NativeSkeneScanGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativeSkeneScanGlobal&>(gs);

        // Building the claim list needs every file's row group count, so it
        // needs every file's footer. Done once, by whichever worker arrives
        // first, rather than in make_global(): make_global has no error channel,
        // and a failure to map a file must reach the driver as an error rather
        // than as an empty scan.
        std::call_once(g.init, [&g, this] {
            g.init_ok = g.work.build(*files_, g.init_err);
        });
        if (!g.init_ok) {
            err.code = 1;
            err.msg = g.init_err.c_str();   // stable: written before call_once returned
            return SourceResult::FINISHED;
        }

        const std::vector<SkeneClaim>& claims = g.work.claims();
        while (true) {
            const size_t idx = g.next_claim.fetch_add(1, std::memory_order_relaxed);
            if (idx >= claims.size()) return SourceResult::FINISHED;

            const SkeneClaim claim = claims[idx];
            const std::string& path = (*files_)[claim.file_idx];
            const SkeneFileMapping& mapping = g.work.mapping(claim.file_idx);

            skene::ReadOptions options;
            options.columns = *column_names_;

            auto morsel = std::make_shared<CxxMorsel>();
            skene::Status status =
                skene::read_morsel(mapping.data(), mapping.size(), claim.row_group,
                                   options, morsel.get());
            if (!status.is_ok()) {
                err.code = 1;
                err_msg_ = "NativeSkeneScanSource: '" + path + "' row group " +
                           std::to_string(claim.row_group) + ": " + status.message();
                err.msg = err_msg_.c_str();
                return SourceResult::FINISHED;
            }

            // Validate against the bound schema and rename to plan identities.
            // Name-keyed, never positional: the reader's column order is not part
            // of the contract, and a positional rename would silently mislabel
            // columns if it ever changed.
            if (!rename_to_identities(*morsel, path, err)) return SourceResult::FINISHED;

            // Empty row groups are dropped when the claim list is built, so
            // this is belt and braces rather than the common path: there is no
            // filter here, so a zero-row morsel is simply nothing to emit.
            if (morsel->num_rows() == 0) continue;

            out = std::move(morsel);
            return SourceResult::HAVE_MORE;
        }
    }

  private:
    bool rename_to_identities(CxxMorsel& morsel, const std::string& path, ErrCtx& err) {
        // Validation + the sanctioned INT64→TIMESTAMP64 retag live in
        // skene_map_decoded_columns, shared with the two-pass late-materialization
        // Source so the allowlist cannot drift between the two.
        std::vector<size_t> decoded_to_plan;
        if (!skene_map_decoded_columns(morsel, *column_names_, *column_types_,
                                       *retag_units_, path, decoded_to_plan, err,
                                       err_msg_))
            return false;
        for (size_t i = 0; i < morsel.names.size(); ++i) {
            morsel.names[i] = (*out_identities_)[decoded_to_plan[i]];
        }
        return true;
    }

    const std::vector<std::string>* files_;
    const std::vector<std::string>* column_names_;
    const std::vector<std::string>* out_identities_;
    const std::vector<int>* column_types_;
    // Parallel to column_types_: the draken timestamp unit for a column the plan
    // declares TIMESTAMP64, else -1. See the retag allowlist above.
    const std::vector<int>* retag_units_;
    // Error text must outlive the call (ErrCtx.msg is a borrowed const char*).
    // One Source instance reports at most one error before the scan stops, so a
    // single member is enough; a second failing worker overwrites a message for
    // a scan that is already ending.
    std::string err_msg_;
};

}  // namespace opteryx::engine
