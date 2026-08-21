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
//   - PREDICATES ARE PUSHED (architect ruling, 2026-08-21 — this REVERSES the
//     earlier "skene declines all predicate pushdown" decision). A pushed
//     predicate is evaluated HERE, on the decode worker that produced the row
//     group, before the morsel is emitted. `FileSystemTable::can_push` accepts
//     for skene, so the Filter node is CONSUMED by the pushdown strategy and
//     this Source is the only thing that applies it — there is no Filter above
//     to catch a row this misses.
//
//     The program is the SAME one the Filter node would have run: the compiler
//     lowers it through `_lower_expression` (the gate `add_expr_filter` itself
//     enforces) and hands over the identical (instrs, count, col_idx, lit_dv)
//     tuple and the identical `ExprFilterFn` span. So "the pushed answer equals
//     the un-pushed answer" is true by construction, not by argument: there is
//     one predicate implementation, called from a different place.
//
//     Rationale is NOT a benchmark. Pushing selection and projection toward the
//     scan is a rule applied without cost information; the earlier decline rested
//     on a measurement of a DIFFERENT thing (a serialising row filter on the
//     compile-time materialized path, which no longer exists) and its "+460ms
//     reader-side serialization" rationale was stale before it was reversed.
//
//   - ROW-GROUP SKIPPING on footer statistics IS here (see SkeneZoneMap below).
//     A .skene FILE footer carries per-row-group, per-column min/max ORDINALS —
//     the format's own words: "a reader prunes on `column_statistics` and only
//     then range-reads the surviving row groups" (skene/include/skene/reader.h).
//     SkeneClaimSet::build already parses that footer to count row groups, so a
//     row group provably holding no matching row is dropped at CLAIM time and
//     never decoded at all. It composes with plan-time FILE pruning rather than
//     replacing it: the manifest drops whole files, this drops row groups inside
//     the files that survived.
//
//     No type reasoning happens here. The plan hands over `(column, op, ordinal)`
//     triples already resolved by Manifest.ordinal_zone_map_terms — which owns
//     the ordinal dialect, the NaN-visibility rule and the temporal-domain guard
//     — and this code does integer comparisons. A second site deciding any of
//     that would be a second dialect, and ordinalize has a well-documented
//     near-twin it must never be confused with.
//
//     Bloom filters are NOT probed yet. Row-group ColumnMetadata carries one, but
//     it lives in the ROW GROUP footer, not the file footer, so probing it costs a
//     second parse per row group — a different trade from this one, and unmeasured.
//   - Rows in a SURVIVING row group are still decoded before they are filtered.
//     skene::read_morsel has no row-mask parameter, so the read set (projection ∪
//     predicate-only columns) is materialized in full for a surviving row group
//     and then masked. Skipping the decode of individual rows that cannot survive
//     needs a reader-side row mask in skene itself, and is the increment after
//     this one.
//   - The two-pass late-materialization sibling
//     (native_skene_latmat_scan_source.hpp) attacks the OTHER half: not decoding
//     the columns nobody reads for rows that lose a top-n race. It now takes its
//     predicate from the same pushed `scan.predicates` this Source does. The
//     shared decode contract (skene_map_decoded_columns below) is what the two
//     hold in common.
//   - Projection IS pushed: only the requested columns are materialized, and
//     predicate-only columns are dropped HERE (see `emit_indices_`) rather than
//     by a downstream Select — they never leave the scan.
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

#include "native_expression.hpp"   // ExprProgram / ExprFilterFn — the pushed predicate
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

// ─── Row-group zone maps ────────────────────────────────────────────────────
//
// The plan hands over a CONJUNCTION of `(column, op, ordinal)` terms — every one
// must be satisfiable for a row group to be worth reading, so a single term that
// proves emptiness skips it. Op codes mirror Manifest.ZONE_OP_* exactly; they are
// small ints rather than strings because nothing here should carry a vocabulary of
// SQL operators.
//
// This code decides NOTHING about types. Which conjuncts are safely prunable, what
// a literal's ordinal is, whether a NaN could hide outside the bounds, whether two
// temporal domains are even comparable — all of that is settled in
// Manifest.ordinal_zone_map_terms, which is the one place that knows the column's
// type. A term that arrives here is one Python already proved sound to compare.
//
// The statistics are over NON-NULL values (skene/format.h). That is what makes
// these five ops sound without a null rule: a NULL satisfies none of them, so a
// row group whose non-null values all fail cannot be rescued by its nulls. It is
// also why kStatMin/kStatMax being ABSENT means "cannot prune" and never "empty" —
// an all-null column carries neither flag.
enum SkeneZoneOp : int {
    kSkeneZoneEq   = 0,
    kSkeneZoneGt   = 1,
    kSkeneZoneGtEq = 2,
    kSkeneZoneLt   = 3,
    kSkeneZoneLtEq = 4,
};

// Does `stats` prove NO row in this row group satisfies `op ordinal`?
inline bool skene_zone_excludes(const skene::ColumnStatistics& stats, int op,
                                int64_t ordinal) {
    const uint32_t need = skene::kStatMin | skene::kStatMax;
    if ((stats.flags & need) != need) return false;   // untracked, not empty
    switch (op) {
        case kSkeneZoneEq:   return ordinal < stats.min_ordinal || ordinal > stats.max_ordinal;
        case kSkeneZoneGt:   return stats.max_ordinal <= ordinal;
        case kSkeneZoneGtEq: return stats.max_ordinal <  ordinal;
        case kSkeneZoneLt:   return stats.min_ordinal >= ordinal;
        case kSkeneZoneLtEq: return stats.min_ordinal >  ordinal;
        default:             return false;            // unknown op: read it
    }
}

// Size of one schema column's subtree, itself included.
//
// `RowGroupSummary::column_statistics` is flattened PRE-ORDER depth first over
// `FileMetadata::columns` with ARRAY children included (reader_v2.cpp's
// `count_schema_columns` / `parse_statistics`), so a top-level column's index into
// it is the sum of its predecessors' subtree sizes — NOT its position among the
// top-level columns. Getting that wrong reads another column's bounds and prunes
// away real rows, which is why this is derived rather than assumed.
inline uint32_t skene_schema_subtree_size(const skene::ColumnSchema& column) {
    uint32_t total = 1;
    for (const skene::ColumnSchema& child : column.children)
        total += skene_schema_subtree_size(child);
    return total;
}

// The plan-side zone terms, borrowed. Three parallel vectors rather than a vector
// of structs so the Cython plan object can own them as plain cppvectors.
struct SkeneZoneMap {
    const std::vector<std::string>* columns  = nullptr;   // physical (in-file) names
    const std::vector<int>*         ops      = nullptr;
    const std::vector<int64_t>*     ordinals = nullptr;

    bool empty() const { return columns == nullptr || columns->empty(); }
    size_t size() const { return columns == nullptr ? 0 : columns->size(); }
};

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
    // `zone` may be empty, in which case every non-empty row group is claimed.
    // `out_total` / `out_pruned` are the run-time counts this scan reports as
    // telemetry; they are written once, here, under the caller's call_once.
    bool build(const std::vector<std::string>& files, const SkeneZoneMap& zone,
               int64_t* out_total, int64_t* out_pruned, std::string& err_buf) {
        int64_t total = 0;
        int64_t pruned = 0;
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
            // Resolve each zone term's column to its index in this file's
            // flattened per-row-group statistics, ONCE per file rather than per
            // row group. By NAME, never by the plan's position: the read set is
            // the plan's order and this array is the FILE's schema order, and
            // nothing requires the two to agree. A term whose column this file
            // does not have resolves to -1 and simply does not prune — the
            // schema guard in skene_map_decoded_columns is what fails loud about
            // a divergent file, not this.
            std::vector<int32_t> stat_index(zone.size(), -1);
            if (!zone.empty()) {
                uint32_t offset = 0;
                for (const skene::ColumnSchema& column : metadata.columns) {
                    for (size_t t = 0; t < zone.size(); ++t) {
                        if ((*zone.columns)[t] == column.name)
                            stat_index[t] = static_cast<int32_t>(offset);
                    }
                    offset += skene_schema_subtree_size(column);
                }
            }

            for (uint32_t g = 0; g < metadata.row_groups.size(); ++g) {
                // A row group with no rows is skipped at CLAIM time rather than
                // decoded and dropped: the work item would cost a footer parse
                // and a worker's turn to produce nothing. It is NOT counted as
                // pruned — nothing was skipped that would have been read.
                if (metadata.row_groups[g].row_count == 0) continue;
                total += 1;
                if (zone_excludes_row_group(zone, stat_index, metadata.row_groups[g])) {
                    pruned += 1;
                    continue;
                }
                claims_.push_back(SkeneClaim{static_cast<uint32_t>(i), g});
            }
            mappings_.push_back(std::move(mapping));
        }
        if (out_total != nullptr) *out_total = total;
        if (out_pruned != nullptr) *out_pruned = pruned;
        return true;
    }

    // Backward-compatible entry for callers with no zone map (the two-pass
    // late-materialization Source). Not pruning is always correct; wiring its
    // plan up with terms is a separate change.
    bool build(const std::vector<std::string>& files, std::string& err_buf) {
        return build(files, SkeneZoneMap{}, nullptr, nullptr, err_buf);
    }

    // True when the zone terms PROVE this row group holds no matching row.
    static bool zone_excludes_row_group(const SkeneZoneMap& zone,
                                        const std::vector<int32_t>& stat_index,
                                        const skene::RowGroupSummary& summary) {
        for (size_t t = 0; t < zone.size(); ++t) {
            const int32_t index = stat_index[t];
            if (index < 0) continue;
            if (static_cast<size_t>(index) >= summary.column_statistics.size()) continue;
            const skene::RowGroupColumnStatistics& column_stats =
                summary.column_statistics[static_cast<size_t>(index)];
            // `present` means TRACKED. Absent is never "zero" (skene/format.h).
            if (!column_stats.present) continue;
            if (skene_zone_excludes(column_stats.statistics, (*zone.ops)[t],
                                    (*zone.ordinals)[t]))
                return true;   // the terms are ANDed: one emptiness proof is enough
        }
        return false;
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
    // `emit_indices` are positions in the READ SET (column_names/out_identities)
    // this scan emits, in emit order. It is the projection; the read set is the
    // projection PLUS any column only the predicate touches. When the two are the
    // same set in the same order the narrowing step is skipped entirely.
    //
    // `filter_fn` is null when nothing was pushed, and then `filter` is unused.
    // Both are BORROWED like every other pointer here: the ExprProgram lives in
    // the SkeneScanPlan, which the NativePlan holds for the driver's lifetime.
    NativeSkeneScanSource(const std::vector<std::string>* files,
                          const std::vector<std::string>* column_names,
                          const std::vector<std::string>* out_identities,
                          const std::vector<int>* column_types,
                          const std::vector<int>* retag_units,
                          const std::vector<int>* emit_indices,
                          ExprFilterFn filter_fn,
                          ExprProgram* filter,
                          SkeneZoneMap zone,
                          int64_t* row_groups_total,
                          int64_t* row_groups_pruned)
        : files_(files),
          column_names_(column_names),
          out_identities_(out_identities),
          column_types_(column_types),
          retag_units_(retag_units),
          emit_indices_(emit_indices),
          filter_fn_(filter_fn),
          filter_(filter),
          zone_(zone),
          row_groups_total_(row_groups_total),
          row_groups_pruned_(row_groups_pruned),
          narrows_(emit_indices_ != nullptr && !is_identity_emit(*emit_indices,
                                                                 column_names->size())) {}

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
            g.init_ok = g.work.build(*files_, zone_, row_groups_total_,
                                     row_groups_pruned_, g.init_err);
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

            // Validate against the bound schema, put the columns in READ-SET
            // order, and rename to plan identities. Name-keyed, never positional:
            // the reader's column order is not part of the contract, and both the
            // predicate below and every downstream operator address columns by
            // POSITION in the read set — so the order has to be established here
            // rather than assumed.
            if (!align_to_read_set(*morsel, path, err)) return SourceResult::FINISHED;

            // The pushed predicate, on this worker, over this row group. Nothing
            // downstream re-checks it: the Filter node was consumed at plan time.
            if (filter_fn_ != nullptr && morsel->num_rows() != 0) {
                CxxMorsel* filtered = nullptr;
                int err_op = 0;
                const char* kernel_msg = nullptr;
                const int rc = filter_fn_(
                    filter_->instrs, filter_->count, morsel.get(),
                    filter_->col_idx.data(), filter_->lit_dv.data(),
                    filter_->const_col_idx.data(), filter_->const_scalar_dv.data(),
                    static_cast<int>(filter_->const_col_idx.size()),
                    &filtered, &err_op, &kernel_msg);
                if (rc != 0) {
                    err.code = 1;
                    err_msg_ = std::string("NativeSkeneScanSource: pushed predicate "
                                           "evaluation failed on '") + path +
                               "' row group " + std::to_string(claim.row_group) +
                               " (err_op=" + std::to_string(err_op) + "): " +
                               (kernel_msg != nullptr ? kernel_msg : "");
                    err.msg = err_msg_.c_str();
                    return SourceResult::FINISHED;
                }
                // The span returns a new'd CxxMorsel it hands ownership of, the
                // same contract ExprFilterOperator adopts it under.
                morsel = std::shared_ptr<CxxMorsel>(filtered);
                if (!morsel) {
                    err.code = 1;
                    err_msg_ = "NativeSkeneScanSource: pushed predicate returned no "
                               "morsel for '" + path + "'";
                    err.msg = err_msg_.c_str();
                    return SourceResult::FINISHED;
                }
            }

            // A row group can now legitimately produce nothing — every row failed
            // the predicate. (Empty row groups are already dropped when the claim
            // list is built, so that is not what this catches.)
            if (morsel->num_rows() == 0) continue;

            // Predicate-only columns end here: the projection is what leaves the
            // scan. Done in the Source rather than as a downstream Select because
            // it is a container operation over columns that are already filtered.
            narrow_to_emit_set(morsel);

            out = std::move(morsel);
            return SourceResult::HAVE_MORE;
        }
    }

  private:
    // True when `emit` is 0,1,...,read_width-1 — the projection IS the read set,
    // in order, so narrowing would rebuild the morsel to no effect.
    static bool is_identity_emit(const std::vector<int>& emit, size_t read_width) {
        if (emit.size() != read_width) return false;
        for (size_t i = 0; i < emit.size(); ++i)
            if (emit[i] != static_cast<int>(i)) return false;
        return true;
    }

    // Validate against the bound schema, permute the decoded columns into READ-SET
    // order, and rename them to plan identities.
    //
    // The permutation is not cosmetic: the pushed predicate's `col_idx` and every
    // downstream operator's column indices are resolved at PLAN time against the
    // read-set order, so a decode that hands columns back in another order would
    // silently address the wrong column. skene returns them in the requested order
    // today, which is why the fast path below is the one that runs — but "today"
    // is not a contract, and the cost of not relying on it is a bijection check.
    bool align_to_read_set(CxxMorsel& morsel, const std::string& path, ErrCtx& err) {
        // Validation + the sanctioned INT64→TIMESTAMP64 retag live in
        // skene_map_decoded_columns, shared with the two-pass late-materialization
        // Source so the allowlist cannot drift between the two.
        std::vector<size_t> decoded_to_plan;
        if (!skene_map_decoded_columns(morsel, *column_names_, *column_types_,
                                       *retag_units_, path, decoded_to_plan, err,
                                       err_msg_))
            return false;
        const size_t width = column_names_->size();
        std::vector<size_t> plan_to_decoded(width, width);
        for (size_t i = 0; i < decoded_to_plan.size(); ++i)
            plan_to_decoded[decoded_to_plan[i]] = i;
        for (size_t p = 0; p < width; ++p) {
            // Unset means two decoded columns claimed the same plan column, i.e.
            // the file repeated a name. skene_map_decoded_columns proves every
            // decoded name IS a plan column and that the counts match; only
            // duplication can break the bijection, and it must fail loud.
            if (plan_to_decoded[p] >= morsel.columns.size()) {
                err.code = 1;
                err_msg_ = "skene scan: '" + path + "': column '" +
                           (*column_names_)[p] +
                           "' was not decoded exactly once — this file's column "
                           "names are not distinct";
                err.msg = err_msg_.c_str();
                return false;
            }
        }
        bool in_order = true;
        for (size_t p = 0; p < width && in_order; ++p) in_order = plan_to_decoded[p] == p;
        if (in_order) {
            for (size_t p = 0; p < width; ++p) morsel.names[p] = (*out_identities_)[p];
            return true;
        }
        std::vector<CxxColumn> columns;
        std::vector<std::string> names;
        columns.reserve(width);
        names.reserve(width);
        for (size_t p = 0; p < width; ++p) {
            columns.push_back(std::move(morsel.columns[plan_to_decoded[p]]));
            names.push_back((*out_identities_)[p]);
        }
        morsel.columns = std::move(columns);
        morsel.names = std::move(names);
        return true;
    }

    // Narrow the read set down to the projection. A pure container operation —
    // column owners are shared, no buffer is copied. A zero-column result is the
    // genuine `COUNT(*) WHERE ...` shape and carries its (post-filter) row count
    // on zero_col_rows, which is the contract CountStar reads.
    void narrow_to_emit_set(MorselPtr& morsel) const {
        if (!narrows_) return;
        auto narrowed = std::make_shared<CxxMorsel>();
        narrowed->columns.reserve(emit_indices_->size());
        narrowed->names.reserve(emit_indices_->size());
        for (int index : *emit_indices_) {
            narrowed->columns.push_back(morsel->columns[static_cast<size_t>(index)]);
            narrowed->names.push_back(morsel->names[static_cast<size_t>(index)]);
        }
        narrowed->zero_col_rows = morsel->num_rows();
        narrowed->state = morsel->state;
        morsel = std::move(narrowed);
    }

    const std::vector<std::string>* files_;
    const std::vector<std::string>* column_names_;
    const std::vector<std::string>* out_identities_;
    const std::vector<int>* column_types_;
    // Parallel to column_types_: the draken timestamp unit for a column the plan
    // declares TIMESTAMP64, else -1. See the retag allowlist above.
    const std::vector<int>* retag_units_;
    // Positions in the read set this scan emits (the projection). See the ctor.
    const std::vector<int>* emit_indices_;
    // The pushed predicate. `filter_fn_ == nullptr` means nothing was pushed.
    ExprFilterFn filter_fn_;
    ExprProgram* filter_;
    // Row-group zone terms (empty = no skipping) and the run-time counts the
    // claim builder writes back for telemetry. The two int64_t* point at fields
    // of the plan object, which the NativePlan holds for the driver's lifetime;
    // they are written exactly once, inside the call_once above, and read by
    // Python only after the driver has finished.
    SkeneZoneMap zone_;
    int64_t* row_groups_total_;
    int64_t* row_groups_pruned_;
    // Precomputed in the ctor: does emit_indices_ actually change the morsel?
    bool narrows_;
    // Error text must outlive the call (ErrCtx.msg is a borrowed const char*).
    // One Source instance reports at most one error before the scan stops, so a
    // single member is enough; a second failing worker overwrites a message for
    // a scan that is already ending.
    std::string err_msg_;
};

}  // namespace opteryx::engine
