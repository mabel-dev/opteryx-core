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
//   - Local files only. A v3 file is read by PLANNED POSITIONAL READS — the
//     file's suffix (tail + footer in one read when the footer fits), the read
//     set's directory blocks (each running on through block 0 when block 0 is
//     claimed whole), then one range per (column, run of surviving row groups)
//     per block — exactly the requests a remote reader would issue, counted
//     (SkeneIo). A v2 file keeps its decode metadata in per-row-group footers
//     and is mapped whole.
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
//     Bloom filters are NOT probed yet. A v3 bloom is an index section in its
//     column's INDEX region, outside the chunk ranges a block fetches, so
//     probing one costs an extra range per (column, row group) — a different
//     trade from this one, and unmeasured.
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

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
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
#include "runtime_bound.hpp"       // RuntimeKeyBound — runtime min/max join filter

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

// ─── Runtime min/max join filter (docs/RUNTIME_MINMAX_FILTER_DESIGN.md) ──────
//
// A bound this scan may ADD to its zone map, discovered at run time on a join's
// build side rather than at plan time from a literal. Each entry names a
// PHYSICAL (in-file) column and points at an engine-owned RuntimeKeyBound that
// Engine::run() filled when the build pipeline completed.
//
// The timing is not a race and needs no wait: pipelines run strictly one at a
// time (engine.hpp), and the build pipeline that fills the bound is created —
// and therefore runs — before any pipeline of the probe leg. By the time this
// Source's first get_morsel() builds the claim list, the bound is complete.
// `valid == 0` (never filled, or no usable bound) simply contributes no term,
// which costs a read and never an answer.
//
// Whether a bound is SOUND to apply at all — the join mode, the probe key being
// a direct scan column, ordinal comparability of the two sides' types — is
// settled at plan time by the compiler. Nothing here re-decides it; this only
// turns a filled bound into two ordinary zone terms.
// The shape moved to runtime_bound.hpp when the parquet scan gained the same
// filter — one definition, so "parallel arrays" cannot come to mean two things.
using SkeneRuntimeBounds = RuntimeBoundSet;

// ─── Skene IO: planned positional reads (v3) ────────────────────────────────
//
// A v3 file is laid out column-major (skene/FORMAT.md §3): a column's chunks for
// every row group are one contiguous range, and the footer alone locates every
// block of every column. So this scan reads a v3 file the way a remote reader
// would — tail, footer, the read set's directory blocks, then one planned range
// per (column, run of surviving row groups) per block — with pread, so every
// request is a real, countable read (design R8, "measured on disk"). The same
// planner output is what a GCS fetcher would issue.
//
// A v2 file keeps its decode metadata in per-row-group footers and cannot be read
// by range plan; it is mapped whole and read as before. That is dispatch on the
// file's VERSION, the format's own two-version window — not a fallback.

// Per-scan IO knobs (in) and counters (out), owned by the plan object for the
// driver's lifetime. The counters are -1 until the scan runs; the claim builder
// zeroes them once, then workers add with relaxed atomics. Read only after the
// driver finishes.
//
// `metadata_requests` counts the reads an open issues: the file's suffix, the
// footer when the suffix did not hold it, and the directory ranges. A directory
// range planned THROUGH block 0 carries block 0's chunks too, and is counted
// here once — the block-0 work item then issues no `requests` of its own.
struct SkeneIo {
    double   waste_ratio = 0.0;       // skene_io_coalesce_waste_ratio
    uint64_t max_bytes = 0;           // skene_io_coalesce_max_bytes; 0 = uncapped
    int64_t  requests = -1;           // chunk range reads issued
    int64_t  metadata_requests = -1;  // suffix, footer and directory reads
    int64_t  bytes_fetched = -1;      // bytes of every read above
};

// How much of a file's end one open reads before it knows the footer's size:
// the tail, and — whenever it fits — the footer with it, so opening a file is
// ONE read instead of two (the parquet reader's speculative footer read, sized
// for skene). A v3 footer grows with columns x row groups (a 56-byte statistics
// blob per column per row group) and data with rows x columns, so a fixed
// fraction of the file tracks the footer across file sizes; the floor covers
// small files — wholly, when they are smaller than it. A miss costs the one
// read this saves, never an answer.
inline constexpr uint64_t kSkeneSuffixFloor   = 64 * 1024;
inline constexpr uint64_t kSkeneSuffixDivisor = 1024;
inline uint64_t skene_suffix_bytes(uint64_t file_bytes) {
    const uint64_t want = std::max(kSkeneSuffixFloor, file_bytes / kSkeneSuffixDivisor);
    return std::min(want, file_bytes);
}

inline void skene_io_add(int64_t* counter, int64_t n) {
    if (counter != nullptr) __atomic_fetch_add(counter, n, __ATOMIC_RELAXED);
}

// Bytes read from a file: allocated WITHOUT initialisation, because pread
// overwrites every one of them. (A std::vector<uint8_t> resize zero-fills first
// — measured in `sample` as bzero under skene_pread on every fetched byte.) The
// storage is heap-owned, so `data` is stable when the owner moves.
struct SkeneReadBuffer {
    std::unique_ptr<uint8_t[]> bytes;
    size_t                     length = 0;
    const uint8_t* data() const { return bytes.get(); }
    size_t size() const { return length; }
};

// Reads exactly [offset, offset + bytes) of `fd` into `out`. Fails loud on a
// short read: a range the plan named is a range the file must hold.
inline bool skene_pread(int fd, uint64_t offset, uint64_t bytes, SkeneReadBuffer* out,
                        const std::string& path, std::string& err_buf) {
    out->bytes.reset(new uint8_t[static_cast<size_t>(bytes)]);  // default-init: no fill
    out->length = static_cast<size_t>(bytes);
    uint8_t* dst = out->bytes.get();
    uint64_t done = 0;
    while (done < bytes) {
        const ssize_t n = ::pread(fd, dst + done, static_cast<size_t>(bytes - done),
                                  static_cast<off_t>(offset + done));
        if (n < 0) {
            if (errno == EINTR) continue;
            err_buf = "skene scan: '" + path + "': read failed at " +
                      std::to_string(offset + done) + ": " + std::strerror(errno);
            return false;
        }
        if (n == 0) {
            err_buf = "skene scan: '" + path + "': file ended at " +
                      std::to_string(offset + done) + " inside a planned range ending at " +
                      std::to_string(offset + bytes);
            return false;
        }
        done += static_cast<uint64_t>(n);
    }
    return true;
}

// The bytes one work item fetched, shared by every row group decoded from them.
// `ranges` may also name bytes this block does not own — the file's suffix,
// held by the SkeneFile for the scan's lifetime.
struct SkeneFetchedBlock {
    std::vector<SkeneReadBuffer>      buffers;
    std::vector<skene::FetchedRange>  ranges;
};

// Reads every range of `plan` that `held` does not already cover whole, and
// lists `held` among the ranges so a decode can find what it serves. A range
// only partly inside `held` is read whole: correct, and rare (a directory run
// straddling the suffix start). `held.bytes == 0` means nothing is held.
inline bool skene_fetch_ranges(int fd, const std::vector<skene::ByteRange>& plan,
                               const skene::FetchedRange& held, SkeneFetchedBlock* out,
                               int64_t* request_counter, int64_t* byte_counter,
                               const std::string& path, std::string& err_buf) {
    out->buffers.clear();
    out->ranges.clear();
    out->buffers.reserve(plan.size());
    out->ranges.reserve(plan.size() + 1);
    int64_t requests = 0;
    int64_t bytes = 0;
    for (const skene::ByteRange& r : plan) {
        if (held.bytes > 0 && r.offset >= held.offset &&
                r.offset + r.bytes <= held.offset + held.bytes)
            continue;
        out->buffers.emplace_back();
        if (!skene_pread(fd, r.offset, r.bytes, &out->buffers.back(), path, err_buf))
            return false;
        out->ranges.push_back(skene::FetchedRange{r.offset, r.bytes, out->buffers.back().data()});
        requests += 1;
        bytes += static_cast<int64_t>(r.bytes);
    }
    if (held.bytes > 0) out->ranges.push_back(held);
    skene_io_add(request_counter, requests);
    skene_io_add(byte_counter, bytes);
    return true;
}

// One scanned file: a v3 file open for positional reads, or a v2 file mapped.
struct SkeneFile {
    std::string                        path;
    uint16_t                           version = 0;
    int                                fd = -1;         // v3
    std::unique_ptr<SkeneFileMapping>  mapping;         // v2
    skene::FileReader                  reader;
    // v3: the end of the file read at open (skene_suffix_bytes) — the tail, the
    // footer when it fit, and on a small file everything. Never resized after
    // open, so `held` may point into it for the scan's lifetime.
    SkeneReadBuffer                    suffix;
    skene::FetchedRange                held{};

    SkeneFile() = default;
    SkeneFile(const SkeneFile&) = delete;
    SkeneFile& operator=(const SkeneFile&) = delete;
    ~SkeneFile() {
        if (fd >= 0) ::close(fd);
    }
};

// ─── The claim unit ─────────────────────────────────────────────────────────
//
// ROW GROUPS OF ONE FILE — never a whole file. A v3 claim is one BLOCK: the
// surviving row groups of `block_row_groups` consecutive ones (FORMAT.md §3.2),
// whose chunks are fetched as one planned set of ranges — the request unit —
// while each row group is still decoded on its own. A v2 claim is one row group.
//
// Decode balance is not given up for the bigger fetch: the worker that fetches a
// block decodes one of its row groups and queues the rest, which any worker
// takes before claiming another block (design D-4). The 64k-row decode claim is
// what measured faster at SF1; the block only changes how bytes arrive.
//
// Block 0 of a file whose block 0 survived whole is fetched at OPEN, in the same
// range as each column's directory block (design §3.1): its claim carries those
// bytes in `prefetched` and issues no read.
struct SkeneClaim {
    uint32_t                     file_idx = 0;
    std::vector<uint32_t>        row_groups;   // ascending; one for v2
    std::vector<skene::ByteRange> fetch;       // v3: the planned ranges; empty for v2
    // v3 block 0, when merged. `mutable` so fetch() can MOVE it out: a claim is
    // handed to exactly one worker, and the bytes must go when that worker's
    // decode items do, not live on in the claim list for the whole scan.
    mutable std::shared_ptr<SkeneFetchedBlock> prefetched;
};

// Every file opened, pruned on its footer, and flattened into claims one atomic
// counter hands out.
class SkeneClaimSet {
  public:
    // `zone` may be empty, in which case every non-empty row group is claimed.
    // `out_total` / `out_pruned` are the run-time counts this scan reports as
    // telemetry; they are written once, here, under the caller's call_once.
    //
    // `runtime_from` is the index in `zone` at which RUNTIME terms begin (==
    // zone.size() when there are none), and `out_pruned_runtime` counts the row
    // groups a runtime term excluded that no PLAN-TIME term had already
    // excluded (docs/RUNTIME_MINMAX_FILTER_DESIGN.md §6.2).
    //
    // `read_columns` is every column any read of this scan will decode — their
    // directory blocks are fetched and attached here, once per file, and the
    // blocks' fetch plans cover exactly them. Empty means every column, as
    // skene::ReadOptions.columns does. When every non-empty row group of block 0
    // survives pruning, each directory range runs on through block 0 and that
    // block's claim is served from the same read.
    //
    // `per_row_group` makes every claim a single row group with no fetch plan
    // (the late-materialization Source plans its own reads per pass).
    //
    // `out_bytes_claimed` is the bytes the planned fetches of the CLAIMED row
    // groups cover — projection-aware. A v2 file has no per-column extent at
    // claim time, so a scan touching one leaves it at -1: not measured, rather
    // than estimated.
    bool build(const std::vector<std::string>& files,
               const std::vector<std::string>& read_columns, const SkeneZoneMap& zone,
               int64_t* out_total, int64_t* out_pruned, std::string& err_buf,
               size_t runtime_from, int64_t* out_pruned_runtime,
               int64_t* out_bytes_claimed, SkeneIo* io, bool per_row_group) {
        int64_t total = 0;
        int64_t pruned = 0;
        int64_t pruned_runtime = 0;
        int64_t bytes_claimed = 0;
        bool bytes_measured = true;
        if (out_pruned_runtime == nullptr) runtime_from = zone.size();
        if (io != nullptr) {
            io->requests = 0;
            io->metadata_requests = 0;
            io->bytes_fetched = 0;
        }
        policy_.waste_ratio = io == nullptr ? 0.0 : io->waste_ratio;
        policy_.max_bytes   = io == nullptr ? 0 : io->max_bytes;
        read_columns_ = read_columns;
        io_ = io;

        files_.reserve(files.size());
        for (size_t i = 0; i < files.size(); ++i) {
            files_.push_back(std::make_unique<SkeneFile>());
            SkeneFile& file = *files_.back();
            file.path = files[i];
            if (!open_file(file, err_buf)) return false;

            const skene::FileMetadata& metadata = file.reader.metadata();
            // A file with no row groups cannot be produced by the writer and is
            // rejected by the reader, so reaching here would mean the two
            // disagree — fail rather than silently scan nothing.
            if (metadata.row_groups.empty()) {
                err_buf = "NativeSkeneScanSource: '" + files[i] + "' declares no row groups";
                return false;
            }
            // Resolve each zone term's column to its index in this file's
            // flattened per-row-group statistics, ONCE per file, by NAME. A term
            // whose column this file does not have resolves to -1 and does not
            // prune; the schema guard in skene_map_decoded_columns is what fails
            // loud about a divergent file.
            std::vector<int32_t> stat_index(zone.size(), -1);
            if (!zone.empty()) {
                uint32_t offset = 0;
                for (const skene::ColumnSchema& column : metadata.columns) {
                    for (size_t t = 0; t < zone.size(); ++t)
                        if ((*zone.columns)[t] == column.name)
                            stat_index[t] = static_cast<int32_t>(offset);
                    offset += skene_schema_subtree_size(column);
                }
            }

            std::vector<uint32_t> surviving;
            for (uint32_t g = 0; g < metadata.row_groups.size(); ++g) {
                // An empty row group is skipped at CLAIM time, and NOT counted as
                // pruned — nothing was skipped that would have been read.
                if (metadata.row_groups[g].row_count == 0) continue;
                total += 1;
                const int proof = zone_excluding_term(zone, stat_index,
                                                      metadata.row_groups[g]);
                if (proof >= 0) {
                    pruned += 1;
                    if (static_cast<size_t>(proof) >= runtime_from) pruned_runtime += 1;
                    continue;
                }
                surviving.push_back(g);
            }
            if (surviving.empty()) continue;

            if (file.version == 2 || per_row_group) {
                if (file.version == 2) bytes_measured = false;
                if (file.version == 3 && !attach_read_set(file, false, nullptr, err_buf))
                    return false;
                for (uint32_t g : surviving) {
                    if (file.version == 3) {
                        // What a per-row-group read of the read set covers — the
                        // passes themselves plan narrower reads.
                        std::vector<skene::ByteRange> plan;
                        skene::Status status = skene::plan_fetch(
                            file.reader, read_columns_, {g}, policy_, &plan);
                        if (!status.is_ok()) {
                            err_buf = "NativeSkeneScanSource: '" + file.path + "': " +
                                      status.message();
                            return false;
                        }
                        for (const skene::ByteRange& r : plan)
                            bytes_claimed += static_cast<int64_t>(r.bytes);
                    }
                    claims_.push_back(SkeneClaim{static_cast<uint32_t>(i), {g}, {}});
                }
                continue;
            }

            // v3: the read set's directory blocks — through block 0 when block 0
            // is claimed whole — then one claim per block.
            const uint32_t G = metadata.block_row_groups;
            const uint32_t first_block_end =
                std::min<uint32_t>(G, static_cast<uint32_t>(metadata.row_groups.size()));
            size_t first_block_claimed = 0, first_block_rows = 0;
            for (uint32_t g = 0; g < first_block_end; ++g)
                if (metadata.row_groups[g].row_count > 0) ++first_block_rows;
            for (uint32_t g : surviving)
                if (g < first_block_end) ++first_block_claimed;
            const bool merge_first_block =
                first_block_rows > 0 && first_block_claimed == first_block_rows;
            std::shared_ptr<SkeneFetchedBlock> first_block;
            if (!attach_read_set(file, merge_first_block,
                                 merge_first_block ? &first_block : nullptr, err_buf))
                return false;
            size_t k = 0;
            while (k < surviving.size()) {
                SkeneClaim claim;
                claim.file_idx = static_cast<uint32_t>(i);
                const uint32_t block = surviving[k] / G;
                while (k < surviving.size() && surviving[k] / G == block)
                    claim.row_groups.push_back(surviving[k++]);
                skene::Status status = skene::plan_fetch(file.reader, read_columns_,
                                                         claim.row_groups, policy_,
                                                         &claim.fetch);
                if (!status.is_ok()) {
                    err_buf = "NativeSkeneScanSource: '" + file.path + "': " + status.message();
                    return false;
                }
                // Claimed bytes are the block's chunk ranges whichever read
                // carries them — the directory reads' extra bytes are metadata.
                for (const skene::ByteRange& r : claim.fetch)
                    bytes_claimed += static_cast<int64_t>(r.bytes);
                if (block == 0 && merge_first_block) {
                    claim.prefetched = first_block;
                    claim.fetch.clear();
                }
                claims_.push_back(std::move(claim));
            }
        }
        if (out_total != nullptr) *out_total = total;
        if (out_pruned != nullptr) *out_pruned = pruned;
        if (out_pruned_runtime != nullptr) *out_pruned_runtime = pruned_runtime;
        if (out_bytes_claimed != nullptr) *out_bytes_claimed = bytes_measured ? bytes_claimed : -1;
        return true;
    }

    // The index of the FIRST zone term that PROVES this row group holds no
    // matching row, or -1 when none does. The terms are ANDed, so one proof is
    // enough — returning WHICH term proved it is what lets the caller attribute
    // a skip to plan-time or run-time pruning.
    static int zone_excluding_term(const SkeneZoneMap& zone,
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
                return static_cast<int>(t);
        }
        return -1;
    }

    const std::vector<SkeneClaim>& claims() const { return claims_; }
    const SkeneFile& file(uint32_t file_idx) const { return *files_[file_idx]; }

    // One claim's bytes (v3): the block fetched at open when it was merged into
    // the directory reads, else its planned ranges, read now and counted as
    // `requests` — less any the file's suffix already holds.
    bool fetch(const SkeneClaim& claim, std::shared_ptr<SkeneFetchedBlock>* out,
               std::string& err_buf) const {
        if (claim.prefetched) {
            *out = std::move(claim.prefetched);
            return true;
        }
        const SkeneFile& f = *files_[claim.file_idx];
        auto block = std::make_shared<SkeneFetchedBlock>();
        if (!skene_fetch_ranges(f.fd, claim.fetch, f.held, block.get(),
                                io_ == nullptr ? nullptr : &io_->requests,
                                io_ == nullptr ? nullptr : &io_->bytes_fetched,
                                f.path, err_buf))
            return false;
        *out = std::move(block);
        return true;
    }

    // Decodes ONE row group with `options`, whichever version the file is. For
    // v3, `block` holds the ranges fetched for it; null means "plan and fetch
    // exactly what this read needs now" — the late-materialization Source's
    // per-pass reads, which differ in column set.
    skene::Status read_row_group(uint32_t file_idx, uint32_t row_group,
                                 const skene::ReadOptions& options,
                                 const SkeneFetchedBlock* block, CxxMorsel* out,
                                 std::string& err_buf) const {
        const SkeneFile& f = *files_[file_idx];
        if (f.version == 2) return skene::read_morsel(f.reader, row_group, options, out);
        if (block != nullptr)
            return skene::read_morsel(f.reader, row_group, options, block->ranges, out);
        std::vector<skene::ByteRange> plan;
        skene::Status status =
            skene::plan_fetch(f.reader, options.columns, {row_group}, policy_, &plan);
        if (!status.is_ok()) return status;
        SkeneFetchedBlock fetched;
        if (!skene_fetch_ranges(f.fd, plan, f.held, &fetched,
                                io_ == nullptr ? nullptr : &io_->requests,
                                io_ == nullptr ? nullptr : &io_->bytes_fetched, f.path,
                                err_buf))
            return skene::Status(skene::Code::kMalformed, err_buf);
        return skene::read_morsel(f.reader, row_group, options, fetched.ranges, out);
    }

  private:
    // Opens `file`: v3 for positional reads (the suffix, then the footer only if
    // the suffix missed it), v2 mapped whole.
    bool open_file(SkeneFile& file, std::string& err_buf) {
        file.fd = ::open(file.path.c_str(), O_RDONLY);
        if (file.fd < 0) {
            err_buf = "NativeSkeneScanSource: cannot open file '" + file.path + "': " +
                      std::strerror(errno);
            return false;
        }
        struct stat st {};
        if (::fstat(file.fd, &st) != 0 || st.st_size < static_cast<off_t>(skene::kMinFileBytes)) {
            err_buf = "NativeSkeneScanSource: '" + file.path + "' is too small to be a .skene file";
            return false;
        }
        const uint64_t size = static_cast<uint64_t>(st.st_size);

        const uint64_t suffix_bytes = skene_suffix_bytes(size);
        const uint64_t suffix_offset = size - suffix_bytes;
        SkeneReadBuffer suffix;
        if (!skene_pread(file.fd, suffix_offset, suffix_bytes, &suffix, file.path, err_buf))
            return false;
        count_metadata(1, static_cast<int64_t>(suffix_bytes));
        const uint8_t* tail = suffix.data() + suffix.size() - skene::kFileTailBytes;
        uint64_t footer_offset = 0, footer_bytes = 0;
        skene::Status status = skene::footer_extent(tail, skene::kFileTailBytes, size,
                                                    &footer_offset, &footer_bytes);
        if (!status.is_ok()) {
            err_buf = "NativeSkeneScanSource: '" + file.path + "': " + status.message();
            return false;
        }
        skene::FileTail parsed_tail;
        std::memcpy(&parsed_tail, tail, sizeof(parsed_tail));
        file.version = parsed_tail.version;

        if (file.version == 2) {
            ::close(file.fd);
            file.fd = -1;
            file.mapping = std::make_unique<SkeneFileMapping>(file.path);
            if (!file.mapping->ok()) {
                err_buf = "NativeSkeneScanSource: cannot map file '" + file.path + "'";
                return false;
            }
            status = skene::open_reader(file.mapping->data(), file.mapping->size(),
                                        &file.reader);
        } else if (footer_offset >= suffix_offset) {
            status = skene::open_reader_ranged(
                tail, skene::kFileTailBytes, suffix.data() + (footer_offset - suffix_offset),
                static_cast<size_t>(footer_bytes), footer_offset, size, &file.reader);
        } else {
            SkeneReadBuffer footer;
            if (!skene_pread(file.fd, footer_offset, footer_bytes, &footer, file.path, err_buf))
                return false;
            count_metadata(1, static_cast<int64_t>(footer_bytes));
            status = skene::open_reader_ranged(tail, skene::kFileTailBytes, footer.data(),
                                               footer.size(), footer_offset, size,
                                               &file.reader);
        }
        if (status.is_ok() && file.version != 2) {
            // Kept: whatever else of the file the suffix covers is not re-read.
            file.suffix = std::move(suffix);
            file.held = skene::FetchedRange{suffix_offset, suffix_bytes, file.suffix.data()};
        }
        if (!status.is_ok()) {
            err_buf = "NativeSkeneScanSource: '" + file.path + "': " + status.message();
            return false;
        }
        return true;
    }

    // Fetches and attaches the read set's directory blocks (v3). With
    // `through_first_block`, each range runs on through the column's block 0 and
    // the fetched bytes are handed back in `first_block` for block 0's claim.
    bool attach_read_set(SkeneFile& file, bool through_first_block,
                         std::shared_ptr<SkeneFetchedBlock>* first_block,
                         std::string& err_buf) {
        std::vector<skene::ByteRange> plan;
        skene::Status status = skene::plan_directory_fetch(
            file.reader, read_columns_, through_first_block, policy_, &plan);
        if (status.is_ok()) {
            auto fetched = std::make_shared<SkeneFetchedBlock>();
            if (!skene_fetch_ranges(file.fd, plan, file.held, fetched.get(),
                                    io_ == nullptr ? nullptr : &io_->metadata_requests,
                                    io_ == nullptr ? nullptr : &io_->bytes_fetched,
                                    file.path, err_buf))
                return false;
            status = skene::attach_directories(&file.reader, read_columns_, fetched->ranges);
            if (status.is_ok() && first_block != nullptr) *first_block = std::move(fetched);
        }
        if (!status.is_ok()) {
            err_buf = "NativeSkeneScanSource: '" + file.path + "': " + status.message();
            return false;
        }
        return true;
    }

    void count_metadata(int64_t requests, int64_t bytes) {
        if (io_ == nullptr) return;
        skene_io_add(&io_->metadata_requests, requests);
        skene_io_add(&io_->bytes_fetched, bytes);
    }

    std::vector<std::unique_ptr<SkeneFile>> files_;
    std::vector<SkeneClaim>                 claims_;
    std::vector<std::string>                read_columns_;
    skene::FetchPolicy                      policy_;
    SkeneIo*                                io_ = nullptr;
};

// One row group queued for decode from a block another worker fetched.
struct SkeneDecodeItem {
    uint32_t                           file_idx = 0;
    uint32_t                           row_group = 0;
    std::shared_ptr<SkeneFetchedBlock> block;   // null for v2
};

// Global state: the claim counter over blocks, and the queue of row groups
// waiting to be decoded from blocks already fetched. Workers drain the queue
// before claiming another block, so a fetched block is decoded by as many
// workers as it has row groups.
struct NativeSkeneScanGlobal : GlobalSourceState {
    std::once_flag      init;
    bool                init_ok = false;
    std::string         init_err;   // stable once `init` has run; err.msg borrows it
    SkeneClaimSet       work;
    std::atomic<size_t> next_claim{0};
    std::mutex                  pending_mtx;
    std::condition_variable     pending_cv;
    std::deque<SkeneDecodeItem> pending;
    // Blocks claimed whose row groups are not all queued yet. A worker that finds
    // the claims exhausted and the queue empty must NOT finish while this is
    // non-zero: the block being fetched is about to queue rows for it, and a
    // worker that left would cost the scan exactly the decode parallelism the
    // queue exists for. Guarded by pending_mtx.
    size_t                      blocks_in_flight = 0;
    // Runtime min/max filter only: the plan terms and the resolved runtime terms
    // concatenated into ONE conjunction, owned here so the SkeneZoneMap the
    // claim builder walks has a stable address for this pipeline's lifetime.
    // Untouched (and unallocated) when no runtime bound is wired.
    std::vector<std::string> zone_columns;
    std::vector<int>         zone_ops;
    std::vector<int64_t>     zone_ordinals;
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
                          const std::vector<int>* length_only,
                          ExprFilterFn filter_fn,
                          ExprProgram* filter,
                          SkeneZoneMap zone,
                          int64_t* row_groups_total,
                          int64_t* row_groups_pruned,
                          int64_t* row_groups_pruned_runtime,
                          int64_t* bytes_claimed,
                          SkeneIo* io)
        : files_(files),
          column_names_(column_names),
          out_identities_(out_identities),
          column_types_(column_types),
          retag_units_(retag_units),
          emit_indices_(emit_indices),
          length_only_(length_only == nullptr
                           ? std::vector<uint8_t>()
                           : std::vector<uint8_t>(length_only->begin(),
                                                  length_only->end())),
          filter_fn_(filter_fn),
          filter_(filter),
          zone_(zone),
          row_groups_total_(row_groups_total),
          row_groups_pruned_(row_groups_pruned),
          row_groups_pruned_runtime_(row_groups_pruned_runtime),
          bytes_claimed_(bytes_claimed),
          io_(io),
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
            // No runtime bound wired: the plan's borrowed terms are the whole
            // zone map and nothing is copied — byte-for-byte the pre-feature
            // path.
            if (runtime_.empty()) {
                g.init_ok = g.work.build(*files_, *column_names_, zone_,
                                         row_groups_total_, row_groups_pruned_,
                                         g.init_err, 0, nullptr, bytes_claimed_, io_,
                                         /*per_row_group=*/false);
                // Left UNWRITTEN (at its -1 sentinel) on purpose: the telemetry
                // fold reports the runtime count only when a bound was actually
                // wired, so "the filter did not fire here" stays distinguishable
                // from "it fired and pruned nothing".
                return;
            }
            // Plan terms FIRST, then the runtime terms — the order is what makes
            // "first proving term wins" attribute a skip the plan already made
            // to plan time, so the runtime counter reports only its marginal win.
            for (size_t t = 0; t < zone_.size(); ++t) {
                g.zone_columns.push_back((*zone_.columns)[t]);
                g.zone_ops.push_back((*zone_.ops)[t]);
                g.zone_ordinals.push_back((*zone_.ordinals)[t]);
            }
            const size_t runtime_from = g.zone_columns.size();
            for (size_t b = 0; b < runtime_.size(); ++b) {
                const RuntimeKeyBound* bound = runtime_.bounds[b];
                // Unfilled or unusable: contributes NO term. A missing bound
                // costs a read, never an answer.
                if (bound == nullptr || bound->valid == 0) continue;
                g.zone_columns.push_back(runtime_.columns[b]);
                g.zone_ops.push_back(kSkeneZoneGtEq);
                g.zone_ordinals.push_back(bound->lo);
                g.zone_columns.push_back(runtime_.columns[b]);
                g.zone_ops.push_back(kSkeneZoneLtEq);
                g.zone_ordinals.push_back(bound->hi);
            }
            SkeneZoneMap effective;
            effective.columns  = &g.zone_columns;
            effective.ops      = &g.zone_ops;
            effective.ordinals = &g.zone_ordinals;
            int64_t pruned_runtime = 0;
            g.init_ok = g.work.build(*files_, *column_names_, effective,
                                     row_groups_total_, row_groups_pruned_, g.init_err,
                                     runtime_from, &pruned_runtime, bytes_claimed_, io_,
                                     /*per_row_group=*/false);
            if (row_groups_pruned_runtime_ != nullptr)
                *row_groups_pruned_runtime_ = pruned_runtime;
        });
        if (!g.init_ok) {
            err.code = 1;
            err.msg = g.init_err.c_str();   // stable: written before call_once returned
            return SourceResult::FINISHED;
        }

        const std::vector<SkeneClaim>& claims = g.work.claims();
        while (true) {
            // A row group queued from a block another worker fetched comes first:
            // that is what keeps decode balanced when the fetch unit is a block.
            SkeneDecodeItem item;
            bool have_item = false;
            size_t idx = claims.size();
            {
                std::unique_lock<std::mutex> lock(g.pending_mtx);
                while (true) {
                    if (!g.pending.empty()) {
                        item = std::move(g.pending.front());
                        g.pending.pop_front();
                        have_item = true;
                        break;
                    }
                    idx = g.next_claim.fetch_add(1, std::memory_order_relaxed);
                    if (idx < claims.size()) {
                        ++g.blocks_in_flight;   // released below, once queued
                        break;
                    }
                    // Claims exhausted and nothing queued: finished only when no
                    // block is still being fetched to queue rows for us.
                    if (g.blocks_in_flight == 0) return SourceResult::FINISHED;
                    g.pending_cv.wait(lock);
                }
            }
            if (!have_item) {
                const SkeneClaim& claim = claims[idx];
                item.file_idx = claim.file_idx;
                item.row_group = claim.row_groups.front();
                bool fetched = true;
                if (g.work.file(claim.file_idx).version == 3)
                    fetched = g.work.fetch(claim, &item.block, err_msg_);
                {
                    std::lock_guard<std::mutex> lock(g.pending_mtx);
                    if (fetched)
                        for (size_t r = 1; r < claim.row_groups.size(); ++r)
                            g.pending.push_back(SkeneDecodeItem{
                                claim.file_idx, claim.row_groups[r], item.block});
                    --g.blocks_in_flight;
                }
                g.pending_cv.notify_all();
                if (!fetched) {
                    err.code = 1;
                    err.msg = err_msg_.c_str();
                    return SourceResult::FINISHED;
                }
            }

            const std::string& path = (*files_)[item.file_idx];
            skene::ReadOptions options;
            options.columns = *column_names_;
            options.length_only = length_only_;

            auto morsel = std::make_shared<CxxMorsel>();
            std::string read_err;
            skene::Status status = g.work.read_row_group(item.file_idx, item.row_group,
                                                         options, item.block.get(),
                                                         morsel.get(), read_err);
            if (!status.is_ok()) {
                err.code = 1;
                err_msg_ = "NativeSkeneScanSource: '" + path + "' row group " +
                           std::to_string(item.row_group) + ": " + status.message();
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
                               "' row group " + std::to_string(item.row_group) +
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
    // Parallel to column_names_ (or empty): 1 = the optimizer PROVED every read
    // of that column is length-answerable, so the reader records each value's
    // length and never materializes the payload arena. Owned rather than
    // borrowed because ReadOptions wants uint8_t and the plan holds int — an
    // 8-byte-per-column copy made once, not per row group. See
    // skene::ReadOptions::length_only for the contract this asserts.
    const std::vector<uint8_t> length_only_;
    // The pushed predicate. `filter_fn_ == nullptr` means nothing was pushed.
    ExprFilterFn filter_fn_;
    ExprProgram* filter_;
    // Row-group zone terms (empty = no skipping) and the run-time counts the
    // claim builder writes back for telemetry. The int64_t* point at fields
    // of the plan object, which the NativePlan holds for the driver's lifetime;
    // they are written exactly once, inside the call_once above, and read by
    // Python only after the driver has finished.
    SkeneZoneMap zone_;
    int64_t* row_groups_total_;
    int64_t* row_groups_pruned_;
    int64_t* row_groups_pruned_runtime_;
    // Bytes the claimed row groups' planned fetches cover (-1 when a v2 file
    // made that unmeasurable); same lifetime and single-write contract as the
    // three counters above.
    int64_t* bytes_claimed_;
    // IO knobs and counters, owned by the plan (see SkeneIo).
    SkeneIo* io_;
    // Runtime min/max join filter — OWNED (not borrowed): a handful of strings
    // and pointers, appended at plan time (see add_runtime_bound below) AFTER
    // this Source was constructed, because the probe scan is compiled before the
    // join that supplies the bound is finished wiring. Empty for every scan the
    // compiler did not find eligible, which is the overwhelming majority.
    SkeneRuntimeBounds runtime_;
    // Precomputed in the ctor: does emit_indices_ actually change the morsel?
    bool narrows_;
  public:
    // Plan-time only, on the compiler's thread, before run() is entered. `bound`
    // is an engine-owned RuntimeKeyBound slot whose address is stable for the
    // query; it may still be unfilled at this point (it is filled when the build
    // pipeline completes) and an unfilled bound contributes no term.
    void add_runtime_bound(std::string physical_column, const RuntimeKeyBound* bound) {
        runtime_.columns.push_back(std::move(physical_column));
        runtime_.bounds.push_back(bound);
    }

  private:
    // Error text must outlive the call (ErrCtx.msg is a borrowed const char*).
    // One Source instance reports at most one error before the scan stops, so a
    // single member is enough; a second failing worker overwrites a message for
    // a scan that is already ending.
    std::string err_msg_;
};

}  // namespace opteryx::engine
