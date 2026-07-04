#pragma once
// src/cpp/engine/native_sort.hpp — ORDER BY breaker sinks for the engine.
//
// SortSink: accumulate all input (per-worker, lock-free) -> combine (append under
// one mutex per worker) -> finalize: build normalized sort keys, stable-sort a row
// permutation, gather the rows into fresh dense morsels (chunked) in sorted order.
// TopNSink: the ORDER BY + LIMIT fusion (HeapSortNode) — same comparator, but each
// worker keeps only a bounded candidate set (periodic compaction to the top N), so
// memory stays O(N), never O(input).
//
// Ordering contract (matches the retired morsel_sort/compress semantics where they
// were CORRECT, deliberately not bug-for-bug):
//   - NULLS FIRST under ASC (null key < every value); DESC flips → NULLS LAST.
//   - Floats: IEEE total order -inf .. -0.0==+0.0 .. +inf, NaN sorts HIGHEST
//     (draken rule; -0.0 canonicalized to +0.0). NOTE: the old shim compress()
//     mapped negative floats ABOVE positives (missing sign-bit set on positives) —
//     that was a bug, not a contract; this file implements the correct order.
//   - Strings (VARCHAR/NVARCHAR/VARBINARY): unsigned byte-wise comparison
//     (== codepoint order for UTF-8), shorter prefix first.
//   - Multi-key: lexicographic, most significant first; the sort is stable.
//
// The row gather here is the engine's general "take these rows, in this order,
// from this list of morsels" utility — also used by LimitOperator's partial slice.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

#include "operator.hpp"
#include "pipeline_buffers.hpp"
#include "core/string_slot.h"    // DrakenStringSlot, str_length, str_data
#include "core/vector_owner.h"   // VectorOwner, OwnedBuffer

namespace opteryx::engine {

struct SortKeySpec {
    size_t col_idx;
    bool ascending;
};

// CANONICAL string layout (buffers.h / draken's own kernels, e.g.
// string_predicates.h): a string DrakenVector's `data` points at a
// DrakenStringArena STRUCT — slots and arena live inside it. NOT a raw
// DrakenStringSlot array with the arena on the owner; that convention exists in
// some engine headers (native_hash_join payloads, scan_filter_demo compaction)
// but mismatches everything the live scan actually produces — flagged, not
// copied. Read and WRITE the canonical form here.
inline const DrakenStringArena* string_arena_of(const DrakenVector& v) {
    return static_cast<const DrakenStringArena*>(v.data);
}

inline bool sort_row_valid(const DrakenVector& v, uint32_t row) {
    return v.validity == nullptr || ((v.validity[row >> 3] >> (row & 7)) & 1u);
}

inline bool sort_type_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline bool sort_key_type_supported(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_DECIMAL: case DRAKEN_DATE32: case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME32: case DRAKEN_TIME64: case DRAKEN_BOOL:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
        case DRAKEN_VARCHAR: case DRAKEN_NVARCHAR: case DRAKEN_VARBINARY:
        case DRAKEN_DECIMAL128:   // int128 lane in SortKeyColumn
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:  // E33
            return true;
        default:
            return false;   // ARRAY/INTERVAL/VARIANT keys: fail loud
    }
}

// Order-preserving uint64 key for a non-null fixed-width value (validity is the
// caller's dimension). Integers/temporals/bool: sign-flip into unsigned order.
// Floats: negatives -> ~bits, positives -> bits|SIGN — the CORRECT total order.
inline uint64_t sort_num_key(const DrakenVector& v, uint32_t row) {
    constexpr uint64_t SIGN = 0x8000000000000000ULL;
    uint32_t phys = v.selection[row];
    int64_t sv = 0;
    switch (v.type) {
        case DRAKEN_INT8:   sv = static_cast<const int8_t*>(v.data)[phys]; break;
        case DRAKEN_INT16:  sv = static_cast<const int16_t*>(v.data)[phys]; break;
        case DRAKEN_INT32:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32: sv = static_cast<const int32_t*>(v.data)[phys]; break;
        case DRAKEN_INT64:
        case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_TIME64: sv = static_cast<const int64_t*>(v.data)[phys]; break;
        case DRAKEN_BOOL:
            sv = (static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u;
            break;
        // E33 — genuinely unsigned: already naturally ordered when compared as
        // uint64_t directly, so no sign-flip (unlike the signed cases above,
        // which need `^ SIGN` to make unsigned-comparison equal signed-order).
        // Return here rather than falling through to the `sv ^ SIGN` tail.
        case DRAKEN_UINT8:  return static_cast<uint64_t>(static_cast<const uint8_t* >(v.data)[phys]);
        case DRAKEN_UINT16: return static_cast<uint64_t>(static_cast<const uint16_t*>(v.data)[phys]);
        case DRAKEN_UINT32: return static_cast<uint64_t>(static_cast<const uint32_t*>(v.data)[phys]);
        case DRAKEN_UINT64: return static_cast<const uint64_t*>(v.data)[phys];
        case DRAKEN_FLOAT32:
        case DRAKEN_FLOAT64: {
            double d = (v.type == DRAKEN_FLOAT32)
                ? static_cast<double>(static_cast<const float*>(v.data)[phys])
                : static_cast<const double*>(v.data)[phys];
            if (d != d) return UINT64_MAX;   // NaN sorts highest (draken rule)
            if (d == 0.0) d = 0.0;           // canonicalize -0.0
            uint64_t bits;
            std::memcpy(&bits, &d, sizeof(bits));
            return (bits & SIGN) ? ~bits : (bits | SIGN);
        }
        default: return 0;   // unreachable — sort_key_type_supported checked first
    }
    return static_cast<uint64_t>(sv) ^ SIGN;
}

// ---- normalized key columns over a flattened morsel list -------------------------

struct SortKeyColumn {
    bool asc = true;
    bool is_str = false;
    bool is_i128 = false;
    std::vector<uint8_t> valid;
    std::vector<uint64_t> num;              // fixed-width path
    std::vector<__int128> num128;           // DECIMAL128 path (raw ordering == value
                                            //  ordering at one scale)
    std::vector<const uint8_t*> sptr;       // string path (points into source buffers,
    std::vector<uint32_t> slen;             //  which the caller keeps alive)
};

inline bool build_sort_keys(const std::vector<MorselPtr>& ms,
                            const std::vector<SortKeySpec>& spec,
                            size_t n, std::vector<SortKeyColumn>& out, ErrCtx& err) {
    out.clear();
    out.resize(spec.size());
    for (size_t k = 0; k < spec.size(); ++k) {
        SortKeyColumn& col = out[k];
        col.asc = spec[k].ascending;
        col.valid.reserve(n);
        bool typed = false;
        for (const MorselPtr& m : ms) {
            if (m->num_rows() == 0) continue;
            if (spec[k].col_idx >= m->columns.size()) {
                err.code = 1;
                err.msg = "SortSink: key column index out of range";
                return false;
            }
            const CxxColumn& c = m->columns[spec[k].col_idx];
            const DrakenVector& v = c.view;
            if (!typed) {
                if (!sort_key_type_supported(v.type)) {
                    err.code = 1;
                    err.msg = "SortSink: unsupported ORDER BY key column type — fail "
                              "loud, never a silent wrong order";
                    return false;
                }
                col.is_str = sort_type_is_string(v.type);
                col.is_i128 = (v.type == DRAKEN_DECIMAL128);
                if (col.is_str) { col.sptr.reserve(n); col.slen.reserve(n); }
                else if (col.is_i128) { col.num128.reserve(n); }
                else { col.num.reserve(n); }
                typed = true;
            }
            const DrakenStringArena* sa = col.is_str ? string_arena_of(v) : nullptr;
            for (uint32_t r = 0; r < v.length; ++r) {
                bool ok = sort_row_valid(v, r);
                col.valid.push_back(ok ? 1 : 0);
                if (col.is_str) {
                    if (ok) {
                        const DrakenStringSlot* slot = &sa->slots[v.selection[r]];
                        col.slen.push_back(str_length(slot));
                        col.sptr.push_back(
                            reinterpret_cast<const uint8_t*>(str_data(slot, sa->arena)));
                    } else {
                        col.slen.push_back(0);
                        col.sptr.push_back(nullptr);
                    }
                } else if (col.is_i128) {
                    __int128 kv = 0;
                    if (ok) {
                        std::memcpy(&kv, static_cast<const uint8_t*>(v.data)
                                            + static_cast<size_t>(v.selection[r]) * 16u,
                                    16u);
                    }
                    col.num128.push_back(kv);
                } else {
                    col.num.push_back(ok ? sort_num_key(v, r) : 0);
                }
            }
        }
    }
    return true;
}

// Stable multi-key permutation over `perm` (pre-filled with row ids).
// `take_first`: rows actually consumed downstream. SIZE_MAX (full sort) keeps
// the stable order; a real limit uses partial_sort — O(n log k) instead of
// O(n log n), the difference between compacting 65k TopN candidates to 10 and
// fully sorting them. Ties at the boundary are unspecified either way (SQL's
// ORDER BY..LIMIT contract; cross-worker compaction is already tie-unstable).
inline void sort_perm(const std::vector<SortKeyColumn>& keys, std::vector<uint32_t>& perm,
                      size_t take_first = SIZE_MAX) {
    auto cmp = [&](uint32_t a, uint32_t b) {
        for (const SortKeyColumn& c : keys) {
            int cmp;
            uint8_t va = c.valid[a], vb = c.valid[b];
            if (!va || !vb) {
                cmp = (va == vb) ? 0 : (va ? 1 : -1);   // NULL below values (asc)
            } else if (c.is_str) {
                uint32_t la = c.slen[a], lb = c.slen[b];
                uint32_t common = la < lb ? la : lb;
                int r = common ? std::memcmp(c.sptr[a], c.sptr[b], common) : 0;
                cmp = r != 0 ? r : (la < lb ? -1 : (la > lb ? 1 : 0));
            } else if (c.is_i128) {
                cmp = c.num128[a] < c.num128[b] ? -1
                    : (c.num128[a] > c.num128[b] ? 1 : 0);
            } else {
                cmp = c.num[a] < c.num[b] ? -1 : (c.num[a] > c.num[b] ? 1 : 0);
            }
            if (cmp != 0) return c.asc ? (cmp < 0) : (cmp > 0);
        }
        return false;   // equal — stability preserves arrival order
    };
    if (take_first < perm.size()) {
        std::partial_sort(perm.begin(),
                          perm.begin() + static_cast<ptrdiff_t>(take_first),
                          perm.end(), cmp);
        return;
    }
    std::stable_sort(perm.begin(), perm.end(), cmp);
}

// ---- general row gather -----------------------------------------------------------
// Copy `order[first..first+count)` (global row ids over `ms`, any order) into ONE
// fresh dense morsel. `row_m`/`row_r` map a global row id to (morsel, local row).

inline size_t gather_elem_size(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_UINT8:                          return 1;
        case DRAKEN_INT16: case DRAKEN_UINT16:                        return 2;
        case DRAKEN_INT32: case DRAKEN_UINT32: case DRAKEN_FLOAT32:
        case DRAKEN_DATE32: case DRAKEN_TIME32:                       return 4;
        case DRAKEN_INT64: case DRAKEN_UINT64: case DRAKEN_FLOAT64: case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64: case DRAKEN_TIME64:                  return 8;
        case DRAKEN_DECIMAL128:                                       return 16;
        default:                                                       return 0;
    }
}

inline MorselPtr gather_rows(const std::vector<MorselPtr>& ms,
                             const std::vector<uint32_t>& order,
                             size_t first, size_t count,
                             const std::vector<uint32_t>& row_m,
                             const std::vector<uint32_t>& row_r,
                             const std::vector<std::string>& names,
                             ErrCtx& err) {
    uint32_t n = static_cast<uint32_t>(count);
    auto out = std::make_shared<CxxMorsel>();
    out->names = names;
    out->zero_col_rows = n;
    if (ms.empty()) return out;
    size_t ncols = ms.front()->columns.size();
    out->columns.reserve(ncols);
    size_t vbytes = (static_cast<size_t>(n) + 7) / 8;

    for (size_t ci = 0; ci < ncols; ++ci) {
        DrakenType t = ms.front()->columns[ci].view.type;
        // Parameterized physical types (DECIMAL scale, TIMESTAMP unit, …) carry a
        // registry-interned logical descriptor on the owner — it must survive the
        // gather or the cursor's materialization fails loud.
        const LogicalType* src_lt =
            ms.front()->columns[ci].own ? ms.front()->columns[ci].own->logical_type
                                        : nullptr;

        // Validity: allocate lazily on the first NULL encountered.
        uint8_t* vbits = nullptr;
        auto mark_null = [&](uint32_t i) {
            if (vbits == nullptr) {
                vbits = static_cast<uint8_t*>(draken_malloc(vbytes == 0 ? 1 : vbytes));
                std::memset(vbits, 0xFF, vbytes == 0 ? 1 : vbytes);
            }
            vbits[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
        };

        if (sort_type_is_string(t)) {
            // Two-pass string gather into ONE canonical consolidated block:
            // [DrakenStringArena header | slots[n] | arena bytes] — `data` points at
            // the header, exactly what draken's own kernels read (buffers.h contract).
            size_t total_arena = 0;
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) continue;
                const DrakenStringSlot* slot = &string_arena_of(v)->slots[v.selection[r]];
                if (!str_is_inline(slot)) total_arena += str_length(slot);
            }
            size_t slots_off = sizeof(DrakenStringArena);
            size_t arena_off = slots_off + static_cast<size_t>(n == 0 ? 1 : n) * sizeof(DrakenStringSlot);
            uint8_t* blk = static_cast<uint8_t*>(draken_malloc(arena_off + total_arena));
            auto* sa_out = reinterpret_cast<DrakenStringArena*>(blk);
            auto* dst = reinterpret_cast<DrakenStringSlot*>(blk + slots_off);
            uint8_t* out_arena = total_arena > 0 ? blk + arena_off : nullptr;
            sa_out->slots = dst;
            sa_out->arena = out_arena;
            sa_out->length = n;
            sa_out->arena_used = total_arena;
            sa_out->arena_cap = total_arena;
            sa_out->null_bitmap = nullptr;
            sa_out->owns_buffers = 0;   // the VectorOwner frees the one block
            sa_out->type = t;
            size_t arena_pos = 0;
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) {
                    std::memset(&dst[i], 0, sizeof(DrakenStringSlot));
                    mark_null(i);
                    continue;
                }
                const DrakenStringArena* sa = string_arena_of(v);
                const DrakenStringSlot* slot = &sa->slots[v.selection[r]];
                if (str_is_inline(slot)) {
                    dst[i] = *slot;
                } else {
                    uint32_t slen = str_length(slot);
                    std::memcpy(out_arena + arena_pos, str_data(slot, sa->arena), slen);
                    str_clone_with_offset(&dst[i], slot, static_cast<uint32_t>(arena_pos));
                    arena_pos += slen;
                }
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = sa_out; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = vbits; v.type = t;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(blk),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = src_lt;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
            continue;
        }

        if (t == DRAKEN_BOOL) {
            // Bit-packed values: gather bit by bit.
            size_t dbytes = (static_cast<size_t>(n) + 7) / 8;
            uint8_t* data = static_cast<uint8_t*>(draken_malloc(dbytes == 0 ? 1 : dbytes));
            std::memset(data, 0, dbytes == 0 ? 1 : dbytes);
            for (uint32_t i = 0; i < n; ++i) {
                uint32_t g = order[first + i];
                const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
                uint32_t r = row_r[g];
                if (!sort_row_valid(v, r)) { mark_null(i); continue; }
                uint32_t phys = v.selection[r];
                if ((static_cast<const uint8_t*>(v.data)[phys >> 3] >> (phys & 7)) & 1u) {
                    data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                }
            }
            uint32_t* sel = static_cast<uint32_t*>(
                draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
            for (uint32_t i = 0; i < n; ++i) sel[i] = i;
            DrakenVector v;
            v.data = data; v.selection = sel; v.data_length = n; v.length = n;
            v.validity = vbits; v.type = t;
            v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
            CxxColumn c;
            c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                                  OwnedBuffer<uint8_t>(vbits),
                                                  OwnedBuffer<void>(sel));
            c.own->logical_type = src_lt;
            c.view = c.own->vec;
            out->columns.push_back(std::move(c));
            continue;
        }

        size_t es = gather_elem_size(t);
        if (es == 0) {
            err.code = 1;
            err.msg = "gather_rows: unsupported column type (e.g. ARRAY/INTERVAL/"
                      "VARIANT) — fail loud, never silent corruption";
            return nullptr;
        }
        uint8_t* data = static_cast<uint8_t*>(
            draken_malloc((n == 0 ? 1 : static_cast<size_t>(n)) * es));
        for (uint32_t i = 0; i < n; ++i) {
            uint32_t g = order[first + i];
            const DrakenVector& v = ms[row_m[g]]->columns[ci].view;
            uint32_t r = row_r[g];
            if (!sort_row_valid(v, r)) {
                std::memset(data + static_cast<size_t>(i) * es, 0, es);
                mark_null(i);
                continue;
            }
            std::memcpy(data + static_cast<size_t>(i) * es,
                        static_cast<const uint8_t*>(v.data)
                            + static_cast<size_t>(v.selection[r]) * es,
                        es);
        }
        uint32_t* sel = static_cast<uint32_t*>(
            draken_malloc((n == 0 ? 1 : n) * sizeof(uint32_t)));
        for (uint32_t i = 0; i < n; ++i) sel[i] = i;
        DrakenVector v;
        v.data = data; v.selection = sel; v.data_length = n; v.length = n;
        v.validity = vbits; v.type = t;
        v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        CxxColumn c;
        c.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(data),
                                              OwnedBuffer<uint8_t>(vbits),
                                              OwnedBuffer<void>(sel));
        c.own->logical_type = src_lt;
        c.view = c.own->vec;
        out->columns.push_back(std::move(c));
    }
    return out;
}

// Flatten a morsel list into global-row maps. Returns total row count.
inline size_t flatten_rows(const std::vector<MorselPtr>& ms,
                           std::vector<uint32_t>& row_m, std::vector<uint32_t>& row_r) {
    size_t n = 0;
    for (const MorselPtr& m : ms) n += m->num_rows();
    row_m.reserve(n);
    row_r.reserve(n);
    for (uint32_t mi = 0; mi < ms.size(); ++mi) {
        uint32_t rows = ms[mi]->num_rows();
        for (uint32_t r = 0; r < rows; ++r) {
            row_m.push_back(mi);
            row_r.push_back(r);
        }
    }
    return n;
}

// Sort `ms` and append the fully sorted rows, chunked, into `out`.
inline void sort_and_emit(const std::vector<MorselPtr>& ms,
                          const std::vector<SortKeySpec>& spec,
                          size_t take_first,          // SIZE_MAX = all rows
                          size_t chunk_rows,
                          MorselBuffer* out, ErrCtx& err) {
    std::vector<MorselPtr> src;
    src.reserve(ms.size());
    for (const MorselPtr& m : ms) if (m->num_rows() > 0) src.push_back(m);
    if (src.empty()) return;

    std::vector<uint32_t> row_m, row_r;
    size_t n = flatten_rows(src, row_m, row_r);
    std::vector<SortKeyColumn> keys;
    if (!build_sort_keys(src, spec, n, keys, err)) return;
    std::vector<uint32_t> perm(n);
    for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
    sort_perm(keys, perm, take_first);

    size_t total = n < take_first ? n : take_first;
    const std::vector<std::string>& names = src.front()->names;
    for (size_t start = 0; start < total; start += chunk_rows) {
        size_t count = std::min(chunk_rows, total - start);
        MorselPtr m = gather_rows(src, perm, start, count, row_m, row_r, names, err);
        if (err.code != 0) return;
        out->morsels.push_back(std::move(m));
    }
}

// ---- SortSink ---------------------------------------------------------------------

struct SortLocal : LocalSinkState { std::vector<MorselPtr> morsels; };
struct SortGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> morsels;
};

struct SortSink : Sink {
    std::vector<SortKeySpec> spec;
    MorselBuffer* out;
    size_t chunk_rows;

    SortSink(std::vector<SortKeySpec> s, MorselBuffer* b, size_t chunk = 131072)
        : spec(std::move(s)), out(b), chunk_rows(chunk) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<SortGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<SortLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        if (in->num_rows() > 0) static_cast<SortLocal&>(ls).morsels.push_back(in);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<SortGlobal&>(gs);
        auto& l = static_cast<SortLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<SortGlobal&>(gs);
        sort_and_emit(g.morsels, spec, SIZE_MAX, chunk_rows, out, err);
    }
};

// ---- TopNSink (ORDER BY + LIMIT fused — HeapSortNode) -------------------------------

struct TopNLocal : LocalSinkState {
    std::vector<MorselPtr> morsels;
    size_t rows = 0;
};
struct TopNGlobal : GlobalSinkState {
    std::mutex mtx;
    std::vector<MorselPtr> candidates;
};

struct TopNSink : Sink {
    std::vector<SortKeySpec> spec;
    size_t n_limit;
    MorselBuffer* out;
    size_t compact_threshold;

    TopNSink(std::vector<SortKeySpec> s, size_t n, MorselBuffer* b)
        : spec(std::move(s)), n_limit(n), out(b),
          compact_threshold(std::max<size_t>(4 * n, 65536)) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<TopNGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<TopNLocal>();
    }

    // Reduce the worker's candidate set to its top N (bounds memory to O(N)).
    void compact(TopNLocal& l, ErrCtx& err) {
        MorselBuffer tmp;
        sort_and_emit(l.morsels, spec, n_limit, n_limit == 0 ? 1 : n_limit, &tmp, err);
        if (err.code != 0) return;
        l.morsels = std::move(tmp.morsels);
        l.rows = 0;
        for (const MorselPtr& m : l.morsels) l.rows += m->num_rows();
    }

    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx& err) override {
        auto& l = static_cast<TopNLocal&>(ls);
        if (in->num_rows() == 0) return SinkResult::CONTINUE;
        l.morsels.push_back(in);
        l.rows += in->num_rows();
        if (l.rows > compact_threshold) compact(l, err);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx& err) override {
        auto& g = static_cast<TopNGlobal&>(gs);
        auto& l = static_cast<TopNLocal&>(ls);
        if (l.rows > n_limit) compact(l, err);
        if (err.code != 0) return;
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.candidates.push_back(std::move(m));
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<TopNGlobal&>(gs);
        sort_and_emit(g.candidates, spec, n_limit, n_limit == 0 ? 1 : n_limit, out, err);
    }
};

// ---- WindowSink (ROW_NUMBER / RANK / DENSE_RANK) -----------------------------------
// OVER (PARTITION BY p... ORDER BY o...). Breaker: buffer all input, sort by
// (partition keys ASC, order keys with their asc), one pass assigns the rank per
// partition, appends them as INT64 columns, emits in sorted order. Sort-key equality
// (win_keys_equal) defines partition boundaries and order-ties EXACTLY (value
// compare, not a hash).

enum class WinFn : uint8_t { RowNumber = 0, Rank = 1, DenseRank = 2 };
struct WindowFnSpec { WinFn kind; std::string name; };

struct WindowLocal : LocalSinkState { std::vector<MorselPtr> morsels; };
struct WindowGlobal : GlobalSinkState { std::mutex mtx; std::vector<MorselPtr> morsels; };

inline bool win_keys_equal(const std::vector<SortKeyColumn>& keys, uint32_t a,
                           uint32_t b, size_t kb, size_t ke) {
    for (size_t k = kb; k < ke; ++k) {
        const SortKeyColumn& c = keys[k];
        uint8_t va = c.valid[a], vb = c.valid[b];
        if (va != vb) return false;
        if (!va) continue;                       // both NULL → equal on this key
        if (c.is_str) {
            if (c.slen[a] != c.slen[b]) return false;
            if (c.slen[a] && std::memcmp(c.sptr[a], c.sptr[b], c.slen[a]) != 0)
                return false;
        } else if (c.is_i128) {
            if (c.num128[a] != c.num128[b]) return false;
        } else {
            if (c.num[a] != c.num[b]) return false;
        }
    }
    return true;
}

struct WindowSink : Sink {
    std::vector<SortKeySpec> sort_spec;   // [partition keys asc..., order keys...]
    size_t n_part;                        // # partition keys at the front of sort_spec
    std::vector<WindowFnSpec> funcs;
    MorselBuffer* out;
    size_t chunk_rows;

    WindowSink(std::vector<SortKeySpec> s, size_t np, std::vector<WindowFnSpec> f,
               MorselBuffer* b, size_t chunk = 131072)
        : sort_spec(std::move(s)), n_part(np), funcs(std::move(f)), out(b),
          chunk_rows(chunk) {}

    std::unique_ptr<GlobalSinkState> make_global() override {
        return std::make_unique<WindowGlobal>();
    }
    std::unique_ptr<LocalSinkState> make_local(GlobalSinkState&) override {
        return std::make_unique<WindowLocal>();
    }
    SinkResult sink(const MorselPtr& in, GlobalSinkState&, LocalSinkState& ls,
                    ErrCtx&) override {
        if (in->num_rows() > 0) static_cast<WindowLocal&>(ls).morsels.push_back(in);
        return SinkResult::CONTINUE;
    }
    void combine(GlobalSinkState& gs, LocalSinkState& ls, ErrCtx&) override {
        auto& g = static_cast<WindowGlobal&>(gs);
        auto& l = static_cast<WindowLocal&>(ls);
        std::lock_guard<std::mutex> lk(g.mtx);
        for (MorselPtr& m : l.morsels) g.morsels.push_back(std::move(m));
    }
    void finalize(GlobalSinkState& gs, ErrCtx& err) override {
        auto& g = static_cast<WindowGlobal&>(gs);
        std::vector<MorselPtr> src;
        for (const MorselPtr& m : g.morsels) if (m->num_rows() > 0) src.push_back(m);
        if (src.empty()) return;

        std::vector<uint32_t> row_m, row_r;
        size_t n = flatten_rows(src, row_m, row_r);
        std::vector<SortKeyColumn> keys;
        if (!build_sort_keys(src, sort_spec, n, keys, err)) return;
        std::vector<uint32_t> perm(n);
        for (size_t i = 0; i < n; ++i) perm[i] = static_cast<uint32_t>(i);
        sort_perm(keys, perm);

        // Rank numbers in perm order (gather_rows emits rows in perm order too).
        size_t nf = funcs.size();
        std::vector<std::vector<int64_t>> ranks(nf, std::vector<int64_t>(n));
        std::vector<int64_t> prev(nf, 0);
        size_t part_start = 0;
        for (size_t i = 0; i < n; ++i) {
            bool new_part = (i == 0) ||
                !win_keys_equal(keys, perm[i], perm[i - 1], 0, n_part);
            if (new_part) part_start = i;
            bool same_order = !new_part &&
                win_keys_equal(keys, perm[i], perm[i - 1], n_part, sort_spec.size());
            int64_t pos = static_cast<int64_t>(i - part_start) + 1;
            for (size_t f = 0; f < nf; ++f) {
                int64_t val;
                switch (funcs[f].kind) {
                    case WinFn::RowNumber: val = pos; break;
                    case WinFn::Rank:
                        val = new_part ? 1 : (same_order ? prev[f] : pos); break;
                    default:  // DenseRank
                        val = new_part ? 1 : (same_order ? prev[f] : prev[f] + 1); break;
                }
                ranks[f][i] = val;
                prev[f] = val;
            }
        }

        const std::vector<std::string>& names = src.front()->names;
        for (size_t start = 0; start < n; start += chunk_rows) {
            size_t count = std::min(chunk_rows, n - start);
            MorselPtr m = gather_rows(src, perm, start, count, row_m, row_r, names, err);
            if (err.code != 0) return;
            uint32_t cn = static_cast<uint32_t>(count);
            for (size_t f = 0; f < nf; ++f) {
                int64_t* data = static_cast<int64_t*>(
                    draken_malloc((cn == 0 ? 1 : cn) * sizeof(int64_t)));
                for (uint32_t j = 0; j < cn; ++j) data[j] = ranks[f][start + j];
                uint32_t* sel = static_cast<uint32_t*>(
                    draken_malloc((cn == 0 ? 1 : cn) * sizeof(uint32_t)));
                for (uint32_t j = 0; j < cn; ++j) sel[j] = j;
                DrakenVector v;
                v.data = data; v.selection = sel; v.data_length = cn; v.length = cn;
                v.validity = nullptr; v.type = DRAKEN_INT64;
                v.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
                CxxColumn c;
                c.own = std::make_shared<VectorOwner>(
                    v, OwnedBuffer<void>(data), OwnedBuffer<uint8_t>(nullptr),
                    OwnedBuffer<void>(sel));
                c.own->logical_type = nullptr;
                c.view = c.own->vec;
                m->columns.push_back(std::move(c));
                m->names.push_back(funcs[f].name);
            }
            out->morsels.push_back(std::move(m));
        }
    }
};

}  // namespace opteryx::engine
