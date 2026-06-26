// opteryx/compiled/nanobind/vector_string_misc.cpp — Milestone E.15, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, three functions:
//
//   vector_levenshtein(a, b)               — bytewise edit distance (INT64).
//   vector_position(haystack, needle)      — bytewise 1-based position (INT64).
//   vector_random_strings(row_count, width) — random ASCII VARCHAR column.
//
// Null TVL:
//   vector_levenshtein: any null input row → null output row.
//   vector_position:    any null input row → null output row.
//   vector_random_strings: no nulls; output validity is always nullptr.
//
// Bytewise note: all ops treat strings as raw byte sequences.  For VARCHAR and
// VARBINARY this is exact; for NVARCHAR multi-byte codepoints produce
// byte-distance / byte-position rather than codepoint-distance / codepoint-
// position.  Codepoint-aware variants are deferred (UTF-8 indexing unit TBD).
//
// Replaces:
//   opteryx/compiled/vector_ops/vector_levenshtein.pyx
//   opteryx/compiled/vector_ops/vector_position.pyx
//   opteryx/compiled/vector_ops/vector_random_string.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <chrono>
#include <stdexcept>
#include <string>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static inline bool is_valid_at(const DrakenVector* dv, uint32_t i) {
    if (!dv->validity) return true;
    return ((dv->validity[i >> 3] >> (i & 7u)) & 1u) != 0u;
}

// Unwrap a string-family Vector.  Accepts VARCHAR, NVARCHAR, VARBINARY,
// DICTIONARY, CONSTANT.  Raises TypeError on non-Vector or non-string type.
static const DrakenVector* unwrap_str(nb::object obj, const char* fn) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    const bool is_str =
        dv->type == DRAKEN_VARCHAR  ||
        dv->type == DRAKEN_NVARCHAR ||
        dv->type == DRAKEN_VARBINARY;
    if (!is_str)
        throw nb::type_error(
            (std::string(fn) + ": expected a string Vector "
             "(VARCHAR, NVARCHAR, or VARBINARY)").c_str());
    return dv;
}

// Allocate a validity bitmap, all bits set to 1 (valid).
// Returns nullptr when n == 0.
static uint8_t* alloc_validity_all_valid(uint32_t n) {
    if (n == 0u) return nullptr;
    const uint32_t nbytes = (n + 7u) >> 3;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(nbytes));
    if (!v) throw std::bad_alloc();
    std::memset(v, 0xFF, nbytes);
    return v;
}

// ---------------------------------------------------------------------------
// PCG32 — inline (PCG-XSH-RR, oneseq variant).
// Matches the statistical quality of the old Cython PCG wrapper.
// Non-deterministic seed from steady_clock + stack-address mix.
// ---------------------------------------------------------------------------

struct Pcg32 {
    uint64_t state;
    uint64_t inc;

    void seed(uint64_t s) {
        state = 0u;
        inc   = 1u;
        next(); state += s; next();
    }

    uint32_t next() {
        const uint64_t old = state;
        state = old * 6364136223846793005ULL + inc;
        const uint32_t xsh = static_cast<uint32_t>(((old >> 18u) ^ old) >> 27u);
        const uint32_t rot = static_cast<uint32_t>(old >> 59u);
        return (xsh >> rot) | (xsh << ((~rot + 1u) & 31u));
    }
};

// ---------------------------------------------------------------------------
// vector_levenshtein — bytewise edit distance, null TVL
// ---------------------------------------------------------------------------

// Myers' bit-parallel edit distance (Myers 1999 / Hyyrö). Computes the exact
// Levenshtein distance between a pattern P (length m ≤ 64, one machine word) and
// text T (length n) in O(n) word ops — the whole pattern column is processed in
// parallel, replacing the O(n·m) cell-by-cell DP. PEq is a caller-owned 256-entry
// table kept zeroed across calls: we set the pattern's bits, run, then reset only
// the touched entries so it stays clean (O(m), not O(256), per row).
static inline int64_t lev_myers(const uint8_t* P, uint32_t m,
                                const uint8_t* T, uint32_t n,
                                uint64_t* PEq) {
    for (uint32_t k = 0u; k < m; ++k) PEq[P[k]] |= (uint64_t)1 << k;
    uint64_t VP = (m < 64u) ? (((uint64_t)1 << m) - 1u) : ~(uint64_t)0;
    uint64_t VN = 0u;
    const uint64_t mask = (uint64_t)1 << (m - 1u);
    int64_t score = static_cast<int64_t>(m);
    for (uint32_t j = 0u; j < n; ++j) {
        const uint64_t Eq = PEq[T[j]];
        const uint64_t Xv = Eq | VN;
        const uint64_t Xh = (((Eq & VP) + VP) ^ VP) | Eq;
        uint64_t Ph = VN | ~(Xh | VP);
        uint64_t Mh = VP & Xh;
        if (Ph & mask) ++score;
        if (Mh & mask) --score;
        Ph = (Ph << 1) | 1u;
        Mh = Mh << 1;
        VP = Mh | ~(Xv | Ph);
        VN = Ph & Xv;
    }
    for (uint32_t k = 0u; k < m; ++k) PEq[P[k]] = 0u;
    return score;
}

// Bit-parallel (Myers) for the shorter side ≤ 64 bytes; two-row rolling DP
// fallback (O(min(len_a, len_b)) space) for the rare longer case.
static nb::object impl_levenshtein(nb::object a_obj, nb::object b_obj) {
    const DrakenVector* a = unwrap_str(a_obj, "vector_levenshtein");
    const DrakenVector* b = unwrap_str(b_obj, "vector_levenshtein");
    const uint32_t n = a->length;
    if (b->length != n)
        throw std::invalid_argument(
            "vector_levenshtein: input vectors must have the same length");

    // GIL-free compute: a_obj/b_obj keep the source arenas alive for the whole
    // call and the DP below is pure native (draken_malloc + int workspace). Drop
    // the GIL; the gil_scoped_acquire at the tail re-takes it for the Python
    // hand-off. Re-acquires in the destructor during exception unwind too.
    nb::gil_scoped_release _rel;

    // Output data buffer.
    int64_t* out_data = static_cast<int64_t*>(
        draken_malloc(n > 0u ? n * sizeof(int64_t) : sizeof(int64_t)));
    if (!out_data) throw std::bad_alloc();

    const DrakenStringArena* sa = static_cast<const DrakenStringArena*>(a->data);
    const DrakenStringArena* sb = static_cast<const DrakenStringArena*>(b->data);

    // Rolling workspace: two rows, grown as needed (DP fallback only).
    size_t   ws_cap = 0u;
    int64_t* prev   = nullptr;
    int64_t* curr   = nullptr;

    // Myers PEq table: 256 entries, zeroed. Each row sets its pattern's bits and
    // resets them afterwards, so the table is invariant-zero between rows.
    uint64_t PEq[256] = {0};

    // Validity bitmap — allocated lazily when first null row is seen.
    const uint32_t vbytes = (n + 7u) >> 3;
    uint8_t* validity = nullptr;
    bool     any_null = false;

    for (uint32_t i = 0; i < n; ++i) {
        if (!is_valid_at(a, i) || !is_valid_at(b, i)) {
            if (!validity) {
                validity = alloc_validity_all_valid(n);
                // alloc_validity_all_valid throws on OOM
            }
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            out_data[i] = 0;
            any_null = true;
            continue;
        }

        const DrakenStringSlot* slot_a = &sa->slots[a->selection[i]];
        const DrakenStringSlot* slot_b = &sb->slots[b->selection[i]];
        const uint8_t* pa = str_data(slot_a, sa->arena);
        const uint8_t* pb = str_data(slot_b, sb->arena);
        uint32_t la = str_length(slot_a);
        uint32_t lb = str_length(slot_b);

        // Keep shorter string in lb/pb (Myers pattern = shorter side; DP
        // fallback workspace is O(min)).
        if (la < lb) {
            const uint8_t* tmp_p = pa; pa = pb; pb = tmp_p;
            const uint32_t tmp_l = la; la = lb; lb = tmp_l;
        }

        // Cheap exact early-outs.
        if (lb == 0u) { out_data[i] = static_cast<int64_t>(la); continue; }
        if (la == lb && std::memcmp(pa, pb, la) == 0) { out_data[i] = 0; continue; }

        // Bit-parallel path: shorter side (pb, length lb) is the pattern.
        if (lb <= 64u) {
            out_data[i] = lev_myers(pb, lb, pa, la, PEq);
            continue;
        }

        // DP fallback (shorter side > 64 bytes). Grow workspace rows if needed.
        const size_t s_len = static_cast<size_t>(lb) + 1u;
        if (s_len > ws_cap) {
            draken_free(prev); prev = nullptr;
            draken_free(curr); curr = nullptr;
            prev = static_cast<int64_t*>(draken_malloc(s_len * sizeof(int64_t)));
            curr = static_cast<int64_t*>(draken_malloc(s_len * sizeof(int64_t)));
            if (!prev || !curr) {
                draken_free(prev);  prev = nullptr;
                draken_free(curr);  curr = nullptr;
                draken_free(out_data);
                if (validity) draken_free(validity);
                throw std::bad_alloc();
            }
            ws_cap = s_len;
        }

        // Row 0: dist(empty, pb[0..j-1]) = j.
        for (uint32_t j = 0u; j <= lb; ++j) prev[j] = j;

        for (uint32_t r = 1u; r <= la; ++r) {
            curr[0] = static_cast<int64_t>(r);
            for (uint32_t c = 1u; c <= lb; ++c) {
                if (pa[r - 1u] == pb[c - 1u]) {
                    curr[c] = prev[c - 1u];
                } else {
                    int64_t best = prev[c];                // delete
                    if (curr[c - 1u] < best) best = curr[c - 1u]; // insert
                    if (prev[c - 1u] < best) best = prev[c - 1u]; // substitute
                    curr[c] = 1 + best;
                }
            }
            // Swap rows in-place (just pointers).
            int64_t* tmp = prev; prev = curr; curr = tmp;
        }

        out_data[i] = prev[lb];
    }

    draken_free(prev);
    draken_free(curr);

    // If no nulls were found, discard unused validity bitmap.
    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    PyObject* out = draken_vector_own_raw(out_data, validity, n, DRAKEN_INT64);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_position — SQL POSITION(needle IN haystack), 1-based, null TVL
// ---------------------------------------------------------------------------
//
// SQL-92 semantics (E021-11):
//   POSITION('foo' IN 'foobar')  → 1
//   POSITION('baz' IN 'foobar')  → 0   (not found)
//   POSITION('' IN 'foobar')     → 1   (empty needle always found at 1)
//   null input                   → null output (null TVL)
//
// Bytewise BMH.  For NVARCHAR inputs, result is byte-position not
// codepoint-position (codepoint-aware variant is deferred).

static int64_t bmh_position(
    const uint8_t* hay, uint32_t hay_len,
    const uint8_t* ndl, uint32_t ndl_len)
{
    if (ndl_len == 0u) return 1;             // empty needle → position 1
    if (hay_len < ndl_len) return 0;

    // Single-byte fast path.
    if (ndl_len == 1u) {
        const void* p = std::memchr(hay, static_cast<int>(ndl[0]), hay_len);
        return p ? static_cast<int64_t>(
            static_cast<const uint8_t*>(p) - hay) + 1 : 0;
    }

    // Boyer-Moore-Horspool skip table.
    uint32_t skip[256];
    for (int k = 0; k < 256; ++k) skip[k] = ndl_len;
    for (uint32_t k = 0u; k < ndl_len - 1u; ++k)
        skip[ndl[k]] = ndl_len - k - 1u;

    const uint8_t last = ndl[ndl_len - 1u];
    uint32_t i = 0u;
    while (i <= hay_len - ndl_len) {
        if (hay[i + ndl_len - 1u] == last &&
            std::memcmp(hay + i, ndl, ndl_len) == 0)
            return static_cast<int64_t>(i) + 1;
        i += skip[hay[i + ndl_len - 1u]];
    }
    return 0;
}

static nb::object impl_position(nb::object hay_obj, nb::object ndl_obj) {
    const DrakenVector* hay = unwrap_str(hay_obj, "vector_position");
    const DrakenVector* ndl = unwrap_str(ndl_obj, "vector_position");
    const uint32_t n = hay->length;
    if (ndl->length != n)
        throw std::invalid_argument(
            "vector_position: haystack and needle must have the same length");

    // GIL-free compute — see impl_levenshtein for the rationale.
    nb::gil_scoped_release _rel;

    int64_t* out_data = static_cast<int64_t*>(
        draken_malloc(n > 0u ? n * sizeof(int64_t) : sizeof(int64_t)));
    if (!out_data) throw std::bad_alloc();

    const DrakenStringArena* sh = static_cast<const DrakenStringArena*>(hay->data);
    const DrakenStringArena* sn = static_cast<const DrakenStringArena*>(ndl->data);

    const uint32_t vbytes = (n + 7u) >> 3;
    uint8_t* validity = nullptr;
    bool     any_null = false;

    for (uint32_t i = 0u; i < n; ++i) {
        if (!is_valid_at(hay, i) || !is_valid_at(ndl, i)) {
            if (!validity) { validity = alloc_validity_all_valid(n); }
            validity[i >> 3] &= ~static_cast<uint8_t>(1u << (i & 7u));
            out_data[i] = 0;
            any_null = true;
            continue;
        }

        // Uniform access via selection — works for dense, dict, and constant.
        const DrakenStringSlot* hs = &sh->slots[hay->selection[i]];
        const DrakenStringSlot* ns = &sn->slots[ndl->selection[i]];
        out_data[i] = bmh_position(
            str_data(hs, sh->arena), str_length(hs),
            str_data(ns, sn->arena), str_length(ns));
    }

    if (!any_null && validity) { draken_free(validity); validity = nullptr; }

    nb::gil_scoped_acquire _acq;  // re-take the GIL to publish the result to Python
    PyObject* out = draken_vector_own_raw(out_data, validity, n, DRAKEN_INT64);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_random_strings — fixed-width random VARCHAR column
// ---------------------------------------------------------------------------
//
// Signature matches old vector_random_strings(row_count, width): two plain ints.
// Alphabet (64 chars, 6 bits of entropy per char, matches old .pyx):
//   a-z A-Z 0-9 _ /
// RNG: PCG32 seeded non-deterministically.

static nb::object impl_random_strings(int row_count, int width) {
    if (row_count < 0)
        throw std::invalid_argument("vector_random_strings: row_count must be >= 0");
    if (width < 0)
        throw std::invalid_argument("vector_random_strings: width must be >= 0");

    const uint32_t n = static_cast<uint32_t>(row_count);
    const uint32_t w = static_cast<uint32_t>(width);

    static const uint8_t ALPHABET[64] = {
        'a','b','c','d','e','f','g','h','i','j','k','l','m',
        'n','o','p','q','r','s','t','u','v','w','x','y','z',
        'A','B','C','D','E','F','G','H','I','J','K','L','M',
        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
        '0','1','2','3','4','5','6','7','8','9','_','/'
    };

    // Seed PCG non-deterministically.
    Pcg32 rng;
    {
        uint64_t t = static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        t ^= static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&rng));
        rng.seed(t);
    }

    // Allocate output slots.
    const size_t slots_sz = (n > 0u ? n : 1u) * sizeof(DrakenStringSlot);
    auto* slots = static_cast<DrakenStringSlot*>(draken_malloc(slots_sz));
    if (!slots) throw std::bad_alloc();
    std::memset(slots, 0, slots_sz);

    // Arena: needed only when w > STR_INLINE_MAX.
    uint8_t* arena     = nullptr;
    size_t   arena_used = 0u;

    if (w > STR_INLINE_MAX && n > 0u) {
        const size_t arena_sz = static_cast<size_t>(n) * w;
        arena = static_cast<uint8_t*>(draken_malloc(arena_sz));
        if (!arena) { draken_free(slots); throw std::bad_alloc(); }
    }

    // RAII guard: free slots + arena on exception.
    struct Guard {
        DrakenStringSlot* s; uint8_t* a;
        ~Guard() { if (s) draken_free(s); if (a) draken_free(a); }
        void release() { s = nullptr; a = nullptr; }
    } g{slots, arena};

    for (uint32_t i = 0u; i < n; ++i) {
        if (w == 0u) {
            str_init_inline(&slots[i], nullptr, 0u);
        } else if (w <= STR_INLINE_MAX) {
            uint8_t buf[STR_INLINE_MAX];
            for (uint32_t k = 0u; k < w; ++k)
                buf[k] = ALPHABET[rng.next() & 0x3Fu];
            str_init_inline(&slots[i], buf, w);
        } else {
            const uint32_t off  = static_cast<uint32_t>(arena_used);
            uint8_t*       dest = arena + off;
            for (uint32_t k = 0u; k < w; ++k)
                dest[k] = ALPHABET[rng.next() & 0x3Fu];
            draken_build_string_slot(&slots[i], dest, w, off);
            arena_used += w;
        }
    }

    g.release();

    // Ownership transfers to draken_vector_own_string on success or failure.
    // validity = nullptr → all rows valid.
    PyObject* out = draken_vector_own_string(
        slots, arena, arena_used, nullptr, n, DRAKEN_VARCHAR);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_string_misc(nb::module_ &m) {

    m.def("vector_levenshtein",
        [](nb::object a, nb::object b) -> nb::object {
            return impl_levenshtein(a, b);
        },
        nb::arg("a"), nb::arg("b"),
        "Bytewise Levenshtein edit distance for each row pair.\n"
        "Output: DRAKEN_INT64. Null TVL: any null input row → null output.\n"
        "For NVARCHAR, distance is over UTF-8 bytes (codepoint variant deferred).");

    m.def("vector_position",
        [](nb::object hay, nb::object ndl) -> nb::object {
            return impl_position(hay, ndl);
        },
        nb::arg("haystack"), nb::arg("needle"),
        "SQL POSITION(needle IN haystack): 1-based byte position of needle in each row.\n"
        "Returns 0 when not found. Output: DRAKEN_INT64.\n"
        "Null TVL: any null input row → null output.\n"
        "Empty needle → 1 (SQL convention). "
        "For NVARCHAR, result is byte-position (codepoint variant deferred).");

    m.def("vector_random_strings",
        [](int row_count, int width) -> nb::object {
            return impl_random_strings(row_count, width);
        },
        nb::arg("row_count"), nb::arg("width"),
        "Generate row_count random fixed-width ASCII strings of length width.\n"
        "Alphabet: a-z A-Z 0-9 _ / (64 chars, 6 bits per char, matches old .pyx).\n"
        "Output: DRAKEN_VARCHAR. Non-deterministic (PCG32, time-seeded).");
}
