// draken/ops/kernels/function_like_any.cpp — `LIKE ANY` / `ILIKE ANY` (and the
// NOT variants) as a genuine draken-native kernel. Zero RE2, zero Python.
//
// The pattern set is ALWAYS a pre-compiled matcher blob, produced at plan time
// by opteryx.compiled.vector_ops.compile_like_any (see that module's docstring
// for the blob format and the shape-bucketing rationale). This kernel just
// walks that blob: literal buckets (exact/prefix/suffix) are memcmp checks, the
// contains bucket is a single Aho-Corasick pass (states LINEAR in total needle
// length — no determinisation blow-up, unlike an alternation DFA), and the
// residual bucket is the shared byte-glob matcher. First bucket that hits wins.
//
// The blob rides in `ctx` (like draken_in_list's set), copied once at bind time
// (kernel_alloc_like_any_ctx). ci (ILIKE) and negate (NOT) are carried in the
// blob flags. Two subject shapes, chosen from args[0]->type at run time:
//   - scalar VARCHAR/NVARCHAR/VARBINARY: one verdict per string (dict-deduped).
//   - ARRAY<VARCHAR>: row matches iff ANY element matches — args[1] is the flat
//     child vector (BC_C_NATIVE_CHILD), args[0] carries the int32 offsets.
//
// SQL three-valued ANY: a NULL pattern in the set (has_null) turns a non-match
// into NULL, not false; NOT flips TRUE<->FALSE and leaves NULL as NULL; a NULL
// subject row (or NULL array) is NULL.

#include <cstdint>
#include <cstring>
#include <vector>

#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"     // draken_identity_sel
#include "ops/vec_result.h"
#include "ops/kernels/result_helpers.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/glob_match.h"   // draken_glob::like_match — shared with draken_like
#include "ops/kernels/utf8_ci_match.h" // draken_utf8ci::casefold — Unicode ci fold (NVARCHAR)

namespace {

inline bool la_row_valid(const DrakenVector* v, uint32_t row) {
    return v->validity == nullptr || ((v->validity[row >> 3] >> (row & 7)) & 1u);
}

inline bool la_is_string(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

inline uint32_t la_rd_u32(const uint8_t*& p) {
    uint32_t v = static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
                 (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
    p += 4;
    return v;
}

struct Str { const uint8_t* p; uint32_t n; };

// Parsed view over the compile_like_any blob (no copies — points into the ctx).
struct Matcher {
    bool ci = false, negate = false, always = false, has_null = false;
    std::vector<Str> exact, prefix, suffix, glob;
    uint32_t ac_n = 0;
    const uint8_t* accept = nullptr;      // accept bitmap (ac_n bits)
    const uint32_t* next = nullptr;       // ac_n * 256 transition table (LE targets)

    bool parse(const uint8_t* b, size_t len) {
        if (len < 8) return false;
        const uint8_t* q = b;
        const uint8_t* end = b + len;
        if (*q++ != 1) return false;      // version
        uint8_t flags = *q++;
        ci = (flags & 1) != 0;
        negate = (flags & 2) != 0;
        always = (*q++ != 0);
        has_null = (*q++ != 0);
        if (!parse_strs(q, end, exact)) return false;
        if (!parse_strs(q, end, prefix)) return false;
        if (!parse_strs(q, end, suffix)) return false;
        if (!parse_strs(q, end, glob)) return false;
        if (end - q < 4) return false;
        ac_n = la_rd_u32(q);
        if (ac_n > 0) {
            size_t abl = (static_cast<size_t>(ac_n) + 7) / 8;
            size_t tbl = static_cast<size_t>(ac_n) * 256 * 4;
            if (static_cast<size_t>(end - q) != abl + tbl) return false;
            accept = q;
            next = reinterpret_cast<const uint32_t*>(q + abl);
        } else if (q != end) {
            return false;
        }
        return true;
    }

    static bool parse_strs(const uint8_t*& q, const uint8_t* end, std::vector<Str>& out) {
        if (end - q < 4) return false;
        uint32_t n = la_rd_u32(q);
        out.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            if (end - q < 4) return false;
            uint32_t l = la_rd_u32(q);
            if (static_cast<size_t>(end - q) < l) return false;
            out.push_back(Str{q, l});
            q += l;
        }
        return true;
    }

    inline bool ac_accepts(uint32_t st) const {
        return ((accept[st >> 3] >> (st & 7)) & 1u) != 0;
    }

    // Does any residual glob pattern contain byte `b`? Used to reject `_`
    // (one-BYTE wildcard) against NVARCHAR, where one byte != one codepoint —
    // the same byte-safety contract draken_like enforces. `_`/`%` are ASCII
    // (< 0x80) so they survive casefold unchanged and this byte scan is exact.
    bool glob_has_byte(uint8_t b) const {
        for (const Str& g : glob)
            for (uint32_t i = 0; i < g.n; ++i)
                if (g.p[i] == b) return true;
        return false;
    }

    // Raw match (real patterns only; negate/null resolved by the caller).
    // `sl`/`sll` = the subject already folded to the SAME canonical form the
    // needle buckets were folded to at plan time (ASCII fold for VARCHAR,
    // Unicode simple-fold for NVARCHAR; identity when not ci). Every bucket —
    // including the residual glob — therefore matches byte-exact (ci=false):
    // the fold already happened, so no matcher re-folds. This is why the glob
    // needs no separate raw subject and no ASCII `ci` flag.
    bool matches(const uint8_t* sl, uint32_t sll) const {
        if (always) return true;
        for (const Str& e : exact)
            if (e.n == sll && std::memcmp(sl, e.p, e.n) == 0) return true;
        for (const Str& p : prefix)
            if (p.n <= sll && std::memcmp(sl, p.p, p.n) == 0) return true;
        for (const Str& s2 : suffix)
            if (s2.n <= sll && std::memcmp(sl + (sll - s2.n), s2.p, s2.n) == 0) return true;
        if (ac_n > 0) {
            uint32_t st = 0;
            if (ac_accepts(0)) return true;
            for (uint32_t i = 0; i < sll; ++i) {
                st = next[st * 256u + sl[i]];
                if (ac_accepts(st)) return true;
            }
        }
        for (const Str& g : glob)
            if (draken_glob::like_match(sl, sll, g.p, g.n, false)) return true;
        return false;
    }
};

// Resolve raw hit -> (set output bit?, is row NULL?) under negate + has_null.
inline void la_resolve(const Matcher& m, bool hit, bool& set_bit, bool& is_null) {
    bool like_true, like_null;
    if (hit) { like_true = true; like_null = false; }
    else if (m.has_null) { like_true = false; like_null = true; }
    else { like_true = false; like_null = false; }
    if (m.negate && !like_null) like_true = !like_true;
    set_bit = like_true && !like_null;
    is_null = like_null;
}

// Fold a subject into `scratch` when ci; return the (ptr,len) to match against.
// `utf8` selects Unicode simple-fold (NVARCHAR) vs ASCII fold (VARCHAR); it MUST
// match the fold the plan-time compiler applied to the needles (compile_like_any
// always Unicode-folds, which equals ASCII fold on the ASCII bytes a VARCHAR
// holds — so ASCII fold here is the same bytes, just cheaper). Both folds are
// byte-length-preserving, so scratch stays size n.
inline const uint8_t* la_fold(const Matcher& m, bool utf8, const uint8_t* s, uint32_t n,
                              std::vector<uint8_t>& scratch) {
    if (!m.ci) return s;
    scratch.resize(n);
    if (utf8) {
        draken_utf8ci::casefold(s, n, scratch.data());
    } else {
        for (uint32_t i = 0; i < n; ++i) scratch[i] = draken_glob::ascii_lower(s[i]);
    }
    return scratch.empty() ? s : scratch.data();
}

}  // namespace

extern "C" {

VecResult draken_like_any(void* ctx, const DrakenVector* const* args, uint32_t nargs) {
    if (nargs < 1)
        return draken_error_sentinel("draken_like_any: expected at least 1 argument");
    if (ctx == nullptr)
        return draken_error_sentinel("draken_like_any: missing bind-time ctx (matcher blob)");

    const auto* blob = reinterpret_cast<const uint8_t*>(ctx);
    // The ctx block is [u32 blob_len][blob bytes]; kernel_alloc_like_any_ctx wrote it.
    uint32_t blob_len = static_cast<uint32_t>(blob[0]) | (static_cast<uint32_t>(blob[1]) << 8) |
                        (static_cast<uint32_t>(blob[2]) << 16) | (static_cast<uint32_t>(blob[3]) << 24);
    Matcher m;
    if (!m.parse(blob + 4, blob_len))
        return draken_error_sentinel("draken_like_any: malformed matcher blob");

    const DrakenVector* subject = args[0];
    const uint32_t n = subject->length;
    const size_t nb = (static_cast<size_t>(n) + 7) / 8;
    const size_t nb_alloc = nb > 0 ? nb : 1;

    auto* out = static_cast<uint8_t*>(draken_malloc(nb_alloc));
    if (out == nullptr) return draken_error_sentinel("allocation failed");
    std::memset(out, 0, nb_alloc);

    // Validity is needed when the subject can be null OR a NULL pattern makes
    // non-matches NULL (three-valued ANY).
    uint8_t* validity = nullptr;
    if (subject->validity != nullptr || m.has_null) {
        validity = static_cast<uint8_t*>(draken_malloc(nb_alloc));
        if (validity == nullptr) { draken_free(out); return draken_error_sentinel("allocation failed"); }
        std::memset(validity, 0xFF, nb_alloc);
        if (n > 0 && (n & 7) != 0)
            validity[nb - 1] &= static_cast<uint8_t>((1u << (n & 7)) - 1);
    }

    std::vector<uint8_t> scratch;

    if (subject->type == DRAKEN_ARRAY) {
        // ARRAY<string> subject: row matches iff ANY element matches. Child is
        // the flat element vector, passed as args[1] via BC_C_NATIVE_CHILD.
        if (nargs < 2)
            { draken_free(out); if (validity) draken_free(validity);
              return draken_error_sentinel("draken_like_any: ARRAY subject needs a child operand"); }
        const DrakenVector* child = args[1];
        if (!la_is_string(child->type))
            { draken_free(out); if (validity) draken_free(validity);
              return draken_error_sentinel("draken_like_any: ARRAY elements must be string-typed"); }
        // VARBINARY elements are a legal ILIKE ANY subject: la_fold with utf8=false
        // is the ASCII fold VARCHAR already gets. See draken_like for the ruling.
        const bool child_utf8 = child->type == DRAKEN_NVARCHAR;
        if (child_utf8 && m.glob_has_byte('_'))
            { draken_free(out); if (validity) draken_free(validity);
              return draken_error_sentinel("draken_like_any: '_' against NVARCHAR (UTF-8) is not byte-safe (fail loud)"); }
        const int32_t* offsets = static_cast<const int32_t*>(subject->data);
        const auto* csa = static_cast<const DrakenStringArena*>(child->data);
        for (uint32_t i = 0; i < n; ++i) {
            if (!la_row_valid(subject, i)) {
                if (validity) validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            const int32_t start = offsets[subject->selection[i]];
            const int32_t end = offsets[subject->selection[i] + 1u];
            bool hit = false;
            for (int32_t j = start; j < end && !hit; ++j) {
                const uint32_t jj = static_cast<uint32_t>(j);
                if (!la_row_valid(child, jj)) continue;   // NULL element never matches
                const DrakenStringSlot* es = &csa->slots[child->selection[jj]];
                const uint8_t* ed = reinterpret_cast<const uint8_t*>(str_data(es, csa->arena));
                const uint32_t el = str_length(es);
                const uint8_t* ef = la_fold(m, child_utf8, ed, el, scratch);
                if (m.matches(ef, el)) hit = true;
            }
            bool set_bit, is_null;
            la_resolve(m, hit, set_bit, is_null);
            if (is_null) { if (validity) validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7))); }
            else if (set_bit) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    } else {
        if (!la_is_string(subject->type))
            { draken_free(out); if (validity) draken_free(validity);
              return draken_error_sentinel("draken_like_any: string or ARRAY<string> subject required"); }
        // A VARBINARY subject folds ASCII, like VARCHAR — see draken_like.
        const bool subj_utf8 = subject->type == DRAKEN_NVARCHAR;
        if (subj_utf8 && m.glob_has_byte('_'))
            { draken_free(out); if (validity) draken_free(validity);
              return draken_error_sentinel("draken_like_any: '_' against NVARCHAR (UTF-8) is not byte-safe (fail loud)"); }
        const auto* vsa = static_cast<const DrakenStringArena*>(subject->data);
        // §11 dict-dedup: match each distinct physical value once, scatter.
        const uint32_t k = subject->data_length;
        std::vector<uint8_t> uhit(k > 0 ? k : 1, 0);
        for (uint32_t j = 0; j < k; ++j) {
            const DrakenStringSlot* vs = &vsa->slots[j];
            const uint8_t* vd = reinterpret_cast<const uint8_t*>(str_data(vs, vsa->arena));
            const uint32_t vl = str_length(vs);
            const uint8_t* vf = la_fold(m, subj_utf8, vd, vl, scratch);
            uhit[j] = m.matches(vf, vl) ? 1 : 0;
        }
        for (uint32_t i = 0; i < n; ++i) {
            if (!la_row_valid(subject, i)) {
                if (validity) validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7)));
                continue;
            }
            bool set_bit, is_null;
            la_resolve(m, uhit[subject->selection[i]] != 0, set_bit, is_null);
            if (is_null) { if (validity) validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7))); }
            else if (set_bit) out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
    }

    VecResult r{};
    r.data = out;
    r.validity = validity;
    r.selection = draken_identity_sel(n);
    r.owns_selection = false;
    r.data_length = n;
    r.length = n;
    r.type = DRAKEN_BOOL;
    r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
    return r;
}

}  // extern "C"
