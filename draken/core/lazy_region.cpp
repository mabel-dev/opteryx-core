// Row selection, narrowing and scatter for LAZY branch evaluation — see
// lazy_region.h. All access goes through the uniform data[selection[i]] path
// (CLAUDE.md §11); nothing here specialises on encoding shape.
#include "lazy_region.h"

#include <cstdint>
#include <cstring>
#include <exception>
#include <new>
#include <vector>

#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "ops/string_gather.h"              // str_take (string family)
#include "ops/kernels/error_handling.h"     // draken_error_sentinel[_fmt]

namespace {

constexpr uint32_t kAbsent = 0xFFFFFFFFu;

inline bool bit_at(const uint8_t* bm, uint32_t i) noexcept {
    return ((bm[i >> 3] >> (i & 7u)) & 1u) != 0u;
}
inline bool row_valid(const uint8_t* validity, uint32_t i) noexcept {
    return validity == nullptr || bit_at(validity, i);
}

// One guard, decoded once so the per-row loop is branch-light.
struct Guard {
    const uint8_t*  data;      // BOOL bitmap (nullptr for a non-BOOL / NULL-typed guard)
    const uint32_t* sel;
    const uint8_t*  validity;
    bool            all_null;  // DRAKEN_NULL: every row is NULL
    bool valid(uint32_t i) const noexcept { return !all_null && row_valid(validity, i); }
    bool value(uint32_t i) const noexcept { return data != nullptr && bit_at(data, sel[i]); }
};

inline Guard make_guard(const DrakenVector* g) noexcept {
    Guard o;
    o.all_null = (g->type == DRAKEN_NULL);
    o.data     = (g->type == DRAKEN_BOOL) ? static_cast<const uint8_t*>(g->data) : nullptr;
    o.sel      = g->selection;
    o.validity = g->validity;
    return o;
}

// Byte width of one fixed-width value; 0 = not a fixed-width type handled here.
inline size_t fixed_width(DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8:  case DRAKEN_UINT8:  return 1;
        case DRAKEN_INT16: case DRAKEN_UINT16: return 2;
        case DRAKEN_INT32: case DRAKEN_UINT32: case DRAKEN_FLOAT32:
        case DRAKEN_DATE32: case DRAKEN_TIME32: return 4;
        case DRAKEN_INT64: case DRAKEN_UINT64: case DRAKEN_FLOAT64: case DRAKEN_DECIMAL:
        case DRAKEN_TIMESTAMP64: case DRAKEN_TIME64: return 8;
        case DRAKEN_DECIMAL128: case DRAKEN_INTERVAL: return 16;
        default: return 0;
    }
}

inline bool is_string_family(DrakenType t) noexcept {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY ||
           t == DRAKEN_VARIANT;
}

inline VecResult dense_result(void* data, uint8_t* validity, uint32_t m, DrakenType t) {
    VecResult r{};
    r.data           = data;
    r.validity       = validity;
    r.selection      = draken_identity_sel(m);
    r.owns_selection = false;
    r.data_length    = m;
    r.length         = m;
    r.type           = t;
    r.flags          = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
    return r;
}

// out row i = row idx[i] of v, or a NULL row where idx[i] == kAbsent.
VecResult lz_gather(const DrakenVector& v, const uint32_t* idx, uint32_t m) {
    bool has_absent = false;
    for (uint32_t i = 0; i < m; ++i) has_absent |= (idx[i] == kAbsent);
    const bool need_validity = has_absent || v.validity != nullptr;
    const size_t vb = (static_cast<size_t>(m) + 7u) >> 3;

    if (is_string_family(v.type)) {
        std::vector<int32_t> ix(m);
        for (uint32_t i = 0; i < m; ++i)
            ix[i] = idx[i] == kAbsent ? 0 : static_cast<int32_t>(idx[i]);
        VecResult r = draken::ops::str_take(v, ix.data(), m);
        if (has_absent) {
            if (r.validity == nullptr) {
                r.validity = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
                if (r.validity == nullptr) throw std::bad_alloc();
                std::memset(r.validity, 0xFF, vb > 0 ? vb : 1);
            }
            for (uint32_t i = 0; i < m; ++i)
                if (idx[i] == kAbsent) r.validity[i >> 3] &= static_cast<uint8_t>(~(1u << (i & 7u)));
        }
        return r;
    }

    uint8_t* validity = nullptr;
    if (need_validity) {
        validity = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
        if (validity == nullptr) throw std::bad_alloc();
        std::memset(validity, 0, vb > 0 ? vb : 1);
    }

    if (v.type == DRAKEN_BOOL) {
        uint8_t* data = static_cast<uint8_t*>(draken_malloc(vb > 0 ? vb : 1));
        if (data == nullptr) { draken_free(validity); throw std::bad_alloc(); }
        std::memset(data, 0, vb > 0 ? vb : 1);
        const uint8_t* src = static_cast<const uint8_t*>(v.data);
        for (uint32_t i = 0; i < m; ++i) {
            if (idx[i] == kAbsent) continue;
            const uint32_t s = idx[i];
            if (!row_valid(v.validity, s)) continue;
            if (validity != nullptr) validity[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            if (bit_at(src, v.selection[s])) data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        }
        return dense_result(data, validity, m, DRAKEN_BOOL);
    }

    const size_t w = fixed_width(v.type);
    if (w == 0) {
        draken_free(validity);
        throw std::invalid_argument("lazy branch evaluation: unsupported operand type");
    }
    uint8_t* data = static_cast<uint8_t*>(draken_malloc((m > 0 ? m : 1) * w));
    if (data == nullptr) { draken_free(validity); throw std::bad_alloc(); }
    const uint8_t* src = static_cast<const uint8_t*>(v.data);
    for (uint32_t i = 0; i < m; ++i) {
        if (idx[i] == kAbsent) { std::memset(data + static_cast<size_t>(i) * w, 0, w); continue; }
        const uint32_t s = idx[i];
        std::memcpy(data + static_cast<size_t>(i) * w,
                    src + static_cast<size_t>(v.selection[s]) * w, w);
        if (validity != nullptr && row_valid(v.validity, s))
            validity[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    }
    return dense_result(data, validity, m, v.type);
}

}  // namespace

extern "C" {

uint32_t draken_lz_rows(int kind, const DrakenVector* const* guards, uint32_t nguards,
                        uint32_t n, uint32_t* out_rows) {
    std::vector<Guard> gs;
    gs.reserve(nguards);
    for (uint32_t g = 0; g < nguards; ++g) gs.push_back(make_guard(guards[g]));
    uint32_t k = 0;
    for (uint32_t i = 0; i < n; ++i) {
        bool admit = true;
        switch (kind) {
            case DRAKEN_LZ_NOT_FALSE:
                for (const Guard& g : gs)
                    if (g.valid(i) && !g.value(i)) { admit = false; break; }
                break;
            case DRAKEN_LZ_NOT_TRUE:
                for (const Guard& g : gs)
                    if (g.valid(i) && g.value(i)) { admit = false; break; }
                break;
            case DRAKEN_LZ_TRUE:
                admit = gs[0].valid(i) && gs[0].value(i);
                break;
            case DRAKEN_LZ_ALL_NULL:
                for (const Guard& g : gs)
                    if (g.valid(i)) { admit = false; break; }
                break;
            case DRAKEN_LZ_VALID:
                admit = gs[0].valid(i);
                break;
            default:
                admit = true;
        }
        if (admit) out_rows[k++] = i;
    }
    return k;
}

VecResult draken_lz_narrow(const DrakenVector* v, const uint32_t* rows, uint32_t k) {
    try {
        return lz_gather(*v, rows, k);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("lazy branch evaluation: unknown error");
    }
}

VecResult draken_lz_scatter(const DrakenVector* compact, const uint32_t* rows,
                            uint32_t k, uint32_t n) {
    try {
        std::vector<uint32_t> idx(n, kAbsent);
        for (uint32_t j = 0; j < k; ++j) idx[rows[j]] = j;
        return lz_gather(*compact, idx.data(), n);
    } catch (const std::exception& e) {
        return draken_error_sentinel(e.what());
    } catch (...) {
        return draken_error_sentinel("lazy branch evaluation: unknown error");
    }
}

}  // extern "C"
