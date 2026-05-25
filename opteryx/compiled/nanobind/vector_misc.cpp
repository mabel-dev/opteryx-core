// opteryx/compiled/nanobind/vector_misc.cpp — Milestone E.14.
//
// C′ pattern: pure nanobind C++, zero Cython. One NB_MODULE, three functions.
//
//   vector_log(v, base_v)             — LOG(v, base) element-wise → FLOAT64
//   vector_in_list(v, literals, neg)  — v IN (literals) → BOOL
//   vector_ip_in_cidr(v, cidr)        — IP IN CIDR → BOOL
//
// All functions:
//   1. Unwrap nb::object operands via draken_vector_unwrap (raises TypeError).
//   2. Call kernel from draken/ops/*.h.
//   3. Wrap VecResult via draken_vector_own_raw → return new Vector.
//
// simd_hash_i64, draken_vector_unwrap, draken_vector_own_raw are resolved
// at import time via RTLD_GLOBAL set in draken/__init__.py.
//
// §1 EXCEPTION (design docs 02, 07): CarcharSet is hash-only; no key
//   verification. In_list inherits this exception, documented here.
//
// Replaces: opteryx/compiled/vector_ops/vector_{log,in_list,ip_in_cidr}.pyx
//           (deleted as part of E.14).

#include <Python.h>
#include <nanobind/nanobind.h>

#include <cstdint>
#include <cstring>
#include <cmath>
#include <stdexcept>

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#endif
#if defined(__AVX2__)
#include <immintrin.h>
#endif
#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>
#endif

// Draken core
#include "core/buffers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/draken_bridge.h"
#include "core/string_slot.h"   // draken_build_string_slot, str_hash_seed, str_data, str_length

// Draken ops
#include "ops/vec_result.h"
#include "ops/float_log.h"          // draken::ops::float_log
#include "ops/int64_predicates.h"   // draken::ops::i64_in_list
#include "ops/string_predicates.h"  // draken::ops::str_in_list
#include "ops/float_ops.h"          // draken::ops::f32_in_list, f64_in_list, fp_canon, fp_bits64
#include "ops/bool_logical.h"       // bool_get_val
#include "ops/int64_compare.h"      // cmp_alloc_bool_buf, cmp_copy_validity

// Hashing
#include "simd_hash.h"              // simd_hash_i64 (RTLD_GLOBAL), NULL_HASH
#include "carchar_set.hpp"          // opteryx::carchar::CarcharSet

namespace nb = nanobind;
using CarcharSet = opteryx::carchar::CarcharSet;

// ---------------------------------------------------------------------------
// Shared unwrap / wrap helpers (mirror vector_math.cpp)
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    return dv;
}

static nb::object wrap(VecResult res) {
    PyObject* out = draken_vector_own_raw(res.data, res.validity, res.length, res.type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// vector_in_list helpers
// ---------------------------------------------------------------------------

// Invert a bit-packed bool result in-place, applying the validity mask so
// null rows remain 0.  Tail bits beyond n are cleared unconditionally.
//
// SIMD: two paths share the same structure — load 16/32/vl bytes, apply NOT
// (and optional AND with validity), store.  The partial-tail byte is patched
// scalar after the SIMD loop since it requires a bit-mask that differs per call.
static inline void bm_negate_inplace(
    uint8_t* data, const uint8_t* validity, uint32_t n) noexcept
{
    const uint32_t nb = (n + 7u) >> 3;

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
    // 4×16 = 64-byte unroll; matches the compiler's auto-vectorisation grain
    // and avoids a regression vs the compiler's own loop on large bitmaps.
    uint32_t i = 0;
    if (validity == nullptr) {
        for (; i + 64u <= nb; i += 64u) {
            vst1q_u8(data+i+ 0, vmvnq_u8(vld1q_u8(data+i+ 0)));
            vst1q_u8(data+i+16, vmvnq_u8(vld1q_u8(data+i+16)));
            vst1q_u8(data+i+32, vmvnq_u8(vld1q_u8(data+i+32)));
            vst1q_u8(data+i+48, vmvnq_u8(vld1q_u8(data+i+48)));
        }
        for (; i + 16u <= nb; i += 16u) vst1q_u8(data+i, vmvnq_u8(vld1q_u8(data+i)));
        for (; i < nb; ++i) data[i] = ~data[i];
    } else {
        for (; i + 64u <= nb; i += 64u) {
            // dest = ~data & validity (fused, one pass, no temp buffer)
            vst1q_u8(data+i+ 0, vandq_u8(vmvnq_u8(vld1q_u8(data+i+ 0)), vld1q_u8(validity+i+ 0)));
            vst1q_u8(data+i+16, vandq_u8(vmvnq_u8(vld1q_u8(data+i+16)), vld1q_u8(validity+i+16)));
            vst1q_u8(data+i+32, vandq_u8(vmvnq_u8(vld1q_u8(data+i+32)), vld1q_u8(validity+i+32)));
            vst1q_u8(data+i+48, vandq_u8(vmvnq_u8(vld1q_u8(data+i+48)), vld1q_u8(validity+i+48)));
        }
        for (; i + 16u <= nb; i += 16u)
            vst1q_u8(data+i, vandq_u8(vmvnq_u8(vld1q_u8(data+i)), vld1q_u8(validity+i)));
        for (; i < nb; ++i) data[i] = ~data[i] & validity[i];
    }

#elif defined(__AVX2__)
    uint32_t i = 0;
    const __m256i ones = _mm256_set1_epi8(-1);
    if (validity == nullptr) {
        for (; i + 32u <= nb; i += 32u) {
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i),
                                _mm256_xor_si256(v, ones));
        }
        for (; i < nb; ++i) data[i] = ~data[i];
    } else {
        for (; i + 32u <= nb; i += 32u) {
            __m256i d = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            __m256i vl = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(validity + i));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i),
                                _mm256_and_si256(_mm256_xor_si256(d, ones), vl));
        }
        for (; i < nb; ++i) data[i] = ~data[i] & validity[i];
    }

#elif defined(__riscv) && defined(__riscv_vector)
    size_t i = 0;
    if (validity == nullptr) {
        while (i < nb) {
            size_t vl = __riscv_vsetvl_e8m8(nb - i);
            vuint8m8_t v = __riscv_vle8_v_u8m8(data + i, vl);
            __riscv_vse8_v_u8m8(data + i,
                __riscv_vxor_vx_u8m8(v, (uint8_t)0xFF, vl), vl);
            i += vl;
        }
    } else {
        while (i < nb) {
            size_t vl = __riscv_vsetvl_e8m8(nb - i);
            vuint8m8_t d = __riscv_vle8_v_u8m8(data + i, vl);
            vuint8m8_t vv = __riscv_vle8_v_u8m8(validity + i, vl);
            __riscv_vse8_v_u8m8(data + i,
                __riscv_vand_vv_u8m8(__riscv_vxor_vx_u8m8(d, (uint8_t)0xFF, vl), vv, vl), vl);
            i += vl;
        }
    }

#else
    if (validity == nullptr) {
        for (uint32_t i = 0; i < nb; ++i) data[i] = ~data[i];
    } else {
        for (uint32_t i = 0; i < nb; ++i) data[i] = ~data[i] & validity[i];
    }
#endif

    // Clear tail bits in the partial last byte so callers see a clean bitmap.
    if (n & 7u)
        data[nb - 1u] &= static_cast<uint8_t>((1u << (n & 7u)) - 1u);
}

// ---------------------------------------------------------------------------
// Build a CarcharSet from a Python sequence of literals.
//
// The hash path is determined by `vtype` (the probe vector type) so that
// set-building and probe-side hashes are identical — §1 exception inherited.
//
// Python literal types expected per vtype family:
//   INT family   : int, bool (bool is subclass of int in Python)
//   FLOAT family : float (or int, promoted to double)
//   STRING family: bytes
//   BOOL         : bool (or int 0/1)
//   None         : skipped (null rows never probe the set)
//
// Raises TypeError for unexpected literal types.
// ---------------------------------------------------------------------------

static CarcharSet build_carchar_from_list(nb::object literals, DrakenType vtype) {
    // Identify which hash family to use
    const bool is_int_family = (vtype == DRAKEN_INT8  || vtype == DRAKEN_INT16
                              || vtype == DRAKEN_INT32  || vtype == DRAKEN_INT64
                              || vtype == DRAKEN_DATE32 || vtype == DRAKEN_TIME32
                              || vtype == DRAKEN_TIME64 || vtype == DRAKEN_TIMESTAMP64);
    const bool is_float_family = (vtype == DRAKEN_FLOAT32 || vtype == DRAKEN_FLOAT64);
    const bool is_str_family   = (vtype == DRAKEN_VARCHAR || vtype == DRAKEN_NVARCHAR
                                || vtype == DRAKEN_VARBINARY);
    const bool is_bool         = (vtype == DRAKEN_BOOL);

    if (!is_int_family && !is_float_family && !is_str_family && !is_bool)
        throw std::invalid_argument("vector_in_list: unsupported vector type");

    // Use iteration protocol so lists, tuples, and sets all work.
    PyObject* iter = PyObject_GetIter(literals.ptr());
    if (!iter) throw nb::python_error();
    nb::object iter_obj = nb::steal<nb::object>(iter);

    // Size hint for CarcharSet: use len() if available, else default.
    Py_ssize_t hint = PyObject_Size(literals.ptr());
    if (hint < 0) { PyErr_Clear(); hint = 8; }

    CarcharSet set(static_cast<size_t>(hint > 0 ? hint : 1));

    uint64_t scratch, hash_out;

    while (true) {
        PyObject* raw_item = PyIter_Next(iter);
        if (!raw_item) {
            if (PyErr_Occurred()) throw nb::python_error();
            break;
        }
        nb::object item = nb::steal<nb::object>(raw_item);

        if (item.is_none()) continue;  // NULL rows never probe; skip.

        if (is_int_family || is_bool) {
            // Hash as int64: same cast as i64_in_list probe side.
            int64_t val;
            if (PyBool_Check(item.ptr())) {
                val = (item.ptr() == Py_True) ? int64_t(1) : int64_t(0);
            } else {
                val = PyLong_AsLongLong(item.ptr());
                if (val == -1 && PyErr_Occurred()) throw nb::python_error();
            }
            scratch = static_cast<uint64_t>(val);
            simd_hash_i64(&scratch, &hash_out, 1);
            set.insert_or_ignore(hash_out);

        } else if (is_float_family) {
            // Hash as double via fp_canon + fp_bits64: same path as f32/f64_in_list.
            double dval;
            if (PyFloat_Check(item.ptr())) {
                dval = PyFloat_AsDouble(item.ptr());
            } else if (PyLong_Check(item.ptr())) {
                dval = PyLong_AsDouble(item.ptr());
                if (dval == -1.0 && PyErr_Occurred()) throw nb::python_error();
            } else {
                throw std::invalid_argument(
                    "vector_in_list: FLOAT IN LIST expects float/int literals");
            }
            scratch = draken::ops::fp_bits64(draken::ops::fp_canon(dval));
            simd_hash_i64(&scratch, &hash_out, 1);
            set.insert_or_ignore(hash_out);

        } else {
            // STRING family: hash via str_hash_seed → simd_hash_i64 (same as str_in_list).
            if (!PyBytes_Check(item.ptr()))
                throw std::invalid_argument(
                    "vector_in_list: STRING IN LIST expects bytes literals");

            const uint8_t* bytes = reinterpret_cast<const uint8_t*>(
                PyBytes_AS_STRING(item.ptr()));
            const uint32_t len = static_cast<uint32_t>(PyBytes_GET_SIZE(item.ptr()));

            DrakenStringSlot slot;
            draken_build_string_slot(&slot, bytes, len, 0u);
            scratch = draken::ops::str_hash_seed(&slot);
            simd_hash_i64(&scratch, &hash_out, 1);
            set.insert_or_ignore(hash_out);
        }
    }

    return set;
}

// ---------------------------------------------------------------------------
// vector_in_list dispatch — calls the type-specific *_in_list kernel.
// ---------------------------------------------------------------------------

static VecResult dispatch_in_list(
    const DrakenVector& v, const CarcharSet& set)
{
    switch (v.type) {
        case DRAKEN_INT8:
        case DRAKEN_INT16:
        case DRAKEN_INT32:
        case DRAKEN_INT64:
        case DRAKEN_DATE32:
        case DRAKEN_TIME32:
        case DRAKEN_TIME64:
        case DRAKEN_TIMESTAMP64:
            return draken::ops::i64_in_list(v, set);

        case DRAKEN_FLOAT32:
            return draken::ops::f32_in_list(v, set);
        case DRAKEN_FLOAT64:
            return draken::ops::f64_in_list(v, set);

        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARBINARY:
            return draken::ops::str_in_list(v, set);

        case DRAKEN_BOOL: {
            // Bit-packed BOOL: extract each value as int64 (0/1) and hash via int64 path.
            const uint32_t  n        = v.length;
            const uint8_t*  data     = static_cast<const uint8_t*>(v.data);
            const uint8_t*  src_null = v.validity;

            uint8_t* dst = draken::ops::cmp_alloc_bool_buf(n);
            uint8_t* out_null = nullptr;
            if (src_null != nullptr) {
                try { out_null = draken::ops::cmp_copy_validity(src_null, n); }
                catch (...) { draken_free(dst); throw; }
            }

            uint64_t scratch[1024], hashes[1024];
            uint32_t i = 0;
            while (i < n) {
                const uint32_t block = (n - i < 1024u) ? (n - i) : 1024u;
                for (uint32_t j = 0; j < block; ++j)
                    scratch[j] = static_cast<uint64_t>(
                        draken::ops::bool_get_val(data, v.selection[i + j]));
                simd_hash_i64(scratch, hashes, block);
                if (src_null == nullptr) {
                    for (uint32_t j = 0; j < block; ++j)
                        if (set.contains(hashes[j]))
                            dst[(i + j) >> 3] |= static_cast<uint8_t>(1u << ((i + j) & 7));
                } else {
                    for (uint32_t j = 0; j < block; ++j) {
                        const uint32_t row = i + j;
                        if ((src_null[row >> 3] >> (row & 7)) & 1u)
                            if (set.contains(hashes[j]))
                                dst[row >> 3] |= static_cast<uint8_t>(1u << (row & 7));
                    }
                }
                i += block;
            }

            VecResult r;
            r.data = dst; r.validity = out_null;
            r.selection = draken_identity_sel(n); r.owns_selection = false;
            r.data_length = n; r.length = n; r.type = DRAKEN_BOOL;
            r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
            return r;
        }

        default:
            throw std::invalid_argument("vector_in_list: unsupported vector type");
    }
}

// ---------------------------------------------------------------------------
// vector_ip_in_cidr helpers
// ---------------------------------------------------------------------------

// Parse an IPv4 address from a NUL-free byte buffer.
// Returns 0 on success, -1 on any parse error.
static int parse_ip_to_int(
    const uint8_t* ip, uint32_t length, uint32_t* out) noexcept
{
    uint32_t result = 0u;
    uint32_t num;
    int shift = 24;
    uint32_t i = 0;
    int octet_count = 0;

    while (octet_count < 4) {
        num = 0u;
        int digit_count = 0;
        while (i < length) {
            uint8_t c = ip[i];
            if (c < '0' || c > '9') break;
            num = num * 10u + static_cast<uint32_t>(c - '0');
            ++digit_count;
            ++i;
        }
        if (digit_count == 0) return -1;
        if (num > 255u) return -1;
        result += (num << shift);
        shift -= 8;
        ++octet_count;
        if (octet_count < 4) {
            if (i >= length || ip[i] != '.') return -1;
            ++i;
        } else {
            if (i < length) return -1;
        }
    }
    *out = result;
    return 0;
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_misc, m) {

    m.def("vector_log",
        [](nb::object v, nb::object base_v) -> nb::object {
            return wrap(draken::ops::float_log(*unwrap(v), *unwrap(base_v)));
        },
        nb::arg("v"), nb::arg("base_v"),
        "LOG(v, base): element-wise ln(v)/ln(base) → FLOAT64. "
        "INT types promoted to FLOAT64. Broadcast: one operand may have length==1. "
        "IEEE semantics: log(0)=-inf, log(-1)=NaN, log(1)=0. NULL propagates.");

    m.def("vector_in_list",
        [](nb::object v_obj, nb::object literals, bool negate) -> nb::object {
            const DrakenVector* dv = unwrap(v_obj);
            CarcharSet set = build_carchar_from_list(literals, dv->type);
            VecResult res = dispatch_in_list(*dv, set);
            if (negate)
                bm_negate_inplace(
                    static_cast<uint8_t*>(res.data), res.validity, res.length);
            return wrap(res);
        },
        nb::arg("v"), nb::arg("literals"), nb::arg("negate") = false,
        "v IN literals: hash-only membership test → BOOL. "
        "literals is a Python sequence of scalars (bytes/int/float/bool/None). "
        "§1 EXCEPTION: hash-only, no key verification. NULL propagates (TVL). "
        "Set negate=True for NOT IN LIST.");

    m.def("vector_ip_in_cidr",
        [](nb::object v_obj, nb::object cidr_obj) -> nb::object {
            const DrakenVector* dv = unwrap(v_obj);
            if (dv->type != DRAKEN_VARCHAR && dv->type != DRAKEN_NVARCHAR
                    && dv->type != DRAKEN_VARBINARY)
                throw std::invalid_argument(
                    "vector_ip_in_cidr: v must be a string (VARCHAR) vector");

            const DrakenVector* cv = unwrap(cidr_obj);
            if (cv->type != DRAKEN_VARCHAR && cv->type != DRAKEN_NVARCHAR
                    && cv->type != DRAKEN_VARBINARY)
                throw std::invalid_argument(
                    "vector_ip_in_cidr: cidr must be a string (VARCHAR) vector");
            if (cv->length == 0)
                throw std::invalid_argument("vector_ip_in_cidr: cidr vector must not be empty");

            // Read CIDR string from row 0 of cidr vector.
            if (cv->validity != nullptr && !((cv->validity[0] >> 0) & 1u))
                throw std::invalid_argument("vector_ip_in_cidr: cidr row 0 must not be NULL");

            const DrakenStringArena* ca =
                static_cast<const DrakenStringArena*>(cv->data);
            const DrakenStringSlot* cslot = &ca->slots[cv->selection[0]];
            const uint8_t* cidr_bytes  = str_data(cslot, ca->arena);
            const uint32_t cidr_len    = str_length(cslot);

            // Find '/' separator.
            uint32_t slash = 0;
            while (slash < cidr_len && cidr_bytes[slash] != '/') ++slash;
            if (slash == cidr_len)
                throw std::invalid_argument(
                    "vector_ip_in_cidr: CIDR notation missing '/'");

            // Parse mask size.
            const uint8_t* mask_str = cidr_bytes + slash + 1u;
            const uint32_t mask_len = cidr_len - slash - 1u;
            uint32_t mask_size = 0u;
            for (uint32_t k = 0; k < mask_len; ++k) {
                uint8_t c = mask_str[k];
                if (c < '0' || c > '9')
                    throw std::invalid_argument(
                        "vector_ip_in_cidr: CIDR mask is not a valid integer");
                mask_size = mask_size * 10u + static_cast<uint32_t>(c - '0');
            }
            if (mask_size > 32u)
                throw std::invalid_argument(
                    "vector_ip_in_cidr: CIDR mask out of range (> 32)");

            uint32_t netmask = mask_size == 0u
                ? 0u
                : (0xFFFFFFFFu << (32u - mask_size)) & 0xFFFFFFFFu;

            uint32_t base_ip = 0u;
            if (parse_ip_to_int(cidr_bytes, slash, &base_ip) != 0)
                throw std::invalid_argument(
                    "vector_ip_in_cidr: invalid CIDR base address");
            base_ip &= netmask;

            // Probe each row of dv (the IP vector).
            const uint32_t          n     = dv->length;
            const DrakenStringArena* arena =
                static_cast<const DrakenStringArena*>(dv->data);
            const DrakenStringSlot*  slots = arena->slots;
            const uint8_t*           nulls = dv->validity;

            uint8_t* dst = draken::ops::cmp_alloc_bool_buf(n);

            for (uint32_t i = 0; i < n; ++i) {
                if (nulls != nullptr && !((nulls[i >> 3] >> (i & 7)) & 1u))
                    continue;

                const DrakenStringSlot* slot = &slots[dv->selection[i]];
                const uint32_t slen  = str_length(slot);
                const uint8_t* sptr  = str_data(slot, arena->arena);

                if (slen == 0) continue;

                uint32_t ip_int = 0u;
                if (parse_ip_to_int(sptr, slen, &ip_int) != 0) {
                    // Copy up to 64 bytes for the error message (safe local buffer).
                    char buf[68];
                    uint32_t cp = slen < 64u ? slen : 64u;
                    std::memcpy(buf, sptr, cp);
                    buf[cp] = '\0';
                    draken_free(dst);
                    throw std::invalid_argument(
                        std::string("vector_ip_in_cidr: invalid IP address: ") + buf);
                }

                if ((ip_int & netmask) == base_ip)
                    dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
            }

            VecResult r;
            r.data     = dst;
            r.validity = nullptr;
            r.selection      = draken_identity_sel(n);
            r.owns_selection = false;
            r.data_length    = n;
            r.length         = n;
            r.type           = DRAKEN_BOOL;
            r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
            return wrap(r);
        },
        nb::arg("v"), nb::arg("cidr"),
        "v IN CIDR: per-row IPv4 CIDR membership → BOOL. "
        "cidr is a string Vector; value is read from row 0. "
        "Invalid IP raises ValueError. Null input rows → False (not NULL).");

}
