// opteryx/compiled/nanobind/vector_misc.cpp — Milestone E.14.
//
// C′ pattern: pure nanobind C++, zero Cython. One NB_MODULE, three functions.
//
//   vector_log(v, base_v)             — LOG(v, base) element-wise → FLOAT64
//   vector_in_list(v, literals, neg)  — v IN (literals) → BOOL
//   vector_ipv4_in_cidr(v, cidr)      — IPv4 (uint32) IN CIDR → BOOL
//
// All functions:
//   1. Unwrap nb::object operands via draken_vector_unwrap (raises TypeError).
//   2. Call kernel from draken/ops/*.h.
//   3. Wrap VecResult via draken_vector_own_raw → return new Vector.
//
// simd_hash_i64, draken_vector_unwrap, draken_vector_own_raw are resolved
// at import time via RTLD_GLOBAL set in draken/__init__.py.
//
// CarcharSet is hash-only; no key verification. String literals use the same
// full-content long-string hash path as draken/ops/string_hash.h.
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
#include "core/ipv4.h"          // draken::ipv4::parse_cidr, netmask (shared with casts/IP_TRUNC)
#include "core/string_slot.h"   // draken_build_string_slot, str_hash_seed, str_data, str_length

// Draken ops
#include "ops/vec_result.h"
#include "ops/float_log.h"          // draken::ops::float_log
#include "ops/int64_predicates.h"   // draken::ops::i64_in_list
#include "ops/fixed_int_ops.h"      // draken::ops::i8_in_list, i16_in_list, i32_in_list
#include "ops/string_predicates.h"  // draken::ops::str_in_list
#include "ops/float_ops.h"          // draken::ops::f32_in_list, f64_in_list, fp_canon, fp_bits64
#include "ops/bool_logical.h"       // bool_get_val
#include "ops/int64_compare.h"      // cmp_alloc_bool_buf, cmp_copy_validity

// Hashing
#include "simd_hash.h"              // simd_hash_i64 (RTLD_GLOBAL), NULL_HASH
#include "carchar_set.hpp"          // opteryx::carchar::CarcharSet

extern "C" VecResult draken_ip_trunc(void* ctx, const DrakenVector* const* args, uint32_t nargs);

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
// set-building and probe-side hashes are identical.
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
            scratch = draken::ops::str_hash_seed(&slot, bytes);
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
        // Narrow ints must use their own-width kernels: i64_in_list reads
        // v.data as int64_t*, which over-reads a 1/2/4-byte buffer and probes
        // garbage. Each fixed-width kernel reads the correct element width.
        case DRAKEN_INT8:
            return draken::ops::i8_in_list(v, set);
        case DRAKEN_INT16:
            return draken::ops::i16_in_list(v, set);
        case DRAKEN_INT32:
        // DATE32 and TIME32 are 4-byte (int32) storage. They must use the
        // 4-byte kernel: i64_in_list reads v.data as int64_t*, which over-reads
        // a 4-byte buffer and probes garbage hashes. i32_in_list reads int32 and
        // sign-extends to int64 before hashing — the same int64 hash path the
        // set-builder (build_carchar_from_list, is_int_family) uses for the
        // literals, so build and probe hashes match.
        case DRAKEN_DATE32:
        case DRAKEN_TIME32:
            return draken::ops::i32_in_list(v, set);
        case DRAKEN_INT64:
        // TIME64 and TIMESTAMP64 are genuinely 8-byte; they stay on i64_in_list.
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
// NB_MODULE
// ---------------------------------------------------------------------------

void register_vector_misc(nb::module_ &m) {

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
        "Hash-only, no key verification. NULL propagates (TVL). "
        "Set negate=True for NOT IN LIST.");

    // vector_ipv4_in_cidr — the UINT32 twin of vector_ip_in_cidr above, backing
    // the `<<=` / `>>=` containment operators on a native IPv4 column.
    //
    // Where the string version re-parses dotted-decimal text on EVERY row, this
    // reads the address as the 32-bit integer it already is: the whole point of
    // the IPv4 storage model. Per row the work is one load, one AND and one
    // compare — no parsing, no branching on text.
    //
    // CIDR parsing is delegated to draken::ipv4::parse_cidr so this kernel, the
    // casts and IP_TRUNC cannot disagree about what a prefix means (including
    // the /0 case, where a 32-bit shift by 32 would be undefined behaviour).
    // Python-evaluator counterpart to the registered draken_ip_trunc C-ABI kernel.
    // Calls the SAME kernel so the two paths cannot disagree on prefix validation
    // or masking. Result is UINT32; the IPV4 descriptor is re-attached from the
    // bound output type by the projection, not here.
    m.def("vector_ip_trunc",
        [](nb::object v_obj, nb::object prefix_obj) -> nb::object {
            const DrakenVector* a = unwrap(v_obj);
            const DrakenVector* p = unwrap(prefix_obj);
            const DrakenVector* argv[2] = {a, p};
            VecResult r = draken_ip_trunc(nullptr, argv, 2u);
            if (r.data == nullptr)
                throw std::invalid_argument(
                    r.error_msg ? r.error_msg : "IP_TRUNC failed");
            return wrap(r);
        },
        nb::arg("v"), nb::arg("prefix"),
        "IP_TRUNC(ip, prefix): network address of ip within a /prefix network. "
        "prefix is read from row 0 and must be 0..32; out of range raises.");

    m.def("vector_ipv4_in_cidr",
        [](nb::object v_obj, nb::object cidr_obj) -> nb::object {
            const DrakenVector* dv = unwrap(v_obj);
            if (dv->type != DRAKEN_UINT32)
                throw std::invalid_argument(
                    "vector_ipv4_in_cidr: v must be a UINT32 (IPv4) vector");

            const DrakenVector* cv = unwrap(cidr_obj);
            if (cv->type != DRAKEN_VARCHAR && cv->type != DRAKEN_NVARCHAR
                    && cv->type != DRAKEN_VARBINARY)
                throw std::invalid_argument(
                    "vector_ipv4_in_cidr: cidr must be a string (VARCHAR) vector");
            if (cv->length == 0)
                throw std::invalid_argument(
                    "vector_ipv4_in_cidr: cidr vector must not be empty");
            if (cv->validity != nullptr && !((cv->validity[0] >> 0) & 1u))
                throw std::invalid_argument(
                    "vector_ipv4_in_cidr: cidr row 0 must not be NULL");

            const DrakenStringArena* ca =
                static_cast<const DrakenStringArena*>(cv->data);
            const DrakenStringSlot* cslot = &ca->slots[cv->selection[0]];

            uint32_t base_ip = 0u;
            uint32_t prefix  = 0u;
            if (!draken::ipv4::parse_cidr(str_data(cslot, ca->arena),
                                          str_length(cslot), &base_ip, &prefix)) {
                char buf[68];
                const uint32_t slen = str_length(cslot);
                const uint32_t cp   = slen < 64u ? slen : 64u;
                std::memcpy(buf, str_data(cslot, ca->arena), cp);
                buf[cp] = '\0';
                throw std::invalid_argument(
                    std::string("vector_ipv4_in_cidr: invalid CIDR: ") + buf);
            }
            const uint32_t netmask = draken::ipv4::netmask(prefix);

            const uint32_t  n     = dv->length;
            const uint8_t*  nulls = dv->validity;
            uint8_t* dst = draken::ops::cmp_alloc_bool_buf(n);
            if (dst == nullptr)
                throw std::bad_alloc();

            // Pure C++ from here to the wrap: no Python object is touched, so the
            // GIL is released for the scan itself.
            {
                nb::gil_scoped_release _gil;
                const uint32_t* codes = dv->selection;
                const uint32_t* data  = static_cast<const uint32_t*>(dv->data);
                for (uint32_t i = 0; i < n; ++i) {
                    // A NULL address is not contained by anything. Left as 0 (false)
                    // rather than propagated as NULL, matching vector_ip_in_cidr.
                    if (nulls != nullptr && !((nulls[i >> 3] >> (i & 7)) & 1u))
                        continue;
                    if ((data[codes[i]] & netmask) == base_ip)
                        dst[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
                }
            }

            VecResult r;
            r.data           = dst;
            r.validity       = nullptr;
            r.selection      = draken_identity_sel(n);
            r.owns_selection = false;
            r.data_length    = n;
            r.length         = n;
            r.type           = DRAKEN_BOOL;
            r.flags = static_cast<uint8_t>(DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION);
            return wrap(r);
        },
        nb::arg("v"), nb::arg("cidr"),
        "IPv4 CIDR containment over a UINT32 address column → BOOL. "
        "cidr is a string Vector; value is read from row 0. "
        "Invalid CIDR raises ValueError. Null address rows → False (not NULL).");

}
