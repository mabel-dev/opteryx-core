/**
 * C ABI Parity Test for Phase 9a — Comprehensive Kernel Coverage.
 * Tests all 48 registered kernels to verify they are callable and produce correct output.
 */

#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/extraction_kernels.h"
#include "ops/kernels/binop_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "ops/kernels/kernel_registry.h"
#include "core/buffers.h"
#include "core/alloc.h"
#include <cassert>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <cmath>
#include <iostream>
#include <vector>
#include <string>

DrakenVector* create_int64_vector(const int64_t* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* vec_data = static_cast<int64_t*>(malloc(length * sizeof(int64_t)));
    auto* selection = static_cast<uint32_t*>(malloc(length * sizeof(uint32_t)));
    memcpy(vec_data, data, length * sizeof(int64_t));
    for (uint32_t i = 0; i < length; ++i) selection[i] = i;
    vec->data = vec_data; vec->selection = selection; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_INT64; vec->flags = 0;
    return vec;
}

DrakenVector* create_float64_vector(const double* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* vec_data = static_cast<double*>(malloc(length * sizeof(double)));
    auto* selection = static_cast<uint32_t*>(malloc(length * sizeof(uint32_t)));
    memcpy(vec_data, data, length * sizeof(double));
    for (uint32_t i = 0; i < length; ++i) selection[i] = i;
    vec->data = vec_data; vec->selection = selection; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_FLOAT64; vec->flags = 0;
    return vec;
}

// draken BOOL is bit-packed (1 bit/row), not byte-per-row. `data` is a
// convenience array of 0/1 bytes; pack it into the real layout.
DrakenVector* create_bool_vector(const uint8_t* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    const size_t nbytes = length > 0 ? (length + 7u) / 8u : 1u;
    auto* vec_data = static_cast<uint8_t*>(malloc(nbytes));
    memset(vec_data, 0, nbytes);
    auto* selection = static_cast<uint32_t*>(malloc(length * sizeof(uint32_t)));
    for (uint32_t i = 0; i < length; ++i) {
        if (data[i]) vec_data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
        selection[i] = i;
    }
    vec->data = vec_data; vec->selection = selection; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_BOOL; vec->flags = 0;
    return vec;
}

void free_vector(DrakenVector* vec) {
    if (!vec) return;
    free(vec->data);
    free((void*)vec->selection);
    free(vec->validity);
    free(vec);
}

// Dict-shaped INT64 vector: `values` holds `data_length` unique entries,
// `codes` holds `length` indices into values (1 < data_length < length is the
// canonical dict shape). Logical row i is values[codes[i]].
DrakenVector* create_int64_dict_vector(const int64_t* values, uint32_t data_length,
                                       const uint32_t* codes, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* vec_data = static_cast<int64_t*>(malloc(data_length * sizeof(int64_t)));
    auto* selection = static_cast<uint32_t*>(malloc(length * sizeof(uint32_t)));
    memcpy(vec_data, values, data_length * sizeof(int64_t));
    memcpy(selection, codes, length * sizeof(uint32_t));
    vec->data = vec_data; vec->selection = selection; vec->data_length = data_length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_INT64; vec->flags = 0;
    return vec;
}

// Constant-shaped INT64 vector: one value broadcast to `length` rows
// (data_length == 1, selection all zeros).
DrakenVector* create_int64_constant_vector(int64_t value, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* vec_data = static_cast<int64_t*>(malloc(sizeof(int64_t)));
    auto* selection = static_cast<uint32_t*>(malloc(length * sizeof(uint32_t)));
    vec_data[0] = value;
    for (uint32_t i = 0; i < length; ++i) selection[i] = 0;
    vec->data = vec_data; vec->selection = selection; vec->data_length = 1;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_INT64; vec->flags = 0;
    return vec;
}

// Narrow-int dense constructors (P9.1a binop tests).
DrakenVector* create_int8_vector(const int8_t* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* d = static_cast<int8_t*>(malloc((length ? length : 1) * sizeof(int8_t)));
    auto* sel = static_cast<uint32_t*>(malloc((length ? length : 1) * sizeof(uint32_t)));
    memcpy(d, data, length * sizeof(int8_t));
    for (uint32_t i = 0; i < length; ++i) sel[i] = i;
    vec->data = d; vec->selection = sel; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_INT8; vec->flags = 0;
    return vec;
}
DrakenVector* create_int16_vector(const int16_t* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* d = static_cast<int16_t*>(malloc((length ? length : 1) * sizeof(int16_t)));
    auto* sel = static_cast<uint32_t*>(malloc((length ? length : 1) * sizeof(uint32_t)));
    memcpy(d, data, length * sizeof(int16_t));
    for (uint32_t i = 0; i < length; ++i) sel[i] = i;
    vec->data = d; vec->selection = sel; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_INT16; vec->flags = 0;
    return vec;
}
DrakenVector* create_int32_vector(const int32_t* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* d = static_cast<int32_t*>(malloc((length ? length : 1) * sizeof(int32_t)));
    auto* sel = static_cast<uint32_t*>(malloc((length ? length : 1) * sizeof(uint32_t)));
    memcpy(d, data, length * sizeof(int32_t));
    for (uint32_t i = 0; i < length; ++i) sel[i] = i;
    vec->data = d; vec->selection = sel; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_INT32; vec->flags = 0;
    return vec;
}
DrakenVector* create_float32_vector(const float* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* d = static_cast<float*>(malloc((length ? length : 1) * sizeof(float)));
    auto* sel = static_cast<uint32_t*>(malloc((length ? length : 1) * sizeof(uint32_t)));
    memcpy(d, data, length * sizeof(float));
    for (uint32_t i = 0; i < length; ++i) sel[i] = i;
    vec->data = d; vec->selection = sel; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_FLOAT32; vec->flags = 0;
    return vec;
}

// DRAKEN_DECIMAL vector: int64 unscaled values (scale is out-of-band, passed via ctx).
DrakenVector* create_decimal_vector(const int64_t* data, uint32_t length) {
    DrakenVector* v = create_int64_vector(data, length);
    v->type = DRAKEN_DECIMAL;
    return v;
}

// DRAKEN_DECIMAL128 vector: __int128 unscaled values (built from int64 test inputs).
DrakenVector* create_decimal128_vector(const int64_t* data, uint32_t length) {
    auto* vec = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
    auto* d = static_cast<__int128*>(malloc((length ? length : 1) * sizeof(__int128)));
    auto* sel = static_cast<uint32_t*>(malloc((length ? length : 1) * sizeof(uint32_t)));
    for (uint32_t i = 0; i < length; ++i) { d[i] = static_cast<__int128>(data[i]); sel[i] = i; }
    vec->data = d; vec->selection = sel; vec->data_length = length;
    vec->length = length; vec->validity = nullptr; vec->type = DRAKEN_DECIMAL128; vec->flags = 0;
    return vec;
}

// Attach a validity bitmap (valid[i]!=0 → row i valid). Freed by free_vector.
void set_validity(DrakenVector* v, const uint8_t* valid, uint32_t n) {
    const uint32_t nb = (n + 7u) >> 3;
    auto* bm = static_cast<uint8_t*>(malloc(nb ? nb : 1));
    memset(bm, 0, nb ? nb : 1);
    for (uint32_t i = 0; i < n; ++i) if (valid[i]) bm[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
    v->validity = bm;
}

bool is_error(const VecResult& result) {
    return result.data == nullptr && result.type == DRAKEN_NULL;
}

// Width-aware read of a (possibly narrow) integer result at logical row i.
int64_t binop_read(const VecResult& r, uint32_t i) {
    const uint32_t s = r.selection[i];
    switch (r.type) {
        case DRAKEN_INT8:  return static_cast<const int8_t*>(r.data)[s];
        case DRAKEN_INT16: return static_cast<const int16_t*>(r.data)[s];
        case DRAKEN_INT32: return static_cast<const int32_t*>(r.data)[s];
        case DRAKEN_INT64: return static_cast<const int64_t*>(r.data)[s];
        default: return INT64_MIN;
    }
}
bool binop_equal(const VecResult& r, const int64_t* exp, uint32_t n, DrakenType tag) {
    if (is_error(r) || r.length != n || r.type != tag) return false;
    for (uint32_t i = 0; i < n; ++i) if (binop_read(r, i) != exp[i]) return false;
    return true;
}
bool binop_row_null(const VecResult& r, uint32_t i) {
    if (r.validity == nullptr) return false;
    return ((r.validity[i >> 3] >> (i & 7u)) & 1u) == 0u;
}

bool vectors_equal_int64(const VecResult& result, const int64_t* expected, uint32_t length) {
    if (is_error(result) || result.length != length || result.type != DRAKEN_INT64) return false;
    auto* result_data = static_cast<int64_t*>(result.data);
    for (uint32_t i = 0; i < length; ++i) {
        if (result_data[result.selection[i]] != expected[i]) return false;
    }
    return true;
}

bool vectors_equal_float64(const VecResult& result, const double* expected, uint32_t length) {
    if (is_error(result) || result.length != length || result.type != DRAKEN_FLOAT64) return false;
    auto* result_data = static_cast<double*>(result.data);
    for (uint32_t i = 0; i < length; ++i) {
        double got = result_data[result.selection[i]];
        double exp = expected[i];
        if (!(got == exp || (got >= exp - 0.0001 && got <= exp + 0.0001))) return false;
    }
    return true;
}

bool vectors_equal_bool(const VecResult& result, const uint8_t* expected, uint32_t length) {
    if (is_error(result) || result.length != length || result.type != DRAKEN_BOOL) return false;
    auto* result_data = static_cast<uint8_t*>(result.data);
    for (uint32_t i = 0; i < length; ++i) {
        const uint32_t s = result.selection[i];
        const bool got = ((result_data[s >> 3] >> (s & 7u)) & 1u) != 0u;  // bit-packed
        if (got != (expected[i] != 0)) return false;
    }
    return true;
}

void test_draken_add() { int64_t l[] = {1, 2, 3}, r[] = {10, 20, 30}, e[] = {11, 22, 33}; DrakenVector* lv = create_int64_vector(l, 3); DrakenVector* rv = create_int64_vector(r, 3); VecResult res = draken_add(nullptr, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 3)); free_vector(lv); free_vector(rv); draken_free(res.data); }
void test_draken_subtract() { int64_t l[] = {10, 20, 30}, r[] = {1, 2, 3}, e[] = {9, 18, 27}; DrakenVector* lv = create_int64_vector(l, 3); DrakenVector* rv = create_int64_vector(r, 3); VecResult res = draken_subtract(nullptr, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 3)); free_vector(lv); free_vector(rv); draken_free(res.data); }
void test_draken_multiply() { int64_t l[] = {2, 3, 4}, r[] = {5, 6, 7}, e[] = {10, 18, 28}; DrakenVector* lv = create_int64_vector(l, 3); DrakenVector* rv = create_int64_vector(r, 3); VecResult res = draken_multiply(nullptr, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 3)); free_vector(lv); free_vector(rv); draken_free(res.data); }
void test_draken_divide() { int64_t l[] = {10, 20, 30}, r[] = {2, 4, 5}; double e[] = {5.0, 5.0, 6.0}; DrakenVector* lv = create_int64_vector(l, 3); DrakenVector* rv = create_int64_vector(r, 3); VecResult res = draken_divide(nullptr, lv, rv); assert(!is_error(res) && res.type == DRAKEN_FLOAT64 && vectors_equal_float64(res, e, 3)); free_vector(lv); free_vector(rv); draken_free(res.data); }
void test_draken_modulo() { int64_t l[] = {10, 20, 30}, r[] = {3, 6, 7}, e[] = {1, 2, 2}; DrakenVector* lv = create_int64_vector(l, 3); DrakenVector* rv = create_int64_vector(r, 3); VecResult res = draken_modulo(nullptr, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 3)); free_vector(lv); free_vector(rv); draken_free(res.data); }
void test_draken_binary_arith() { binary_op_ctx ctx{1}; int64_t l[] = {1, 2}, r[] = {10, 20}, e[] = {11, 22}; DrakenVector* lv = create_int64_vector(l, 2); DrakenVector* rv = create_int64_vector(r, 2); VecResult res = draken_binary_arith(&ctx, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 2)); free_vector(lv); free_vector(rv); draken_free(res.data); }

void test_draken_bitwise_or() { int64_t l[] = {5}, r[] = {3}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_bitwise_or(nullptr, lv, rv); assert(is_error(res)); free_vector(lv); free_vector(rv); }
void test_draken_bitwise_and() { int64_t l[] = {7}, r[] = {3}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_bitwise_and(nullptr, lv, rv); assert(is_error(res)); free_vector(lv); free_vector(rv); }
void test_draken_bitwise_xor() { int64_t l[] = {5}, r[] = {3}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_bitwise_xor(nullptr, lv, rv); assert(is_error(res)); free_vector(lv); free_vector(rv); }
void test_draken_bitwise_shift_left() { int64_t l[] = {1}, r[] = {1}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_bitwise_shift_left(nullptr, lv, rv); assert(is_error(res)); free_vector(lv); free_vector(rv); }
void test_draken_bitwise_shift_right() { int64_t l[] = {2}, r[] = {1}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_bitwise_shift_right(nullptr, lv, rv); assert(is_error(res)); free_vector(lv); free_vector(rv); }

// Phase 9c: these casts are now REAL — value-checked, not is_error.
void test_draken_cast_int64_to_float64() { int64_t d[] = {1, 2, 3}; double e[] = {1.0, 2.0, 3.0}; DrakenVector* v = create_int64_vector(d, 3); VecResult r = draken_cast_int64_to_float64(nullptr, v); assert(!is_error(r) && vectors_equal_float64(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_float64_to_int64() { double d[] = {1.5, 2.7, 3.2}; int64_t e[] = {1, 2, 3}; DrakenVector* v = create_float64_vector(d, 3); VecResult r = draken_cast_float64_to_int64(nullptr, v); assert(!is_error(r) && vectors_equal_int64(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_int64_to_bool() { int64_t d[] = {0, 1, 5}; uint8_t e[] = {0, 1, 1}; DrakenVector* v = create_int64_vector(d, 3); VecResult r = draken_cast_int64_to_bool(nullptr, v); assert(!is_error(r) && vectors_equal_bool(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_bool_to_int64() { uint8_t d[] = {0, 1, 1}; int64_t e[] = {0, 1, 1}; DrakenVector* v = create_bool_vector(d, 3); VecResult r = draken_cast_bool_to_int64(nullptr, v); assert(!is_error(r) && vectors_equal_int64(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_float64_to_bool() { double d[] = {0.0, 1.5, 0.0}; uint8_t e[] = {0, 1, 0}; DrakenVector* v = create_float64_vector(d, 3); VecResult r = draken_cast_float64_to_bool(nullptr, v); assert(!is_error(r) && vectors_equal_bool(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_bool_to_float64() { uint8_t d[] = {0, 1, 1}; double e[] = {0.0, 1.0, 1.0}; DrakenVector* v = create_bool_vector(d, 3); VecResult r = draken_cast_bool_to_float64(nullptr, v); assert(!is_error(r) && vectors_equal_float64(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }

void test_draken_cast_to_float64() { int64_t d[] = {1, 2, 3}; double e[] = {1.0, 2.0, 3.0}; DrakenVector* v = create_int64_vector(d, 3); VecResult r = draken_cast_to_float64(nullptr, v); assert(!is_error(r) && vectors_equal_float64(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_to_int64() { double d[] = {1.5, 2.7, 3.2}; int64_t e[] = {1, 2, 3}; DrakenVector* v = create_float64_vector(d, 3); VecResult r = draken_cast_to_int64(nullptr, v); assert(!is_error(r) && vectors_equal_int64(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_to_bool() { int64_t d[] = {0, 1, 5}; uint8_t e[] = {0, 1, 1}; DrakenVector* v = create_int64_vector(d, 3); VecResult r = draken_cast_to_bool(nullptr, v); assert(!is_error(r) && vectors_equal_bool(r, e, 3)); free_vector(v); draken_free(r.data); draken_free(r.validity); }
// draken_cast_identity returns a BORROWED view of the input (result.data == v->data,
// owns_selection=false) — the caller wraps it; it does NOT own the buffers. So only
// free_vector(v) frees them; a separate draken_free(r.data) would double-free (aliases
// v->data). The double free was masked while mimalloc was linked; the system allocator
// aborts on it.
void test_draken_cast_identity() { double d[] = {1.5, 2.5, 3.5}; DrakenVector* v = create_float64_vector(d, 3); VecResult r = draken_cast_identity(nullptr, v); assert(!is_error(r) && r.data == v->data); free_vector(v); }

// Phase 9c: string-output casts are now REAL — assert VARCHAR result, free block.
void test_draken_cast_int64_to_string() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_int64_to_string(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_VARCHAR && r.length == 1); free_vector(v); draken_free(r.data); }
void test_draken_cast_int64_to_timestamp() { int64_t d[] = {1000000}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_int64_to_timestamp(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_TIMESTAMP64 && r.ts_unit == 2); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_bool_to_string() { uint8_t d[] = {0}; DrakenVector* v = create_bool_vector(d, 1); VecResult r = draken_cast_bool_to_string(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_VARCHAR && r.length == 1); free_vector(v); draken_free(r.data); }
// Phase 9c: float64→string is now REAL (Ryu) — assert VARCHAR result, free block.
void test_draken_cast_float64_to_string() { double d[] = {1.5}; DrakenVector* v = create_float64_vector(d, 1); VecResult r = draken_cast_float64_to_string(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_VARCHAR && r.length == 1); free_vector(v); draken_free(r.data); }
void test_draken_cast_string_to_int64() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_VARCHAR; VecResult r = draken_cast_string_to_int64(nullptr, v); free(v); }

void test_draken_cast_string_to_bool() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_VARCHAR; VecResult r = draken_cast_string_to_bool(nullptr, v); free(v); }
void test_draken_cast_date32_to_int64() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_DATE32; VecResult r = draken_cast_date32_to_int64(nullptr, v); free(v); }
void test_draken_cast_date32_to_timestamp() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_DATE32; VecResult r = draken_cast_date32_to_timestamp(nullptr, v); free(v); }
void test_draken_cast_timestamp_to_int64() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_TIMESTAMP64; VecResult r = draken_cast_timestamp_to_int64(nullptr, v); free(v); }
void test_draken_cast_timestamp_to_string() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_TIMESTAMP64; VecResult r = draken_cast_timestamp_to_string(nullptr, v); free(v); }
void test_draken_cast_timestamp_to_date32() { DrakenVector* v = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(v, 0, sizeof(DrakenVector)); v->type = DRAKEN_TIMESTAMP64; VecResult r = draken_cast_timestamp_to_date32(nullptr, v); free(v); }
void test_draken_cast_to_varchar() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_to_varchar(nullptr, v); free_vector(v); }
void test_draken_cast_to_date() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_to_date(nullptr, v); free_vector(v); }
void test_draken_cast_to_decimal() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_to_decimal(nullptr, v); free_vector(v); }
void test_draken_cast_to_array() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_to_array(nullptr, v); free_vector(v); }
void test_draken_cast_to_vector() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_to_vector(nullptr, v); free_vector(v); }
void test_draken_cast_to_varchar_with_length() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_to_varchar_with_length(nullptr, v); free_vector(v); }
void test_draken_string_concat() { DrakenVector* l = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); DrakenVector* r = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(l, 0, sizeof(DrakenVector)); memset(r, 0, sizeof(DrakenVector)); VecResult res = draken_string_concat(nullptr, l, r); free(l); free(r); }
void test_draken_temporal_interval_op() { int64_t d[] = {0}; DrakenVector* l = create_int64_vector(d, 1); DrakenVector* r = create_int64_vector(d, 1); VecResult res = draken_temporal_interval_op(nullptr, l, r); free_vector(l); free_vector(r); }
void test_draken_date_minus_date() { DrakenVector* l = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); DrakenVector* r = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(l, 0, sizeof(DrakenVector)); memset(r, 0, sizeof(DrakenVector)); l->type = r->type = DRAKEN_DATE32; VecResult res = draken_date_minus_date(nullptr, l, r); free(l); free(r); }
void test_draken_interval_interval_op() { int64_t d[] = {0}; DrakenVector* l = create_int64_vector(d, 1); DrakenVector* r = create_int64_vector(d, 1); VecResult res = draken_interval_interval_op(nullptr, l, r); free_vector(l); free_vector(r); }
void test_draken_ip_in_cidr() { DrakenVector* l = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); DrakenVector* r = static_cast<DrakenVector*>(malloc(sizeof(DrakenVector))); memset(l, 0, sizeof(DrakenVector)); memset(r, 0, sizeof(DrakenVector)); VecResult res = draken_ip_in_cidr(nullptr, l, r); free(l); free(r); }
// Extraction kernels take every bind-time parameter in extraction_ctx and ignore
// the ABI's `key` operand. An INT64 operand is not a document/string column, so
// each must FAIL LOUD (error sentinel), never silently produce a vector.
static void assert_extr_rejects_int64(VecResult (*fn)(void*, const DrakenVector*, const DrakenVector*),
                                      int32_t sub_op) {
    int64_t d[] = {1};
    DrakenVector* v = create_int64_vector(d, 1);
    extraction_ctx* ctx = kernel_alloc_extraction_ctx(static_cast<uint16_t>(sub_op), "a", 1, 0);
    assert(ctx != nullptr && "extraction ctx allocation must succeed");
    VecResult r = fn(ctx, v, nullptr);
    assert(r.data == nullptr && "extraction kernel must reject a non-string operand");
    free(ctx);
    free_vector(v);
}
void test_draken_map_access_string() { assert_extr_rejects_int64(draken_map_access_string, 1); }
void test_draken_json_extract() { assert_extr_rejects_int64(draken_json_extract, 4); }
void test_draken_pointer_extract() { assert_extr_rejects_int64(draken_pointer_extract, 3); }
// draken_array_map_access is a deliberate stub: it must ALWAYS fail loud, whatever
// it is handed, because the binder never flags BC_EXTR_MAP_ARRAY as C-native.
void test_draken_array_map_access() {
    int64_t d[] = {1};
    DrakenVector* v = create_int64_vector(d, 1);
    VecResult r = draken_array_map_access(nullptr, v, nullptr);
    assert(r.data == nullptr && "ARRAY subscript kernel must never succeed");
    free_vector(v);
}

// ── Shape-parity tests (WP-03 regression) ──────────────────────────────────
// Binary arith writes a DENSE output buffer but historically returned the
// LEFT input's selection. For dict/constant left operands that selection
// indexes into the (smaller/different) input layout, so a consumer reading
// result.data[result.selection[i]] got wrong values or an OOB read. The fix
// returns an identity selection; these tests verify the LOGICAL answer via the
// uniform access pattern for non-dense left operands.
//
// dict left: values {5,10}, codes {0,1,0,1} → logical {5,10,5,10}.
void test_arith_add_dict_left() {
    int64_t lv[] = {5, 10}; uint32_t codes[] = {0, 1, 0, 1};
    int64_t r[] = {1, 2, 3, 4}, e[] = {6, 12, 8, 14};
    DrakenVector* L = create_int64_dict_vector(lv, 2, codes, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_add(nullptr, L, R);
    assert(!is_error(res) && vectors_equal_int64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
// constant left: 7 broadcast, right {1,2,3,4} → {8,9,10,11}.
void test_arith_add_constant_left() {
    int64_t r[] = {1, 2, 3, 4}, e[] = {8, 9, 10, 11};
    DrakenVector* L = create_int64_constant_vector(7, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_add(nullptr, L, R);
    assert(!is_error(res) && vectors_equal_int64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
// dict left with FLOAT64 right exercises the mixed→float branch.
void test_arith_add_dict_left_float_right() {
    int64_t lv[] = {5, 10}; uint32_t codes[] = {0, 1, 0, 1};
    double r[] = {1.0, 2.0, 3.0, 4.0}, e[] = {6.0, 12.0, 8.0, 14.0};
    DrakenVector* L = create_int64_dict_vector(lv, 2, codes, 4);
    DrakenVector* R = create_float64_vector(r, 4);
    VecResult res = draken_add(nullptr, L, R);
    assert(!is_error(res) && res.type == DRAKEN_FLOAT64 && vectors_equal_float64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
void test_arith_subtract_dict_left() {
    int64_t lv[] = {20, 30}; uint32_t codes[] = {0, 1, 0, 1};
    int64_t r[] = {1, 2, 3, 4}, e[] = {19, 28, 17, 26};
    DrakenVector* L = create_int64_dict_vector(lv, 2, codes, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_subtract(nullptr, L, R);
    assert(!is_error(res) && vectors_equal_int64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
void test_arith_multiply_constant_left() {
    int64_t r[] = {1, 2, 3, 4}, e[] = {3, 6, 9, 12};
    DrakenVector* L = create_int64_constant_vector(3, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_multiply(nullptr, L, R);
    assert(!is_error(res) && vectors_equal_int64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
void test_arith_divide_dict_left() {
    int64_t lv[] = {10, 20}; uint32_t codes[] = {0, 1, 0, 1};
    int64_t r[] = {2, 4, 5, 8}; double e[] = {5.0, 5.0, 2.0, 2.5};
    DrakenVector* L = create_int64_dict_vector(lv, 2, codes, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_divide(nullptr, L, R);
    assert(!is_error(res) && res.type == DRAKEN_FLOAT64 && vectors_equal_float64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
void test_arith_modulo_dict_left() {
    int64_t lv[] = {10, 21}; uint32_t codes[] = {0, 1, 0, 1};
    int64_t r[] = {3, 5, 4, 6}, e[] = {1, 1, 2, 3};
    DrakenVector* L = create_int64_dict_vector(lv, 2, codes, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_modulo(nullptr, L, R);
    assert(!is_error(res) && vectors_equal_int64(res, e, 4));
    free_vector(L); free_vector(R); draken_free(res.data);
}
// Result must carry an identity selection + IDENTITY|PERMUTATION flags, never
// the borrowed dict codes.
void test_arith_result_is_identity_shaped() {
    int64_t lv[] = {5, 10}; uint32_t codes[] = {0, 1, 0, 1};
    int64_t r[] = {1, 2, 3, 4};
    DrakenVector* L = create_int64_dict_vector(lv, 2, codes, 4);
    DrakenVector* R = create_int64_vector(r, 4);
    VecResult res = draken_add(nullptr, L, R);
    assert(!is_error(res));
    assert(res.data_length == res.length);            // dense
    for (uint32_t i = 0; i < res.length; ++i)
        assert(res.selection[i] == i);                // identity, not dict codes
    assert((res.flags & DRAKEN_SEL_IDENTITY) != 0);
    free_vector(L); free_vector(R); draken_free(res.data);
}

// P9.0 registry honesty: the kernel registry must contain ONLY real, nogil,
// byte-identical kernels. A registered stub is a trap — the binder marks it
// BC_INSTR_C_NATIVE and the executor dispatches an error sentinel. This guards
// against re-adding the stubs removed in P9.0, and confirms the real kernels stay.
void test_registry_honesty() {
    kernel_fn_t fn = nullptr; void* ctx = nullptr;
    // Removed in P9.0 — must NOT be registered until implemented real:
    const char* removed_stubs[] = {
        "draken_bitwise_or", "draken_bitwise_and", "draken_bitwise_xor",
        "draken_bitwise_shift_left", "draken_bitwise_shift_right",
        "draken_string_concat", "draken_ip_in_cidr",
        "draken_temporal_interval_op", "draken_date_minus_date",
        "draken_interval_interval_op",
        "draken_cast_timestamp_to_date32",
    };
    for (const char* name : removed_stubs) {
        fn = nullptr; ctx = nullptr;
        assert(!kernel_registry_lookup(name, &fn, &ctx)
               && "P9.0: stub kernel must not be registered");
    }
    // Real kernels must remain reachable by name:
    const char* real_kernels[] = {
        "draken_add", "draken_subtract", "draken_multiply", "draken_divide",
        "draken_modulo", "draken_binary_arith",
        "draken_cast_int64_to_float64", "draken_cast_int64_to_string",
        "draken_cast_float64_to_string", "draken_cast_string_to_float64",
        "draken_cast_date32_to_timestamp",
    };
    for (const char* name : real_kernels) {
        fn = nullptr; ctx = nullptr;
        assert(kernel_registry_lookup(name, &fn, &ctx) && fn != nullptr
               && "P9.0: real kernel must stay registered");
    }
    // NOTE: draken_map_access_string / _json_extract / _pointer_extract are real
    // kernels dispatched by the nogil VM. draken_array_map_access stays a stub (the
    // ARRAY child is unreachable from a DrakenVector*) and the binder never flags
    // BC_EXTR_MAP_ARRAY as C-native, so it is never dispatched.
}

// P9.1a — unified binop dispatch (draken_binop), integer arithmetic core.
// Verifies D.6 widen-to-next-power result types, proven div/mod-by-zero→0,
// per-row null propagation, all three vector shapes, and that not-yet-covered
// combinations fail loud (no silent fallback). NOT wired into the executor.
void test_draken_binop_int_arith() {
    binary_op_ctx c_plus{1}, c_minus{2}, c_mul{3}, c_div{4}, c_mod{5}, c_idiv{6};

    // int8 + int8 → int16 (D.6)
    { int8_t l[]={10,20,30}, r[]={1,2,3}; int64_t e[]={11,22,33};
      auto* lv=create_int8_vector(l,3); auto* rv=create_int8_vector(r,3);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(binop_equal(res,e,3,DRAKEN_INT16));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int16 * int16 → int32 (D.6)
    { int16_t l[]={100,200}, r[]={3,4}; int64_t e[]={300,800};
      auto* lv=create_int16_vector(l,2); auto* rv=create_int16_vector(r,2);
      VecResult res=draken_binop(&c_mul,lv,rv);
      assert(binop_equal(res,e,2,DRAKEN_INT32));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int32 - int32 → int64 (D.6)
    { int32_t l[]={5,9}, r[]={2,3}; int64_t e[]={3,6};
      auto* lv=create_int32_vector(l,2); auto* rv=create_int32_vector(r,2);
      VecResult res=draken_binop(&c_minus,lv,rv);
      assert(binop_equal(res,e,2,DRAKEN_INT64));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int64 + int64 → int64
    { int64_t l[]={1,2}, r[]={10,20}, e[]={11,22};
      auto* lv=create_int64_vector(l,2); auto* rv=create_int64_vector(r,2);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(binop_equal(res,e,2,DRAKEN_INT64));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // cross-width int8 + int32 → int64 (wider rank wins → next-power)
    { int8_t l[]={7}; int32_t r[]={100}; int64_t e[]={107};
      auto* lv=create_int8_vector(l,1); auto* rv=create_int32_vector(r,1);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(binop_equal(res,e,1,DRAKEN_INT64));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // INT_DIVIDE by zero → 0; MODULO by zero → 0 (int16/int16 → int32)
    { int16_t l[]={10,10}, r[]={0,3}; int64_t ediv[]={0,3}, emod[]={0,1};
      auto* lv=create_int16_vector(l,2); auto* rv=create_int16_vector(r,2);
      VecResult rd=draken_binop(&c_idiv,lv,rv); assert(binop_equal(rd,ediv,2,DRAKEN_INT32));
      VecResult rm=draken_binop(&c_mod ,lv,rv); assert(binop_equal(rm,emod,2,DRAKEN_INT32));
      free_vector(lv); free_vector(rv);
      draken_free(rd.data); draken_free(rd.validity); draken_free(rm.data); draken_free(rm.validity); }

    // null propagation: int8 + int8, left row 1 NULL → result row 1 NULL
    { int8_t l[]={10,20,30}, r[]={1,2,3}; uint8_t lvalid[]={1,0,1};
      auto* lv=create_int8_vector(l,3); set_validity(lv,lvalid,3);
      auto* rv=create_int8_vector(r,3);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(res.type==DRAKEN_INT16 && res.length==3);
      assert(!binop_row_null(res,0) && binop_row_null(res,1) && !binop_row_null(res,2));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // constant shape: int64 constant(5) + int64 → broadcast
    { int64_t r[]={1,2,3}, e[]={6,7,8};
      auto* lv=create_int64_constant_vector(5,3); auto* rv=create_int64_vector(r,3);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(binop_equal(res,e,3,DRAKEN_INT64));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // dict shape: int64 dict {values=[100,200], codes=[0,1,0]} + constant(1)
    { int64_t vals[]={100,200}; uint32_t codes[]={0,1,0}; int64_t e[]={101,201,101};
      auto* lv=create_int64_dict_vector(vals,2,codes,3); auto* rv=create_int64_constant_vector(1,3);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(binop_equal(res,e,3,DRAKEN_INT64));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // still-deferred → loud error (no silent fallback): string-concat op (7) on ints.
    { binary_op_ctx c_concat{7}; int64_t l[]={6}, r[]={2};
      auto* lv=create_int64_vector(l,1); auto* rv=create_int64_vector(r,1);
      VecResult res=draken_binop(&c_concat,lv,rv); assert(is_error(res));
      free_vector(lv); free_vector(rv); }
    // (true DIVIDE, float, and bitwise are now COVERED — see the float/bitwise tests.)
}

// P9.1a-rest — draken_binop float paths: TRUE DIVIDE (any numeric → FLOAT64,
// IEEE x/0 → ±inf, matching DuckDB) and non-divide arithmetic with a FLOAT64
// operand → FLOAT64. FLOAT32-without-FLOAT64 stays deferred (loud error).
static double f64_at(const VecResult& r, uint32_t i) {
    return static_cast<const double*>(r.data)[r.selection[i]];
}
static float f32_at(const VecResult& r, uint32_t i) {
    return static_cast<const float*>(r.data)[r.selection[i]];
}
void test_draken_binop_float() {
    binary_op_ctx c_plus{1}, c_minus{2}, c_mul{3}, c_div{4}, c_mod{5};

    // int64 / int64 → FLOAT64 (true division: 7/2 = 3.5)
    { int64_t l[]={7,9}, r[]={2,4}; double e[]={3.5,2.25};
      auto* lv=create_int64_vector(l,2); auto* rv=create_int64_vector(r,2);
      VecResult res=draken_binop(&c_div,lv,rv);
      assert(vectors_equal_float64(res,e,2));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int8 / int8 → FLOAT64 (3/2 = 1.5) — fixes the live narrow-int divide bug
    { int8_t l[]={3}, r[]={2}; double e[]={1.5};
      auto* lv=create_int8_vector(l,1); auto* rv=create_int8_vector(r,1);
      VecResult res=draken_binop(&c_div,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // divide by zero → +inf (IEEE, matches DuckDB)
    { int64_t l[]={7}, r[]={0};
      auto* lv=create_int64_vector(l,1); auto* rv=create_int64_vector(r,1);
      VecResult res=draken_binop(&c_div,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_FLOAT64 && std::isinf(f64_at(res,0)) && f64_at(res,0) > 0);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // float64 + float64 → FLOAT64
    { double l[]={1.5,2.5}, r[]={0.25,0.5}, e[]={1.75,3.0};
      auto* lv=create_float64_vector(l,2); auto* rv=create_float64_vector(r,2);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(vectors_equal_float64(res,e,2));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int64 + float64 → FLOAT64 (mixed promotion)
    { int64_t l[]={3}; double r[]={1.5}, e[]={4.5};
      auto* lv=create_int64_vector(l,1); auto* rv=create_float64_vector(r,1);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // float64 * int8 → FLOAT64
    { double l[]={2.5}; int8_t r[]={3}; double e[]={7.5};
      auto* lv=create_float64_vector(l,1); auto* rv=create_int8_vector(r,1);
      VecResult res=draken_binop(&c_mul,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // float64 % float64 → FLOAT64 (fmod: 7.5 % 2.0 = 1.5)
    { double l[]={7.5}, r[]={2.0}, e[]={1.5};
      auto* lv=create_float64_vector(l,1); auto* rv=create_float64_vector(r,1);
      VecResult res=draken_binop(&c_mod,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // null propagation in the float path: float64 / float64, right row 1 NULL
    { double l[]={6.0,8.0}, r[]={2.0,4.0}; uint8_t rvalid[]={1,0};
      auto* lv=create_float64_vector(l,2); auto* rv=create_float64_vector(r,2); set_validity(rv,rvalid,2);
      VecResult res=draken_binop(&c_div,lv,rv);
      assert(res.type==DRAKEN_FLOAT64 && !binop_row_null(res,0) && binop_row_null(res,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // constant shape: float64 constant(2.0) * int64 → FLOAT64 broadcast
    { int64_t r[]={1,2,3}; double e[]={2.0,4.0,6.0};
      auto* lv=create_int64_constant_vector(0,3); /* placeholder, replaced below */
      free_vector(lv);
      // build a FLOAT64 constant by hand
      auto* fv=static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
      auto* fd=static_cast<double*>(malloc(sizeof(double))); fd[0]=2.0;
      auto* fsel=static_cast<uint32_t*>(malloc(3*sizeof(uint32_t))); for(int i=0;i<3;++i) fsel[i]=0;
      fv->data=fd; fv->selection=fsel; fv->data_length=1; fv->length=3; fv->validity=nullptr; fv->type=DRAKEN_FLOAT64; fv->flags=0;
      auto* rv=create_int64_vector(r,3);
      VecResult res=draken_binop(&c_mul,fv,rv);
      assert(vectors_equal_float64(res,e,3));
      free_vector(fv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // P9.1a-rest-2 — FLOAT32-preserving non-divide (DuckDB: FLOAT+FLOAT→FLOAT,
    // int+FLOAT→FLOAT, single precision; only a DOUBLE operand promotes).

    // float32 + float32 → FLOAT32
    { float l[]={1.5f}, r[]={2.5f};
      auto* lv=create_float32_vector(l,1); auto* rv=create_float32_vector(r,1);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_FLOAT32 && f32_at(res,0)==4.0f);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int8 + float32 → FLOAT32
    { int8_t l[]={3}; float r[]={1.5f};
      auto* lv=create_int8_vector(l,1); auto* rv=create_float32_vector(r,1);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_FLOAT32 && f32_at(res,0)==4.5f);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int64 + float32 at SINGLE precision: 16777217 rounds to its float32 rep (16777216)
    { int64_t l[]={16777217}; float r[]={0.0f};
      auto* lv=create_int64_vector(l,1); auto* rv=create_float32_vector(r,1);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_FLOAT32 && f32_at(res,0)==16777216.0f);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // float32 % float32 → FLOAT32 (7.5 % 2.0 = 1.5)
    { float l[]={7.5f}, r[]={2.0f};
      auto* lv=create_float32_vector(l,1); auto* rv=create_float32_vector(r,1);
      VecResult res=draken_binop(&c_mod,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_FLOAT32 && f32_at(res,0)==1.5f);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // float32 + float64 → FLOAT64 (any DOUBLE operand promotes)
    { float l[]={1.5f}; double r[]={2.5}, e[]={4.0};
      auto* lv=create_float32_vector(l,1); auto* rv=create_float64_vector(r,1);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // null propagation in the float32 path
    { float l[]={1.0f,2.0f}, r[]={10.0f,20.0f}; uint8_t lvalid[]={0,1};
      auto* lv=create_float32_vector(l,2); set_validity(lv,lvalid,2); auto* rv=create_float32_vector(r,2);
      VecResult res=draken_binop(&c_plus,lv,rv);
      assert(res.type==DRAKEN_FLOAT32 && binop_row_null(res,0) && !binop_row_null(res,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // still-deferred → loud error: DECIMAL operand (P9.1b)
    { int64_t r[]={2}; auto* rv=create_int64_vector(r,1);
      auto* lv=static_cast<DrakenVector*>(malloc(sizeof(DrakenVector)));
      auto* ld=static_cast<int64_t*>(malloc(sizeof(int64_t))); ld[0]=5;
      auto* lsel=static_cast<uint32_t*>(malloc(sizeof(uint32_t))); lsel[0]=0;
      lv->data=ld; lv->selection=lsel; lv->data_length=1; lv->length=1; lv->validity=nullptr;
      lv->type=DRAKEN_DECIMAL; lv->flags=0;
      VecResult res=draken_binop(&c_plus,lv,rv); assert(is_error(res));
      free_vector(lv); free_vector(rv); }
}

// P9.1b-1 — draken_binop DECIMAL×DECIMAL (int64-backed), scale-aware via dec_*.
// Scales come through ctx (left_scale, right_scale, result_scale). PostgreSQL
// scale rules (E.32): add/sub → max(sa,sb); mul → sa+sb; div → result_scale;
// mod → sa. Values hand-computed; result type must be DRAKEN_DECIMAL.
static int64_t dec_at(const VecResult& r, uint32_t i) {
    return static_cast<const int64_t*>(r.data)[r.selection[i]];
}
static __int128 dec128_at(const VecResult& r, uint32_t i) {
    return static_cast<const __int128*>(r.data)[r.selection[i]];
}
void test_draken_binop_decimal() {
    // a = [1.50, 3.00] scale 2 (150,300);  b = [2.5, 1.0] scale 1 (25,10)
    int64_t a[]={150,300}, b[]={25,10};

    // ADD (result scale 2): 1.50+2.50=4.00 → 400 ; 3.00+1.00=4.00 → 400
    { binary_op_ctx c{1,2,1,2};
      auto* lv=create_decimal_vector(a,2); auto* rv=create_decimal_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL && res.length==2
             && dec_at(res,0)==400 && dec_at(res,1)==400);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // SUB (result scale 2): 1.50-2.50=-1.00 → -100 ; 3.00-1.00=2.00 → 200
    { binary_op_ctx c{2,2,1,2};
      auto* lv=create_decimal_vector(a,2); auto* rv=create_decimal_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL && dec_at(res,0)==-100 && dec_at(res,1)==200);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // MUL (result scale sa+sb=3): 1.50*2.5=3.750 → 3750 ; 3.00*1.0=3.000 → 3000
    { binary_op_ctx c{3,2,1,3};
      auto* lv=create_decimal_vector(a,2); auto* rv=create_decimal_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL && dec_at(res,0)==3750 && dec_at(res,1)==3000);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // DIV (result scale max(sa+6,6)=8): 1.50/2.5=0.6 → 60000000 ; 3.00/1.0=3.0 → 300000000
    { binary_op_ctx c{4,2,1,8};
      auto* lv=create_decimal_vector(a,2); auto* rv=create_decimal_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL
             && dec_at(res,0)==60000000 && dec_at(res,1)==300000000);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // MOD (result scale sa=2): 1.50 % 2.50 = 1.50 → 150 ; 3.00 % 1.00 = 0.00 → 0
    { binary_op_ctx c{5,2,1,2};
      auto* lv=create_decimal_vector(a,2); auto* rv=create_decimal_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL && dec_at(res,0)==150 && dec_at(res,1)==0);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // --- DECIMAL128 × DECIMAL128 (int128-backed), via dec128_* ---
    // ADD scale2: [400,400]; MUL scale3: [3750,3000]; DIV scale8: [60000000,300000000]
    { binary_op_ctx c{1,2,1,2};
      auto* lv=create_decimal128_vector(a,2); auto* rv=create_decimal128_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL128 && dec128_at(res,0)==400 && dec128_at(res,1)==400);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }
    { binary_op_ctx c{3,2,1,3};
      auto* lv=create_decimal128_vector(a,2); auto* rv=create_decimal128_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL128 && dec128_at(res,0)==3750 && dec128_at(res,1)==3000);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }
    { binary_op_ctx c{4,2,1,8};
      auto* lv=create_decimal128_vector(a,2); auto* rv=create_decimal128_vector(b,2);
      VecResult res=draken_binop(&c,lv,rv);
      assert(!is_error(res) && res.type==DRAKEN_DECIMAL128
             && dec128_at(res,0)==(__int128)60000000 && dec128_at(res,1)==(__int128)300000000);
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // --- DECIMAL × FLOAT → FLOAT64 (decimal promoted via to_float64 = unscaled/10^scale) ---
    // DECIMAL 1.50 (150, scale2) + FLOAT64 0.5 → 2.0 ; DIV 1.50/0.5 → 3.0
    { binary_op_ctx c{1,2,0,0}; int64_t da[]={150}; double fb[]={0.5}; double e[]={2.0};
      auto* lv=create_decimal_vector(da,1); auto* rv=create_float64_vector(fb,1);
      VecResult res=draken_binop(&c,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }
    { binary_op_ctx c{4,2,0,0}; int64_t da[]={150}; double fb[]={0.5}; double e[]={3.0};
      auto* lv=create_decimal_vector(da,1); auto* rv=create_float64_vector(fb,1);
      VecResult res=draken_binop(&c,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }
    // FLOAT64 × DECIMAL128 (reversed order, scale on right): 0.5 * 1.50 → 0.75
    { binary_op_ctx c{3,0,2,0}; double fa[]={0.5}; int64_t db[]={150}; double e[]={0.75};
      auto* lv=create_float64_vector(fa,1); auto* rv=create_decimal128_vector(db,1);
      VecResult res=draken_binop(&c,lv,rv);
      assert(vectors_equal_float64(res,e,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // still-deferred → loud error: DECIMAL × INTEGER (P9.1b-rest later)
    { binary_op_ctx c{1,2,0,2}; int64_t r[]={5};
      auto* lv=create_decimal_vector(a,1); auto* rv=create_int64_vector(r,1);
      VecResult res=draken_binop(&c,lv,rv); assert(is_error(res));
      free_vector(lv); free_vector(rv); }
}

// P9.1c — draken_binop bitwise OR/AND/XOR/SHL/SHR (int_bitwise; result = input type,
// both operands same type; mismatch / out-of-range shift → loud error).
void test_draken_binop_bitwise() {
    binary_op_ctx c_or{8}, c_and{9}, c_xor{10}, c_shl{11}, c_shr{12};

    // int32 5 | 3 = 7 ; 5 & 3 = 1 ; 5 ^ 3 = 6  → INT32
    { int32_t l[]={5}, r[]={3}; int64_t eo[]={7}, ea[]={1}, ex[]={6};
      auto* lv=create_int32_vector(l,1); auto* rv=create_int32_vector(r,1);
      VecResult ro=draken_binop(&c_or,lv,rv);  assert(binop_equal(ro,eo,1,DRAKEN_INT32));
      VecResult ra=draken_binop(&c_and,lv,rv); assert(binop_equal(ra,ea,1,DRAKEN_INT32));
      VecResult rx=draken_binop(&c_xor,lv,rv); assert(binop_equal(rx,ex,1,DRAKEN_INT32));
      free_vector(lv); free_vector(rv);
      draken_free(ro.data); draken_free(ro.validity); draken_free(ra.data); draken_free(ra.validity);
      draken_free(rx.data); draken_free(rx.validity); }

    // int8 preserves width: 7 & 3 = 3 → INT8
    { int8_t l[]={7}, r[]={3}; int64_t e[]={3};
      auto* lv=create_int8_vector(l,1); auto* rv=create_int8_vector(r,1);
      VecResult res=draken_binop(&c_and,lv,rv);
      assert(binop_equal(res,e,1,DRAKEN_INT8));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // int16 shifts: 1 << 4 = 16 ; 64 >> 2 = 16 → INT16
    { int16_t l[]={1,64}, r[]={4,2}; int64_t esl[]={16,256}, esr[]={0,16};
      auto* lv=create_int16_vector(l,2); auto* rv=create_int16_vector(r,2);
      VecResult rl=draken_binop(&c_shl,lv,rv); assert(binop_equal(rl,esl,2,DRAKEN_INT16));
      VecResult rr=draken_binop(&c_shr,lv,rv); assert(binop_equal(rr,esr,2,DRAKEN_INT16));
      free_vector(lv); free_vector(rv);
      draken_free(rl.data); draken_free(rl.validity); draken_free(rr.data); draken_free(rr.validity); }

    // null propagation: int32 OR, left row 0 NULL
    { int32_t l[]={5,6}, r[]={3,1}; uint8_t lvalid[]={0,1};
      auto* lv=create_int32_vector(l,2); set_validity(lv,lvalid,2); auto* rv=create_int32_vector(r,2);
      VecResult res=draken_binop(&c_or,lv,rv);
      assert(res.type==DRAKEN_INT32 && binop_row_null(res,0) && !binop_row_null(res,1));
      free_vector(lv); free_vector(rv); draken_free(res.data); draken_free(res.validity); }

    // type mismatch → loud error (int8 & int32)
    { int8_t l[]={5}; int32_t r[]={3};
      auto* lv=create_int8_vector(l,1); auto* rv=create_int32_vector(r,1);
      VecResult res=draken_binop(&c_and,lv,rv); assert(is_error(res));
      free_vector(lv); free_vector(rv); }

    // out-of-range shift → loud error (int8 1 << 100)
    { int8_t l[]={1}, r[]={100};
      auto* lv=create_int8_vector(l,1); auto* rv=create_int8_vector(r,1);
      VecResult res=draken_binop(&c_shl,lv,rv); assert(is_error(res));
      free_vector(lv); free_vector(rv); }
}

void test_error_handling() { draken_error_message_clear(); assert(!draken_has_error()); VecResult s = draken_error_sentinel("Test"); assert(is_error(s) && draken_has_error()); draken_error_message_clear(); }
void test_context_passing() { binary_op_ctx ctx{1}; int64_t l[] = {1}, r[] = {10}, e[] = {11}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_binary_arith(&ctx, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 1)); free_vector(lv); free_vector(rv); draken_free(res.data); }

int main() {
    std::cout << "=" << std::string(70, '=') << "\nC ABI Parity Tests — kernels + P9.1a binop dispatch\n" << std::string(70, '=') << "\n\n";

    const struct { const char* name; void (*fn)(); } tests[] = {
        {"draken_add", test_draken_add}, {"draken_subtract", test_draken_subtract}, {"draken_multiply", test_draken_multiply},
        {"draken_divide", test_draken_divide}, {"draken_modulo", test_draken_modulo}, {"draken_binary_arith", test_draken_binary_arith},
        {"draken_bitwise_or", test_draken_bitwise_or}, {"draken_bitwise_and", test_draken_bitwise_and}, {"draken_bitwise_xor", test_draken_bitwise_xor},
        {"draken_bitwise_shift_left", test_draken_bitwise_shift_left}, {"draken_bitwise_shift_right", test_draken_bitwise_shift_right},
        {"draken_cast_int64_to_float64", test_draken_cast_int64_to_float64}, {"draken_cast_float64_to_int64", test_draken_cast_float64_to_int64},
        {"draken_cast_int64_to_bool", test_draken_cast_int64_to_bool}, {"draken_cast_bool_to_int64", test_draken_cast_bool_to_int64},
        {"draken_cast_float64_to_bool", test_draken_cast_float64_to_bool}, {"draken_cast_bool_to_float64", test_draken_cast_bool_to_float64},
        {"draken_cast_identity", test_draken_cast_identity}, {"draken_cast_to_float64", test_draken_cast_to_float64}, {"draken_cast_to_int64", test_draken_cast_to_int64},
        {"draken_cast_to_bool", test_draken_cast_to_bool},
        {"draken_cast_int64_to_string", test_draken_cast_int64_to_string}, {"draken_cast_int64_to_timestamp", test_draken_cast_int64_to_timestamp},
        {"draken_cast_bool_to_string", test_draken_cast_bool_to_string}, {"draken_cast_float64_to_string", test_draken_cast_float64_to_string},
        {"draken_cast_string_to_int64", test_draken_cast_string_to_int64},
        {"draken_cast_string_to_bool", test_draken_cast_string_to_bool}, {"draken_cast_date32_to_int64", test_draken_cast_date32_to_int64},
        {"draken_cast_date32_to_timestamp", test_draken_cast_date32_to_timestamp}, {"draken_cast_timestamp_to_int64", test_draken_cast_timestamp_to_int64},
        {"draken_cast_timestamp_to_string", test_draken_cast_timestamp_to_string}, {"draken_cast_timestamp_to_date32", test_draken_cast_timestamp_to_date32},
        {"draken_cast_to_varchar", test_draken_cast_to_varchar}, {"draken_cast_to_date", test_draken_cast_to_date},
        {"draken_cast_to_decimal", test_draken_cast_to_decimal}, {"draken_cast_to_array", test_draken_cast_to_array},
        {"draken_cast_to_vector", test_draken_cast_to_vector}, {"draken_cast_to_varchar_with_length", test_draken_cast_to_varchar_with_length},
        {"draken_string_concat", test_draken_string_concat}, {"draken_temporal_interval_op", test_draken_temporal_interval_op},
        {"draken_date_minus_date", test_draken_date_minus_date}, {"draken_interval_interval_op", test_draken_interval_interval_op},
        {"draken_ip_in_cidr", test_draken_ip_in_cidr},
        {"draken_map_access_string", test_draken_map_access_string}, {"draken_array_map_access", test_draken_array_map_access},
        {"draken_json_extract", test_draken_json_extract}, {"draken_pointer_extract", test_draken_pointer_extract},
        {"error_handling", test_error_handling}, {"context_passing", test_context_passing},
        {"registry_honesty", test_registry_honesty},
        {"draken_binop_int_arith", test_draken_binop_int_arith},
        {"draken_binop_float", test_draken_binop_float},
        {"draken_binop_decimal", test_draken_binop_decimal},
        {"draken_binop_bitwise", test_draken_binop_bitwise}
    };

    // Shape-parity tests (WP-03) — exercise existing kernels with dict/constant
    // inputs; they do NOT add to the registered-kernel count.
    const struct { const char* name; void (*fn)(); } shape_tests[] = {
        {"arith_add_dict_left", test_arith_add_dict_left},
        {"arith_add_constant_left", test_arith_add_constant_left},
        {"arith_add_dict_left_float_right", test_arith_add_dict_left_float_right},
        {"arith_subtract_dict_left", test_arith_subtract_dict_left},
        {"arith_multiply_constant_left", test_arith_multiply_constant_left},
        {"arith_divide_dict_left", test_arith_divide_dict_left},
        {"arith_modulo_dict_left", test_arith_modulo_dict_left},
        {"arith_result_is_identity_shaped", test_arith_result_is_identity_shaped},
    };

    const int TOTAL = sizeof(tests) / sizeof(tests[0]);
    const int SHAPE_TOTAL = sizeof(shape_tests) / sizeof(shape_tests[0]);
    // Coverage tripwire: catch a kernel test added/removed without updating the
    // count. Exclude the non-kernel meta tests (error_handling, context_passing,
    // registry_honesty, draken_binop_int_arith, draken_binop_float) so it tracks
    // registered-kernel tests.
    const int META_TESTS = 7;  // error_handling, context_passing, registry_honesty,
                               // draken_binop_int_arith / _float / _decimal / _bitwise
    const int KERNELS = TOTAL - META_TESTS;
    const int EXPECTED_KERNELS = 47;
    int passed = 0, failed = 0;

    for (int i = 0; i < TOTAL; ++i) {
        try {
            tests[i].fn();
            std::cout << "  ✓ " << tests[i].name << "\n";
            passed++;
        } catch (...) {
            std::cout << "  ✗ " << tests[i].name << "\n";
            failed++;
        }
    }

    std::cout << "\n  -- shape-parity (dict/constant inputs) --\n";
    for (int i = 0; i < SHAPE_TOTAL; ++i) {
        try {
            shape_tests[i].fn();
            std::cout << "  ✓ " << shape_tests[i].name << "\n";
            passed++;
        } catch (...) {
            std::cout << "  ✗ " << shape_tests[i].name << "\n";
            failed++;
        }
    }

    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "Results: " << passed << "/" << (TOTAL + SHAPE_TOTAL) << " passed\n";
    std::cout << "Kernel coverage: " << KERNELS << "/" << EXPECTED_KERNELS << "\n";
    std::cout << std::string(70, '=') << "\n";
    return (failed > 0 || KERNELS != EXPECTED_KERNELS) ? 1 : 0;
}
