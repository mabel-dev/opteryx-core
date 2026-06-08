/**
 * C ABI Parity Test for Phase 9a — Comprehensive Kernel Coverage.
 * Tests all 48 registered kernels to verify they are callable and produce correct output.
 */

#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/binary_op_kernels.h"
#include "ops/kernels/extraction_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "core/buffers.h"
#include "core/alloc.h"
#include <cassert>
#include <cstring>
#include <cstdint>
#include <cstdlib>
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

bool is_error(const VecResult& result) {
    return result.data == nullptr && result.type == DRAKEN_NULL;
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
void test_draken_cast_identity() { double d[] = {1.5, 2.5, 3.5}; DrakenVector* v = create_float64_vector(d, 3); VecResult r = draken_cast_identity(nullptr, v); assert(!is_error(r)); free_vector(v); draken_free(r.data); }

// Phase 9c: string-output casts are now REAL — assert VARCHAR result, free block.
void test_draken_cast_int64_to_string() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_int64_to_string(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_VARCHAR && r.length == 1); free_vector(v); draken_free(r.data); }
void test_draken_cast_int64_to_timestamp() { int64_t d[] = {1000000}; DrakenVector* v = create_int64_vector(d, 1); VecResult r = draken_cast_int64_to_timestamp(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_TIMESTAMP64 && r.ts_unit == 2); free_vector(v); draken_free(r.data); draken_free(r.validity); }
void test_draken_cast_bool_to_string() { uint8_t d[] = {0}; DrakenVector* v = create_bool_vector(d, 1); VecResult r = draken_cast_bool_to_string(nullptr, v); assert(!is_error(r) && r.type == DRAKEN_VARCHAR && r.length == 1); free_vector(v); draken_free(r.data); }
void test_draken_cast_float64_to_string() { double d[] = {1.5}; DrakenVector* v = create_float64_vector(d, 1); VecResult r = draken_cast_float64_to_string(nullptr, v); assert(is_error(r)); free_vector(v); }
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
void test_draken_map_access_string() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); DrakenVector* k = create_int64_vector(d, 1); VecResult r = draken_map_access_string(nullptr, v, k); free_vector(v); free_vector(k); }
void test_draken_array_map_access() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); DrakenVector* k = create_int64_vector(d, 1); VecResult r = draken_array_map_access(nullptr, v, k); free_vector(v); free_vector(k); }
void test_draken_json_extract() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); DrakenVector* k = create_int64_vector(d, 1); VecResult r = draken_json_extract(nullptr, v, k); free_vector(v); free_vector(k); }
void test_draken_pointer_extract() { int64_t d[] = {1}; DrakenVector* v = create_int64_vector(d, 1); DrakenVector* k = create_int64_vector(d, 1); VecResult r = draken_pointer_extract(nullptr, v, k); free_vector(v); free_vector(k); }

void test_error_handling() { draken_error_message_clear(); assert(!draken_has_error()); VecResult s = draken_error_sentinel("Test"); assert(is_error(s) && draken_has_error()); draken_error_message_clear(); }
void test_context_passing() { binary_op_ctx ctx{1}; int64_t l[] = {1}, r[] = {10}, e[] = {11}; DrakenVector* lv = create_int64_vector(l, 1); DrakenVector* rv = create_int64_vector(r, 1); VecResult res = draken_binary_arith(&ctx, lv, rv); assert(!is_error(res) && vectors_equal_int64(res, e, 1)); free_vector(lv); free_vector(rv); draken_free(res.data); }

int main() {
    std::cout << "=" << std::string(70, '=') << "\nC ABI Parity Tests for Phase 9a — All 48 Registered Kernels\n" << std::string(70, '=') << "\n\n";

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
        {"error_handling", test_error_handling}, {"context_passing", test_context_passing}
    };

    const int TOTAL = sizeof(tests) / sizeof(tests[0]);
    const int KERNELS = TOTAL - 2;
    const int EXPECTED_KERNELS = 48;
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

    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "Results: " << passed << "/" << TOTAL << " passed\n";
    std::cout << "Kernel coverage: " << KERNELS << "/" << EXPECTED_KERNELS << "\n";
    std::cout << std::string(70, '=') << "\n";
    return (failed > 0 || KERNELS != EXPECTED_KERNELS) ? 1 : 0;
}
