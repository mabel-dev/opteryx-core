#pragma once

#include <cstddef>
#include <cstdint>
#include "type_widening.hpp"

// C Wrapper Functions for Type Widening (Tier 2C)
// Exposed to Cython for direct SIMD-accelerated conversions

extern "C" {

// Widen int32 array to int64 array using SIMD acceleration
inline void rugo_widen_int32_to_int64(const int32_t* src, int64_t* dst, size_t count) {
  parquet_simd::widen_int32_to_int64(src, dst, count);
}

// Widen float32 array to float64 array using SIMD acceleration
inline void rugo_widen_float32_to_float64(const float* src, double* dst, size_t count) {
  parquet_simd::widen_float32_to_float64(src, dst, count);
}

}  // extern "C"
