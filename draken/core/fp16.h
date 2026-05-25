#pragma once
// Draken fp16 wrappers — thin aliases over the vendored usearch fp16 implementation.
// Provides draken_fp16_to_fp32 / draken_fp32_to_fp16 for consumers (e.g. VectorVector).
#include <stdint.h>
#include <fp16/fp16.h>

static inline float draken_fp16_to_fp32(uint16_t h) {
    return fp16_ieee_to_fp32_value(h);
}

static inline uint16_t draken_fp32_to_fp16(float f) {
    return fp16_ieee_from_fp32_value(f);
}
