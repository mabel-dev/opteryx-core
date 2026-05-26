#include "_distogram_core.h"

#if defined(__AVX2__)
#include <immintrin.h>

int64_t distogram_sum_i64_avx2(const int64_t* values, int64_t length) {
    int64_t i = 0;
    __m256i acc = _mm256_setzero_si256();

    for (; i + 4 <= length; i += 4) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(values + i));
        acc = _mm256_add_epi64(acc, v);
    }

    int64_t lanes[4];
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes), acc);
    int64_t total = lanes[0] + lanes[1] + lanes[2] + lanes[3];

    for (; i < length; ++i) {
        total += values[i];
    }

    return total;
}
#else
int64_t distogram_sum_i64_avx2(const int64_t* values, int64_t length) {
    return distogram_sum_i64_scalar(values, length);
}
#endif
