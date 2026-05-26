#include "_distogram_core.h"
#include "cpu_features.h"
#include "simd_dispatch.h"

#include <atomic>

int64_t distogram_sum_i64_scalar(const int64_t* values, int64_t length) {
    int64_t total = 0;
    for (int64_t i = 0; i < length; ++i) {
        total += values[i];
    }
    return total;
}

distogram_sum_i64_fn distogram_select_sum_i64(void) {
    static std::atomic<distogram_sum_i64_fn> cache{nullptr};
    return simd::select_dispatch<distogram_sum_i64_fn>(
        cache,
        {
#if defined(__AVX2__)
            {&cpu_supports_avx2, distogram_sum_i64_avx2},
#endif
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
            {&cpu_supports_neon, distogram_sum_i64_neon},
#endif
#if defined(__riscv) && defined(__riscv_vector)
            {&cpu_supports_rvv, distogram_sum_i64_rvv},
#endif
        },
        distogram_sum_i64_scalar
    );
}

int64_t distogram_sum_i64(const int64_t* values, int64_t length) {
    return distogram_select_sum_i64()(values, length);
}
