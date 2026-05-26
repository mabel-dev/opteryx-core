#include "_distogram_core.h"

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>

int64_t distogram_sum_i64_neon(const int64_t* values, int64_t length) {
    int64_t i = 0;
    int64x2_t acc0 = vdupq_n_s64(0);
    int64x2_t acc1 = vdupq_n_s64(0);

    for (; i + 4 <= length; i += 4) {
        acc0 = vaddq_s64(acc0, vld1q_s64(values + i));
        acc1 = vaddq_s64(acc1, vld1q_s64(values + i + 2));
    }

    int64_t lanes0[2];
    int64_t lanes1[2];
    vst1q_s64(lanes0, acc0);
    vst1q_s64(lanes1, acc1);
    int64_t total = lanes0[0] + lanes0[1] + lanes1[0] + lanes1[1];

    for (; i < length; ++i) {
        total += values[i];
    }

    return total;
}
#else
int64_t distogram_sum_i64_neon(const int64_t* values, int64_t length) {
    return distogram_sum_i64_scalar(values, length);
}
#endif
