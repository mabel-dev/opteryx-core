#include "_distogram_core.h"

#if defined(__riscv) && defined(__riscv_vector)
#include <riscv_vector.h>

int64_t distogram_sum_i64_rvv(const int64_t* values, int64_t length) {
    int64_t total = 0;
    size_t i = 0;
    const size_t n = static_cast<size_t>(length);

    while (i < n) {
        size_t vl = __riscv_vsetvl_e64m1(n - i);
        vint64m1_t v = __riscv_vle64_v_i64m1(values + i, vl);
        vint64m1_t zero = __riscv_vmv_v_x_i64m1(0, vl);
        vint64m1_t reduced = __riscv_vredsum_vs_i64m1_i64m1(v, zero, vl);
        total += __riscv_vmv_x_s_i64m1_i64(reduced);
        i += vl;
    }

    return total;
}
#else
int64_t distogram_sum_i64_rvv(const int64_t* values, int64_t length) {
    return distogram_sum_i64_scalar(values, length);
}
#endif
