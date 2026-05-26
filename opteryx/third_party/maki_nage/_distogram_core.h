#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t (*distogram_sum_i64_fn)(const int64_t* values, int64_t length);

int64_t distogram_sum_i64_scalar(const int64_t* values, int64_t length);
int64_t distogram_sum_i64_avx2(const int64_t* values, int64_t length);
int64_t distogram_sum_i64_neon(const int64_t* values, int64_t length);
int64_t distogram_sum_i64_rvv(const int64_t* values, int64_t length);

distogram_sum_i64_fn distogram_select_sum_i64(void);
int64_t distogram_sum_i64(const int64_t* values, int64_t length);

#ifdef __cplusplus
}
#endif
