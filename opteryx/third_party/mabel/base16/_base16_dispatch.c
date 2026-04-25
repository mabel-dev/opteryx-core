#include "_base16.h"
#include <string.h>

static int cpu_features_detected = 0;
static b16_cpu_features features = {0};

#ifdef __x86_64__
#include <cpuid.h>

static void x86_cpuid(int function, int subfunction, int* cpuinfo) {
    __cpuid_count(function, subfunction, cpuinfo[0], cpuinfo[1], cpuinfo[2], cpuinfo[3]);
}

static int check_x86_feature(int feature) {
    int cpuinfo[4];
    x86_cpuid(1, 0, cpuinfo);
    return (cpuinfo[2] & feature) != 0;
}

static int check_avx2(void) {
    int cpuinfo[4];
    x86_cpuid(7, 0, cpuinfo);
    return (cpuinfo[1] & (1 << 5)) != 0;
}

static int check_avx512(void) {
    int cpuinfo[4];
    x86_cpuid(7, 0, cpuinfo);
    int has_avx512f = (cpuinfo[1] & (1 << 16)) != 0;
    int has_avx512bw = (cpuinfo[1] & (1 << 30)) != 0;
    return has_avx512f && has_avx512bw;
}
#endif

b16_cpu_features b16_detect_cpu_features(void) {
    if (cpu_features_detected) {
        return features;
    }

    memset(&features, 0, sizeof(features));

    // NEON detection (ARM)
#if defined(__ARM_NEON) || defined(__aarch64__)
    features.neon = 1;
#endif

    // AVX2 and AVX512 detection (x86)
#ifdef __x86_64__
    if (check_x86_feature(1 << 27)) { // OSXSAVE
        if (check_x86_feature(1 << 28)) { // AVX
            features.avx2 = check_avx2();
            features.avx512 = check_avx512();
        }
    }
#endif

    cpu_features_detected = 1;
    return features;
}

void b16_force_scalar(void) {
    features.neon = 0;
    features.avx2 = 0;
    features.avx512 = 0;
    cpu_features_detected = 1;
}

int b16_has_neon(void) {
    if (!cpu_features_detected) {
        b16_detect_cpu_features();
    }
    return features.neon;
}

int b16_has_avx2(void) {
    if (!cpu_features_detected) {
        b16_detect_cpu_features();
    }
    return features.avx2;
}

int b16_has_avx512(void) {
    if (!cpu_features_detected) {
        b16_detect_cpu_features();
    }
    return features.avx512;
}

// Auto-dispatch implementations for core API
void* b16tobin_len(void* restrict dest, const char* restrict src, size_t len) {
    if (!cpu_features_detected) {
        b16_detect_cpu_features();
    }

    if (features.avx512 && len >= 64) {
        // If AVX512 implementations are added later, call them here
        return b16tobin_avx2(dest, src, len);
    } else if (features.avx2 && len >= 32) {
        return b16tobin_avx2(dest, src, len);
    } else if (0 && features.neon && len >= 16) {
        // Disabled NEON decode path temporarily due to correctness bug
        return b16tobin_neon(dest, src, len);
    } else {
        return b16tobin_scalar(dest, src, len);
    }
}

void* b16tobin(void* restrict dest, const char* restrict src) {
    return b16tobin_len(dest, src, strlen(src));
}

char* bintob16(char* restrict dest, const void* restrict src, size_t size) {
    if (!cpu_features_detected) {
        b16_detect_cpu_features();
    }

    if (features.avx512 && size >= 48) {
        return bintob16_avx2(dest, src, size);
    } else if (features.avx2 && size >= 24) {
        return bintob16_avx2(dest, src, size);
#if defined(__ARM_NEON) || defined(__ARM_NEON__) || defined(__aarch64__)
    } else if (features.neon && size >= 12) {
        return bintob16_neon(dest, src, size);
#endif
    } else {
        return bintob16_scalar(dest, src, size);
    }
}
