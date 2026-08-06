#include "simd_env.h"
#include "cpu_features.h"

#include <cstdlib>
#include <cstdio>

static bool env_bool(const char* name) {
    const char* v = std::getenv(name);
    return v != nullptr && v[0] != '\0';
}

// ---------------------------------------------------------------------------
// Machine probe — what the CPU we are RUNNING ON supports, via CPUID.
//
// cpu_features.h's cpu_supports_avx2() is deliberately compile-time (it
// reports what the BUILD targets; hot-path dispatch is resolved at compile
// time and must stay that way). That is the wrong question for this file:
// the guard and cpu_architecture() exist to tell a user whether this wheel
// can run on this machine — a Haswell-built wheel on a pre-2013 x86 dies
// with a bare SIGILL unless the guard fires first. Import-time only; never
// on a hot path.
// ---------------------------------------------------------------------------
#if defined(__x86_64__) || defined(_M_X64)
#include <cpuid.h>

static int machine_avx2_probe() {
    unsigned a, b, c, d;
    if (!__get_cpuid(1, &a, &b, &c, &d)) return 0;
    if (!(c & (1u << 27))) return 0;  /* OSXSAVE */
    if (!(c & (1u << 28))) return 0;  /* AVX */
    if (!__get_cpuid_count(7, 0, &a, &b, &c, &d)) return 0;
    return (b & (1u << 5)) != 0;      /* AVX2 */
}

static int machine_avx2() {
    static const int probed = machine_avx2_probe();
    return probed;
}
#else
static int machine_avx2() { return 0; }
#endif

void opteryx_check_simd_env_or_abort() {
    if (env_bool("OPTERYX_FAIL_IF_NOT_AVX2")) {
        if (!machine_avx2()) {
            std::fprintf(stderr,
                "OPTERYX_FAIL_IF_NOT_AVX2 is set but this CPU does not support "
                "AVX2 (build targets %s). This wheel requires an AVX2-capable "
                "x86-64 CPU (Haswell/2013 or later).\n",
                cpu_supports_avx2() ? "AVX2" : "no AVX2");
            std::abort();
        }
    }
}

// Machine truth on x86 (CPUID); architectural truth elsewhere (NEON is
// baseline on AArch64, so the compile-time answer IS the machine answer).
int opteryx_cpu_supports_avx2() {
#if defined(__x86_64__) || defined(_M_X64)
    return machine_avx2();
#else
    return 0;
#endif
}

int opteryx_cpu_supports_neon() { return cpu_supports_neon() ? 1 : 0; }
