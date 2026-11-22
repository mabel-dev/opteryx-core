// The ONE host CPU count every pool and worker width derives from.
//
// HOST_CPU_COUNT (environment, a positive integer) replaces the hardware's answer
// everywhere - draken, rugo, skene and opteryx, native and Python (Python reads it
// through opteryx.compiled.platform.cpu_count / rugo_native.host_cpu_override, never
// os.cpu_count). It exists so a small host can run at a large host's widths: the
// `make wide` floor leg runs at 190, the shape of the ClickBench c6a.metal box whose
// width-only bugs (66+ vCPU fetch-ahead refusal, 2026-10-04; the slab frontier leak
// and exit deadlock, 2026-11-22) no 4-vCPU runner could otherwise reach. It does NOT
// emulate the host's cores - 190 threads on 4 CPUs are correct, not fast.
//
// Read once per process (the first call); unset or empty = the hardware. A value
// that is not a positive integer aborts with a message - a misspelt override that
// silently ran at the real width would fake the very test it exists for.
// Header-only and Python-free: rugo and draken must run without Python.
#ifndef DRAKEN_CORE_HOST_CPUS_H
#define DRAKEN_CORE_HOST_CPUS_H

#include <cstdio>
#include <cstdlib>
#include <thread>

// The override, or 0 when HOST_CPU_COUNT is unset or empty.
inline unsigned draken_host_cpu_override() {
    static const unsigned value = [] {
        const char* s = std::getenv("HOST_CPU_COUNT");
        if (s == nullptr || s[0] == '\0') return 0u;
        char* end = nullptr;
        const unsigned long v = std::strtoul(s, &end, 10);
        if (*end != '\0' || v == 0 || v > 65536 || s[0] == '-' || s[0] == '+') {
            std::fprintf(stderr, "HOST_CPU_COUNT=%s is not a positive integer (1..65536)\n", s);
            std::fflush(stderr);
            std::abort();
        }
        return static_cast<unsigned>(v);
    }();
    return value;
}

// Drop-in for std::thread::hardware_concurrency(): the override, else the hardware's
// answer - including its 0 for "unknown", which every caller already handles.
inline unsigned draken_host_cpus() {
    const unsigned o = draken_host_cpu_override();
    return o != 0 ? o : std::thread::hardware_concurrency();
}

#endif  // DRAKEN_CORE_HOST_CPUS_H
