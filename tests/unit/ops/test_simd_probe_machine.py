"""P0.4 — the SIMD probe reports the MACHINE, not the compiler flags.

Before this fix, cpu_architecture() returned ["AVX2"] on any x86 build
regardless of the CPU, and OPTERYX_FAIL_IF_NOT_AVX2 could never fire — the
"probe" probed the build target. Now x86 answers come from CPUID at import
time; AArch64 NEON is architectural so the compile-time answer is the machine
answer there.
"""

import os
import platform
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))


def test_probe_reports_machine():
    from opteryx.compiled.simd_probe import check_env_or_abort
    from opteryx.compiled.simd_probe import cpu_architecture

    caps = cpu_architecture()
    arch = platform.machine().lower()
    if arch in ("arm64", "aarch64"):
        assert "NEON" in caps, caps
        assert "AVX2" not in caps, caps  # never the build-flag lie on ARM
    elif arch in ("x86_64", "amd64"):
        assert "NEON" not in caps, caps
        # AVX2 presence is machine-dependent; on any post-2013 CPU it must be
        # reported. os.cpu-level cross-check via /proc where available.
        cpuinfo = "/proc/cpuinfo"
        if os.path.exists(cpuinfo):
            flags_has_avx2 = "avx2" in open(cpuinfo).read()
            assert ("AVX2" in caps) == flags_has_avx2, caps

    # With no OPTERYX_FAIL_IF_* flags set this must be a no-op, not an abort.
    os.environ.pop("OPTERYX_FAIL_IF_NOT_AVX2", None)
    check_env_or_abort()


if __name__ == "__main__":
    test_probe_reports_machine()
    print("✅ okay")
