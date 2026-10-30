"""draken refuses, with a message, to load on an x86-64 CPU below the Haswell floor
its native code is compiled for - instead of dying with SIGILL."""

from draken import _missing_x86_flags

HASWELL = (
    "processor\t: 0\n"
    "flags\t\t: fpu sse sse2 ssse3 sse4_1 sse4_2 popcnt avx avx2 fma f16c bmi1 bmi2 movbe abm aes\n"
)
SANDY_BRIDGE = (
    "processor\t: 0\n"
    "flags\t\t: fpu sse sse2 ssse3 sse4_1 sse4_2 popcnt avx aes\n"
)


def test_a_haswell_cpu_meets_the_floor():
    assert _missing_x86_flags(HASWELL) == frozenset()


def test_an_older_cpu_names_what_it_lacks():
    assert _missing_x86_flags(SANDY_BRIDGE) == {"avx2", "fma", "f16c", "bmi1", "bmi2", "movbe", "abm"}


def test_no_flags_line_decides_nothing():
    assert _missing_x86_flags("processor\t: 0\n") == frozenset()
