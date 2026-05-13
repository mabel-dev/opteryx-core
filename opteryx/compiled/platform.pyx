# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# distutils: language = c++
# distutils: sources = src/cpp/platform.cpp
"""
Cython wrapper for platform-specific system information implemented in C++.

This module exposes a small, dependency-free API for querying:
- total/physical/free/used memory (bytes)
- page size (bytes)
- CPU count (hardware threads)
- OS checks (macOS/Linux)
- memory utilization metrics (fractions)
"""

from libc.stdint cimport uint64_t

cdef extern from "platform.h" namespace "opteryx_platform":
    uint64_t get_virtual_memory_total_bytes() noexcept
    uint64_t get_physical_memory_total_bytes() noexcept
    uint64_t get_free_memory_bytes() noexcept
    uint64_t get_used_memory_bytes() noexcept
    uint64_t get_page_size_bytes() noexcept
    int get_cpu_count() noexcept
    int is_macos() noexcept
    int is_linux() noexcept
    double get_memory_utilization() noexcept
    double get_memory_free_fraction() noexcept


# -------------------------
# Memory-related wrappers
# -------------------------
def virtual_memory_total_bytes() -> int:
    """Total virtual memory in bytes (best-effort)."""
    return int(get_virtual_memory_total_bytes())


def physical_memory_total_bytes() -> int:
    """Total physical (RAM) memory in bytes."""
    return int(get_physical_memory_total_bytes())


def free_memory_bytes() -> int:
    """Free/available memory in bytes (what can be used without swapping)."""
    return int(get_free_memory_bytes())


def used_memory_bytes() -> int:
    """Used memory in bytes (physical - free)."""
    return int(get_used_memory_bytes())


def page_size_bytes() -> int:
    """System page size in bytes (typically 4096)."""
    return int(get_page_size_bytes())


def memory_utilization() -> float:
    """Memory utilization as a fraction between 0.0 and 1.0."""
    return float(get_memory_utilization())


def memory_free_fraction() -> float:
    """Free memory as a fraction of total memory (0.0 - 1.0)."""
    return float(get_memory_free_fraction())


def get_memory_info() -> tuple:
    """
    Return memory info tuple:
      (total_physical_bytes, free_bytes, used_bytes, utilization_fraction, free_fraction)
    """
    cdef uint64_t total = get_physical_memory_total_bytes()
    cdef uint64_t free = get_free_memory_bytes()
    cdef uint64_t used = get_used_memory_bytes()
    cdef double util = get_memory_utilization()
    cdef double free_frac = get_memory_free_fraction()
    return (int(total), int(free), int(used), util, free_frac)


# -------------------------
# CPU / OS wrappers
# -------------------------
def cpu_count() -> int:
    """Number of hardware threads (logical CPUs)."""
    return int(get_cpu_count())


def is_macos_platform() -> bool:
    """True if the host OS is macOS (Darwin)."""
    return bool(is_macos())


def is_linux_platform() -> bool:
    """True if the host OS is Linux."""
    return bool(is_linux())
