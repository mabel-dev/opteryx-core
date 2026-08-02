// src/cpp/platform.h
//
// Platform-specific system information functions.
//
// Supports macOS (ARM) and Linux (x86).
//
// Usage in Cython:
//     cpdef uint64_t get_physical_memory_total_bytes() noexcept
//     cpdef int get_cpu_count() noexcept
//     cpdef bint is_macos() noexcept

#pragma once

#include <cstdint>
#include <cstddef>

namespace opteryx_platform {

// Get total virtual memory in bytes
// On macOS: uses sysctl(CTL_HW, HW_MEMSIZE)
// On Linux: uses /proc/meminfo or sysconf fallback
uint64_t get_virtual_memory_total_bytes() noexcept;

// Get total physical RAM in bytes
// On macOS: uses sysctl(CTL_HW, HW_MEMSIZE)
// On Linux: uses /proc/meminfo or sysconf fallback
uint64_t get_physical_memory_total_bytes() noexcept;

// Get free/available memory in bytes
// Returns the memory that can be allocated without swapping
// On macOS: uses sysctl / host statistics
// On Linux: reads /proc/meminfo or uses sysconf(_SC_AVPHYS_PAGES)
uint64_t get_free_memory_bytes() noexcept;

// Get used memory in bytes
// Returns total_memory - free_memory
uint64_t get_used_memory_bytes() noexcept;

// Get the cgroup memory limit in bytes, i.e. the CONTAINER's ceiling, as
// distinct from get_physical_memory_total_bytes() (the HOST's RAM). Returns 0
// when there is no limit in effect (bare metal, macOS) or it cannot be
// detected — callers must treat 0 as "no limit figure available", not as a
// literal zero-byte limit.
// On Linux: reads cgroup v2 memory.max, falling back to cgroup v1
// memory.limit_in_bytes.
// On macOS: always 0 (no cgroups).
uint64_t get_cgroup_memory_limit_bytes() noexcept;

// Get system page size in bytes
uint64_t get_page_size_bytes() noexcept;



// Get CPU count (hardware threads)
// On macOS/Linux: uses sysconf(_SC_NPROCESSORS_ONLN)
int get_cpu_count() noexcept;

// Check if running on macOS
int is_macos() noexcept;

// Check if running on Linux
int is_linux() noexcept;

// Get memory utilization percentage (0.0 to 1.0)
double get_memory_utilization() noexcept;

// Get free memory as a fraction of total memory (0.0 to 1.0)
double get_memory_free_fraction() noexcept;

} // namespace opteryx_platform