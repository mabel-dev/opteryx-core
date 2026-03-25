// src/cpp/platform.cpp
// Platform-specific system information functions.
//
// All symbols are defined inside namespace opteryx_platform to match the
// declarations in platform.h and the `cdef extern ... namespace "opteryx_platform"`
// block in platform.pyx.
//
// Supported targets:
// - macOS ARM64
// - Linux x86_64

#include "platform.h"

#if defined(__APPLE__)
#include <sys/param.h>
#include <sys/sysctl.h>
#include <mach/mach.h>
#include <unistd.h>
#endif

#if defined(__linux__)
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#endif

#include <cstdint>
#include <cstring>

namespace opteryx_platform {

uint64_t get_virtual_memory_total_bytes() noexcept {
#if defined(__APPLE__)
    int64_t mem = 0;
    size_t size = sizeof(mem);
    int mib[] = {CTL_HW, HW_MEMSIZE};
    if (sysctl(mib, 2, &mem, &size, nullptr, 0) == 0 && mem > 0) {
        return static_cast<uint64_t>(mem);
    }
    return 0;
#elif defined(__linux__)
    std::ifstream meminfo("/proc/meminfo");
    if (meminfo) {
        std::string line;
        while (std::getline(meminfo, line)) {
            if (line.rfind("MemTotal:", 0) == 0) {
                std::istringstream iss(line.substr(9));
                uint64_t kb = 0;
                iss >> kb;
                return kb * 1024ULL;
            }
        }
    }
    const long pages = sysconf(_SC_PHYS_PAGES);
    const long page_size = sysconf(_SC_PAGESIZE);
    if (pages > 0 && page_size > 0) {
        return static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);
    }
    return 0;
#else
    return 0;
#endif
}

uint64_t get_physical_memory_total_bytes() noexcept {
    return get_virtual_memory_total_bytes();
}

// Defined before get_free_memory_bytes so it can be called without a forward
// declaration.
uint64_t get_page_size_bytes() noexcept {
#if defined(__APPLE__) || defined(__linux__)
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size > 0) {
        return static_cast<uint64_t>(page_size);
    }
    return 0;
#else
    return 0;
#endif
}

uint64_t get_free_memory_bytes() noexcept {
#if defined(__APPLE__)
    // VM_FREE_COUNT is a struct field, not a sysctl MIB constant.  Use the
    // Mach host_statistics64 API to obtain the free page count.
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    vm_statistics64_data_t vm_stats;
    if (host_statistics64(mach_host_self(), HOST_VM_INFO64,
                          (host_info64_t)&vm_stats, &count) == KERN_SUCCESS) {
        const uint64_t page_size = get_page_size_bytes();
        if (page_size > 0) {
            return static_cast<uint64_t>(vm_stats.free_count) * page_size;
        }
    }
    const uint64_t phys = get_physical_memory_total_bytes();
    return phys / 4;
#elif defined(__linux__)
    std::ifstream meminfo("/proc/meminfo");
    if (meminfo) {
        std::string line;
        uint64_t available_kb = 0;
        uint64_t free_kb = 0;
        uint64_t buffers_kb = 0;
        uint64_t cached_kb = 0;
        while (std::getline(meminfo, line)) {
            if (line.rfind("MemAvailable:", 0) == 0) {
                std::istringstream iss(line.substr(13));
                iss >> available_kb;
            } else if (line.rfind("MemFree:", 0) == 0) {
                std::istringstream iss(line.substr(8));
                iss >> free_kb;
            } else if (line.rfind("Buffers:", 0) == 0) {
                std::istringstream iss(line.substr(8));
                iss >> buffers_kb;
            } else if (line.rfind("Cached:", 0) == 0) {
                std::istringstream iss(line.substr(7));
                iss >> cached_kb;
            }
        }
        if (available_kb > 0) {
            return available_kb * 1024ULL;
        }
        return (free_kb + buffers_kb + cached_kb) * 1024ULL;
    }
    const long pages = sysconf(_SC_AVPHYS_PAGES);
    const long page_size = sysconf(_SC_PAGESIZE);
    if (pages > 0 && page_size > 0) {
        return static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);
    }
    return 0;
#else
    return 0;
#endif
}

uint64_t get_used_memory_bytes() noexcept {
    const uint64_t total = get_physical_memory_total_bytes();
    const uint64_t free = get_free_memory_bytes();
    if (total == 0 || free >= total) {
        return 0;
    }
    return total - free;
}

int get_cpu_count() noexcept {
#if defined(__APPLE__) || defined(__linux__)
    const long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? static_cast<int>(n) : 0;
#else
    return 0;
#endif
}

int is_macos() noexcept {
#if defined(__APPLE__)
    return 1;
#else
    return 0;
#endif
}

int is_linux() noexcept {
#if defined(__linux__)
    return 1;
#else
    return 0;
#endif
}

double get_memory_utilization() noexcept {
    const uint64_t total = get_physical_memory_total_bytes();
    const uint64_t free = get_free_memory_bytes();
    if (total == 0 || free >= total) {
        return 0.0;
    }
    return static_cast<double>(total - free) / static_cast<double>(total);
}

double get_memory_free_fraction() noexcept {
    const uint64_t total = get_physical_memory_total_bytes();
    const uint64_t free = get_free_memory_bytes();
    if (total == 0) {
        return 0.0;
    }
    return static_cast<double>(free) / static_cast<double>(total);
}

}  // namespace opteryx_platform