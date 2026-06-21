#ifndef THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_
#define THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_

// Hand-maintained replacement for the CMake-generated config.h.
//
// Snappy normally configures itself via CMake feature probes. Opteryx vendors a
// single source tree built for two targets (macOS/ARM dev, Linux/x86 prod), so
// instead of a fixed per-platform file we key every feature off compiler- and
// architecture-predefined macros. This file is compiled identically on both
// targets and resolves to the correct fast paths for each.
//
// It is only honoured when the build defines HAVE_CONFIG_H (see setup.py).

// ---------------------------------------------------------------------------
// Compiler builtins — available on GCC and Clang (both define __GNUC__).
// Without these Snappy falls back to scalar Log2Floor and drops branch hints.
// ---------------------------------------------------------------------------
#if defined(__GNUC__)
#define HAVE_ATTRIBUTE_ALWAYS_INLINE 1
#define HAVE_BUILTIN_CTZ 1
#define HAVE_BUILTIN_EXPECT 1
#define HAVE_BUILTIN_PREFETCH 1
#else
#define HAVE_ATTRIBUTE_ALWAYS_INLINE 0
#define HAVE_BUILTIN_CTZ 0
#define HAVE_BUILTIN_EXPECT 0
#define HAVE_BUILTIN_PREFETCH 0
#endif

// ---------------------------------------------------------------------------
// POSIX headers / functions — present on macOS and Linux, absent on Windows.
// ---------------------------------------------------------------------------
#if !defined(_WIN32)
#define HAVE_FUNC_MMAP 1
#define HAVE_FUNC_SYSCONF 1
#define HAVE_SYS_MMAN_H 1
#define HAVE_SYS_RESOURCE_H 1
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_UIO_H 1
#define HAVE_UNISTD_H 1
#define HAVE_WINDOWS_H 0
#else
#define HAVE_FUNC_MMAP 0
#define HAVE_FUNC_SYSCONF 0
#define HAVE_SYS_MMAN_H 0
#define HAVE_SYS_RESOURCE_H 0
#define HAVE_SYS_TIME_H 0
#define HAVE_SYS_UIO_H 0
#define HAVE_UNISTD_H 0
#define HAVE_WINDOWS_H 1
#endif

// Optional comparison libraries — only used by Snappy's own test tool, never by
// the library we compile. Always off.
#define HAVE_LIBLZO2 0
#define HAVE_LIBZ 0
#define HAVE_LIBLZ4 0

// ---------------------------------------------------------------------------
// SIMD byte-shuffle for the decompression copy loop (IncrementalCopy).
//   - ARM NEON when the target advertises it (Apple Silicon dev, ARM prod).
//   - x86 SSSE3 when the target advertises it (requires -mssse3 / -march).
// SNAPPY_HAVE_BMI2 / SNAPPY_HAVE_X86_CRC32 / SNAPPY_HAVE_NEON_CRC32 are left
// undefined on purpose: snappy.cc auto-detects them from __BMI2__ / __SSE4_2__
// / __ARM_FEATURE_CRC32, and defining them here to 0 would suppress that.
// ---------------------------------------------------------------------------
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#define SNAPPY_HAVE_NEON 1
#else
#define SNAPPY_HAVE_NEON 0
#endif

#if defined(__SSSE3__)
#define SNAPPY_HAVE_SSSE3 1
#else
#define SNAPPY_HAVE_SSSE3 0
#endif

// Byte order — both targets are little-endian.
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define SNAPPY_IS_BIG_ENDIAN 1
#else
#define SNAPPY_IS_BIG_ENDIAN 0
#endif

#endif  // THIRD_PARTY_SNAPPY_OPENSOURCE_CMAKE_CONFIG_H_
