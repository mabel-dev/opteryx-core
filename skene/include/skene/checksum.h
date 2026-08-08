#pragma once
// skene/checksum.h — body integrity.
//
// Every section carries a checksum over its STORED bytes, and the footer carries
// its own. A corrupt column is therefore caught at that column, before a single
// pointer is rebuilt into it — not at some arbitrary later dereference.
//
// xxh3-64, from the xxhash draken already vendors and header-inlines, so this
// adds no dependency. The algorithm is identified in the file head
// (ChecksumAlgorithm) so it can be replaced without a required-section version
// bump.

#include <cstddef>
#include <cstdint>

#ifdef SKENE_FUZZING_SKIP_CHECKSUM
#pragma message("SKENE_FUZZING_SKIP_CHECKSUM is defined — this build ACCEPTS corrupt files and must never ship")
#endif

namespace skene {

uint64_t checksum_xxh3_64(const void* data, size_t bytes) noexcept;

// Whether a computed checksum must match the one the file recorded.
//
// Always true, except in a build that defines SKENE_FUZZING_SKIP_CHECKSUM.
//
// WHY THIS EXISTS. Every mutation a fuzzer makes to a real .skene file breaks a
// checksum, so the read is rejected here — before reaching the structural
// validation and buffer building, which is where a wrong offset turns into
// memory corruption rather than a wrong answer. Measured: 300 random mutations
// of a real file were all rejected, and none reached the interesting code. That
// is the classic problem of fuzzing behind a checksum, and the standard remedy
// is to let the mutator past the integrity layer so the layer beneath it can be
// reached at all.
//
// WHAT IT DOES NOT DO. It skips only the COMPARISON. The checksum is still
// computed, and — critically — every bounds check still runs: both call sites
// validate the section's extent against the data region BEFORE checksumming, so
// this never turns a rejected read into an out-of-bounds one. It also does not
// weaken any other validation: magic, version, endianness, head/tail agreement,
// declared extents, code bounds, arena invariants and offset monotonicity are
// all untouched.
//
// Compile-time rather than an environment variable on purpose: a shipped binary
// must be physically incapable of enabling it. Nothing in setup.py,
// build_common.py or skene/Makefile defines it — only
// tests/fuzzing/native/Makefile does, and `make -C skene check-no-fuzz-flag`
// checks that rather than trusting it.
inline bool checksum_must_match() noexcept {
#ifdef SKENE_FUZZING_SKIP_CHECKSUM
    return false;
#else
    return true;
#endif
}

}  // namespace skene
