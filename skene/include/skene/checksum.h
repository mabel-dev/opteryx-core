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

namespace skene {

uint64_t checksum_xxh3_64(const void* data, size_t bytes) noexcept;

}  // namespace skene
