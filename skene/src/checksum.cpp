#include "skene/checksum.h"

// Header-only build of the vendored xxhash, matching how draken's string_slot.h
// pulls it in. Defined here, in exactly one translation unit.
#define XXH_INLINE_ALL
#include "xxhash.h"

namespace skene {

uint64_t checksum_xxh3_64(const void* data, size_t bytes) noexcept {
    return static_cast<uint64_t>(XXH3_64bits(data, bytes));
}

}  // namespace skene
