/**
 * Explicit template instantiations for Abseil containers used in Opteryx.
 * This ensures linker symbols are available at runtime.
 */

#include <cstdint>
#include <string>
#include <vector>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "identity_hash.h"

// Explicitly instantiate flat_hash_map with string values  
// Used by GroupStateStore for serialized key storage
template class absl::flat_hash_map<std::uint64_t, std::string>;

// Explicitly instantiate flat_hash_map with vector<uint8_t> values and IdentityHash
// Used by GroupStateStore for multi-column key serialization
template class absl::flat_hash_map<std::uint64_t, std::vector<std::uint8_t>, IdentityHash>;

// Explicitly instantiate flat_hash_map with int64_t values and IdentityHash
template class absl::flat_hash_map<std::uint64_t, std::int64_t, IdentityHash>;

// Explicitly instantiate flat_hash_map with double values and IdentityHash
template class absl::flat_hash_map<std::uint64_t, double, IdentityHash>;

// Explicitly instantiate flat_hash_map with uint8_t values and IdentityHash
template class absl::flat_hash_map<std::uint64_t, std::uint8_t, IdentityHash>;

// Explicitly instantiate flat_hash_set with IdentityHash
template class absl::flat_hash_set<std::uint64_t, IdentityHash>;

// Explicitly instantiate nested container for count distinct
template class absl::flat_hash_map<std::uint64_t, absl::flat_hash_set<std::uint64_t, IdentityHash>, IdentityHash>;
