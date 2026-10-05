#pragma once
// rugo/src/chunk_limit.hpp — the most bytes a text reader (JSONL, CSV) parses as one
// buffer. Shared so the two readers cannot disagree about it.
//
// Every position those parsers store is a uint32_t offset from the start of the buffer
// they walk, so no walked buffer may be longer than kMaxChunkBytes: a larger input is
// read as CHUNKS of at most this many bytes, each cut at a row boundary and walked from
// its own start, and a single row longer than this cannot be read at all (it throws
// std::length_error).

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace rugo {

inline constexpr size_t kMaxChunkBytes = UINT32_MAX;

// Throws std::length_error unless `length` fits kMaxChunkBytes. `what` names the caller.
inline void require_chunk_length(size_t length, const char* what) {
    if (length > kMaxChunkBytes)
        throw std::length_error(std::string(what) + ": " + std::to_string(length) +
                                " bytes in one buffer; at most " + std::to_string(kMaxChunkBytes) +
                                " (4 GiB) can be parsed at once");
}

}  // namespace rugo
