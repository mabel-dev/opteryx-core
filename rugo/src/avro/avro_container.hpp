#pragma once
// rugo/src/avro/avro_container.hpp — the Avro object container file.
//
//   magic "Obj\x01" | metadata map<bytes> | 16-byte sync |
//   { long count | long byte_size | byte_size bytes | 16-byte sync }*
//
// Design: docs/AVRO_READER_DESIGN.md §3.1–3.2. Pure C++.
// Codecs: null, deflate (raw RFC 1951, libdeflate), snappy (+ big-endian CRC32 of the
// uncompressed bytes), zstandard. Anything else is refused by name.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "core/append_buffer.h"

struct libdeflate_decompressor;  // third_party/libdeflate (C, global namespace)

namespace rugo::avro {

enum class BlockCodec : uint8_t { Null, Deflate, Snappy, Zstandard };

struct Header {
    std::string schema_json;
    BlockCodec  codec = BlockCodec::Null;
    uint8_t     sync[16];
    // Every metadata entry, raw, in file order (avro.schema and avro.codec included).
    std::vector<std::pair<std::string, std::string>> metadata;
    size_t      first_block = 0;  // offset of the first block
};

// Parse the header. Throws std::runtime_error on bad magic, a malformed map, no
// `avro.schema`, or an unsupported codec.
Header read_header(const uint8_t* data, size_t size);

struct Block {
    int64_t        count = 0;     // records in the block
    const uint8_t* bytes = nullptr;  // the block's (possibly compressed) payload
    size_t         size = 0;
};

// Walks the blocks after the header. next() returns false at a clean end of file;
// a sync mismatch or a block running past the end throws.
class BlockReader {
public:
    BlockReader(const uint8_t* data, size_t size, const Header& h);
    bool next(Block& out);
    size_t index() const { return index_; }  // blocks returned so far
private:
    const uint8_t* data_;
    size_t size_;
    size_t pos_;
    const uint8_t* sync_;
    size_t index_ = 0;
};

// Per-stream decompression state reused across blocks: the libdeflate decompressor
// (allocated once, not per block) and the scratch the payload is decompressed into.
class BlockDecoder {
public:
    BlockDecoder();
    ~BlockDecoder();
    BlockDecoder(const BlockDecoder&) = delete;
    BlockDecoder& operator=(const BlockDecoder&) = delete;

    // The block's records as raw Avro binary: `b.bytes` itself for the null codec,
    // else decompressed into this decoder's scratch (valid until the next call).
    // Throws on corrupt data or a CRC mismatch.
    std::pair<const uint8_t*, size_t> payload(const Block& b, BlockCodec codec);

private:
    ::libdeflate_decompressor* inflater_ = nullptr;  // created on first deflate block
    draken::AppendBuffer<uint8_t> scratch_;
};

}  // namespace rugo::avro
