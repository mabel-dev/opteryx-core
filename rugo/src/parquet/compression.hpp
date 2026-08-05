#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace rugo {
namespace compression {

// Allocator whose value-less construct() default-initializes — i.e. leaves
// trivially-constructible elements uninitialized — instead of zeroing them.
// DecompressInto resizes the scratch buffer to the page's uncompressed size
// and the codec then overwrites every byte, so vector's value-initializing
// resize() is a full memset of the page paid for nothing.
template <typename T, typename A = std::allocator<T>>
struct default_init_allocator : public A {
    template <typename U>
    struct rebind {
        using other = default_init_allocator<
            U, typename std::allocator_traits<A>::template rebind_alloc<U>>;
    };
    using A::A;
    template <typename U>
    void construct(U* ptr)
        noexcept(std::is_nothrow_default_constructible<U>::value) {
        ::new (static_cast<void*>(ptr)) U;
    }
    template <typename U, typename... Args>
    void construct(U* ptr, Args&&... args) {
        std::allocator_traits<A>::construct(
            static_cast<A&>(*this), ptr, std::forward<Args>(args)...);
    }
};

// Reusable decompression scratch: grows without zero-filling the new bytes.
using ScratchBuffer = std::vector<uint8_t, default_init_allocator<uint8_t>>;

enum class CompressionCodec {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,      // legacy Hadoop-framed LZ4 (deprecated; unsupported — see compression.cpp)
    ZSTD = 6,
    LZ4_RAW = 7   // raw LZ4 block — what modern writers (Arrow/parquet-cpp) emit
};

// In-place variant: writes into caller-supplied buffer, resizing as needed.
// Reusing the same buffer across calls avoids repeated heap allocation.
void DecompressInto(
    const uint8_t* compressed_data,
    size_t compressed_size,
    size_t uncompressed_size,
    CompressionCodec codec,
    ScratchBuffer& out_buf
);

// Codec-specific implementations
std::vector<uint8_t> DecompressSnappy(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size
);

std::vector<uint8_t> DecompressZstd(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size
);

// Future extension point for GZIP
std::vector<uint8_t> DecompressGzip(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size
);

// Helper to convert parquet codec integers to our enum
CompressionCodec CodecFromInt(int32_t codec_int);

// Helper to get codec name for debugging
std::string CodecName(CompressionCodec codec);

}  // namespace compression
}  // namespace rugo