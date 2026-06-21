#include "compression.hpp"
#include <stdexcept>
#include <sstream>

// Include vendored compression libraries
#include "snappy.h"   // third_party/snappy (on the include path)
#include "vendor/zstd/zstd.h"

namespace rugo {
namespace compression {

// ---------------------------------------------------------------------------
// Thread-local ZSTD decompression context: created once per OS thread,
// reused for every ZSTD_decompressDCtx() call in that thread.  This avoids
// the ~128 KB internal malloc that ZSTD_decompress() performs per call.
// ---------------------------------------------------------------------------
namespace {
    ZSTD_DCtx* get_thread_dctx() {
        static thread_local ZSTD_DCtx* dctx = ZSTD_createDCtx();
        return dctx;
    }
} // anonymous namespace

std::vector<uint8_t> DecompressData(
    const uint8_t* compressed_data,
    size_t compressed_size, 
    size_t uncompressed_size,
    CompressionCodec codec) {
    
    switch (codec) {
        case CompressionCodec::UNCOMPRESSED:
            // For uncompressed data, just copy it
            return std::vector<uint8_t>(compressed_data, compressed_data + compressed_size);
            
        case CompressionCodec::SNAPPY:
            return DecompressSnappy(compressed_data, compressed_size, uncompressed_size);
            
        case CompressionCodec::ZSTD:
            return DecompressZstd(compressed_data, compressed_size, uncompressed_size);
            
        case CompressionCodec::GZIP:
            return DecompressGzip(compressed_data, compressed_size, uncompressed_size);
            
        default: {
            std::ostringstream oss;
            oss << "Unsupported compression codec: " << static_cast<int>(codec) 
                << " (" << CodecName(codec) << ")";
            throw std::runtime_error(oss.str());
        }
    }
}

std::vector<uint8_t> DecompressSnappy(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size) {
    
    std::vector<uint8_t> output(uncompressed_size);
    
    // Use vendored snappy to decompress
    if (!snappy::RawUncompress(
            reinterpret_cast<const char*>(data), 
            size,
            reinterpret_cast<char*>(output.data()))) {
        throw std::runtime_error("Snappy decompression failed");
    }
    
    return output;
}

std::vector<uint8_t> DecompressZstd(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size) {
    
    std::vector<uint8_t> output(uncompressed_size);
    
    size_t result = ZSTD_decompressDCtx(
        get_thread_dctx(),
        output.data(), 
        uncompressed_size, 
        data, 
        size
    );
    
    if (ZSTD_isError(result)) {
        std::ostringstream oss;
        oss << "Zstd decompression failed: " << ZSTD_getErrorName(result);
        throw std::runtime_error(oss.str());
    }
    
    if (result != uncompressed_size) {
        std::ostringstream oss;
        oss << "Zstd decompressed size mismatch: expected " 
            << uncompressed_size << ", got " << result;
        throw std::runtime_error(oss.str());
    }
    
    return output;
}

std::vector<uint8_t> DecompressGzip(
    const uint8_t* data, 
    size_t size, 
    size_t uncompressed_size) {
    
    // Not implemented yet - could use zlib or miniz later
    throw std::runtime_error("GZIP decompression not implemented yet");
}

CompressionCodec CodecFromInt(int32_t codec_int) {
    switch (codec_int) {
        case 0: return CompressionCodec::UNCOMPRESSED;
        case 1: return CompressionCodec::SNAPPY;
        case 2: return CompressionCodec::GZIP;
        case 3: return CompressionCodec::LZO;
        case 4: return CompressionCodec::BROTLI;
        case 5: return CompressionCodec::LZ4;
        case 6: return CompressionCodec::ZSTD;
        default: return static_cast<CompressionCodec>(codec_int);
    }
}

std::string CodecName(CompressionCodec codec) {
    switch (codec) {
        case CompressionCodec::UNCOMPRESSED: return "UNCOMPRESSED";
        case CompressionCodec::SNAPPY: return "SNAPPY";
        case CompressionCodec::GZIP: return "GZIP";
        case CompressionCodec::LZO: return "LZO";
        case CompressionCodec::BROTLI: return "BROTLI";
        case CompressionCodec::LZ4: return "LZ4";
        case CompressionCodec::ZSTD: return "ZSTD";
        default: return "UNKNOWN";
    }
}

// ---------------------------------------------------------------------------
// In-place decompression — writes into caller-supplied buffer.
// resize() does not reallocate when new size <= capacity, so reusing the same
// out_buf across consecutive pages in the same column chunk avoids per-page
// heap allocation after the first page.
// ---------------------------------------------------------------------------
void DecompressInto(
    const uint8_t* compressed_data,
    size_t compressed_size,
    size_t uncompressed_size,
    CompressionCodec codec,
    std::vector<uint8_t>& out_buf)
{
    switch (codec) {
        case CompressionCodec::UNCOMPRESSED:
            out_buf.assign(compressed_data, compressed_data + compressed_size);
            break;

        case CompressionCodec::SNAPPY: {
            out_buf.resize(uncompressed_size);
            if (!snappy::RawUncompress(
                    reinterpret_cast<const char*>(compressed_data),
                    compressed_size,
                    reinterpret_cast<char*>(out_buf.data()))) {
                throw std::runtime_error("Snappy decompression failed");
            }
            break;
        }

        case CompressionCodec::ZSTD: {
            out_buf.resize(uncompressed_size);
            size_t result = ZSTD_decompressDCtx(
                get_thread_dctx(),
                out_buf.data(), uncompressed_size,
                compressed_data, compressed_size);
            if (ZSTD_isError(result)) {
                std::ostringstream oss;
                oss << "Zstd decompression failed: " << ZSTD_getErrorName(result);
                throw std::runtime_error(oss.str());
            }
            break;
        }

        default: {
            std::ostringstream oss;
            oss << "DecompressInto: unsupported codec " << static_cast<int>(codec);
            throw std::runtime_error(oss.str());
        }
    }
}

}  // namespace compression
}  // namespace rugo