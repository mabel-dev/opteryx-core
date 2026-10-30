#include "compression.hpp"
#include <stdexcept>
#include <sstream>
#include <climits>
#include <cstring>

// Include vendored compression libraries
#include "snappy.h"   // third_party/snappy (on the include path)
#include "zstd.h"           // canonical vendored copy: third_party/zstd
#include "lz4.h"            // LZ4 block codec: LZ4_decompress_safe (third_party/lz4)
#include "libdeflate.h"      // gzip member inflate + CRC32/ISIZE check (third_party/libdeflate)

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

    // -----------------------------------------------------------------------
    // LZ4_RAW (parquet codec 7): a single raw LZ4 *block* — no frame, no length
    // prefix. The decompressed size comes from the page header. Decoded straight
    // through the block API. The LZ4 *frame* format (LZ4F, magic 0x184D2204) is
    // NOT used by parquet and is intentionally not linked.
    // -----------------------------------------------------------------------
    void lz4_raw_decode(const uint8_t* data, size_t size,
                        uint8_t* out, size_t out_size) {
        if (size > static_cast<size_t>(INT_MAX) ||
            out_size > static_cast<size_t>(INT_MAX)) {
            throw std::runtime_error(
                "LZ4 decompression: page exceeds the 2 GiB block limit");
        }
        int produced = LZ4_decompress_safe(
            reinterpret_cast<const char*>(data),
            reinterpret_cast<char*>(out),
            static_cast<int>(size),
            static_cast<int>(out_size));
        if (produced < 0) {
            std::ostringstream oss;
            oss << "LZ4 decompression failed (malformed block, error at input byte "
                << (-produced) << ")";
            throw std::runtime_error(oss.str());
        }
        if (static_cast<size_t>(produced) != out_size) {
            std::ostringstream oss;
            oss << "LZ4 decompressed size mismatch: expected " << out_size
                << ", got " << produced;
            throw std::runtime_error(oss.str());
        }
    }

    // libdeflate decompressor, one per OS thread like the ZSTD context above. It
    // holds no stream state between calls, only the decode tables it rebuilds.
    libdeflate_decompressor* get_thread_inflater() {
        static thread_local libdeflate_decompressor* d = libdeflate_alloc_decompressor();
        if (d == nullptr) {
            throw std::runtime_error("GZIP decompression: out of memory");
        }
        return d;
    }

    // -----------------------------------------------------------------------
    // GZIP (parquet codec 2): an RFC 1952 gzip stream — one OR MORE concatenated
    // members, each a header + raw DEFLATE payload + 8-byte CRC32/ISIZE trailer.
    // Some writers (notably Hadoop/Java) emit multi-member pages, so we loop:
    // libdeflate_gzip_decompress_ex inflates one whole member into `out` at the
    // running offset — parsing its header and verifying its CRC32 and ISIZE —
    // and reports the input bytes it consumed, which is where the next member
    // starts (member boundaries aren't otherwise discoverable).
    // -----------------------------------------------------------------------
    void gzip_decode(const uint8_t* data, size_t size,
                     uint8_t* out, size_t out_size) {
        // Minimum valid stream: 10-byte header + 8-byte trailer.
        if (size < 18) {
            throw std::runtime_error(
                "GZIP decompression: input too short to be a gzip stream");
        }
        libdeflate_decompressor* d = get_thread_inflater();
        size_t total_out = 0;  // bytes written to `out` across all members
        size_t pos = 0;        // input cursor over the whole gzip stream
        while (pos < size) {
            size_t in_used = 0;
            size_t produced = 0;
            libdeflate_result rc = libdeflate_gzip_decompress_ex(
                d, data + pos, size - pos,
                out + total_out, out_size - total_out, &in_used, &produced);
            if (rc == LIBDEFLATE_INSUFFICIENT_SPACE) {
                std::ostringstream oss;
                oss << "GZIP decompressed size mismatch: the page decompresses to "
                       "more than the expected " << out_size << " bytes";
                throw std::runtime_error(oss.str());
            }
            if (rc != LIBDEFLATE_SUCCESS) {
                throw std::runtime_error(
                    "GZIP decompression failed (malformed gzip member: bad header, "
                    "corrupt DEFLATE data, or CRC32/length mismatch)");
            }
            total_out += produced;
            pos += in_used;

            // Fast path / normal termination: once the page's expected output is
            // fully produced, stop — trailing bytes belong to no further member.
            if (total_out == out_size) break;
        }
        if (total_out != out_size) {
            std::ostringstream oss;
            oss << "GZIP decompressed size mismatch: expected " << out_size
                << ", got " << total_out;
            throw std::runtime_error(oss.str());
        }
    }

    // RawUncompress writes as many bytes as the stream's own preamble says, so
    // that length must equal the buffer the page header sized for us.
    void snappy_decode(const uint8_t* src, size_t n, uint8_t* out, size_t out_size) {
        size_t len = 0;
        if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(src), n, &len))
            throw std::runtime_error("Snappy decompression failed: bad length preamble");
        if (len != out_size) {
            std::ostringstream oss;
            oss << "Snappy decompressed size mismatch: expected " << out_size
                << ", stream declares " << len;
            throw std::runtime_error(oss.str());
        }
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(src), n,
                                   reinterpret_cast<char*>(out)))
            throw std::runtime_error("Snappy decompression failed");
    }
} // anonymous namespace

std::vector<uint8_t> DecompressSnappy(
    const uint8_t* data,
    size_t size,
    size_t uncompressed_size) {

    std::vector<uint8_t> output(uncompressed_size);
    snappy_decode(data, size, output.data(), uncompressed_size);
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
    
    std::vector<uint8_t> output(uncompressed_size);
    gzip_decode(data, size, output.data(), uncompressed_size);
    return output;
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
        case 7: return CompressionCodec::LZ4_RAW;
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
        case CompressionCodec::LZ4_RAW: return "LZ4_RAW";
        default: return "UNKNOWN";
    }
}

// ---------------------------------------------------------------------------
// In-place decompression — writes into caller-supplied buffer.
// resize() does not reallocate when new size <= capacity, so reusing the same
// out_buf across consecutive pages in the same column chunk avoids per-page
// heap allocation after the first page.
// ---------------------------------------------------------------------------
void DecompressIntoRaw(
    const uint8_t* compressed_data,
    size_t compressed_size,
    size_t uncompressed_size,
    CompressionCodec codec,
    uint8_t* out)
{
    switch (codec) {
        case CompressionCodec::UNCOMPRESSED:
            if (compressed_size != uncompressed_size) {
                throw std::runtime_error(
                    "uncompressed page: stored size differs from the header's uncompressed size");
            }
            std::memcpy(out, compressed_data, compressed_size);
            break;

        case CompressionCodec::SNAPPY: {
            snappy_decode(compressed_data, compressed_size, out, uncompressed_size);
            break;
        }

        case CompressionCodec::ZSTD: {
            size_t result = ZSTD_decompressDCtx(
                get_thread_dctx(),
                out, uncompressed_size,
                compressed_data, compressed_size);
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
            break;
        }

        case CompressionCodec::GZIP: {
            gzip_decode(compressed_data, compressed_size, out, uncompressed_size);
            break;
        }

        case CompressionCodec::LZ4_RAW: {
            lz4_raw_decode(compressed_data, compressed_size, out, uncompressed_size);
            break;
        }

        case CompressionCodec::LZ4: {
            // Legacy Hadoop-framed LZ4 (parquet codec 5): deprecated and
            // ambiguously framed. Modern writers (Arrow/parquet-cpp) emit
            // LZ4_RAW (codec 7) instead. We do not guess the Hadoop framing —
            // fail loud with a clear, actionable message rather than silently
            // producing wrong data.
            throw std::runtime_error(
                "LZ4 (Hadoop-framed, parquet codec 5) is not supported; "
                "rewrite the file with LZ4_RAW (codec 7) or ZSTD");
        }

        default: {
            std::ostringstream oss;
            oss << "DecompressInto: unsupported codec " << static_cast<int>(codec);
            throw std::runtime_error(oss.str());
        }
    }
}

void DecompressInto(
    const uint8_t* compressed_data,
    size_t compressed_size,
    size_t uncompressed_size,
    CompressionCodec codec,
    draken::AppendBuffer<uint8_t>& out_buf)
{
    if (codec == CompressionCodec::UNCOMPRESSED) {
        out_buf.assign(compressed_data, compressed_data + compressed_size);
        return;
    }
    out_buf.resize_uninit(uncompressed_size);
    DecompressIntoRaw(compressed_data, compressed_size, uncompressed_size, codec, out_buf.data());
}

}  // namespace compression
}  // namespace rugo