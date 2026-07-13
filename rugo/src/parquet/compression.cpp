#include "compression.hpp"
#include <stdexcept>
#include <sstream>
#include <climits>

// Include vendored compression libraries
#include "snappy.h"   // third_party/snappy (on the include path)
#include "zstd.h"           // canonical vendored copy: third_party/zstd
#include "lz4.h"            // LZ4 block codec: LZ4_decompress_safe (third_party/lz4)
#include "miniz_tinfl.h"      // miniz raw-DEFLATE inflate: tinfl_decompress_mem_to_mem

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

    // Parse one RFC 1952 gzip member header starting at data[pos] and return the
    // offset of its DEFLATE payload. Throws on a malformed/truncated header.
    // Header layout: 10 fixed bytes (ID1 ID2 CM FLG MTIME[4] XFL OS) then, per the
    // FLG bits, optional FEXTRA / FNAME / FCOMMENT / FHCRC fields.
    size_t parse_gzip_header(const uint8_t* data, size_t size, size_t pos) {
        if (pos + 10 > size) {
            throw std::runtime_error("GZIP decompression: truncated member header");
        }
        if (data[pos] != 0x1f || data[pos + 1] != 0x8b) {
            throw std::runtime_error(
                "GZIP decompression: bad magic (expected 1f 8b, parquet GZIP "
                "must be RFC 1952 gzip)");
        }
        if (data[pos + 2] != 8) {
            throw std::runtime_error(
                "GZIP decompression: unsupported compression method (not DEFLATE)");
        }
        const uint8_t flg = data[pos + 3];
        pos += 10;  // fixed header size
        if (flg & 0x04) {  // FEXTRA
            if (pos + 2 > size)
                throw std::runtime_error("GZIP decompression: truncated FEXTRA field");
            size_t xlen = static_cast<size_t>(data[pos]) |
                          (static_cast<size_t>(data[pos + 1]) << 8);
            pos += 2 + xlen;
        }
        if (flg & 0x08) {  // FNAME (NUL-terminated)
            while (pos < size && data[pos] != 0) ++pos;
            ++pos;  // skip the NUL
        }
        if (flg & 0x10) {  // FCOMMENT (NUL-terminated)
            while (pos < size && data[pos] != 0) ++pos;
            ++pos;
        }
        if (flg & 0x02) {  // FHCRC
            pos += 2;
        }
        if (pos + 8 > size) {  // header fields + 8-byte trailer must still fit
            throw std::runtime_error(
                "GZIP decompression: truncated header or trailer");
        }
        return pos;
    }

    // -----------------------------------------------------------------------
    // GZIP (parquet codec 2): an RFC 1952 gzip stream — one OR MORE concatenated
    // members, each a header + raw DEFLATE payload + 8-byte CRC32/ISIZE trailer.
    // Some writers (notably Hadoop/Java) emit multi-member pages, so we loop:
    // parse each member's header, inflate its DEFLATE body into `out` at the
    // running offset, then advance past the DEFLATE bytes tinfl consumed plus the
    // 8-byte trailer to the next member. The low-level tinfl_decompress reports
    // input-bytes-consumed (member boundaries aren't otherwise discoverable).
    // -----------------------------------------------------------------------
    void gzip_decode(const uint8_t* data, size_t size,
                     uint8_t* out, size_t out_size) {
        // Minimum valid stream: 10-byte header + 8-byte trailer.
        if (size < 18) {
            throw std::runtime_error(
                "GZIP decompression: input too short to be a gzip stream");
        }
        tinfl_decompressor decomp;
        size_t total_out = 0;  // bytes written to `out` across all members
        size_t pos = 0;        // input cursor over the whole gzip stream
        while (pos < size) {
            size_t deflate_start = parse_gzip_header(data, size, pos);

            tinfl_init(&decomp);
            size_t in_bytes  = size - deflate_start;      // in: available; out: consumed
            size_t out_bytes = out_size - total_out;      // in: space;     out: produced
            // Each gzip member has an independent DEFLATE window, so the LZ
            // back-reference base is this member's own output start, not the
            // global buffer start (a member must not reference a prior member).
            uint8_t* member_out = out + total_out;
            tinfl_status st = tinfl_decompress(
                &decomp,
                data + deflate_start, &in_bytes,
                member_out, member_out, &out_bytes,
                TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF);
            if (st != TINFL_STATUS_DONE) {
                throw std::runtime_error(
                    "GZIP decompression failed (malformed DEFLATE stream)");
            }
            total_out += out_bytes;
            // Advance past this member's DEFLATE bytes + 8-byte CRC32/ISIZE trailer.
            pos = deflate_start + in_bytes + 8;

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
} // anonymous namespace

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

        case CompressionCodec::GZIP: {
            out_buf.resize(uncompressed_size);
            gzip_decode(compressed_data, compressed_size,
                        out_buf.data(), uncompressed_size);
            break;
        }

        case CompressionCodec::LZ4_RAW: {
            out_buf.resize(uncompressed_size);
            lz4_raw_decode(compressed_data, compressed_size,
                           out_buf.data(), uncompressed_size);
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

}  // namespace compression
}  // namespace rugo