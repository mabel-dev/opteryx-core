/**
 * C++ filesystem abstraction for Parquet footer fetching.
 *
 * Supports:
 *   local files  — POSIX stat() + pread()
 *   HTTP/HTTPS   — libcurl Range request via HttpClient
 *   GCS gs://    — rewritten to https://storage.googleapis.com/... + Range request
 *
 * Single entry point: FetchParquetFooter(path, file_size)
 * Returns the assembled envelope expected by ReadParquetMetadataFromBuffer:
 *   PAR1 + <footer_thrift_bytes> + <footer_len_le32> + PAR1
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "http_client.hpp"

namespace rugo {

static constexpr int64_t  kParquetFooterSuffix  = 8;       // 4-byte len + 4-byte magic

// Adaptive footer prefetch size based on file size.
// Smaller files have smaller footers; larger files benefit from larger prefetch.
// - <10MB: 32KB (small files, footers usually <8KB)
// - 10MB-1GB: 64KB (typical case, ClickBench)
// - >1GB: 128KB (large files with many row groups)
inline int64_t adaptive_footer_prefetch_size(int64_t file_size) {
    if (file_size < 10 * 1024 * 1024)
        return 32 * 1024;
    if (file_size < 1024 * 1024 * 1024)
        return 64 * 1024;
    return 128 * 1024;
}

static const uint8_t kParquetMagic[4] = {0x50, 0x41, 0x52, 0x31};  // "PAR1"

struct ParquetFooterResult {
    std::vector<uint8_t> envelope;  // PAR1 + footer_thrift + len_le32 + PAR1
    int64_t bytes_fetched = 0;
};

/**
 * Shared HttpClient — constructed once, reused across all FetchParquetFooter calls.
 * Thread-safe: curl_easy_perform is per-handle, shared DNS/connection pool.
 */
inline HttpClient& footer_http_client() {
    static HttpClient client(128, 60000);
    return client;
}

static inline std::string gcs_to_https(const std::string& path) {
    return "https://storage.googleapis.com/" + path.substr(5);
}

static int64_t file_size_of(const std::string& path) {
    if (path.substr(0, 5) == "gs://") {
        std::string url = gcs_to_https(path);
        auto hdrs = footer_http_client().head(url, {});
        auto it = hdrs.find("content-length");
        if (it == hdrs.end())
            throw std::runtime_error("HEAD response missing Content-Length: " + url);
        return static_cast<int64_t>(std::stoull(it->second));
    }
    if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
        auto hdrs = footer_http_client().head(path, {});
        auto it = hdrs.find("content-length");
        if (it == hdrs.end())
            throw std::runtime_error("HEAD response missing Content-Length: " + path);
        return static_cast<int64_t>(std::stoull(it->second));
    }
    struct stat st{};
    if (stat(path.c_str(), &st) != 0)
        throw std::runtime_error("stat() failed: " + path);
    return static_cast<int64_t>(st.st_size);
}

static std::vector<uint8_t> read_range(const std::string& path,
                                        int64_t offset, int64_t size) {
    if (path.substr(0, 5) == "gs://") {
        std::string url = gcs_to_https(path);
        std::string hdr = "bytes=" + std::to_string(offset) +
                          "-" + std::to_string(offset + size - 1);
        return footer_http_client().get(url, {{"Range", hdr}});
    }
    if (path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://") {
        std::string hdr = "bytes=" + std::to_string(offset) +
                          "-" + std::to_string(offset + size - 1);
        return footer_http_client().get(path, {{"Range", hdr}});
    }
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0)
        throw std::runtime_error("Cannot open file: " + path);
    ssize_t n = pread(fd, buf.data(), static_cast<size_t>(size), offset);
    close(fd);
    if (n < 0)
        throw std::runtime_error("pread() error: " + path);
    if (static_cast<int64_t>(n) != size)
        throw std::runtime_error("Short read in " + path +
            " (expected " + std::to_string(size) +
            ", got " + std::to_string(n) + ")");
    return buf;
}

/**
 * Fetch and assemble the Parquet footer envelope for `path`.
 *
 * @param path       File path or URL (local / http:// / https:// / gs://)
 * @param file_size  Known file size; pass -1 to auto-detect via stat/HEAD.
 */
inline ParquetFooterResult FetchParquetFooter(const std::string& path,
                                               int64_t file_size = -1) {
    if (file_size <= 0)
        file_size = file_size_of(path);

    if (file_size < kParquetFooterSuffix)
        throw std::runtime_error("File too small to be valid Parquet: " + path);

    int64_t prefetch_size   = std::min(adaptive_footer_prefetch_size(file_size), file_size);
    int64_t prefetch_offset = file_size - prefetch_size;

    std::vector<uint8_t> tail = read_range(path, prefetch_offset, prefetch_size);
    int64_t bytes_fetched = prefetch_size;

    size_t n = tail.size();
    if (n < static_cast<size_t>(kParquetFooterSuffix) ||
        tail[n-4] != 0x50 || tail[n-3] != 0x41 ||
        tail[n-2] != 0x52 || tail[n-1] != 0x31) {
        throw std::runtime_error("Missing Parquet magic bytes (PAR1) at EOF: " + path);
    }

    size_t lp = n - static_cast<size_t>(kParquetFooterSuffix);
    uint32_t footer_length_u32 =
        static_cast<uint32_t>(tail[lp])
        | (static_cast<uint32_t>(tail[lp+1]) << 8)
        | (static_cast<uint32_t>(tail[lp+2]) << 16)
        | (static_cast<uint32_t>(tail[lp+3]) << 24);
    int64_t footer_length = static_cast<int64_t>(footer_length_u32);

    if (footer_length == 0 || footer_length > file_size - kParquetFooterSuffix)
        throw std::runtime_error("Invalid footer length " +
            std::to_string(footer_length) + " in: " + path);

    const uint8_t* footer_ptr;
    std::vector<uint8_t> extra_buf;

    int64_t total_footer_payload = footer_length + kParquetFooterSuffix;
    if (total_footer_payload <= prefetch_size) {
        footer_ptr = tail.data() + (n - static_cast<size_t>(total_footer_payload));
    } else {
        int64_t extra_offset = file_size - kParquetFooterSuffix - footer_length;
        extra_buf = read_range(path, extra_offset, footer_length);
        bytes_fetched += footer_length;
        footer_ptr = extra_buf.data();
    }

    // Assemble envelope: PAR1 + footer_thrift + footer_len_le32 + PAR1
    size_t env_size = 4 + static_cast<size_t>(footer_length) + 4 + 4;
    std::vector<uint8_t> envelope(env_size);
    size_t off = 0;
    envelope[off++] = 0x50; envelope[off++] = 0x41;
    envelope[off++] = 0x52; envelope[off++] = 0x31;
    std::memcpy(envelope.data() + off, footer_ptr, static_cast<size_t>(footer_length));
    off += static_cast<size_t>(footer_length);
    envelope[off++] = static_cast<uint8_t>( footer_length_u32        & 0xff);
    envelope[off++] = static_cast<uint8_t>((footer_length_u32 >>  8) & 0xff);
    envelope[off++] = static_cast<uint8_t>((footer_length_u32 >> 16) & 0xff);
    envelope[off++] = static_cast<uint8_t>((footer_length_u32 >> 24) & 0xff);
    envelope[off++] = 0x50; envelope[off++] = 0x41;
    envelope[off++] = 0x52; envelope[off++] = 0x31;

    return {std::move(envelope), bytes_fetched};
}

}  // namespace rugo
