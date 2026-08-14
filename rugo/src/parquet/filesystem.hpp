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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

// Remote (HTTP/HTTPS/GCS) footer fetching is an opt-in capability gated on
// RUGO_ENABLE_HTTP. The opteryx_core build defines it (and compiles + links
// http_client.cpp / libcurl); the standalone rugo wheel does NOT — rugo is a
// local-filesystem + bytes reader only. When the macro is unset, libcurl is
// never included and remote paths fail loud (see file_size_of / read_range /
// FetchParquetFootersMany). This keeps rugo/ source genuinely curl-free.
#ifdef RUGO_ENABLE_HTTP
#include "http_client.hpp"
#endif

namespace rugo {

// Thrown for a remote path when this build has HTTP compiled out.
[[noreturn]] static inline void reject_remote_path(const std::string& path) {
    throw std::runtime_error(
        "rugo: remote paths (gs://, http://, https://) are not supported in "
        "this build — local filesystem only: " + path);
}

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
#ifdef RUGO_ENABLE_HTTP
inline HttpClient& footer_http_client() {
    static HttpClient client(128, 60000);
    return client;
}
#endif

static inline std::string gcs_to_https(const std::string& path) {
    return "https://storage.googleapis.com/" + path.substr(5);
}

static int64_t file_size_of(const std::string& path) {
#ifdef RUGO_ENABLE_HTTP
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
#else
    if (path.substr(0, 5) == "gs://" ||
        path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://")
        reject_remote_path(path);
#endif
    struct stat st{};
    if (stat(path.c_str(), &st) != 0)
        throw std::runtime_error("stat() failed: " + path);
    return static_cast<int64_t>(st.st_size);
}

static std::vector<uint8_t> read_range(const std::string& path,
                                        int64_t offset, int64_t size) {
#ifdef RUGO_ENABLE_HTTP
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
#else
    if (path.substr(0, 5) == "gs://" ||
        path.substr(0, 7) == "http://" || path.substr(0, 8) == "https://")
        reject_remote_path(path);
#endif
    std::vector<uint8_t> buf(static_cast<size_t>(size));
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0)
        throw std::runtime_error("Cannot open file: " + path);
    // H14 (2026-08-14, unratified): this read_range only ever serves FOOTER
    // fetches — a single ~64 KB pread at EOF whose bytes we use in full and
    // never read around. Default readahead treats it as the start of a
    // sequential stream and pulls far more: MEASURED on x86, a cold scan that
    // projects NO columns requests 100 x 64 KB = 6.4 MB of footer but the device
    // delivers 54 MB (~540 KB/file, 8x), and that 54 MB is ~95% of the cold IO
    // of a narrow-column query (SUM(AdvEngineID), whose column is 0.96 MB,
    // totals 57 MB). POSIX_FADV_RANDOM asks the kernel not to read around a
    // request we know is isolated. Guarded so it cannot fail the read.
    // Toggle: RUGO_FOOTER_FADVISE=0 disables, for A/B from one binary.
#if defined(POSIX_FADV_RANDOM)
    {
        static const bool fadvise_on = []() {
            const char* v = getenv("RUGO_FOOTER_FADVISE");
            return !(v != nullptr && v[0] == '0' && v[1] == '\0');
        }();
        if (fadvise_on)
            posix_fadvise(fd, offset, size, POSIX_FADV_RANDOM);
    }
#endif
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

// True for paths the HttpClient can fetch via Range GET (gs:// is rewritten
// to a storage.googleapis.com URL; http(s):// are already URLs).
static inline bool is_remote_path(const std::string& path) {
    return path.substr(0, 5) == "gs://"
        || path.substr(0, 7) == "http://"
        || path.substr(0, 8) == "https://";
}

// The URL passed to HttpClient for a remote path (gs:// → signed-bucket HTTPS).
static inline std::string footer_http_url(const std::string& path) {
    if (path.substr(0, 5) == "gs://") return gcs_to_https(path);
    return path;
}

// Corruption guard for footer length when the true file size is unknown
// (suffix-range fetch). Real Parquet footers are far below this.
static constexpr int64_t kMaxFooterLength = 512LL * 1024 * 1024;

// Validate the trailing PAR1 magic and decode the 4-byte little-endian footer
// length that precedes it. `tail` must contain the final bytes of the file.
// `max_footer_length` bounds the decoded length (file_size - 8 when known, or
// kMaxFooterLength as a corruption guard when the size is unknown).
static inline uint32_t parse_footer_length(const std::vector<uint8_t>& tail,
                                           int64_t max_footer_length,
                                           const std::string& path) {
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
    if (footer_length_u32 == 0 ||
        static_cast<int64_t>(footer_length_u32) > max_footer_length)
        throw std::runtime_error("Invalid footer length " +
            std::to_string(footer_length_u32) + " in: " + path);
    return footer_length_u32;
}

// Assemble the envelope ReadParquetMetadataFromBuffer expects:
//   PAR1 + footer_thrift + footer_len_le32 + PAR1
static inline std::vector<uint8_t> build_footer_envelope(
        const uint8_t* footer_ptr, uint32_t footer_length_u32) {
    size_t fl = static_cast<size_t>(footer_length_u32);
    std::vector<uint8_t> envelope(4 + fl + 4 + 4);
    size_t off = 0;
    envelope[off++] = 0x50; envelope[off++] = 0x41;
    envelope[off++] = 0x52; envelope[off++] = 0x31;
    std::memcpy(envelope.data() + off, footer_ptr, fl);
    off += fl;
    envelope[off++] = static_cast<uint8_t>( footer_length_u32        & 0xff);
    envelope[off++] = static_cast<uint8_t>((footer_length_u32 >>  8) & 0xff);
    envelope[off++] = static_cast<uint8_t>((footer_length_u32 >> 16) & 0xff);
    envelope[off++] = static_cast<uint8_t>((footer_length_u32 >> 24) & 0xff);
    envelope[off++] = 0x50; envelope[off++] = 0x41;
    envelope[off++] = 0x52; envelope[off++] = 0x31;
    return envelope;
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

    uint32_t footer_length_u32 = parse_footer_length(tail, file_size - kParquetFooterSuffix, path);
    int64_t footer_length = static_cast<int64_t>(footer_length_u32);

    const uint8_t* footer_ptr;
    std::vector<uint8_t> extra_buf;

    int64_t total_footer_payload = footer_length + kParquetFooterSuffix;
    if (total_footer_payload <= prefetch_size) {
        footer_ptr = tail.data() + (tail.size() - static_cast<size_t>(total_footer_payload));
    } else {
        int64_t extra_offset = file_size - kParquetFooterSuffix - footer_length;
        extra_buf = read_range(path, extra_offset, footer_length);
        bytes_fetched += footer_length;
        footer_ptr = extra_buf.data();
    }

    return {build_footer_envelope(footer_ptr, footer_length_u32), bytes_fetched};
}

// First-round suffix-range size: the last N bytes of each file, fetched
// without knowing the file size or issuing a HEAD. 64KB covers the vast
// majority of Parquet footers; oversized footers trigger one extra round.
static constexpr int64_t kFooterSuffixPrefetch = 64 * 1024;

/**
 * Batch-fetch footer envelopes for many files concurrently — no per-file HEAD,
 * no dependency on knowing file sizes.
 *
 * Every remote file's tail is fetched with an HTTP suffix range (bytes=-64k)
 * in a single concurrent get_many() round; files whose footer exceeds 64KB are
 * completed in a second batched suffix-range round. Local files fall back to
 * per-file pread (no network — already cheap). `file_sizes` is only consulted
 * for local files (pass -1 when unknown); it is ignored for remote files.
 *
 * Returns one ParquetFooterResult per input path, in input order.
 */
inline std::vector<ParquetFooterResult> FetchParquetFootersMany(
        const std::vector<std::string>& paths,
        const std::vector<int64_t>& file_sizes) {
    const size_t count = paths.size();
    std::vector<ParquetFooterResult> results(count);

    std::vector<size_t> remote_idx;
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> reqs;
    remote_idx.reserve(count);
    reqs.reserve(count);

    std::vector<size_t> local_idx;
    for (size_t i = 0; i < count; ++i) {
        if (!is_remote_path(paths[i])) {
            local_idx.push_back(i);
            continue;
        }
        remote_idx.push_back(i);
        reqs.emplace_back(footer_http_url(paths[i]),
                          std::map<std::string, std::string>{
                              {"Range", "bytes=-" + std::to_string(kFooterSuffixPrefetch)}});
    }

    // PROTOTYPE (2026-08-14, unratified) — H5: local footers in parallel.
    // A cold local footer costs ~3ms (open walks directory + inode metadata,
    // then two dependent preads chase the tail), and the serial loop this
    // replaces paid that once per file — a fixed ~0.3s plan-time tax on a
    // 100-file cold scan, on EVERY query. The reads are independent, so run
    // them across a bounded thread fan-out (the same idea the remote branch
    // already applies via get_many). Exceptions are captured per slot and the
    // first is rethrown after join — same fail-fast surface as the serial
    // loop, never a silent partial result. Remote handling is untouched.
    if (!local_idx.empty()) {
        const size_t n_threads = std::min<size_t>(local_idx.size(), 16);
        std::vector<std::exception_ptr> errs(local_idx.size());
        std::atomic<size_t> next{0};
        auto worker = [&]() {
            for (;;) {
                size_t k = next.fetch_add(1, std::memory_order_relaxed);
                if (k >= local_idx.size()) return;
                size_t i = local_idx[k];
                try {
                    int64_t sz = (i < file_sizes.size()) ? file_sizes[i] : -1;
                    results[i] = FetchParquetFooter(paths[i], sz);
                } catch (...) {
                    errs[k] = std::current_exception();
                }
            }
        };
        if (n_threads == 1) {
            worker();
        } else {
            std::vector<std::thread> threads;
            threads.reserve(n_threads);
            for (size_t t = 0; t < n_threads; ++t) threads.emplace_back(worker);
            for (auto& th : threads) th.join();
        }
        for (auto& e : errs)
            if (e) std::rethrow_exception(e);
    }

    if (remote_idx.empty())
        return results;

#ifndef RUGO_ENABLE_HTTP
    reject_remote_path(paths[remote_idx[0]]);
#else
    std::vector<std::vector<uint8_t>> tails = footer_http_client().get_many(reqs);

    // Round 2: footers larger than the 64KB suffix window — fetch footer+suffix
    // exactly, again via a suffix range so no file size is needed.
    std::vector<size_t> extra_remote;  // index into remote_idx
    std::vector<uint32_t> extra_len;
    std::vector<std::pair<std::string, std::map<std::string, std::string>>> extra_reqs;

    for (size_t k = 0; k < remote_idx.size(); ++k) {
        size_t i = remote_idx[k];
        const std::vector<uint8_t>& tail = tails[k];
        uint32_t fl = parse_footer_length(tail, kMaxFooterLength, paths[i]);
        int64_t total = static_cast<int64_t>(fl) + kParquetFooterSuffix;
        if (total <= static_cast<int64_t>(tail.size())) {
            const uint8_t* fp = tail.data() + (tail.size() - static_cast<size_t>(total));
            results[i].envelope = build_footer_envelope(fp, fl);
            results[i].bytes_fetched = static_cast<int64_t>(tail.size());
        } else {
            extra_remote.push_back(k);
            extra_len.push_back(fl);
            extra_reqs.emplace_back(footer_http_url(paths[i]),
                                    std::map<std::string, std::string>{
                                        {"Range", "bytes=-" + std::to_string(total)}});
        }
    }

    if (!extra_reqs.empty()) {
        std::vector<std::vector<uint8_t>> extras = footer_http_client().get_many(extra_reqs);
        for (size_t e = 0; e < extra_remote.size(); ++e) {
            size_t k = extra_remote[e];
            size_t i = remote_idx[k];
            uint32_t fl = extra_len[e];
            const std::vector<uint8_t>& ex = extras[e];
            // ex holds [footer_thrift(fl)][len32][PAR1]; the footer starts at 0.
            if (static_cast<int64_t>(ex.size()) < static_cast<int64_t>(fl) + kParquetFooterSuffix)
                throw std::runtime_error("Short footer fetch for: " + paths[i]);
            results[i].envelope = build_footer_envelope(ex.data(), fl);
            results[i].bytes_fetched = static_cast<int64_t>(ex.size());
        }
    }

    return results;
#endif  // RUGO_ENABLE_HTTP
}

}  // namespace rugo
