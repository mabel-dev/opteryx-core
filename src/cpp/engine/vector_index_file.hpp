// vector_index_file.hpp — the vector index file format (docs/VECTOR_INDEX_DESIGN.md §5.2,
// ruled 2026-10-04: one flat file per data file, not skene).
//
// An IVF index on object storage is read by BYTE RANGE, never decoded: the cost of a remote
// read is the round trip, not the bytes, so the format exists to make every search a
// handful of large parallel range GETs. Nothing in it is compressed (fp16 embeddings do not
// compress) and nothing needs a decoder.
//
//   BODY     blocks, back to back. A block is ONE cluster's rows, as the build flushed them
//            (ops/ann ClusterStream: a cluster's rows arrive interleaved with other clusters'
//            and are flushed `flush_rows` at a time, so a cluster is one or a few blocks):
//                uint32 ordinal[n]           the rows' PHYSICAL ordinals in the data file
//                uint16 vector[n][dims]      their fp16 embeddings
//            Block b starts at (rows before it) x (4 + 2 x dims): every block, and so every
//            cluster, is a byte range computed from the footer alone.
//   FOOTER   FooterHead, uint16 centroid[clusters][dims], BlockRec[blocks]
//   TAIL     24 bytes: footer length, footer checksum (XXH3-64), version, magic.
//
// Opening a file is ONE read when the caller knows the footer length (the catalog records
// it with the file size, as it records every index file's size): the footer and tail
// together, at file_bytes - footer_bytes - 24. Without it, two reads (tail, then footer).
// The body is never verified by checksum - its length is (file_bytes against the footer's
// row count), and a file whose body does not match its footer is refused.
//
// The writer streams: blocks as they are flushed, then the footer, then the tail. One
// object, written once, finished once - a GCS resumable session needs no compose.
//
// Byte order is the host's; every target (§6) is little-endian.

#pragma once

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "filesystem.hpp"        // rugo::gcs_to_https
#include "http_client.hpp"
#include "skene/checksum.h"      // skene::checksum_xxh3_64 (the one vendored xxhash)
#include "skene/writer.h"        // skene::OutputStream, skene::Status (the stream contract)

namespace opteryx::engine {

namespace vif {

constexpr uint32_t kMagic   = 0x58444956u;   // "VIDX"
constexpr uint32_t kVersion = 1u;
constexpr size_t   kTailBytes = 24u;

// Reads are issued in requests of at most this many bytes, in waves of at most kWaveBytes
// in flight: a 258 MB exact search is ~16 requests a wave, 2 waves, bounded memory.
constexpr uint64_t kMaxRequestBytes = 16u << 20;
constexpr uint64_t kWaveBytes       = 128u << 20;
// Two wanted ranges are read as one request when the bytes between them are at most this,
// or at most a tenth of what the merged request would carry — a round trip costs more than
// the gap.
constexpr uint64_t kMergeGapBytes   = 64u << 10;

struct FooterHead {
    uint32_t dims;
    uint32_t clusters;
    uint32_t blocks;
    uint32_t reserved;   // 0
    uint64_t rows;
};
static_assert(sizeof(FooterHead) == 24u, "FooterHead layout drift");

struct BlockRec {
    uint32_t cluster;
    uint32_t rows;
};
static_assert(sizeof(BlockRec) == 8u, "BlockRec layout drift");

struct Tail {
    uint64_t footer_bytes;
    uint64_t footer_checksum;
    uint32_t version;
    uint32_t magic;
};
static_assert(sizeof(Tail) == kTailBytes, "Tail layout drift");

inline uint64_t row_bytes(uint32_t dims) { return 4u + 2u * static_cast<uint64_t>(dims); }

inline uint64_t footer_bytes_for(uint32_t dims, uint32_t clusters, uint32_t blocks) {
    return sizeof(FooterHead) + 2u * static_cast<uint64_t>(clusters) * dims +
           sizeof(BlockRec) * static_cast<uint64_t>(blocks);
}

}  // namespace vif

// ── Writer ──────────────────────────────────────────────────────────────────────────────

struct VectorIndexFileSizes {
    uint64_t file_bytes = 0;
    uint64_t footer_bytes = 0;
    uint64_t rows = 0;
    uint32_t clusters = 0;
    uint32_t blocks = 0;
};

// Streams one index file to an OutputStream: blocks as they are handed in, then the footer
// and tail at finish. The centroids are given at construction (the build trained them
// before any row is written).
class VectorIndexFileWriter {
  public:
    VectorIndexFileWriter(skene::OutputStream* out, const uint16_t* centroids, uint32_t clusters, uint32_t dims)
        : out_(out), dims_(dims), clusters_(clusters),
          centroids_(centroids, centroids + static_cast<size_t>(clusters) * dims) {}

    // One block: cluster `c`'s next `n` rows.
    bool add_block(uint32_t c, const uint32_t* ordinals, const uint16_t* vectors, uint32_t n, std::string* err) {
        if (n == 0u) return true;
        if (c >= clusters_) { *err = "vector index file: a block names a cluster the index does not have"; return false; }
        if (!put(ordinals, 4u * static_cast<size_t>(n), err)) return false;
        if (!put(vectors, 2u * static_cast<size_t>(n) * dims_, err)) return false;
        blocks_.push_back({c, n});
        rows_ += n;
        return true;
    }

    bool finish(VectorIndexFileSizes* sizes, std::string* err) {
        const uint32_t blocks = static_cast<uint32_t>(blocks_.size());
        const uint64_t fbytes = vif::footer_bytes_for(dims_, clusters_, blocks);
        std::vector<uint8_t> footer(static_cast<size_t>(fbytes));
        uint8_t* p = footer.data();
        const vif::FooterHead head{dims_, clusters_, blocks, 0u, rows_};
        std::memcpy(p, &head, sizeof head); p += sizeof head;
        std::memcpy(p, centroids_.data(), centroids_.size() * 2u); p += centroids_.size() * 2u;
        std::memcpy(p, blocks_.data(), blocks_.size() * sizeof(vif::BlockRec));
        const vif::Tail tail{fbytes, skene::checksum_xxh3_64(footer.data(), footer.size()), vif::kVersion, vif::kMagic};
        if (!put(footer.data(), footer.size(), err) || !put(&tail, sizeof tail, err)) return false;
        sizes->file_bytes = written_;
        sizes->footer_bytes = fbytes;
        sizes->rows = rows_;
        sizes->clusters = clusters_;
        sizes->blocks = blocks;
        return true;
    }

    uint64_t rows() const noexcept { return rows_; }

  private:
    bool put(const void* data, size_t n, std::string* err) {
        skene::Status st = out_->write(data, n);
        if (!st.is_ok()) { *err = "vector index file: " + st.message(); return false; }
        written_ += n;
        return true;
    }

    skene::OutputStream*        out_;
    uint32_t                    dims_;
    uint32_t                    clusters_;
    std::vector<uint16_t>       centroids_;
    std::vector<vif::BlockRec>  blocks_;
    uint64_t                    rows_ = 0;
    uint64_t                    written_ = 0;
};

// ── Reader ──────────────────────────────────────────────────────────────────────────────

// One block of the body, as fetched: `n` rows' ordinals and vectors.
struct VectorIndexBlock {
    uint32_t        block = 0;
    uint32_t        cluster = 0;
    uint32_t        rows = 0;
    const uint32_t* ordinals = nullptr;
    const uint16_t* vectors = nullptr;
};

struct VectorIndexReadStats {
    uint32_t blocks_read = 0;
    uint64_t bytes_read = 0;
    uint32_t requests = 0;       // range reads issued (a local pread, or a GET)
};

class VectorIndexFile {
  public:
    VectorIndexFile() = default;
    VectorIndexFile(const VectorIndexFile&) = delete;
    VectorIndexFile& operator=(const VectorIndexFile&) = delete;
    ~VectorIndexFile() { if (fd_ >= 0) ::close(fd_); }

    // `location`: a local path, a gs:// object (read with `auth_header`, a bearer token) or
    // an http(s) URL carrying its own credential. `file_bytes` is the file's size, and
    // `footer_bytes` its footer's (0 = unknown: one more round trip).
    bool open(const std::string& location, uint64_t file_bytes, uint64_t footer_bytes,
              const std::string& auth_header, std::string* err) {
        location_ = location;
        file_bytes_ = file_bytes;
        auth_header_ = auth_header;
        const bool gcs = location.rfind("gs://", 0) == 0;
        remote_ = gcs || location.rfind("http://", 0) == 0 || location.rfind("https://", 0) == 0;
        if (!remote_ && location.find("://") != std::string::npos) {
            *err = "vector index: " + location + " is neither a local path, a gs:// object nor an http(s) URL";
            return false;
        }
        if (gcs && auth_header.empty()) {
            *err = "vector index: " + location + " is a gs:// object and no Authorization header was given";
            return false;
        }
        url_ = gcs ? rugo::gcs_to_https(location) : location;
        if (remote_) {
            http_ = std::make_unique<HttpClient>(16, 120000);
        } else {
            fd_ = ::open(location.c_str(), O_RDONLY);
            if (fd_ < 0) { *err = "vector index: cannot open " + location + ": " + std::strerror(errno); return false; }
        }
        if (file_bytes < vif::kTailBytes + sizeof(vif::FooterHead)) { *err = "vector index: " + location + " is too small"; return false; }

        // The footer and tail: one read when the footer length is known, else tail first.
        std::vector<uint8_t> end;
        vif::Tail tail{};
        if (footer_bytes > 0u) {
            if (footer_bytes + vif::kTailBytes > file_bytes) { *err = "vector index: " + location + ": the recorded footer is larger than the file"; return false; }
            if (!fetch_one(file_bytes - footer_bytes - vif::kTailBytes, footer_bytes + vif::kTailBytes, &end, err)) return false;
            std::memcpy(&tail, end.data() + footer_bytes, sizeof tail);
            if (tail.footer_bytes != footer_bytes) { *err = "vector index: " + location + ": the recorded footer length is not the file's"; return false; }
        } else {
            std::vector<uint8_t> t;
            if (!fetch_one(file_bytes - vif::kTailBytes, vif::kTailBytes, &t, err)) return false;
            std::memcpy(&tail, t.data(), sizeof tail);
            if (tail.magic != vif::kMagic) { *err = "vector index: " + location + " is not a vector index file"; return false; }
            if (tail.footer_bytes + vif::kTailBytes > file_bytes) { *err = "vector index: " + location + ": the footer is larger than the file"; return false; }
            if (!fetch_one(file_bytes - tail.footer_bytes - vif::kTailBytes, tail.footer_bytes, &end, err)) return false;
            footer_bytes = tail.footer_bytes;
        }
        if (tail.magic != vif::kMagic) { *err = "vector index: " + location + " is not a vector index file"; return false; }
        if (tail.version != vif::kVersion) { *err = "vector index: " + location + " is version " + std::to_string(tail.version) + "; this reader reads version " + std::to_string(vif::kVersion); return false; }
        if (skene::checksum_must_match() && skene::checksum_xxh3_64(end.data(), footer_bytes) != tail.footer_checksum) {
            *err = "vector index: " + location + ": the footer's checksum does not match";
            return false;
        }
        footer_bytes_ = footer_bytes;
        if (footer_bytes < sizeof(vif::FooterHead)) { *err = "vector index: " + location + ": the footer is truncated"; return false; }
        vif::FooterHead head{};
        std::memcpy(&head, end.data(), sizeof head);
        if (head.dims == 0u || head.clusters == 0u) { *err = "vector index: " + location + ": the footer names no dimensions or clusters"; return false; }
        if (vif::footer_bytes_for(head.dims, head.clusters, head.blocks) != footer_bytes) {
            *err = "vector index: " + location + ": the footer's length does not match its counts";
            return false;
        }
        dims_ = head.dims;
        clusters_ = head.clusters;
        rows_ = head.rows;
        const uint8_t* p = end.data() + sizeof head;
        centroids_.assign(reinterpret_cast<const uint16_t*>(p), reinterpret_cast<const uint16_t*>(p) + static_cast<size_t>(clusters_) * dims_);
        p += static_cast<size_t>(clusters_) * dims_ * 2u;
        blocks_.resize(head.blocks);
        std::memcpy(blocks_.data(), p, blocks_.size() * sizeof(vif::BlockRec));

        // The body's extent, and each block's: one pass, verified against the file size.
        offsets_.assign(blocks_.size() + 1u, 0u);
        cluster_rows_.assign(clusters_, 0u);
        by_cluster_.assign(clusters_, {});
        uint64_t rows = 0;
        for (uint32_t b = 0; b < blocks_.size(); ++b) {
            if (blocks_[b].cluster >= clusters_ || blocks_[b].rows == 0u) { *err = "vector index: " + location + ": block " + std::to_string(b) + " is malformed"; return false; }
            rows += blocks_[b].rows;
            offsets_[b + 1u] = rows * vif::row_bytes(dims_);
            cluster_rows_[blocks_[b].cluster] += blocks_[b].rows;
            by_cluster_[blocks_[b].cluster].push_back(b);
        }
        if (rows != rows_ || offsets_.back() + footer_bytes + vif::kTailBytes != file_bytes) {
            *err = "vector index: " + location + ": the body does not match the footer (" + std::to_string(file_bytes) +
                   " bytes for " + std::to_string(rows) + " rows)";
            return false;
        }
        return true;
    }

    uint32_t dims() const noexcept { return dims_; }
    uint32_t clusters() const noexcept { return clusters_; }
    uint64_t rows() const noexcept { return rows_; }
    uint32_t blocks() const noexcept { return static_cast<uint32_t>(blocks_.size()); }
    const uint16_t* centroids() const noexcept { return centroids_.data(); }
    const uint32_t* cluster_rows() const noexcept { return cluster_rows_.data(); }
    const std::vector<uint32_t>& cluster_blocks(uint32_t c) const { return by_cluster_[c]; }

    // Every block of `clusters`, ascending by position in the file.
    std::vector<uint32_t> blocks_of(const std::vector<uint32_t>& clusters) const {
        std::vector<uint32_t> out;
        for (uint32_t c : clusters) out.insert(out.end(), by_cluster_[c].begin(), by_cluster_[c].end());
        std::sort(out.begin(), out.end());
        return out;
    }

    // Read `wanted` (ascending block ids) and call `visit(block)` for each, in file order.
    // Reads are coalesced into requests of at most kMaxRequestBytes, issued in parallel in
    // waves of at most kWaveBytes (remote: one get_many per wave). A block's buffers are
    // valid only during its visit.
    bool for_each_block(const std::vector<uint32_t>& wanted, const std::function<void(const VectorIndexBlock&)>& visit,
                        VectorIndexReadStats* stats, std::string* err) {
        struct Request { uint64_t offset, bytes; uint32_t first_block, end_block; };
        std::vector<Request> requests;
        for (size_t i = 0; i < wanted.size(); ++i) {
            const uint32_t b = wanted[i];
            if (b >= blocks_.size()) { *err = "vector index: " + location_ + ": block " + std::to_string(b) + " is outside the file"; return false; }
            if (i > 0 && wanted[i - 1] >= b) { *err = "vector index: blocks must be ascending and distinct"; return false; }
            const uint64_t off = offsets_[b], bytes = offsets_[b + 1u] - off;
            if (!requests.empty()) {
                Request& last = requests.back();
                const uint64_t end = last.offset + last.bytes;
                const uint64_t gap = off - end;
                const uint64_t merged = (off + bytes) - last.offset;
                if (merged <= vif::kMaxRequestBytes && (gap <= vif::kMergeGapBytes || gap * 10u <= merged)) {
                    last.bytes = merged;
                    last.end_block = b + 1u;
                    continue;
                }
            }
            requests.push_back({off, bytes, b, b + 1u});
        }
        // A block larger than one request is read as one request of its own size.
        size_t at = 0;
        while (at < requests.size()) {
            uint64_t wave = 0;
            size_t end = at;
            while (end < requests.size() && (end == at || wave + requests[end].bytes <= vif::kWaveBytes)) {
                wave += requests[end].bytes;
                ++end;
            }
            std::vector<std::vector<uint8_t>> buffers;
            if (!fetch_many(requests.begin() + at, requests.begin() + end, &buffers, err)) return false;
            stats->requests += static_cast<uint32_t>(end - at);
            stats->bytes_read += wave;
            for (size_t r = at; r < end; ++r) {
                const Request& q = requests[r];
                const uint8_t* base = buffers[r - at].data();
                for (uint32_t b = q.first_block; b < q.end_block; ++b) {
                    // Only the blocks asked for: a merged request may span unwanted ones.
                    if (!std::binary_search(wanted.begin(), wanted.end(), b)) continue;
                    const uint8_t* p = base + (offsets_[b] - q.offset);
                    VectorIndexBlock blk;
                    blk.block = b;
                    blk.cluster = blocks_[b].cluster;
                    blk.rows = blocks_[b].rows;
                    blk.ordinals = reinterpret_cast<const uint32_t*>(p);
                    blk.vectors = reinterpret_cast<const uint16_t*>(p + 4u * static_cast<size_t>(blk.rows));
                    visit(blk);
                    ++stats->blocks_read;
                }
            }
            at = end;
        }
        return true;
    }

    // Every block of the file.
    bool for_each_block(const std::function<void(const VectorIndexBlock&)>& visit, VectorIndexReadStats* stats, std::string* err) {
        std::vector<uint32_t> all(blocks_.size());
        for (uint32_t b = 0; b < all.size(); ++b) all[b] = b;
        return for_each_block(all, visit, stats, err);
    }

  private:
    template <typename It>
    bool fetch_many(It first, It last, std::vector<std::vector<uint8_t>>* out, std::string* err) {
        if (remote_) {
            std::vector<std::pair<std::string, std::map<std::string, std::string>>> reqs;
            for (It it = first; it != last; ++it) {
                std::map<std::string, std::string> h{{"Range", "bytes=" + std::to_string(it->offset) + "-" + std::to_string(it->offset + it->bytes - 1u)}};
                if (!auth_header_.empty()) h.emplace("Authorization", auth_header_);
                reqs.emplace_back(url_, std::move(h));
            }
            try {
                *out = http_->get_many(reqs);
            } catch (const std::exception& e) {
                *err = "vector index: cannot read " + location_ + ": " + e.what();
                return false;
            }
            size_t i = 0;
            for (It it = first; it != last; ++it, ++i)
                if ((*out)[i].size() != it->bytes) {
                    *err = "vector index: " + location_ + " answered " + std::to_string((*out)[i].size()) + " bytes for a " + std::to_string(it->bytes) + "-byte range";
                    return false;
                }
            return true;
        }
        out->clear();
        for (It it = first; it != last; ++it) {
            out->emplace_back();
            if (!pread_all(it->offset, it->bytes, &out->back(), err)) return false;
        }
        return true;
    }

    bool fetch_one(uint64_t offset, uint64_t bytes, std::vector<uint8_t>* out, std::string* err) {
        struct R { uint64_t offset, bytes; } one{offset, bytes};
        std::vector<std::vector<uint8_t>> got;
        if (!fetch_many(&one, &one + 1, &got, err)) return false;
        *out = std::move(got[0]);
        return true;
    }

    bool pread_all(uint64_t offset, uint64_t bytes, std::vector<uint8_t>* out, std::string* err) {
        out->resize(bytes);
        uint64_t done = 0;
        while (done < bytes) {
            const ssize_t n = ::pread(fd_, out->data() + done, bytes - done, static_cast<off_t>(offset + done));
            if (n <= 0) {
                *err = "vector index: cannot read " + location_ + (n < 0 ? std::string(": ") + std::strerror(errno) : ": short file");
                return false;
            }
            done += static_cast<uint64_t>(n);
        }
        return true;
    }

    std::string                  location_, url_, auth_header_;
    uint64_t                     file_bytes_ = 0, footer_bytes_ = 0, rows_ = 0;
    bool                         remote_ = false;
    int                          fd_ = -1;
    std::unique_ptr<HttpClient>  http_;
    uint32_t                     dims_ = 0, clusters_ = 0;
    std::vector<uint16_t>        centroids_;
    std::vector<vif::BlockRec>   blocks_;
    std::vector<uint64_t>        offsets_;        // block b's body offset; back() = body bytes
    std::vector<uint32_t>        cluster_rows_;
    std::vector<std::vector<uint32_t>> by_cluster_;
};

}  // namespace opteryx::engine
