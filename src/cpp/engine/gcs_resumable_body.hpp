// gcs_resumable_body.hpp — stream bytes into an already-open GCS resumable upload session.
//
// The vector index's vectors file is streamed (skene::FileWriter's lead column), and its
// body can be far larger than a worker's memory or memory-backed disk. The control plane
// opens the resumable session (it holds the credentials) and hands its session URI here;
// this class PUTs the body to it in chunks, natively, while the build runs. A session URI
// is its own credential for a week, so a multi-hour native build never refreshes a token.
//
// Protocol (https://cloud.google.com/storage/docs/performing-resumable-uploads), the same
// as the catalog's _GcsOutputStream (opteryx-catalog iops/gcs.py):
//   * every chunk but the last is a multiple of 256 KiB, sent as
//     `Content-Range: bytes <first>-<last>/*` and answered 308 with `Range: bytes=0-<held>`;
//   * the final chunk carries the total (`bytes <first>-<last>/<total>`, or `bytes */<total>`
//     when nothing is left) and is answered 200/201 — the object exists from then;
//   * bytes leave the buffer only once GCS says it holds them. A failed PUT (transport
//     error, 408/429/5xx) is followed by asking the session what it holds
//     (`bytes */*`), and the remainder is re-sent from THAT offset, never from a guess.
// An unfinished session never becomes an object; a failed build simply never finishes it.

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "http_client.hpp"
#include "skene/writer.h"   // skene::OutputStream, skene::Status

namespace opteryx::engine {

class GcsResumableBody final : public skene::OutputStream {
  public:
    static constexpr size_t kQuantum = 256u * 1024u;

    explicit GcsResumableBody(std::string session_uri, size_t chunk_bytes = 32u << 20,
                              int max_attempts = 5, long timeout_ms = 120000)
        : uri_(std::move(session_uri)), chunk_(chunk_bytes), attempts_(max_attempts),
          timeout_ms_(timeout_ms), http_(4, timeout_ms) {}

    bool valid(std::string* err) const {
        if (uri_.empty()) { *err = "no upload session URI"; return false; }
        if (chunk_ == 0u || chunk_ % kQuantum != 0u) {
            *err = "chunk_bytes must be a positive multiple of 256 KiB";
            return false;
        }
        if (attempts_ < 1) { *err = "max_attempts must be >= 1"; return false; }
        return true;
    }

    skene::Status write(const void* data, size_t n) override {
        const uint8_t* p = static_cast<const uint8_t*>(data);
        buffer_.insert(buffer_.end(), p, p + n);
        while (buffer_.size() >= chunk_) {
            skene::Status st = send(false, chunk_);
            if (!st.is_ok()) return st;
        }
        return skene::Status::ok();
    }

    // Send what is left as the final chunk; the object then exists.
    skene::Status finish() { return send(true, buffer_.size()); }

    uint64_t committed() const noexcept { return committed_; }

  private:
    static bool retryable(long status) {
        return status == 408 || status == 429 || status == 500 || status == 502 ||
               status == 503 || status == 504;
    }

    // "bytes=0-N" -> N + 1 held bytes; absent -> 0.
    static uint64_t held_from(const std::map<std::string, std::string>& headers) {
        auto it = headers.find("range");
        if (it == headers.end()) return 0u;
        const size_t dash = it->second.find('-');
        if (dash == std::string::npos) return 0u;
        return std::stoull(it->second.substr(dash + 1)) + 1u;
    }

    skene::Status failure(const std::string& what) const {
        return skene::Status(skene::Code::kMalformed, "GCS upload: " + what);
    }

    // What the session holds; nullopt once the object is finished.
    std::optional<uint64_t> query(std::string* err) {
        std::map<std::string, std::string> h{{"Content-Range", "bytes */*"}};
        HttpClient::PutResponse r = http_.put(uri_, nullptr, 0u, h, 30000);
        if (r.status == 200 || r.status == 201) return std::nullopt;
        if (r.status != 308) {
            *err = "status query answered " + std::to_string(r.status);
            return 0u;
        }
        return held_from(r.headers);
    }

    void drop(uint64_t end) {
        buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<ptrdiff_t>(end - committed_));
        committed_ = end;
    }

    skene::Status send(bool final, size_t length) {
        const uint64_t end = committed_ + length;
        uint64_t start = committed_;
        std::string last;
        for (int attempt = 0; attempt < attempts_; ++attempt) {
            if (attempt > 0 && !last.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(
                    std::min<long>(16000, 500L << std::min(attempt, 5))));
                std::string qerr;
                std::optional<uint64_t> held;
                try {
                    held = query(&qerr);
                } catch (const HttpError& e) {
                    last = e.what();
                    continue;
                }
                if (!qerr.empty()) return failure(qerr);
                if (!held.has_value()) {
                    if (!final) return failure("the session finished before the last chunk");
                    drop(end);
                    return skene::Status::ok();
                }
                if (*held < committed_ || *held > end)
                    return failure("desynchronised: the session holds " + std::to_string(*held) +
                                   " bytes, this chunk spans " + std::to_string(committed_) + "-" +
                                   std::to_string(end));
                start = *held;
            }
            last.clear();
            const uint8_t* body = buffer_.data() + (start - committed_);
            const size_t n = static_cast<size_t>(end - start);
            const std::string total = final ? std::to_string(end) : "*";
            std::map<std::string, std::string> h;
            h["Content-Range"] = n == 0u ? "bytes */" + total
                                         : "bytes " + std::to_string(start) + "-" +
                                               std::to_string(end - 1u) + "/" + total;
            HttpClient::PutResponse r;
            try {
                r = http_.put(uri_, body, n, h, timeout_ms_);
            } catch (const HttpError& e) {
                if (!e.retryable) return failure(e.what());
                last = e.what();
                continue;
            }
            if (final && (r.status == 200 || r.status == 201)) {
                drop(end);
                return skene::Status::ok();
            }
            if (r.status == 308) {
                const uint64_t held = held_from(r.headers);
                if (!final && held == end) {
                    drop(end);
                    return skene::Status::ok();
                }
                if (held < committed_ || held > end)
                    return failure("desynchronised: the session holds " + std::to_string(held) +
                                   " bytes after a chunk ending at " + std::to_string(end));
                // Part of the chunk was kept (or a final PUT was not taken whole):
                // resend the remainder from what the session says it holds.
                start = held;
                last = "the session kept " + std::to_string(held - committed_) + " of " +
                       std::to_string(end - committed_) + " bytes";
                continue;
            }
            if (!retryable(r.status))
                return failure("PUT answered " + std::to_string(r.status) + ": " +
                               std::string(r.body.begin(), r.body.begin() +
                                           static_cast<ptrdiff_t>(std::min<size_t>(r.body.size(), 300u))));
            last = "PUT answered " + std::to_string(r.status);
        }
        return failure("gave up after " + std::to_string(attempts_) + " attempts: " + last);
    }

    std::string          uri_;
    size_t               chunk_;
    int                  attempts_;
    long                 timeout_ms_;
    HttpClient           http_;
    std::vector<uint8_t> buffer_;
    uint64_t             committed_ = 0;
};

}  // namespace opteryx::engine
