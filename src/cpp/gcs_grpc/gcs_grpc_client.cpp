#include "gcs_grpc_client.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstring>

#include "alts.hpp"
#include "crc32c.hpp"
#include "pb_wire.hpp"
#include "transport.hpp"

namespace gcs_grpc {

namespace {

const std::string kReadObjectPath = "/google.storage.v2.Storage/ReadObject";
constexpr const char* kStorageHost = "storage.googleapis.com";

// Percent-encodes a routing value for x-goog-request-params.
std::string url_encode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char ch : s) {
        if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('%');
            out.push_back(hex[ch >> 4]);
            out.push_back(hex[ch & 15]);
        }
    }
    return out;
}

bool retryable_status(int s) {
    return s == kUnavailable || s == kResourceExhausted || s == kDeadlineExceeded || s == kAborted ||
           s == kInternal;
}

// Counts down the streams of one read_many; the caller waits for zero.
struct Batch {
    std::mutex mu;
    std::condition_variable cv;
    size_t open = 0;
    void done() {
        std::lock_guard<std::mutex> g(mu);
        if (--open == 0) cv.notify_all();
    }
};

// One ReadObject stream. Parses ReadObjectResponse messages incrementally as
// body bytes arrive (on the connection's I/O thread), copying
// ChecksummedData.content straight into `dest`:
//
//   ReadObjectResponse { ChecksummedData checksummed_data = 1; ... Object metadata = 4; }
//   ChecksummedData    { bytes content = 1; optional fixed32 crc32c = 2; }
//   Object             { ... int64 generation = 3; ... }
//
// Every other field is skipped by its wire type.
class RangeStream : public CallHandler {
public:
    RangeStream(Batch* batch, uint8_t* dest, int64_t length)
        : batch_(batch), dest_(dest), length_(length), last_progress_ns_(now_ns()) {}

    static int64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    // How long since this stream last received body bytes (since it started, before
    // the first); 0 once closed. Read by the waiting caller, written on the I/O thread.
    int64_t idle_ms(int64_t now) const {
        if (closed_.load(std::memory_order_acquire)) return 0;
        return (now - last_progress_ns_.load(std::memory_order_relaxed)) / 1000000;
    }

    bool on_body(const uint8_t* p, size_t n) override {
        last_progress_ns_.store(now_ns(), std::memory_order_relaxed);
        try {
            consume(p, n);
            return true;
        } catch (const std::exception& e) {
            error_ = e.what();
            return false;
        }
    }

    void on_close(int status, const std::string& message) override {
        status_ = status;
        message_ = message;
        closed_.store(true, std::memory_order_release);
        batch_->done();  // nothing touches `this` after this line
    }

    int status() const { return status_; }
    const std::string& message() const { return message_; }
    const std::string& error() const { return error_; }
    int64_t generation() const { return generation_; }
    int64_t received() const { return pos_; }
    // A clean end of stream leaves no message half-parsed.
    bool at_message_boundary() const { return st_ == St::Prefix && small_n_ == 0; }

private:
    enum class St { Prefix, Key, Len, SkipVarint, Fixed, Skip, Content, Meta };
    enum class After { None, EnterData, Content, Meta, Skip, Crc };

    // Bytes consumed inside the current message (and ChecksummedData).
    void advance(size_t k) {
        if (k > msg_left_) throw std::runtime_error("GCS gRPC: field overruns its message");
        msg_left_ -= k;
        if (in_data_) {
            if (k > data_left_) throw std::runtime_error("GCS gRPC: field overruns ChecksummedData");
            data_left_ -= k;
        }
    }

    // A field finished: close ChecksummedData / the message if they ended.
    void field_done() {
        st_ = St::Key;
        if (in_data_ && data_left_ == 0) {
            if (has_crc_ && crc_run_ != crc_want_)
                throw std::runtime_error("GCS gRPC: CRC32C mismatch at range offset " + std::to_string(pos_));
            in_data_ = false;
        }
        if (msg_left_ == 0) {
            if (in_data_) throw std::runtime_error("GCS gRPC: message ended inside ChecksummedData");
            st_ = St::Prefix;
        }
    }

    // Accumulates a varint byte-by-byte; true when complete (value in `v`).
    bool varint_byte(uint8_t b, uint64_t& v) {
        if (small_n_ >= 10) throw std::runtime_error("GCS gRPC: varint too long");
        small_[small_n_++] = b;
        if (b & 0x80) return false;
        v = 0;
        for (size_t i = 0; i < small_n_; ++i) v |= static_cast<uint64_t>(small_[i] & 0x7F) << (7 * i);
        small_n_ = 0;
        return true;
    }

    void on_key(uint64_t key) {
        const uint32_t field = static_cast<uint32_t>(key >> 3);
        const auto wt = static_cast<pb::WireType>(key & 7);
        after_ = After::None;
        if (!in_data_ && field == 1 && wt == pb::kLen) { after_ = After::EnterData; st_ = St::Len; return; }
        if (!in_data_ && field == 4 && wt == pb::kLen) { after_ = After::Meta; st_ = St::Len; return; }
        if (in_data_ && field == 1 && wt == pb::kLen) { after_ = After::Content; st_ = St::Len; return; }
        if (in_data_ && field == 2 && wt == pb::kFixed32) { after_ = After::Crc; fixed_need_ = 4; st_ = St::Fixed; return; }
        switch (wt) {
            case pb::kVarint: st_ = St::SkipVarint; return;
            case pb::kFixed32: fixed_need_ = 4; st_ = St::Fixed; return;
            case pb::kFixed64: fixed_need_ = 8; st_ = St::Fixed; return;
            case pb::kLen: after_ = After::Skip; st_ = St::Len; return;
            default: throw std::runtime_error("GCS gRPC: unsupported wire type " + std::to_string(wt));
        }
    }

    void on_len(uint64_t len) {
        if (len > msg_left_) throw std::runtime_error("GCS gRPC: length-delimited field overruns its message");
        switch (after_) {
            case After::EnterData:
                in_data_ = true;
                data_left_ = static_cast<size_t>(len);
                has_crc_ = false;
                crc_run_ = 0;
                if (len == 0) field_done(); else st_ = St::Key;
                return;
            case After::Content:
                if (pos_ + static_cast<int64_t>(len) > length_)
                    throw std::runtime_error("GCS gRPC: more data than the range requested");
                left_ = static_cast<size_t>(len);
                if (len == 0) field_done(); else st_ = St::Content;
                return;
            case After::Meta:
                meta_.clear();
                left_ = static_cast<size_t>(len);
                if (len == 0) field_done(); else st_ = St::Meta;
                return;
            default:
                left_ = static_cast<size_t>(len);
                if (len == 0) field_done(); else st_ = St::Skip;
                return;
        }
    }

    void parse_metadata() {
        pb::Reader r(meta_.data(), meta_.size());
        pb::Field f;
        while (r.next(f))
            if (f.number == 3 && f.type == pb::kVarint) generation_ = static_cast<int64_t>(f.u);
    }

    void consume(const uint8_t* p, size_t n) {
        while (n) {
            switch (st_) {
                case St::Prefix: {
                    small_[small_n_++] = *p++;
                    --n;
                    if (small_n_ < 5) break;
                    if (small_[0] != 0) throw std::runtime_error("GCS gRPC: unexpected compressed message");
                    msg_left_ = (size_t(small_[1]) << 24) | (size_t(small_[2]) << 16) | (size_t(small_[3]) << 8) |
                                size_t(small_[4]);
                    small_n_ = 0;
                    st_ = msg_left_ ? St::Key : St::Prefix;
                    break;
                }
                case St::Key:
                case St::Len:
                case St::SkipVarint: {
                    const uint8_t b = *p++;
                    --n;
                    advance(1);
                    uint64_t v;
                    if (!varint_byte(b, v)) break;
                    if (st_ == St::Key) {
                        if ((v >> 3) == 0) throw std::runtime_error("GCS gRPC: field number 0");
                        on_key(v);
                    } else if (st_ == St::Len) {
                        on_len(v);
                    } else {
                        field_done();
                    }
                    break;
                }
                case St::Fixed: {
                    small_[small_n_++] = *p++;
                    --n;
                    advance(1);
                    if (small_n_ < fixed_need_) break;
                    if (after_ == After::Crc) {
                        uint32_t c;
                        std::memcpy(&c, small_, 4);
                        crc_want_ = c;
                        has_crc_ = true;
                    }
                    small_n_ = 0;
                    field_done();
                    break;
                }
                case St::Content: {
                    const size_t k = std::min(n, left_);
                    advance(k);
                    std::memcpy(dest_ + pos_, p, k);
                    crc_run_ = crc32c_extend(crc_run_, p, k);
                    pos_ += static_cast<int64_t>(k);
                    p += k;
                    n -= k;
                    left_ -= k;
                    if (left_ == 0) field_done();
                    break;
                }
                case St::Meta:
                case St::Skip: {
                    const size_t k = std::min(n, left_);
                    advance(k);
                    if (st_ == St::Meta) meta_.insert(meta_.end(), p, p + k);
                    p += k;
                    n -= k;
                    left_ -= k;
                    if (left_ == 0) {
                        if (st_ == St::Meta) parse_metadata();
                        field_done();
                    }
                    break;
                }
            }
        }
    }

    Batch* batch_;
    uint8_t* dest_;
    const int64_t length_;
    std::atomic<int64_t> last_progress_ns_;
    std::atomic<bool> closed_{false};
    int64_t pos_ = 0;

    St st_ = St::Prefix;
    After after_ = After::None;
    uint8_t small_[10];
    size_t small_n_ = 0;
    size_t fixed_need_ = 0;
    size_t msg_left_ = 0;
    bool in_data_ = false;
    size_t data_left_ = 0;
    size_t left_ = 0;
    bool has_crc_ = false;
    uint32_t crc_want_ = 0;
    uint32_t crc_run_ = 0;
    std::vector<uint8_t> meta_;
    int64_t generation_ = 0;

    // Written on the I/O thread before on_close; read by the caller after the
    // batch's mutex hand-off.
    std::string error_;
    int status_ = kUnknown;
    std::string message_;
};

std::string read_object_request(const std::string& bucket_path, const std::string& object, int64_t generation,
                                const ByteRange& r) {
    std::string msg;
    pb::put_bytes(msg, 1, bucket_path);
    pb::put_bytes(msg, 2, object);
    if (generation > 0) pb::put_uint(msg, 3, static_cast<uint64_t>(generation));
    pb::put_uint(msg, 4, static_cast<uint64_t>(r.offset));
    pb::put_uint(msg, 5, static_cast<uint64_t>(r.length));
    std::string body;
    GrpcConnection::frame_message(body, msg);
    return body;
}

}  // namespace

std::string parse_read_object_body(const std::string& body, int64_t length, size_t chunk, std::string& data,
                                   int64_t& generation) {
    Batch batch;
    batch.open = 1;
    data.assign(static_cast<size_t>(length), '\0');
    RangeStream s(&batch, reinterpret_cast<uint8_t*>(data.data()), length);
    const auto* p = reinterpret_cast<const uint8_t*>(body.data());
    for (size_t at = 0; at < body.size(); at += chunk)
        if (!s.on_body(p + at, std::min(chunk, body.size() - at))) return s.error();
    generation = s.generation();
    if (!s.at_message_boundary()) return "stream ended inside a message";
    if (s.received() != length) return "short: " + std::to_string(s.received());
    return "";
}

GcsGrpcClient::GcsGrpcClient(Options options) : options_(std::move(options)) {
    if (options_.connections < 1) throw std::invalid_argument("GcsGrpcClient: connections must be >= 1");
}

std::shared_ptr<GrpcConnection> GcsGrpcClient::new_connection(const std::vector<Endpoint>& endpoints,
                                                             size_t ordinal) {
    if (!options_.direct_path)
        return std::make_shared<GrpcConnection>(
            std::make_unique<TlsTransport>(options_.host, options_.port, options_.ca_file), options_.host);
    if (!handshaker_ || !handshaker_->alive())
        handshaker_ = std::make_shared<GrpcConnection>(
            std::make_unique<PlainTransport>(kAltsHandshakerHost, kAltsHandshakerPort),
            std::string(kAltsHandshakerHost) + ":" + std::to_string(kAltsHandshakerPort));
    const Endpoint& ep = endpoints[ordinal % endpoints.size()];
    return std::make_shared<GrpcConnection>(std::make_unique<AltsTransport>(ep.ip, ep.port, *handshaker_, kStorageHost),
                                            kStorageHost);
}

std::shared_ptr<GrpcConnection> GcsGrpcClient::pick_connection(const std::string& bucket) {
    std::lock_guard<std::mutex> g(mu_);
    std::vector<Endpoint> endpoints;
    std::string key = "front-end";
    if (options_.direct_path) {
        if (!resolver_) resolver_ = std::make_unique<DirectPathResolver>(options_.ca_file);
        endpoints = resolver_->endpoints(bucket, 10000);
        key.clear();
        for (const Endpoint& ep : endpoints) key += ep.ip + ":" + std::to_string(ep.port) + ",";
    }
    auto& pool = pools_[key];
    pool.erase(std::remove_if(pool.begin(), pool.end(), [](const auto& c) { return !c->alive(); }), pool.end());
    if (static_cast<int>(pool.size()) < options_.connections) {
        pool.push_back(new_connection(endpoints, opened_[key]++));
        return pool.back();
    }
    return *std::min_element(pool.begin(), pool.end(),
                             [](const auto& a, const auto& b) { return a->open_calls() < b->open_calls(); });
}

std::vector<std::vector<uint8_t>> GcsGrpcClient::read_many(const std::string& bucket, const std::string& object,
                                                           const std::vector<ByteRange>& ranges,
                                                           const std::string& authorization, int64_t stall_ms,
                                                           int64_t ceiling_ms, int64_t generation,
                                                           int64_t* out_generation) {
    if (stall_ms <= 0 || ceiling_ms <= 0)
        throw std::invalid_argument("GcsGrpcClient: stall_ms and ceiling_ms must be positive");
    for (const ByteRange& r : ranges)
        if (r.offset < 0 || r.length <= 0) throw std::invalid_argument("GcsGrpcClient: invalid byte range");
    if (ranges.empty()) return {};

    const std::string where = "gs://" + bucket + "/" + object;
    const std::string bucket_path = "projects/_/buckets/" + bucket;
    Metadata md;
    md.emplace_back("authorization", authorization);
    md.emplace_back("x-goog-request-params", "bucket=" + url_encode(bucket_path));
    md.emplace_back("grpc-timeout", std::to_string(ceiling_ms) + "m");

    std::vector<std::vector<uint8_t>> out(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) out[i].resize(static_cast<size_t>(ranges[i].length));

    Batch batch;
    batch.open = ranges.size();
    std::vector<std::unique_ptr<RangeStream>> streams;
    std::vector<std::pair<std::shared_ptr<GrpcConnection>, uint64_t>> calls;
    streams.reserve(ranges.size());
    calls.reserve(ranges.size());
    std::string connect_error;
    for (size_t i = 0; i < ranges.size(); ++i) {
        streams.push_back(std::make_unique<RangeStream>(&batch, out[i].data(), ranges[i].length));
        std::shared_ptr<GrpcConnection> conn;
        try {
            conn = pick_connection(bucket);
        } catch (const std::exception& e) {
            connect_error = e.what();
            // This stream and every later one never start; account for them.
            std::lock_guard<std::mutex> g(batch.mu);
            batch.open -= ranges.size() - i;
            streams.pop_back();
            break;
        }
        const uint64_t id = conn->start_call(kReadObjectPath, md,
                                             read_object_request(bucket_path, object, generation, ranges[i]),
                                             streams.back().get());
        calls.emplace_back(std::move(conn), id);
    }

    // Progress deadlines (docs/REMOTE_READ_FLOW_CONTROL_DESIGN.md §4.1): the batch
    // fails when any open stream goes stall_ms without body bytes, or at the ceiling.
    const auto ceiling = std::chrono::steady_clock::now() + std::chrono::milliseconds(ceiling_ms);
    const auto tick = std::chrono::milliseconds(std::max<int64_t>(stall_ms / 5, 10));
    std::string deadline_error;
    GcsGrpcError::Deadline deadline = GcsGrpcError::kNoDeadline;
    {
        std::unique_lock<std::mutex> lk(batch.mu);
        while (batch.open != 0) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= ceiling) {
                deadline_error = "exceeded the " + std::to_string(ceiling_ms) + " ms ceiling";
                deadline = GcsGrpcError::kCeiling;
                break;
            }
            const int64_t now_ns = RangeStream::now_ns();
            for (size_t i = 0; i < streams.size() && deadline_error.empty(); ++i)
                if (streams[i]->idle_ms(now_ns) >= stall_ms) {
                    deadline = GcsGrpcError::kStalled;
                    deadline_error = "stalled (no bytes for " + std::to_string(stall_ms) + " ms) on range " +
                                     std::to_string(i) + " after " + std::to_string(streams[i]->received()) +
                                     " of " + std::to_string(ranges[i].length) + " bytes";
                }
            if (!deadline_error.empty()) break;
            batch.cv.wait_until(lk, std::min(ceiling, now + tick), [&] { return batch.open == 0; });
        }
        if (!deadline_error.empty()) {
            lk.unlock();
            for (auto& c : calls) c.first->cancel(c.second);
            lk.lock();
            batch.cv.wait(lk, [&] { return batch.open == 0; });  // handlers must outlive every callback
        }
    }

    if (!connect_error.empty()) throw GcsGrpcError(connect_error + " (" + where + ")", true, kUnavailable);
    if (!deadline_error.empty())
        throw GcsGrpcError("GCS gRPC read of " + where + " " + deadline_error, true, kDeadlineExceeded, deadline);

    int64_t seen_generation = 0;
    for (size_t i = 0; i < streams.size(); ++i) {
        const RangeStream& s = *streams[i];
        const std::string which = where + " range " + std::to_string(i) + " (offset " +
                                  std::to_string(ranges[i].offset) + ")";
        if (!s.error().empty()) throw GcsGrpcError(s.error() + ": " + which, true, kDataLoss);
        if (s.status() != kOk)
            throw GcsGrpcError("GCS gRPC read of " + which + " failed: status " + std::to_string(s.status()) + " " +
                                   s.message(),
                               retryable_status(s.status()), s.status());
        if (!s.at_message_boundary())
            throw GcsGrpcError("GCS gRPC: stream ended inside a message: " + which, true, kDataLoss);
        if (s.received() != ranges[i].length)
            throw GcsGrpcError("GCS gRPC read of " + which + " ended early: got " + std::to_string(s.received()) +
                                   " of " + std::to_string(ranges[i].length) + " bytes",
                               true, kDataLoss);
        // Ranges run concurrently, so an unpinned read must still see one version.
        if (seen_generation == 0) seen_generation = s.generation();
        if (s.generation() != seen_generation || (generation > 0 && s.generation() != generation))
            throw GcsGrpcError("GCS object generation changed during read of " + where, false, kFailedPrecondition);
    }
    if (out_generation) *out_generation = seen_generation;
    return out;
}

}  // namespace gcs_grpc
