#include "staging.h"

#include <cerrno>
#include <cstdio>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>

namespace skene {
namespace {

// File-mode sink buffer: large enough that a stream of small section bodies
// becomes few syscalls, small against the files it writes.
constexpr size_t kSinkBufferBytes = 4u << 20;

// Scratch copy chunk: bounds pass 2's transient memory for a large section.
constexpr size_t kCopyChunkBytes = 4u << 20;

Status io_error(const char* what, const std::string& path) {
    char buffer[512];
    std::snprintf(buffer, sizeof(buffer), "%s '%s': %s", what, path.c_str(),
                  std::strerror(errno));
    return Status(Code::kMalformed, buffer);
}

Status write_all(int fd, const void* data, size_t bytes, const std::string& path) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    while (bytes > 0) {
        const ssize_t n = ::write(fd, p, bytes);
        if (n < 0) {
            if (errno == EINTR) continue;
            return io_error("write failed on", path);
        }
        p += n;
        bytes -= static_cast<size_t>(n);
    }
    return Status::ok();
}

}  // namespace

// ─── Stage ──────────────────────────────────────────────────────────────────

Stage::~Stage() { close(); }

void Stage::open_memory() {
    file_mode_ = false;
}

Status Stage::open_file(const std::string& path) {
    // O_EXCL: a scratch path already in use is another writer's, and sharing it
    // would interleave two files' bodies.
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
    if (fd_ < 0) return io_error("cannot create scratch file", path);
    file_mode_ = true;
    path_ = path;
    return Status::ok();
}

void Stage::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        ::unlink(path_.c_str());
        fd_ = -1;
    }
    data_.clear();
    index_.clear();
}

Status Stage::append(uint32_t node, bool index, const void* data, size_t bytes,
                     uint64_t* out_offset) {
    staged_ += bytes;
    if (file_mode_) {
        *out_offset = file_end_;
        SKENE_RETURN_IF_ERROR(write_all(fd_, data, bytes, path_));
        file_end_ += bytes;
        return Status::ok();
    }
    std::vector<std::vector<uint8_t>>& streams = index ? index_ : data_;
    if (node >= streams.size()) streams.resize(static_cast<size_t>(node) + 1u);
    std::vector<uint8_t>& stream = streams[node];
    *out_offset = stream.size();
    const uint8_t* p = static_cast<const uint8_t*>(data);
    stream.insert(stream.end(), p, p + bytes);
    return Status::ok();
}

Status Stage::copy_to(uint32_t node, bool index, uint64_t offset, uint64_t bytes,
                      Sink* sink) {
    if (!file_mode_) {
        const std::vector<std::vector<uint8_t>>& streams = index ? index_ : data_;
        if (node >= streams.size() || offset > streams[node].size()
                || bytes > streams[node].size() - offset)
            return Status(Code::kMalformed,
                          "internal: staged section lies outside its node's stream");
        return sink->write(streams[node].data() + offset, static_cast<size_t>(bytes));
    }
    std::vector<uint8_t> chunk(bytes < kCopyChunkBytes ? static_cast<size_t>(bytes)
                                                       : kCopyChunkBytes);
    while (bytes > 0) {
        const size_t want = bytes < chunk.size() ? static_cast<size_t>(bytes) : chunk.size();
        const ssize_t n = ::pread(fd_, chunk.data(), want, static_cast<off_t>(offset));
        if (n < 0) {
            if (errno == EINTR) continue;
            return io_error("read failed on scratch file", path_);
        }
        if (n == 0)
            return Status(Code::kTruncated, "scratch file ended before a staged section");
        SKENE_RETURN_IF_ERROR(sink->write(chunk.data(), static_cast<size_t>(n)));
        offset += static_cast<uint64_t>(n);
        bytes -= static_cast<uint64_t>(n);
    }
    return Status::ok();
}

void Stage::release(uint32_t node, bool index) {
    if (file_mode_) return;
    std::vector<std::vector<uint8_t>>& streams = index ? index_ : data_;
    if (node < streams.size()) std::vector<uint8_t>().swap(streams[node]);
}

// ─── Sink ───────────────────────────────────────────────────────────────────

Sink::~Sink() { abandon(); }

void Sink::open_memory(std::vector<uint8_t>* out) {
    memory_ = out;
    position_ = out->size();
}

Status Sink::open_file(const std::string& path) {
    path_ = path;
    partial_ = path + ".skene-partial";
    fd_ = ::open(partial_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_ < 0) return io_error("cannot create", partial_);
    buffer_.reserve(kSinkBufferBytes);
    position_ = 0;
    return Status::ok();
}

Status Sink::write(const void* data, size_t bytes) {
    if (bytes == 0) return Status::ok();
    position_ += bytes;
    const uint8_t* p = static_cast<const uint8_t*>(data);
    if (memory_ != nullptr) {
        memory_->insert(memory_->end(), p, p + bytes);
        return Status::ok();
    }
    if (buffer_.size() + bytes > kSinkBufferBytes) SKENE_RETURN_IF_ERROR(flush());
    if (bytes >= kSinkBufferBytes) return write_all(fd_, p, bytes, partial_);
    buffer_.insert(buffer_.end(), p, p + bytes);
    return Status::ok();
}

Status Sink::zeros(size_t bytes) {
    static const uint8_t kZeros[64] = {};
    while (bytes > 0) {
        const size_t n = bytes < sizeof(kZeros) ? bytes : sizeof(kZeros);
        SKENE_RETURN_IF_ERROR(write(kZeros, n));
        bytes -= n;
    }
    return Status::ok();
}

Status Sink::flush() {
    if (fd_ < 0 || buffer_.empty()) return Status::ok();
    Status st = write_all(fd_, buffer_.data(), buffer_.size(), partial_);
    buffer_.clear();
    return st;
}

Status Sink::commit() {
    if (memory_ != nullptr) return Status::ok();
    SKENE_RETURN_IF_ERROR(flush());
    if (::close(fd_) != 0) {
        fd_ = -1;
        return io_error("cannot close", partial_);
    }
    fd_ = -1;
    if (std::rename(partial_.c_str(), path_.c_str()) != 0)
        return io_error("cannot rename into place", path_);
    partial_.clear();
    return Status::ok();
}

void Sink::abandon() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    if (!partial_.empty()) {
        std::remove(partial_.c_str());
        partial_.clear();
    }
}

}  // namespace skene
