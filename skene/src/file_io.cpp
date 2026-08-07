#include "skene/file_io.h"

#include <cerrno>
#include <cstdio>
#include <cstring>

#include "skene/format.h"

namespace skene {
namespace {

Status io_error(const char* what, const std::string& path) {
    char buffer[512];
    std::snprintf(buffer, sizeof(buffer), "%s '%s': %s", what, path.c_str(),
                  std::strerror(errno));
    return Status(Code::kMalformed, buffer);
}

}  // namespace

Status write_file(const std::string& path, const std::vector<uint8_t>& bytes) {
    // Write-then-rename. A .skene file is only meaningful whole — the tail
    // carries the footer's location and checksum — so a reader that opens a
    // partially written file sees a truncated object rather than a valid one.
    // Renaming into place means it either exists complete or does not exist.
    const std::string temporary = path + ".skene-partial";

    std::FILE* handle = std::fopen(temporary.c_str(), "wb");
    if (handle == nullptr) return io_error("cannot create", temporary);

    if (!bytes.empty()) {
        const size_t written = std::fwrite(bytes.data(), 1, bytes.size(), handle);
        if (written != bytes.size()) {
            std::fclose(handle);
            std::remove(temporary.c_str());
            return io_error("short write to", temporary);
        }
    }

    if (std::fclose(handle) != 0) {
        std::remove(temporary.c_str());
        return io_error("cannot close", temporary);
    }

    if (std::rename(temporary.c_str(), path.c_str()) != 0) {
        std::remove(temporary.c_str());
        return io_error("cannot rename into place", path);
    }
    return Status::ok();
}

Status read_file(const std::string& path, std::vector<uint8_t>* out) {
    if (out == nullptr) return Status(Code::kMalformed, "read_file: out is null");

    std::FILE* handle = std::fopen(path.c_str(), "rb");
    if (handle == nullptr) return io_error("cannot open", path);

    if (std::fseek(handle, 0, SEEK_END) != 0) {
        std::fclose(handle);
        return io_error("cannot seek", path);
    }
    const long size = std::ftell(handle);
    if (size < 0) {
        std::fclose(handle);
        return io_error("cannot size", path);
    }
    std::rewind(handle);

    if (static_cast<size_t>(size) < kMinFileBytes) {
        std::fclose(handle);
        char buffer[256];
        std::snprintf(buffer, sizeof(buffer),
                      "'%s' is %ld bytes; the smallest well-formed .skene file "
                      "is %zu", path.c_str(), size, kMinFileBytes);
        return Status(Code::kTruncated, buffer);
    }

    out->resize(static_cast<size_t>(size));
    const size_t read = std::fread(out->data(), 1, out->size(), handle);
    std::fclose(handle);
    if (read != out->size()) {
        out->clear();
        return io_error("short read from", path);
    }
    return Status::ok();
}

}  // namespace skene
