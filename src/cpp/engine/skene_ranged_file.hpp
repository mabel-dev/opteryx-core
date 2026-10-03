// skene_ranged_file.hpp — read a .skene file row group by row group, fetching only the
// bytes each read needs: a local file through pread, a remote one through range GETs.
//
// The vector index's files are read by compaction's carry (vector_index_carry.hpp) and, in
// Stage D, by the index scan. Both read files far larger than a worker's memory, so neither
// holds a whole file: this opens the file from its tail and footer (skene's ranged reader,
// FORMAT v3), attaches the directories of the columns it reads, and fetches each row
// group's chunks of those columns when that row group is read.
//
// A remote location is an https URL that carries its own credential (a signed URL): an
// hours-long build cannot refresh a bearer token natively, and a signed GET cannot answer a
// HEAD, so the size is given by the caller (the catalog records every index file's size).

#pragma once

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include "http_client.hpp"
#include "morsels/cxx_morsel.h"
#include "skene/format.h"
#include "skene/reader.h"

namespace opteryx::engine {

class SkeneRangedFile {
  public:
    SkeneRangedFile() = default;
    SkeneRangedFile(const SkeneRangedFile&) = delete;
    SkeneRangedFile& operator=(const SkeneRangedFile&) = delete;
    ~SkeneRangedFile() {
        if (fd_ >= 0) ::close(fd_);
    }

    // Open `location` (a local path, or an http(s) URL carrying its own credential) of
    // `file_bytes` bytes, for reading `columns`.
    bool open(const std::string& location, uint64_t file_bytes,
              const std::vector<std::string>& columns, std::string* err) {
        location_ = location;
        file_bytes_ = file_bytes;
        columns_ = columns;
        remote_ = location.rfind("http://", 0) == 0 || location.rfind("https://", 0) == 0;
        if (!remote_ && location.find("://") != std::string::npos) {
            *err = "skene: " + location + " is neither a local path nor an http(s) URL";
            return false;
        }
        if (remote_) {
            http_ = std::make_unique<HttpClient>(4, 120000);
        } else {
            fd_ = ::open(location.c_str(), O_RDONLY);
            if (fd_ < 0) { *err = "skene: cannot open " + location + ": " + std::strerror(errno); return false; }
        }
        if (file_bytes < skene::kFileTailBytes) { *err = "skene: " + location + " is too small"; return false; }

        std::vector<uint8_t> tail;
        if (!fetch(file_bytes - skene::kFileTailBytes, skene::kFileTailBytes, &tail, err)) return false;
        uint64_t footer_offset = 0, footer_bytes = 0;
        skene::Status st = skene::footer_extent(tail.data(), tail.size(), file_bytes, &footer_offset, &footer_bytes);
        if (!st.is_ok()) return fail(st, err);
        std::vector<uint8_t> footer;
        if (!fetch(footer_offset, footer_bytes, &footer, err)) return false;
        st = skene::open_reader_ranged(tail.data(), tail.size(), footer.data(), footer.size(),
                                       footer_offset, file_bytes, &reader_);
        if (!st.is_ok()) return fail(st, err);

        std::vector<skene::ByteRange> wanted;
        st = skene::plan_directory_fetch(reader_, columns_, false, skene::FetchPolicy(), &wanted);
        if (!st.is_ok()) return fail(st, err);
        Fetched dirs;
        if (!fetch_all(wanted, &dirs, err)) return false;
        st = skene::attach_directories(&reader_, columns_, dirs.ranges);
        if (!st.is_ok()) return fail(st, err);
        return true;
    }

    uint32_t row_groups() const { return static_cast<uint32_t>(reader_.metadata().row_groups.size()); }

    // One row group's columns. `buffers` holds the fetched bytes; it is kept with the
    // morsel because a decoded column may view them.
    struct RowGroup {
        CxxMorsel                         morsel;
        std::vector<std::vector<uint8_t>> buffers;
    };

    bool read(uint32_t row_group, RowGroup* out, std::string* err) {
        std::vector<skene::ByteRange> wanted;
        skene::Status st = skene::plan_fetch(reader_, columns_, {row_group}, skene::FetchPolicy(), &wanted);
        if (!st.is_ok()) return fail(st, err);
        Fetched fetched;
        if (!fetch_all(wanted, &fetched, err)) return false;
        skene::ReadOptions options;
        options.columns = columns_;
        st = skene::read_morsel(reader_, row_group, options, fetched.ranges, &out->morsel);
        if (!st.is_ok()) return fail(st, err);
        out->buffers = std::move(fetched.buffers);
        return true;
    }

  private:
    struct Fetched {
        std::vector<std::vector<uint8_t>> buffers;
        std::vector<skene::FetchedRange>  ranges;
    };

    bool fail(const skene::Status& st, std::string* err) const {
        *err = "skene: " + location_ + ": " + st.message();
        return false;
    }

    bool fetch_all(const std::vector<skene::ByteRange>& wanted, Fetched* out, std::string* err) {
        out->buffers.resize(wanted.size());
        for (size_t i = 0; i < wanted.size(); ++i)
            if (!fetch(wanted[i].offset, wanted[i].bytes, &out->buffers[i], err)) return false;
        for (size_t i = 0; i < wanted.size(); ++i)
            out->ranges.push_back({wanted[i].offset, wanted[i].bytes, out->buffers[i].data()});
        return true;
    }

    bool fetch(uint64_t offset, uint64_t bytes, std::vector<uint8_t>* out, std::string* err) {
        if (offset + bytes > file_bytes_) {
            *err = "skene: a read past the end of " + location_;
            return false;
        }
        if (remote_) {
            try {
                std::map<std::string, std::string> h{
                    {"Range", "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + bytes - 1u)}};
                *out = http_->get(location_, h);
            } catch (const std::exception& e) {
                *err = "skene: cannot read " + location_ + ": " + e.what();
                return false;
            }
            if (out->size() != bytes) {
                *err = "skene: " + location_ + " answered " + std::to_string(out->size()) + " bytes for a " +
                       std::to_string(bytes) + "-byte range";
                return false;
            }
            return true;
        }
        out->resize(bytes);
        uint64_t done = 0;
        while (done < bytes) {
            const ssize_t n = ::pread(fd_, out->data() + done, bytes - done, static_cast<off_t>(offset + done));
            if (n <= 0) {
                *err = "skene: cannot read " + location_ + (n < 0 ? std::string(": ") + std::strerror(errno) : ": short file");
                return false;
            }
            done += static_cast<uint64_t>(n);
        }
        return true;
    }

    std::string                 location_;
    uint64_t                    file_bytes_ = 0;
    std::vector<std::string>    columns_;
    bool                        remote_ = false;
    int                         fd_ = -1;
    std::unique_ptr<HttpClient> http_;
    skene::FileReader           reader_;
};

}  // namespace opteryx::engine
