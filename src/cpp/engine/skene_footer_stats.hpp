#pragma once
// src/cpp/engine/skene_footer_stats.hpp — a skene dataset's footer statistics into
// its planning manifest, opening each file the way the scan does.
//
// Planning reads each file's tail and footer (never the whole file), applies the
// footer's statistics to the manifest row (planner/skene_stats.hpp), and leaves the
// opened reader in SkeneReaderCache — so the scan that follows finds every file
// already open instead of opening it a second time. A file the cache already holds
// is not read at all.
//
// Compiled into opteryx/operators/_operators.pyx ONLY: the cache is a per-module
// singleton, and that module configures its budget and runs the scan.

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "planner/skene_stats.hpp"
#include "skene/format.h"
#include "skene/reader.h"
#include "skene_reader_cache.hpp"

namespace opteryx::engine {

// The reader for the .skene file at `path`: the cached one when this exact file
// (path, size, mtime) is held, else opened from its tail and footer (v3) or its
// whole-file mapping (v2) and cached. nullptr with `err` set on failure.
inline std::shared_ptr<SkeneCachedReader> skene_open_for_planning(const std::string& path,
                                                                  std::string& err) {
    SkeneReaderCache& cache = SkeneReaderCache::instance();
    if (!cache.configured()) {
        err = "the skene reader cache has no budget (SKENE_FOOTER_CACHE_BYTES was never applied)";
        return nullptr;
    }
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        err = "cannot open file '" + path + "': " + std::strerror(errno);
        return nullptr;
    }
    struct FdCloser {
        int fd;
        ~FdCloser() { ::close(fd); }
    } closer{fd};

    struct stat st {};
    if (::fstat(fd, &st) != 0 || st.st_size < static_cast<off_t>(skene::kMinFileBytes)) {
        err = "'" + path + "' is too small to be a .skene file";
        return nullptr;
    }
    const std::string key = SkeneReaderCache::key_for(path, st);
    if (std::shared_ptr<SkeneCachedReader> held = cache.get(key)) return held;

    const uint64_t size = static_cast<uint64_t>(st.st_size);
    SkeneReadBuffer tail;
    if (!skene_pread(fd, size - skene::kFileTailBytes, skene::kFileTailBytes, &tail, path, err))
        return nullptr;
    uint64_t footer_offset = 0, footer_bytes = 0;
    skene::Status status =
        skene::footer_extent(tail.data(), skene::kFileTailBytes, size, &footer_offset, &footer_bytes);
    if (!status.is_ok()) {
        err = "'" + path + "': " + status.message();
        return nullptr;
    }
    skene::FileTail parsed_tail;
    std::memcpy(&parsed_tail, tail.data(), sizeof(parsed_tail));

    auto entry = std::make_shared<SkeneCachedReader>();
    entry->version = parsed_tail.version;
    if (entry->version == 2) {
        entry->mapping = std::make_unique<SkeneFileMapping>(path);
        if (!entry->mapping->ok()) {
            err = "cannot map file '" + path + "'";
            return nullptr;
        }
        status = skene::open_reader(entry->mapping->data(), entry->mapping->size(), &entry->reader);
    } else {
        SkeneReadBuffer footer;
        if (!skene_pread(fd, footer_offset, footer_bytes, &footer, path, err)) return nullptr;
        status = skene::open_reader_ranged(tail.data(), skene::kFileTailBytes, footer.data(),
                                           footer.size(), footer_offset, size, &entry->reader);
    }
    if (!status.is_ok()) {
        err = "'" + path + "': " + status.message();
        return nullptr;
    }
    cache.put(key, entry, static_cast<int64_t>(footer_bytes));
    return entry;
}

// Each file's footer statistics into manifest row `rows[i]` (`paths[i]`), with what
// each applied in `applied[i]`. Returns "" or the first failure, naming its file;
// rows before it are filled, rows after it are not.
inline std::string skene_footers_into(planner::NativeManifest& m, const std::vector<size_t>& rows,
                                      const std::vector<std::string>& paths,
                                      const std::vector<DrakenType>& physical,
                                      std::vector<planner::SkeneApplied>& applied) {
    applied.assign(paths.size(), planner::SkeneApplied{});
    std::string err;
    for (size_t i = 0; i < paths.size(); ++i) {
        std::shared_ptr<SkeneCachedReader> entry = skene_open_for_planning(paths[i], err);
        if (!entry) return err;
        const skene::FileMetadata& meta = entry->reader.metadata();
        applied[i] = planner::apply_skene_footer(m, rows[i], meta, physical,
                                                 planner::skene_slot_positions(meta, m.positions()));
    }
    return std::string();
}

}  // namespace opteryx::engine
