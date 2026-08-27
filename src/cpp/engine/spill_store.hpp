#pragma once
// src/cpp/engine/spill_store.hpp — the disk side of morsel spill.
//
// One SpillStore per Engine run, created lazily on the FIRST flush (a query
// that never spills never creates a directory, let alone a file). It owns one
// directory under the configured spill root, named q<pid>-<seq> so ownership
// is readable from the name alone, and removes it wholesale in its destructor.
//
// This is the native implementation of the per-query spill store that
// `KVSTORE_LOCATION` has documented all along (config.py: "the per-query
// shuffle/spill store, whose keys are scoped by query and operator and whose
// contents are discarded when the query ends") — a contract that until now had
// no first-party caller. The Python KV stores in opteryx/managers/kvstores/
// are unreachable from here: every caller is a worker thread in GIL-released
// C++, and re-acquiring the GIL inside the spill path is exactly what the
// engineering contract forbids. Design: docs/MORSEL_SPILL_DESIGN.md §5, §7.
//
// LIFECYCLE (the failure that must not happen is the orphan):
//   - normal end: ~SpillStore removes the directory recursively.
//   - killed process: the directory survives. The STARTUP SWEEP — run once per
//     root per process, before the first unit is written — parses the pid out
//     of every q<pid>-* sibling and removes directories whose owner is gone
//     (kill(pid, 0) == ESRCH). A crashed writer's partial file lives inside
//     its query directory, so the sweep collects it with everything else;
//     nothing ever reads a unit it did not itself finish writing.
//   - disk exhaustion: a loud error carried back to the operator. NEVER a
//     silent fallback to another path — on Cloud Run the container filesystem
//     is RAM-backed tmpfs, and falling back to it re-creates the OOM this
//     store exists to prevent while masking the cause.

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace opteryx::engine {

// Process-global spill telemetry, groupby_tel.hpp-shaped: inline atomics with
// C accessors, read from _operators.pyx. A spilling query must be VISIBLY a
// spilling query — spill without telemetry is a silent degradation.
namespace spill_tel {
inline std::atomic<int64_t>& units_written() { static std::atomic<int64_t> v{0}; return v; }
inline std::atomic<int64_t>& bytes_written() { static std::atomic<int64_t> v{0}; return v; }
inline std::atomic<int64_t>& bytes_read()    { static std::atomic<int64_t> v{0}; return v; }
inline std::atomic<int64_t>& rows_spilled()  { static std::atomic<int64_t> v{0}; return v; }
inline std::atomic<int64_t>& sweep_removed() { static std::atomic<int64_t> v{0}; return v; }
inline void reset() {
    units_written().store(0); bytes_written().store(0); bytes_read().store(0);
    rows_spilled().store(0);  sweep_removed().store(0);
}
// Plain-value readers for the _operators.pyx externs (Cython cannot bind an
// atomic& return).
inline int64_t units_written_count() { return units_written().load(); }
inline int64_t bytes_written_count() { return bytes_written().load(); }
inline int64_t bytes_read_count()    { return bytes_read().load(); }
inline int64_t rows_spilled_count()  { return rows_spilled().load(); }
inline int64_t sweep_removed_count() { return sweep_removed().load(); }
}  // namespace spill_tel

// RAII read-only mapping of a whole spill unit file, for read-back decode.
// Same shape and posture as native_skene_scan_source.hpp's SkeneFileMapping:
// fails loud (ok() == false) rather than throwing — this runs on worker
// threads in a no-exception context.
class SpillFileMapping {
  public:
    explicit SpillFileMapping(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return;
        struct stat st {};
        if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
            ::close(fd);
            return;
        }
        void* addr = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ,
                            MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (addr == MAP_FAILED) return;
        data_ = addr;
        size_ = static_cast<size_t>(st.st_size);
    }
    ~SpillFileMapping() {
        if (data_ != nullptr) ::munmap(data_, size_);
    }
    SpillFileMapping(const SpillFileMapping&) = delete;
    SpillFileMapping& operator=(const SpillFileMapping&) = delete;
    bool ok() const noexcept { return data_ != nullptr; }
    const void* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }
  private:
    void* data_ = nullptr;
    size_t size_ = 0;
};

class SpillStore {
  public:
    // `root` must be non-empty; the caller (MorselBuffer) only constructs a
    // store once a flush is actually needed and spill is configured.
    explicit SpillStore(const std::string& root) {
        namespace fs = std::filesystem;
        std::error_code ec;
        fs::create_directories(root, ec);
        if (ec) {
            err_ = "spill root '" + root + "': " + ec.message();
            return;
        }
        sweep_once(root);
        static std::atomic<uint64_t> instance_seq{0};
        dir_ = (fs::path(root) /
                ("q" + std::to_string(static_cast<long>(::getpid())) + "-" +
                 std::to_string(instance_seq.fetch_add(1)))).string();
        fs::create_directory(dir_, ec);
        if (ec) {
            err_ = "spill dir '" + dir_ + "': " + ec.message();
            dir_.clear();
            return;
        }
    }

    ~SpillStore() {
        if (!dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(dir_, ec);   // best effort; sweep backstops
        }
    }

    SpillStore(const SpillStore&) = delete;
    SpillStore& operator=(const SpillStore&) = delete;

    bool ok() const noexcept { return err_.empty() && !dir_.empty(); }
    const std::string& error() const noexcept { return err_; }

    // Writes one complete unit file. Returns the full path, or empty with `err`
    // set. O_EXCL: a path collision is a bug, not something to paper over.
    std::string write_unit(const std::vector<uint8_t>& bytes, std::string& err) {
        const std::string path =
            dir_ + "/u" + std::to_string(unit_seq_.fetch_add(1)) + ".skene";
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd < 0) {
            err = "spill write '" + path + "': open: " + std::strerror(errno);
            return {};
        }
        size_t done = 0;
        while (done < bytes.size()) {
            ssize_t n = ::write(fd, bytes.data() + done, bytes.size() - done);
            if (n < 0) {
                if (errno == EINTR) continue;
                err = "spill write '" + path + "': " + std::strerror(errno) +
                      (errno == ENOSPC ? " — spill disk is full; the query cannot "
                                         "continue (this is a capacity failure, "
                                         "not a fallback point)" : "");
                ::close(fd);
                ::unlink(path.c_str());
                return {};
            }
            done += static_cast<size_t>(n);
        }
        if (::close(fd) != 0) {
            err = "spill write '" + path + "': close: " + std::strerror(errno);
            ::unlink(path.c_str());
            return {};
        }
        spill_tel::units_written().fetch_add(1, std::memory_order_relaxed);
        spill_tel::bytes_written().fetch_add(static_cast<int64_t>(bytes.size()),
                                             std::memory_order_relaxed);
        return path;
    }

    void remove_unit(const std::string& path) {
        ::unlink(path.c_str());   // best effort; ~SpillStore and the sweep backstop
    }

  private:
    // Once per root per process. Removes q<pid>-* directories whose pid no
    // longer exists. ESRCH is the only proof of death accepted: EPERM means the
    // process exists but is not ours to signal, and an unparseable name is not
    // ours to delete.
    static void sweep_once(const std::string& root) {
        static std::mutex mtx;
        static std::set<std::string>* swept = new std::set<std::string>();
        std::lock_guard<std::mutex> lk(mtx);
        if (!swept->insert(root).second) return;
        namespace fs = std::filesystem;
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(root, ec)) {
            const std::string name = entry.path().filename().string();
            if (name.size() < 2 || name[0] != 'q') continue;
            size_t dash = name.find('-');
            if (dash == std::string::npos || dash < 2) continue;
            long pid = 0;
            {
                char* end = nullptr;
                pid = std::strtol(name.c_str() + 1, &end, 10);
                if (end != name.c_str() + dash || pid <= 0) continue;
            }
            if (pid == static_cast<long>(::getpid())) continue;
            if (::kill(static_cast<pid_t>(pid), 0) == 0 || errno != ESRCH) continue;
            std::error_code rmec;
            std::filesystem::remove_all(entry.path(), rmec);
            if (!rmec)
                spill_tel::sweep_removed().fetch_add(1, std::memory_order_relaxed);
        }
    }

    std::string dir_;
    std::string err_;
    std::atomic<uint64_t> unit_seq_{0};
};

}  // namespace opteryx::engine
