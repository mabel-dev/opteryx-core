#pragma once
// src/cpp/engine/native_skene_scan_source.hpp — a genuinely native (zero-Python)
// skene scan Source for the morsel-driven engine.
//
// Sibling of native_parquet_scan_source.hpp, and far smaller, because skene's
// decode is a PURE FUNCTION over a byte buffer: skene::read_morsel(bytes, opts,
// &out) touches no shared state, allocates only what the output morsel owns,
// and returns a Status rather than throwing. There is no IO pipeline to submit
// into and no in-flight window to manage — a worker claims a file, decodes it,
// and emits it. Parallelism is therefore the trivial kind: N workers, N files,
// one atomic counter.
//
// This replaces the compile-time materialized path for skene scans, which
// decoded EVERY file on the driver thread before execution began. That cost
// both latency (serial decode, nothing overlapped) and memory (the whole read
// set resident at once — ~880 MB for a full-width TPC-H SF1 lineitem scan).
// Here the scan is pipelined and its memory is O(morsels in flight).
//
// Why the decoded morsel outlives the mapping it was decoded from: the format
// COPIES buffers verbatim and rebuilds absolute pointers from stored offsets
// (skene/include/skene/reader.h). A decoded CxxMorsel therefore shares nothing
// with the source bytes, so the mapping is unmapped as soon as read_morsel
// returns. If that ever changes to a zero-copy/borrowing reader, this unmap
// becomes a use-after-free — the reader's contract is load-bearing here.
//
// Scope (first landing — fail loud, not silently, outside it):
//   - Local files only (mmap). Remote/ranged reads via footer_extent are the
//     next stage; a non-local path fails loud rather than silently falling back.
//   - Whole-file reads. skene's per-column extents make ranged column reads
//     possible, but file_io has no seek path yet and an unused seek path is an
//     untested one.
//   - No predicates, no zone-map/bloom skipping. FILE-level pruning already
//     happened at plan time (manifest bounds from the footer), and row-level
//     filtering runs as a parallel engine Filter above this scan. Reader-side
//     predicates belong here eventually — this Source can filter without
//     serializing, which the materialized path could not — but that is a
//     separate, measurable change.
//   - Projection IS pushed: only the requested columns are materialized.
//   - No schema evolution: every projected column must exist in every file,
//     with the type the plan bound. A divergent file fails loud, naming itself.

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "operator.hpp"

#include "morsels/cxx_morsel.h"
#include "skene/reader.h"
#include "skene/status.h"

namespace opteryx::engine {

// RAII read-only mapping of a whole file. Fails loud (fd < 0 / addr == nullptr)
// rather than throwing — this runs on worker threads in a no-exception context,
// matching skene's own status-code posture.
class SkeneFileMapping {
  public:
    explicit SkeneFileMapping(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return;
        struct stat st {};
        if (::fstat(fd, &st) != 0 || st.st_size <= 0) {
            ::close(fd);
            return;
        }
        void* addr = ::mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ,
                            MAP_PRIVATE, fd, 0);
        // The mapping keeps its own reference to the file; the descriptor is not
        // needed once mmap succeeds, and holding one per in-flight file would
        // burn descriptors on a wide scan.
        ::close(fd);
        if (addr == MAP_FAILED) return;
        data_ = addr;
        size_ = static_cast<size_t>(st.st_size);
    }

    ~SkeneFileMapping() {
        if (data_ != nullptr) ::munmap(data_, size_);
    }

    SkeneFileMapping(const SkeneFileMapping&) = delete;
    SkeneFileMapping& operator=(const SkeneFileMapping&) = delete;

    bool ok() const noexcept { return data_ != nullptr; }
    const void* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }

  private:
    void* data_ = nullptr;
    size_t size_ = 0;
};

// Global state: the claim counter. Files are handed out one at a time rather
// than block-partitioned up front, so a worker that draws a large file does not
// hold the tail of the scan behind it (files are row groups and vary in size).
struct NativeSkeneScanGlobal : GlobalSourceState {
    std::atomic<size_t> next_file{0};
};

class NativeSkeneScanSource : public Source {
  public:
    // Every pointer is BORROWED from the plan (NativePlan holds the owning
    // Python objects alive for the driver's lifetime), matching how
    // NativeParquetScanSource borrows from its NativeScanPlan.
    NativeSkeneScanSource(const std::vector<std::string>* files,
                          const std::vector<std::string>* column_names,
                          const std::vector<std::string>* out_identities,
                          const std::vector<int>* column_types)
        : files_(files),
          column_names_(column_names),
          out_identities_(out_identities),
          column_types_(column_types) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<NativeSkeneScanGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativeSkeneScanGlobal&>(gs);
        while (true) {
            const size_t idx = g.next_file.fetch_add(1, std::memory_order_relaxed);
            if (idx >= files_->size()) return SourceResult::FINISHED;

            const std::string& path = (*files_)[idx];
            SkeneFileMapping mapping(path);
            if (!mapping.ok()) {
                err.code = 1;
                err_msg_ = "NativeSkeneScanSource: cannot map file '" + path + "'";
                err.msg = err_msg_.c_str();
                return SourceResult::FINISHED;
            }

            skene::ReadOptions options;
            options.columns = *column_names_;

            auto morsel = std::make_shared<CxxMorsel>();
            skene::Status status =
                skene::read_morsel(mapping.data(), mapping.size(), options, morsel.get());
            if (!status.is_ok()) {
                err.code = 1;
                err_msg_ = "NativeSkeneScanSource: '" + path + "': " + status.message();
                err.msg = err_msg_.c_str();
                return SourceResult::FINISHED;
            }

            // Validate against the bound schema and rename to plan identities.
            // Name-keyed, never positional: the reader's column order is not part
            // of the contract, and a positional rename would silently mislabel
            // columns if it ever changed.
            if (!rename_to_identities(*morsel, path, err)) return SourceResult::FINISHED;

            // A file whose rows were all... there is no filter here, so an empty
            // file is simply an empty file: skip it rather than emitting a
            // zero-row morsel downstream.
            if (morsel->num_rows() == 0) continue;

            out = std::move(morsel);
            return SourceResult::HAVE_MORE;
        }
    }

  private:
    bool rename_to_identities(CxxMorsel& morsel, const std::string& path, ErrCtx& err) {
        if (morsel.names.size() != column_names_->size()) {
            err.code = 1;
            err_msg_ = "NativeSkeneScanSource: '" + path + "': decoded " +
                       std::to_string(morsel.names.size()) + " columns, projected " +
                       std::to_string(column_names_->size());
            err.msg = err_msg_.c_str();
            return false;
        }
        for (size_t i = 0; i < morsel.names.size(); ++i) {
            // Small N (a projection), so a linear probe beats building a map per
            // morsel — and the common case is that position i already matches.
            size_t want = column_names_->size();
            if (morsel.names[i] == (*column_names_)[i]) {
                want = i;
            } else {
                for (size_t j = 0; j < column_names_->size(); ++j) {
                    if (morsel.names[i] == (*column_names_)[j]) {
                        want = j;
                        break;
                    }
                }
            }
            if (want == column_names_->size()) {
                err.code = 1;
                err_msg_ = "NativeSkeneScanSource: '" + path + "': decoded unexpected "
                           "column '" + morsel.names[i] + "'";
                err.msg = err_msg_.c_str();
                return false;
            }
            if (static_cast<int>(morsel.columns[i].view.type) != (*column_types_)[want]) {
                err.code = 1;
                err_msg_ = "NativeSkeneScanSource: '" + path + "': column '" +
                           morsel.names[i] + "' is type " +
                           std::to_string(static_cast<int>(morsel.columns[i].view.type)) +
                           " in this file but " + std::to_string((*column_types_)[want]) +
                           " at bind time — this dataset's files do not share one schema";
                err.msg = err_msg_.c_str();
                return false;
            }
            morsel.names[i] = (*out_identities_)[want];
        }
        return true;
    }

    const std::vector<std::string>* files_;
    const std::vector<std::string>* column_names_;
    const std::vector<std::string>* out_identities_;
    const std::vector<int>* column_types_;
    // Error text must outlive the call (ErrCtx.msg is a borrowed const char*).
    // One Source instance reports at most one error before the scan stops, so a
    // single member is enough; a second failing worker overwrites a message for
    // a scan that is already ending.
    std::string err_msg_;
};

}  // namespace opteryx::engine
