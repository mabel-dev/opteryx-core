#pragma once
// parallel_read.hpp — multi-core row-group decode for the streaming Parquet reader.
//
// stream_parquet[_from_path] used to decode one row group at a time on the calling
// thread. ParallelRowGroupReader decodes the same row groups on a shared worker pool
// and hands them back strictly in file order, so a caller that iterates sees exactly
// what the serial loop produced.
//
// Scheduling
//   The unit of work is one (row group, column) chunk, claimed in row-group-major
//   order. Row-group granularity alone leaves a 20-row-group file at ~2 rounds on an
//   18-core machine; chunk granularity also parallelises a narrow projection (two
//   columns x 20 row groups = 40 tasks) and a single huge row group.
//
// Bounded memory
//   Only row groups in [next_to_yield, next_to_yield + window) may be claimed, so the
//   decoded data held at once is the window, not the file. The window is sized from
//   the thread count and the projection, then clamped by a byte budget (see
//   AutoWindow). A consumer that stops pulling stops the workers.
//
// No blocking inside the pool
//   A "runner" is a pool task that claims chunks until none is claimable, then exits —
//   it never waits for the consumer. next() re-launches runners when it opens the
//   window. So an abandoned or paused reader cannot hold pool threads hostage, and two
//   readers iterated by one thread (zip) cannot deadlock each other.
//
// Lifetime
//   The mapped file and the RowGroupStats vector are borrowed, not owned. The
//   destructor cancels the job and waits for every chunk currently decoding, so the
//   caller may unmap right after destroying the reader. Runners still queued hold only
//   shared state and exit on their first look at it.

#include "column_materialize.hpp"
#include "decode.hpp"
#include "metadata.hpp"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rugo {

// A column the worker already scattered into Draken buffers (column_materialize.hpp),
// so the consumer only has to wrap it. Owns the buffers until wrapped: `valid` is
// cleared by whoever hands them to wrap_parquet_column (which consumes them, success or
// failure); anything still valid at destruction is freed. `rows` is the row count it was
// built for — the consumer uses it only if that equals the row count it derives itself.
struct PreMaterialized {
  bool valid = false;
  int32_t rows = 0;
  rugo::_parquet::MaterializedColumn mc;

  PreMaterialized() = default;
  PreMaterialized(const PreMaterialized&) = delete;
  PreMaterialized& operator=(const PreMaterialized&) = delete;
  PreMaterialized(PreMaterialized&& o) noexcept : valid(o.valid), rows(o.rows), mc(o.mc) {
    o.valid = false;
  }
  PreMaterialized& operator=(PreMaterialized&& o) noexcept {
    if (this != &o) {
      reset();
      valid = o.valid; rows = o.rows; mc = o.mc;
      o.valid = false;
    }
    return *this;
  }
  ~PreMaterialized() { reset(); }
  void reset() noexcept {
    if (valid) rugo::_parquet::release_materialized(mc);
    valid = false;
  }
};

class ParallelRowGroupReader {
 public:
  // row_groups / data must outlive the reader. `rg_indices` are the row groups to
  // decode (already mask-pruned), in the order they will be yielded. `threads` is the
  // most pool workers this read occupies at once (clamped to the pool size);
  // `window_rgs` of 0 picks AutoWindow.
  ParallelRowGroupReader(const uint8_t* data, size_t size,
                         const std::vector<std::string>& column_names,
                         const std::vector<RowGroupStats>& row_groups,
                         std::vector<int> rg_indices, int threads,
                         size_t window_rgs = 0, bool prematerialize = true);
  ~ParallelRowGroupReader();
  ParallelRowGroupReader(const ParallelRowGroupReader&) = delete;
  ParallelRowGroupReader& operator=(const ParallelRowGroupReader&) = delete;

  // Blocks until the next row group in order is decoded. Returns false once every
  // row group has been yielded. Throws std::runtime_error with the same
  // "row group N, column 'X': reason" text DecodeRowGroupColumns raises when a
  // column fails; the failure surfaces when the reader reaches that row group, and
  // nothing after it is decoded. `out` is left empty for a row group that decodes to
  // no columns (the serial loop's `size() == 0 -> continue`).
  // `pre` is parallel to `out`: when the reader was built with prematerialize, a
  // scalar column that materialized on its worker arrives valid there.
  bool next(std::vector<DecodedColumn>& out, std::vector<PreMaterialized>& pre,
            int& row_group_index);

  // Row groups the window will hold in flight (after AutoWindow), for tests/telemetry.
  size_t window() const;

  // The pool's worker count (hardware threads), or the cap `threads` is clamped to.
  static int pool_size();

  // Window for `threads` workers over `ncols` projected columns when each row group
  // decodes to about `rg_bytes`: enough chunks queued for two per worker (min 2 row
  // groups so the consumer overlaps decode), clamped to `budget_bytes` of decoded
  // data (min 1 row group).
  static size_t AutoWindow(int threads, size_t ncols, uint64_t rg_bytes,
                           uint64_t budget_bytes = 256ull << 20);

 private:
  struct State;
  std::shared_ptr<State> st_;
};

}  // namespace rugo
