// parallel_read.cpp — see parallel_read.hpp for the design.

#include "parallel_read.hpp"
#include "thread_pool.hpp"
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <stdexcept>

namespace rugo {

namespace {

// One row group in flight: its decoded columns and how many are still outstanding.
struct Slot {
  std::vector<DecodedColumn> cols;
  std::vector<PreMaterialized> pre;
  size_t remaining = 0;
  // The failure of the LOWEST-indexed failing column, so the message matches what
  // the serial loop (which stops at the first failing column) would have raised.
  size_t error_col = static_cast<size_t>(-1);
  std::string error;
};

}  // namespace

struct ParallelRowGroupReader::State {
  const uint8_t* data = nullptr;
  size_t size = 0;
  std::vector<std::string> names;
  const std::vector<RowGroupStats>* row_groups = nullptr;
  std::vector<int> idx;     // row groups to yield, in order
  size_t ncols = 0;
  size_t npos = 0;          // idx.size()
  size_t window = 1;
  int threads = 1;
  bool premat = true;

  std::mutex mu;
  std::condition_variable cv_consumer;  // a slot completed
  std::condition_variable cv_idle;      // decoding reached zero
  std::vector<std::unique_ptr<Slot>> slots;
  size_t next_task = 0;     // next unclaimed chunk: pos * ncols + col
  size_t consumed = 0;      // positions already yielded
  int runners = 0;          // queued + running pool tasks
  int decoding = 0;         // chunks inside DecodeColumnFromMemory
  bool cancelled = false;
  // Lowest position holding a failed chunk. Nothing beyond it is claimed, but its
  // own row group is finished so the lowest failing column can be reported.
  size_t failed_pos = static_cast<size_t>(-1);

  size_t total_tasks() const { return npos * ncols; }

  // Unclaimed chunks inside the window. Caller holds mu.
  size_t claimable() const {
    if (cancelled) return 0;
    size_t limit = std::min(total_tasks(), (consumed + window) * ncols);
    if (failed_pos != static_cast<size_t>(-1))
      limit = std::min(limit, (failed_pos + 1) * ncols);
    return limit > next_task ? limit - next_task : 0;
  }

  // Start runners for claimable work, up to the thread budget. Caller holds mu.
  void kick(const std::shared_ptr<State>& self) {
    size_t want = std::min<size_t>(claimable(), static_cast<size_t>(threads));
    while (runners < threads && want > 0) {
      ++runners;
      --want;
      rugo_pool::get_read_pool().push_task([self]() { run(self); });
    }
  }

  static void run(std::shared_ptr<State> st) {
    State& s = *st;
    std::unique_lock<std::mutex> lk(s.mu);
    for (;;) {
      if (s.claimable() == 0) {
        --s.runners;
        return;
      }
      const size_t t = s.next_task++;
      const size_t pos = t / s.ncols, col = t % s.ncols;
      if (!s.slots[pos]) {
        s.slots[pos] = std::make_unique<Slot>();
        s.slots[pos]->cols.resize(s.ncols);
        s.slots[pos]->pre.resize(s.ncols);
        s.slots[pos]->remaining = s.ncols;
      }
      ++s.decoding;
      lk.unlock();

      const int rg = s.idx[pos];
      DecodedColumn dc;
      PreMaterialized pm;
      std::string err;
      try {
        dc = DecodeColumnFromMemory(s.data, s.size, s.names[col],
                                    (*s.row_groups)[rg], rg);
        // A column absent from this row group is the one non-result the serial path
        // tolerates; any other failure is a hard error with its reason.
        if (!dc.success && !dc.absent)
          err = dc.error_message.empty() ? std::string("decode failed (no reason recorded)")
                                         : dc.error_message;
        // Scatter into Draken buffers here, off the consumer. Only for a column whose
        // row count the footer vouches for; a materializer that throws is dropped and
        // the consumer redoes the column, so the error reaches Python with its own type.
        if (err.empty() && s.premat && !dc.absent &&
            dc.num_rows > 0 && dc.num_rows == (*s.row_groups)[rg].num_rows) {
          try {
            pm.valid = rugo::_parquet::materialize_scalar(dc, dc.num_rows, pm.mc);
            pm.rows = dc.num_rows;
          } catch (...) {
            pm.valid = false;
          }
          if (pm.valid) {
            // The consumer needs only the scalars of a prematerialized column (absent,
            // success, num_rows, logical kind); drop the decoded buffers now so a row
            // group in the window holds one copy of its data, not two.
            DecodedColumn stub;
            static_cast<DecodedColumnMeta&>(stub) = static_cast<DecodedColumnMeta&>(dc);
            stub.type = dc.type;
            stub.logical_type = dc.logical_type;
            dc = std::move(stub);
          }
        }
      } catch (const std::exception& e) {
        err = e.what();
      } catch (...) {
        err = "unknown error decoding the column";
      }

      lk.lock();
      Slot& slot = *s.slots[pos];
      if (err.empty()) {
        slot.cols[col] = std::move(dc);
        slot.pre[col] = std::move(pm);
      } else {
        if (col < slot.error_col) {
          slot.error_col = col;
          slot.error = "row group " + std::to_string(rg) + ", column '" + s.names[col] +
                       "': " + err;
        }
        if (pos < s.failed_pos) s.failed_pos = pos;
      }
      --s.decoding;
      if (--slot.remaining == 0) s.cv_consumer.notify_all();
      if (s.cancelled && s.decoding == 0) s.cv_idle.notify_all();
    }
  }
};

int ParallelRowGroupReader::pool_size() {
  return static_cast<int>(rugo_pool::get_read_pool().num_workers());
}

size_t ParallelRowGroupReader::AutoWindow(int threads, size_t ncols, uint64_t rg_bytes,
                                          uint64_t budget_bytes) {
  const size_t t = static_cast<size_t>(std::max(1, threads));
  const size_t c = std::max<size_t>(1, ncols);
  const size_t by_tasks = std::max<size_t>(2, (2 * t + c - 1) / c);
  const size_t by_bytes =
      rg_bytes == 0 ? by_tasks
                    : static_cast<size_t>(std::min<uint64_t>(budget_bytes / rg_bytes, by_tasks));
  return std::min(by_tasks, std::max<size_t>(1, by_bytes));
}

ParallelRowGroupReader::ParallelRowGroupReader(
    const uint8_t* data, size_t size, const std::vector<std::string>& column_names,
    const std::vector<RowGroupStats>& row_groups, std::vector<int> rg_indices,
    int threads, size_t window_rgs, bool prematerialize)
    : st_(std::make_shared<State>()) {
  State& s = *st_;
  s.data = data;
  s.size = size;
  s.names = column_names;
  s.row_groups = &row_groups;
  s.idx = std::move(rg_indices);
  s.ncols = s.names.size();
  s.npos = s.idx.size();
  s.threads = std::max(1, std::min(threads, pool_size()));
  s.premat = prematerialize;
  s.slots.resize(s.npos);

  if (window_rgs == 0) {
    // Decoded bytes per row group, from the footer's uncompressed chunk sizes:
    // the largest of up to four row groups spread across the selection.
    uint64_t rg_bytes = 0;
    const size_t samples = std::min<size_t>(4, s.npos);
    for (size_t k = 0; k < samples; ++k) {
      const RowGroupStats& rg = row_groups[s.idx[k * s.npos / samples]];
      uint64_t b = 0;
      for (const std::string& n : s.names)
        for (const ColumnStats& c : rg.columns)
          if (c.name == n) {
            if (c.total_uncompressed_size > 0) b += static_cast<uint64_t>(c.total_uncompressed_size);
            break;
          }
      rg_bytes = std::max(rg_bytes, b);
    }
    window_rgs = AutoWindow(s.threads, s.ncols, rg_bytes);
  }
  s.window = std::max<size_t>(1, window_rgs);

  std::lock_guard<std::mutex> lk(s.mu);
  s.kick(st_);
}

ParallelRowGroupReader::~ParallelRowGroupReader() {
  State& s = *st_;
  std::unique_lock<std::mutex> lk(s.mu);
  s.cancelled = true;
  s.cv_idle.wait(lk, [&s] { return s.decoding == 0; });
}

size_t ParallelRowGroupReader::window() const { return st_->window; }

bool ParallelRowGroupReader::next(std::vector<DecodedColumn>& out,
                                  std::vector<PreMaterialized>& pre, int& row_group_index) {
  State& s = *st_;
  std::unique_lock<std::mutex> lk(s.mu);
  if (s.consumed >= s.npos) return false;
  const size_t pos = s.consumed;
  row_group_index = s.idx[pos];
  out.clear();
  pre.clear();

  if (s.ncols > 0) {
    s.cv_consumer.wait(lk, [&] { return s.slots[pos] && s.slots[pos]->remaining == 0; });
    Slot& slot = *s.slots[pos];
    if (!slot.error.empty()) {
      s.cancelled = true;
      throw std::runtime_error(slot.error);
    }
    out = std::move(slot.cols);
    pre = std::move(slot.pre);
    s.slots[pos].reset();
  }
  ++s.consumed;
  s.kick(st_);
  return true;
}

}  // namespace rugo
