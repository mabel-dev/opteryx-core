#pragma once
// One worker pool shared by rugo's writers: Parquet column-chunk encoding
// (_parquet_writer.hpp) and CSV / JSONL row rendering (_text_render.hpp).
//
// Sized once to the core count and reused by every write, so a multi-morsel
// export does not spawn a fresh set of threads per morsel. submit_task is
// mutex-guarded, so concurrent writes from different caller threads share the
// pool correctly. A writer task never submits to this pool and waits, so a
// caller is never one of its own workers.

#include "BS_thread_pool.hpp"   // vendored bshoshany pool (same as the readers)

#include <thread>

namespace rugo_writer {

inline size_t pool_width() {
  static const size_t width = [] {
    size_t hw = std::thread::hardware_concurrency();
    return hw == 0 ? (size_t)1 : hw;
  }();
  return width;
}

inline BS::thread_pool<> &pool() {
  static BS::thread_pool<> p(pool_width()); // magic-static: init once
  return p;
}

} // namespace rugo_writer
