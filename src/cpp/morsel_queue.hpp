#pragma once
// MorselQueue — bounded, blocking, multi-producer / single-consumer hand-off of
// `shared_ptr<CxxMorsel>` between scheduler worker threads (producers) and the
// result consumer. Slice 1 of the native execution scheduler rewrite (see
// docs/NATIVE_SCHEDULER_REWRITE_DESIGN.md §4).
//
// Ownership: the queue holds `std::shared_ptr<CxxMorsel>` BY VALUE — C++ shared
// ownership, never a PyObject. A morsel is freed when its last shared_ptr drops.
// No Python object ever sits on this queue (the old pyobject_queue hack is gone).
//
// Backpressure: moodycamel's MPMC `ConcurrentQueue` is natively UNBOUNDED, so the
// bound is enforced by a producer-side counting semaphore (`slots_`) initialised
// to `capacity`: acquire a slot before enqueue, release one after dequeue. The
// blocking *dequeue* (consumer waits when empty) is moodycamel's own. Both waits
// are timed so `close()` is observed promptly without sentinels.
//
// close(): idempotent. Marks the queue closed, drops every queued morsel (freeing
// it) and releases slots so blocked producers wake and return false. Covers the
// LIMIT / early-abandon path the same way MorselRef drains on destruction.
//
// ORDERING: moodycamel guarantees FIFO only WITHIN a single producer token (i.e.
// per producer thread), not globally across producers. The scheduler relies on
// this exactly: the terminal hand-off has ONE producer thread, so its stream is
// FIFO; the per-shape fan-out has N producers whose morsels are order-independent
// (concatenated downstream). Result ordering (ORDER BY) is an operator concern,
// never the queue's — do not assume a global order across producers here.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "concurrentqueue.h"
#include "blockingconcurrentqueue.h"
#include "lightweightsemaphore.h"
#include "morsels/cxx_morsel.h"

class MorselQueue {
  public:
    explicit MorselQueue(std::size_t capacity)
        : slots_(static_cast<ssize_t>(capacity)), capacity_(capacity) {}

    // Producer. Blocks while the queue is full; returns false if the queue is (or
    // becomes) closed — the caller's `shared_ptr` then drops the morsel. The
    // morsel is moved in only once a slot is genuinely held and the queue is open.
    bool put(std::shared_ptr<CxxMorsel> m) noexcept {
        while (!slots_.wait(kWaitUs)) {
            if (closed_.load(std::memory_order_acquire)) return false;
        }
        if (closed_.load(std::memory_order_acquire)) {
            slots_.signal();  // hand the slot back; nothing was enqueued
            return false;
        }
        q_.enqueue(std::move(m));
        return true;
    }

    // Consumer. Blocks until a morsel is available; returns false once the queue
    // is closed AND drained. On success releases the producer slot the morsel held.
    bool get(std::shared_ptr<CxxMorsel>& out) noexcept {
        for (;;) {
            if (q_.wait_dequeue_timed(out, kWaitUs)) {
                slots_.signal();
                return true;
            }
            if (closed_.load(std::memory_order_acquire) && q_.size_approx() == 0) {
                return false;
            }
        }
    }

    // Idempotent. Marks closed, drops every queued morsel (freeing it via its
    // shared_ptr) and releases the slots they held so blocked producers wake.
    void close() noexcept {
        closed_.store(true, std::memory_order_release);
        std::shared_ptr<CxxMorsel> tmp;
        while (q_.try_dequeue(tmp)) {
            slots_.signal();
            tmp.reset();
        }
    }

    bool closed() const noexcept { return closed_.load(std::memory_order_acquire); }
    std::size_t capacity() const noexcept { return capacity_; }
    std::size_t size_approx() const noexcept { return q_.size_approx(); }

  private:
    // Poll granularity for observing close() out of a blocking wait. Morsels are
    // coarse (thousands of rows), so a 1 ms close-latency ceiling is immaterial to
    // throughput and avoids sentinel bookkeeping.
    static constexpr std::int64_t kWaitUs = 1000;

    moodycamel::BlockingConcurrentQueue<std::shared_ptr<CxxMorsel>> q_;
    moodycamel::LightweightSemaphore slots_;
    std::atomic<bool> closed_{false};
    std::size_t capacity_;
};
