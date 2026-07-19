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
//
// finish() / graceful end-of-data: NOT an in-band sentinel. An in-band NULL-morsel
// sentinel (the original design) is enqueued via the SAME per-calling-thread
// sub-queue mechanism as data — if `finish()` is called from a DIFFERENT thread
// than the one(s) that wrote the data (e.g. a driver thread calling finish() after
// joining worker threads that did the writing), moodycamel's cross-producer
// ordering gives NO guarantee the sentinel is dequeued after all real data — found
// as a genuine, reproducible row-loss bug under slow-consumer backpressure (real
// production diagnosis, not a synthetic concern). FIX: finish() increments an
// out-of-band atomic counter; get() only ever reports a finish event when the
// queue is GENUINELY data-empty (size_approx()==0) at the moment of the check —
// never competing with real data sitting in a different producer's sub-queue. This
// is safe because finish() must only be called once its producer's writes are
// PROVABLY complete (e.g. after std::thread::join()) — by the time finish() runs,
// that producer can never enqueue again, so there's no race between "did I miss a
// concurrent write" the way the size_approx() "approximate" caveat would otherwise
// suggest. Supports BOTH single-producer-finishes-once and N-producers-each-
// finish-independently callers (each finish() call yields exactly one FINISHED
// event from get(), order of finish() calls is not guaranteed — matches the
// existing "N producers' morsels are order-independent" contract above).

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

    enum class Status { DATA, FINISHED, ABANDONED };

    // Consumer. Blocks until a morsel is available, a finish() event is reported
    // (queue genuinely data-empty, see the finish()/Status doc above), or the queue
    // is closed-and-drained (ABANDONED). On DATA, releases the producer slot the
    // morsel held.
    Status get(std::shared_ptr<CxxMorsel>& out) noexcept {
        for (;;) {
            if (q_.wait_dequeue_timed(out, kWaitUs)) {
                slots_.signal();
                return Status::DATA;
            }
            if (closed_.load(std::memory_order_acquire) && q_.size_approx() == 0) {
                return Status::ABANDONED;
            }
            long long seen = finish_count_.load(std::memory_order_acquire);
            if (seen > 0 && q_.size_approx() == 0) {
                // CAS-claim one signal (single-consumer by design; defensive against
                // a future multi-consumer caller racing for the same signal).
                long long expected = seen;
                if (finish_count_.compare_exchange_strong(
                        expected, seen - 1, std::memory_order_acq_rel)) {
                    return Status::FINISHED;
                }
                // lost the CAS — another get() claimed it; loop and re-check.
            }
        }
    }

    // Signal ONE producer's graceful completion. Safe to call once (single
    // producer) or N times (N independent producers, each finishing on its own
    // thread) — see the class-level doc for why this is safe regardless of which
    // thread wrote the data.
    void finish() noexcept {
        finish_count_.fetch_add(1, std::memory_order_acq_rel);
        // Monotonic "finish() ever happened" flag + wakeup, for wait_finished().
        // Kept separate from finish_count_ (which get() decrements as it claims
        // FINISHED events) so wait_finished() sees the driver's terminal signal even
        // on paths where get() consumed the count first.
        finished_ever_.store(true, std::memory_order_release);
        finish_sem_.signal();
    }

    // Block until finish() has been called at least once (ever). The result consumer
    // uses this ONLY on the early-abandon path: after close() fast-stops the producer,
    // it waits here for the detached driver to observe the closed sink, unwind
    // eng.run() (every worker joined), and call finish() — so teardown never races a
    // still-running driver. On the normal path the consumer instead observes FINISHED
    // from get(), which is itself proof the driver is done, and never calls this.
    // Timed poll (self-healing); returns immediately if finish() already happened.
    void wait_finished() noexcept {
        while (!finished_ever_.load(std::memory_order_acquire)) {
            finish_sem_.wait(kWaitUs);
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
    // Poll granularity for observing close()/finish() out of a blocking wait.
    // finish() is out-of-band (an atomic, not an enqueue — see the class doc for
    // why), so it CANNOT wake a consumer parked in q_.wait_dequeue_timed(); the
    // consumer only notices FINISHED after the current wait times out. That makes
    // this value a completion-latency FLOOR paid once per query: the final get()
    // (which returns FINISHED after the last data morsel) always sleeps out one
    // full kWaitUs. On a large result that 1 ms was amortised to nothing, but on a
    // metadata-only / tiny query it WAS the whole query — the dominant term in the
    // ~2 ms fixed floor measured across the odata_dashboard + ClickBench cheap
    // queries. Dropped 1000 → 50 µs: ~20x less end-of-query dead wait, at the cost
    // of a genuinely-idle consumer re-checking close()/finish() 20x more often
    // (each check is an atomic load + size_approx() — nanoseconds, and only while
    // actually blocked with no data). Morsels are coarse enough that 50 µs is still
    // immaterial as a throughput ceiling. The real fix (a shared wakeup channel so
    // finish() has zero poll latency) is tracked separately.
    static constexpr std::int64_t kWaitUs = 50;

    moodycamel::BlockingConcurrentQueue<std::shared_ptr<CxxMorsel>> q_;
    moodycamel::LightweightSemaphore slots_;
    moodycamel::LightweightSemaphore finish_sem_;
    std::atomic<bool> closed_{false};
    std::atomic<long long> finish_count_{0};
    std::atomic<bool> finished_ever_{false};
    std::size_t capacity_;
};
