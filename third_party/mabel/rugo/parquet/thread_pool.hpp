#pragma once
// Minimal fixed-size thread pool with a work-stealing-free FIFO queue.
//
// Workers are created once at construction and live until destruction.
// Task submission is a queue-push + condvar notify — no OS thread creation
// per task, unlike std::async(std::launch::async).
//
// Usage:
//   ThreadPool pool(4);
//   auto fut = pool.submit([]() -> DecodedColumn { ... });
//   DecodedColumn col = fut.get();

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
public:
    explicit ThreadPool(std::size_t n) : stop_(false) {
        workers_.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) t.join();
    }

    // Non-copyable, non-movable (threads hold a pointer to *this)
    ThreadPool(const ThreadPool&)            = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Submit a callable, get back a future for its return value.
    template<class F>
    auto submit(F&& f) -> std::future<std::invoke_result_t<F>> {
        using R = std::invoke_result_t<F>;
        auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));
        std::future<R> fut = task->get_future();
        {
            std::unique_lock<std::mutex> lk(mu_);
            tasks_.emplace([task]() { (*task)(); });
        }
        cv_.notify_one();
        return fut;
    }

    std::size_t size() const { return workers_.size(); }

private:
    void worker_loop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            task();
        }
    }

    std::vector<std::thread>          workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex                        mu_;
    std::condition_variable           cv_;
    bool                              stop_;
};

// ---------------------------------------------------------------------------
// Process-lifetime pool accessor.
//
// The pool is created once (on first call) with hardware_concurrency() workers
// and reused for the lifetime of the process.  Workers are idle (sleeping on
// the condvar) when there is no work, so they have zero CPU cost at rest.
//
// Returns nullptr when the caller requests serial (num_threads <= 1).
// ---------------------------------------------------------------------------
inline ThreadPool* rugo_get_pool(int num_threads) {
    if (num_threads <= 1) return nullptr;
    static ThreadPool pool(std::thread::hardware_concurrency()
                               ? std::thread::hardware_concurrency()
                               : 4);
    return &pool;
}
