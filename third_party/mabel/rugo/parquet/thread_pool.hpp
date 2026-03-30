#pragma once

#include <functional>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <memory>
#include <atomic>

// Simple thread pool for page-level parallelism in parquet decoding.
//
// Design: Work queue with bounded thread pool.
// - Task push via queue (protected by mutex)
// - Blocking wait for all tasks to complete
// - Thread-safe destruction (joins all worker threads)
//
// Usage:
//   PageDecodePool pool(4);  // Create 4 worker threads
//   for (const auto& page : pages) {
//       pool.push_task([&]() { decode_single_page(page); });
//   }
//   pool.wait_for_tasks();  // Block until all complete
//
// Note: This is a functional implementation. For production, consider:
// - BS::thread_pool for work-stealing and NUMA awareness
// - Lock-free queue to reduce contention
// - Better work distribution heuristics

class SimpleThreadPool {
private:
    using Task = std::function<void()>;

    std::vector<std::thread> workers;
    std::queue<Task> task_queue;
    std::mutex queue_mutex;
    std::condition_variable cv_work;
    std::condition_variable cv_done;
    std::atomic<bool> shutdown{false};
    std::atomic<int> active_tasks{0};

    void worker_loop() {
        while (!shutdown) {
            Task task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                cv_work.wait(lock, [this]() { return !task_queue.empty() || shutdown; });
                if (shutdown && task_queue.empty()) break;
                if (!task_queue.empty()) {
                    task = std::move(task_queue.front());
                    task_queue.pop();
                    active_tasks++;
                }
            }
            if (task) {
                task();
                if (--active_tasks == 0) {
                    cv_done.notify_all();
                }
            }
        }
    }

public:
    explicit SimpleThreadPool(size_t num_threads) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers.emplace_back([this]() { worker_loop(); });
        }
    }

    ~SimpleThreadPool() {
        shutdown = true;
        cv_work.notify_all();
        for (auto& worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

    // Push a task to the work queue
    void push_task(Task&& task) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            task_queue.push(std::move(task));
        }
        cv_work.notify_one();
    }

    // Block until all tasks are complete and no tasks are queued
    void wait_for_tasks() {
        std::unique_lock<std::mutex> lock(queue_mutex);
        cv_done.wait(lock, [this]() { return task_queue.empty() && active_tasks == 0; });
    }

    size_t num_workers() const { return workers.size(); }
};

// Alias for easier adoption
using PageDecodePool = SimpleThreadPool;

// ---------------------------------------------------------------------------
// Module-level pool for page-parallel decoding.
//
// Created once on first use (lazy init via call_once); never destroyed.
// Avoids the expensive thread creation/join overhead of per-call pools.
// Thread count: all hardware threads (no artificial cap).
// ---------------------------------------------------------------------------
namespace rugo_pool {

inline PageDecodePool& get_page_decode_pool() {
    static std::once_flag init_flag;
    static PageDecodePool* pool = nullptr;
    std::call_once(init_flag, []() {
        int hw = (int)std::thread::hardware_concurrency();
        int num_threads = (hw > 0) ? hw : 4;
        pool = new PageDecodePool((size_t)num_threads);
    });
    return *pool;
}

} // namespace rugo_pool
