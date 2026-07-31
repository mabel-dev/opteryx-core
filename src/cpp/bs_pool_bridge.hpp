/**
 * Bridge between BS::thread_pool and Python concurrent.futures
 *
 * Provides utilities to submit tasks to BS::thread_pool and extract results
 * into Python concurrent.futures.Future objects with minimal GIL overhead.
 *
 * GIL contract (all rules are strict):
 *   - Python C API (including Py_INCREF/DECREF) requires the GIL.
 *   - BS::thread_pool workers are NOT Python threads; they use
 *     PyGILState_Ensure/Release to borrow the GIL for Python work.
 *   - All destructors that touch PyObject* must acquire the GIL themselves,
 *     because they may fire from worker threads after GIL has been released.
 *   - notify_python_future() nulls its PyObject* members after use so that
 *     the destructor is a safe no-op.
 *
 * Exception handling (Python >= 3.12):
 *   - PyErr_GetRaisedException() returns the exception *instance* (new ref)
 *     and clears the indicator atomically.  This preserves the message and
 *     traceback on the Python Future, unlike the old PyErr_Occurred() (type only).
 */

#ifndef BS_POOL_BRIDGE_HPP
#define BS_POOL_BRIDGE_HPP

#include <thread>
#include <queue>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include "BS_thread_pool.hpp"

// Gap #3 Phase 2b: a plain (non-template) name for this specific instantiation, so
// Cython's `cdef extern from` (which declares TYPE-parameterised cppclasses, not
// non-type/enum-value template parameters like BS::tp::priority) can reference it
// as `shared_ptr[PriorityPool]`. Identical type to BS::thread_pool<BS::tp::priority>
// wherever that's spelled out directly (e.g. rugo's io_pipeline.hpp) — a `using`
// alias, not a distinct type; no cast needed crossing between the two spellings.
using PriorityPool = BS::thread_pool<BS::tp::priority>;


/**
 * Thread-safe result container: holds the result or exception until the Python
 * Future is notified.  All Python object references are managed with GIL held.
 */
class ResultContainer {
private:
    mutable std::mutex mutex_;
    PyObject* py_future_;
    PyObject* result_;
    PyObject* exception_;
    bool ready_;

public:
    ResultContainer(PyObject* py_future)
        : py_future_(py_future), result_(nullptr), exception_(nullptr), ready_(false) {
        // Called with GIL held (from BSThreadPoolBridge::submit).
        Py_INCREF(py_future_);
    }

    ~ResultContainer() {
        // May be called from a BS worker thread after PyGILState_Release.
        // Acquire GIL to safely decrement any remaining Python references.
        if (py_future_ || result_ || exception_) {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_XDECREF(py_future_);
            Py_XDECREF(result_);
            Py_XDECREF(exception_);
            PyGILState_Release(gstate);
        }
    }

    /**
     * Store the task result.  Called with GIL held.
     */
    void set_result(PyObject* result) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ready_) {
            result_ = result;
            Py_XINCREF(result_);
            ready_ = true;
        }
    }

    /**
     * Store the task exception.  Called with GIL held.
     * Expects an exception *instance* (new reference — caller must Py_DECREF after).
     */
    void set_exception(PyObject* exc) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ready_) {
            exception_ = exc;
            Py_XINCREF(exception_);
            ready_ = true;
        }
    }

    /**
     * Resolve the Python Future.  Must be called with GIL held.
     * Nulls all PyObject* members so the destructor is a no-op.
     */
    void notify_python_future() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!ready_ || !py_future_) {
            return;
        }
        // Format string is "(O)", NOT "O": PyObject_CallMethod builds its argument
        // tuple via Py_BuildValue, which for a SINGLE format unit returns the
        // converted object itself rather than a 1-tuple containing it (documented
        // Py_BuildValue behaviour). With plain "O", a result_/exception_ that is
        // itself a tuple (e.g. the common `return idx, value` task shape) is then
        // used AS the call's argument tuple, so its elements are unpacked as
        // separate positional arguments — set_result(*result_) instead of
        // set_result(result_) — raising a silently-swallowed TypeError below and
        // leaving the Future PENDING forever. The parens force Py_BuildValue to
        // always wrap in a genuine 1-tuple regardless of what result_/exception_
        // is. (Found via a hung concurrent.futures.Future when the pooled task's
        // return value was a tuple.)
        if (exception_) {
            PyObject_CallMethod(py_future_, "set_exception", "(O)", exception_);
        } else if (result_) {
            PyObject_CallMethod(py_future_, "set_result", "(O)", result_);
        }
        PyErr_Clear();  // Swallow any error from the set_* call itself.

        // Null out after use so ~ResultContainer is a no-op for these refs.
        Py_XDECREF(py_future_);   py_future_  = nullptr;
        Py_XDECREF(result_);      result_     = nullptr;
        Py_XDECREF(exception_);   exception_  = nullptr;
    }
};


/**
 * Wraps a single Python callable + arguments for deferred execution on a
 * BS::thread_pool worker.
 *
 * Ownership: constructor increments all PyObject* refs (called with GIL held).
 * Cleanup:   operator() nulls refs while still holding the GIL so that the
 *            destructor (which fires without GIL on the worker thread) is safe.
 *            If operator() was never called (e.g., pool shut down before task
 *            ran), the destructor re-acquires the GIL to clean up.
 */
class TaskWrapper {
private:
    std::shared_ptr<ResultContainer> container_;
    PyObject* callable_;
    PyObject* args_;
    PyObject* kwargs_;

public:
    TaskWrapper(std::shared_ptr<ResultContainer> container, PyObject* callable,
                PyObject* args, PyObject* kwargs)
        : container_(container), callable_(callable), args_(args), kwargs_(kwargs) {
        // Called with GIL held from BSThreadPoolBridge::submit.
        Py_INCREF(callable_);
        Py_INCREF(args_);
        Py_XINCREF(kwargs_);
    }

    ~TaskWrapper() {
        // operator() nulls the pointers after use, making this a no-op in the
        // common case.  If they are still set (pool shutdown before task ran),
        // we must acquire the GIL to safely decrement.
        if (callable_) {
            PyGILState_STATE gstate = PyGILState_Ensure();
            Py_DECREF(callable_);
            Py_DECREF(args_);
            Py_XDECREF(kwargs_);
            PyGILState_Release(gstate);
        }
    }

    /**
     * Execute the task.  Called by a BS::thread_pool worker thread.
     *
     * Acquires the GIL for all Python work, then releases it.  Python object
     * refs are nulled before the GIL is released so the destructor (which runs
     * without the GIL) touches no Python state.
     */
    void operator()() {
        PyGILState_STATE gstate = PyGILState_Ensure();

        // --- Run the callable --------------------------------------------------
        PyObject* result = nullptr;
        if (kwargs_) {
            result = PyObject_Call(callable_, args_, kwargs_);
        } else {
            result = PyObject_CallObject(callable_, args_);
        }

        // --- Record result or exception ----------------------------------------
        if (result) {
            container_->set_result(result);
            Py_DECREF(result);
            container_->notify_python_future();
        } else {
            // PyErr_GetRaisedException: Python >= 3.12.
            // Returns exception *instance* (new ref) and clears indicator.
            PyObject* exc = PyErr_GetRaisedException();
            if (exc) {
                container_->set_exception(exc);
                Py_DECREF(exc);   // balance the new ref we own
                container_->notify_python_future();
            }
        }

        // --- Release Python refs while GIL is still held -----------------------
        // Nulling prevents ~TaskWrapper() from calling Py_DECREF without GIL.
        Py_DECREF(callable_);   callable_ = nullptr;
        Py_DECREF(args_);       args_     = nullptr;
        Py_XDECREF(kwargs_);    kwargs_   = nullptr;

        PyGILState_Release(gstate);
    }
};


/**
 * Wrapper for BS::thread_pool that provides Python-compatible task submission.
 *
 * Each call to submit() creates a concurrent.futures.Future, dispatches the
 * task to BS::thread_pool, and returns the Future to the caller.  The worker
 * resolves the Future via notify_python_future() once the callable completes.
 */
class BSThreadPoolBridge {
private:
    // Gap #3 Phase 2b: priority-capable (same vendored template as before,
    // light_thread_pool IS thread_pool<tp::none> — see BS_thread_pool.hpp), so the
    // execution engine's pool can be handed to a scan (ParquetIOPipeline's injecting
    // constructor, rugo/src/parquet/io_pipeline.hpp) and share one CPU budget instead
    // of running two uncoordinated pools. Decode tasks submit at BS::pr::high there;
    // everything submitted through this bridge (aggregate/sort/join/etc.) defaults to
    // BS::pr::normal, unchanged from today's effective FIFO ordering.
    std::shared_ptr<PriorityPool> pool_;
    std::string name_;
    int max_workers_;

public:
    BSThreadPoolBridge(int max_workers, const std::string& name)
        : pool_(std::make_shared<PriorityPool>(max_workers)),
          name_(name),
          max_workers_(max_workers) {}

    /**
     * Submit a Python callable to the thread pool.
     *
     * Must be called with GIL held.
     * Returns a new reference to a concurrent.futures.Future.
     * Returns NULL and sets a Python exception on failure.
     */
    PyObject* submit(PyObject* callable, PyObject* args, PyObject* kwargs) {
        // Import is cached by Python's import system after the first call.
        PyObject* futures_module = PyImport_ImportModule("concurrent.futures");
        if (!futures_module) {
            return nullptr;
        }
        PyObject* Future = PyObject_GetAttrString(futures_module, "Future");
        Py_DECREF(futures_module);
        if (!Future) {
            return nullptr;
        }
        PyObject* py_future = PyObject_CallObject(Future, nullptr);
        Py_DECREF(Future);
        if (!py_future) {
            return nullptr;
        }

        auto container = std::make_shared<ResultContainer>(py_future);
        auto task_ptr  = std::make_shared<TaskWrapper>(container, callable, args, kwargs);

        try {
            pool_->detach_task([task_ptr]() { (*task_ptr)(); });
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            Py_DECREF(py_future);
            return nullptr;
        }

        // Return new reference (the one we got from PyObject_CallObject).
        return py_future;
    }

    /**
     * Submit a NATIVE task: a C function pointer + opaque arg, run on a pool
     * worker with NO Python callable and NO Future. The bridge touches no Python
     * state here — the task body owns its own GIL policy (in free-threaded 3.14t
     * there is no global lock to contend). The caller owns synchronisation:
     * submit N tasks, then `wait_native()` (or `shutdown`) to barrier. This is the
     * native-worker-drive path — the per-morsel drive runs without a Python
     * callable bouncing through `ResultContainer`/`TaskWrapper`.
     */
    void submit_native(void (*fn)(void*), void* arg, BS::priority_t priority = BS::pr::normal) {
        pool_->detach_task([fn, arg]() { fn(arg); }, priority);
    }

    /**
     * Gap #3 Phase 2b: expose the underlying pool so it can be shared with a
     * scan (ParquetIOPipeline's injecting constructor). Caller must not outlive
     * this bridge's own pool teardown — the returned shared_ptr keeps the pool
     * alive independently, but submitting to it after this bridge shuts down
     * races with nothing since BS::thread_pool::detach_task after destruction
     * is undefined; callers are expected to stop submitting once the query that
     * owns this bridge completes, same lifetime discipline as every other use
     * of this pool today.
     */
    std::shared_ptr<PriorityPool> pool_handle() const {
        return pool_;
    }

    /**
     * Block until all queued tasks complete WITHOUT tearing the pool down (unlike
     * `shutdown`), so the pool can be reused for a second native fan-out (e.g. the
     * HASH_REPARTITION read-out after the accumulate pass). In 3.14t no GIL is
     * held here.
     */
    void wait_native() {
        if (pool_) {
            pool_->wait();
        }
    }

    /**
     * Wait for all queued tasks to complete, then destroy the pool.
     * Release the GIL while waiting so worker threads can finish.
     */
    void shutdown(bool wait = true) {
        if (pool_) {
            if (wait) {
                pool_->wait();
            }
            pool_.reset();
        }
    }

    int max_workers() const {
        return max_workers_;
    }

    const std::string& name() const {
        return name_;
    }
};

/**
 * Spawn ONE detached native task on its own OS thread — deliberately NOT a
 * BS::thread_pool task. Used for a coordinator/"driver" task that itself submits
 * further native tasks to a *shared* BSThreadPoolBridge and blocks on wait_native():
 * submitting that driver as a task ON the same pool it then recurses into corrupts
 * the pool's internal task queue (BS::thread_pool's task submission is not meant to
 * be re-entered from a thread that is itself one of the pool's own workers — this
 * was reproduced as a real SIGSEGV inside BS::move_only_function's placement-new).
 * A single ad-hoc thread here is safe under free-threaded CPython (the concurrent
 * new-thread-state deadlock this whole bridge exists to avoid is a multi-thread
 * pile-up, not a lone thread attaching alone).
 */
inline void spawn_detached_native_task(void (*fn)(void*), void* arg) {
    std::thread(fn, arg).detach();
}

#endif // BS_POOL_BRIDGE_HPP
