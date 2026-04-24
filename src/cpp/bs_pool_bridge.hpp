/**
 * Bridge between BS::thread_pool and Python concurrent.futures
 *
 * Provides utilities to submit tasks to BS::thread_pool and extract results
 * into Python concurrent.futures.Future objects with minimal GIL overhead.
 */

#ifndef BS_POOL_BRIDGE_HPP
#define BS_POOL_BRIDGE_HPP

#include <thread>
#include <queue>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include "../../../third_party/bshoshany/BS_thread_pool.hpp"

/**
 * Thread-safe result container for storing task results.
 */
class ResultContainer {
private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    PyObject* py_future_;
    PyObject* result_;
    PyObject* exception_;
    bool ready_;

public:
    ResultContainer(PyObject* py_future)
        : py_future_(py_future), result_(nullptr), exception_(nullptr), ready_(false) {
        Py_INCREF(py_future_);
    }

    ~ResultContainer() {
        Py_XDECREF(py_future_);
        Py_XDECREF(result_);
        Py_XDECREF(exception_);
    }

    /**
     * Set the result (call from worker thread, no GIL needed).
     */
    void set_result(PyObject* result) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!ready_) {
                result_ = result;
                Py_XINCREF(result_);
                ready_ = true;
            } else {
                Py_XDECREF(result);
            }
        }
        cv_.notify_one();
    }

    /**
     * Set exception (call from worker thread, no GIL needed).
     */
    void set_exception(PyObject* exc) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!ready_) {
                exception_ = exc;
                Py_XINCREF(exc);
                ready_ = true;
            } else {
                Py_XDECREF(exc);
            }
        }
        cv_.notify_one();
    }

    /**
     * Wait for result to be ready (blocking).
     */
    void wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return ready_; });
    }

    /**
     * Notify the Python future with the result (call with GIL held).
     */
    void notify_python_future() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (ready_ && py_future_) {
            if (exception_) {
                PyObject_CallMethod(py_future_, "set_exception", "O", exception_);
            } else if (result_) {
                PyObject_CallMethod(py_future_, "set_result", "O", result_);
            }
            PyErr_Clear();  // Clear any errors from the method call
        }
    }
};

/**
 * Wrapper for task execution that bridges to Python futures.
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
        Py_INCREF(callable_);
        Py_INCREF(args_);
        Py_XINCREF(kwargs_);
    }

    ~TaskWrapper() {
        Py_DECREF(callable_);
        Py_DECREF(args_);
        Py_XDECREF(kwargs_);
    }

    /**
     * Execute the task and store result (called in worker thread).
     * This method MUST NOT hold the GIL during execution.
     */
    void operator()() {
        // Call the Python function (requires GIL)
        PyGILState_STATE gstate = PyGILState_Ensure();
        try {
            PyObject* result = nullptr;
            if (kwargs_) {
                result = PyObject_Call(callable_, args_, kwargs_);
            } else {
                result = PyObject_CallObject(callable_, args_);
            }

            if (result) {
                container_->set_result(result);
                Py_DECREF(result);
            } else {
                PyObject* exc = PyErr_Occurred();
                if (exc) {
                    container_->set_exception(exc);
                    PyErr_Clear();
                }
            }
        } catch (const std::exception& e) {
            PyObject* exc_type = PyExc_RuntimeError;
            PyErr_SetString(exc_type, e.what());
            PyObject* exc = PyErr_Occurred();
            if (exc) {
                container_->set_exception(exc);
            }
            PyErr_Clear();
        } catch (...) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown exception in task wrapper");
            PyObject* exc = PyErr_Occurred();
            if (exc) {
                container_->set_exception(exc);
            }
            PyErr_Clear();
        }

        PyGILState_Release(gstate);
    }
};

/**
 * Wrapper for BS::thread_pool that provides Python-friendly interface.
 * Uses BS::light_thread_pool (BS::thread_pool<BS::tp::none>) for simplicity.
 */
class BSThreadPoolBridge {
private:
    std::shared_ptr<BS::light_thread_pool> pool_;
    std::string name_;
    int max_workers_;

public:
    BSThreadPoolBridge(int max_workers, const std::string& name)
        : pool_(std::make_shared<BS::light_thread_pool>(max_workers)),
          name_(name),
          max_workers_(max_workers) {}

    /**
     * Submit a task to the thread pool and return a Python future.
     *
     * This method creates a Python concurrent.futures.Future, submits the task
     * to BS::thread_pool, and returns the future to the caller. The task will
     * update the future with its result when complete.
     *
     * Must be called with GIL held.
     */
    PyObject* submit(PyObject* callable, PyObject* args, PyObject* kwargs) {
        // Import concurrent.futures.Future
        PyObject* futures_module = PyImport_ImportModule("concurrent.futures");
        if (!futures_module) {
            return nullptr;
        }

        PyObject* Future = PyObject_GetAttrString(futures_module, "Future");
        Py_DECREF(futures_module);
        if (!Future) {
            return nullptr;
        }

        // Create a Python Future
        PyObject* py_future = PyObject_CallObject(Future, nullptr);
        Py_DECREF(Future);
        if (!py_future) {
            return nullptr;
        }

        // Create result container
        auto container = std::make_shared<ResultContainer>(py_future);

        // Wrap in shared_ptr so the lambda can be moved into the pool's queue
        // without invalidating PyObject* references (TaskWrapper owns them).
        auto task_ptr = std::make_shared<TaskWrapper>(container, callable, args, kwargs);

        // Submit to thread pool (detach — result tracked via ResultContainer/Future)
        try {
            pool_->detach_task([task_ptr]() { (*task_ptr)(); });
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            Py_DECREF(py_future);
            return nullptr;
        }

        // Return the Python future to caller
        return py_future;
    }

    /**
     * Shutdown the thread pool.
     */
    void shutdown(bool wait = true) {
        if (pool_) {
            if (wait) {
                pool_->wait();
            }
            pool_.reset();
        }
    }

    /**
     * Get pool name.
     */
    const std::string& name() const {
        return name_;
    }

    /**
     * Get max workers.
     */
    int max_workers() const {
        return max_workers_;
    }

    /**
     * Get number of queued tasks.
     */
    size_t get_queued_tasks_count() const {
        if (pool_) {
            return pool_->get_tasks_queued();
        }
        return 0;
    }

    /**
     * Get number of running tasks.
     */
    size_t get_running_tasks_count() const {
        if (pool_) {
            return pool_->get_tasks_running();
        }
        return 0;
    }
};

#endif // BS_POOL_BRIDGE_HPP
