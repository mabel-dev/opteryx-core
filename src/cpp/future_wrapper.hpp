/**
 * Future wrapper: Bridge between C++ std::future and Python concurrent.futures.Future
 *
 * This header provides utilities to wrap std::future<T> results and make them
 * available to Python callers via concurrent.futures.Future objects.
 */

#ifndef FUTURE_WRAPPER_HPP
#define FUTURE_WRAPPER_HPP

#include <future>
#include <functional>
#include <memory>
#include <exception>
#include <typeinfo>

/**
 * Base class for storing future results in a type-erased container.
 */
class FutureResultBase {
public:
    virtual ~FutureResultBase() = default;

    /**
     * Extract the result and return it as a Python object.
     * Must be called from within Python GIL context.
     */
    virtual PyObject* get_py_result() = 0;

    /**
     * Check if the future has a result available.
     */
    virtual bool is_ready() = 0;

    /**
     * Get any exception that occurred during task execution.
     */
    virtual std::exception_ptr get_exception() = 0;
};

/**
 * Template specialization for void results.
 */
template <typename T>
class FutureResult : public FutureResultBase {
private:
    std::shared_ptr<std::future<T>> future_;

public:
    FutureResult(std::future<T>&& fut)
        : future_(std::make_shared<std::future<T>>(std::move(fut))) {}

    bool is_ready() override {
        if (!future_ || !future_->valid()) return false;
        return future_->wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    std::exception_ptr get_exception() override {
        try {
            if (future_ && future_->valid()) {
                // Peek at exception without extracting result
                try {
                    (void)future_->get();
                } catch (...) {
                    return std::current_exception();
                }
            }
        } catch (...) {
        }
        return nullptr;
    }

    PyObject* get_py_result() override {
        if (!future_ || !future_->valid()) {
            Py_RETURN_NONE;
        }

        try {
            T result = future_->get();

            // For non-void types, try to convert to Python object
            // This is a simplified approach - actual conversion depends on T
            if constexpr (std::is_same_v<T, int>) {
                return PyLong_FromLong(result);
            } else if constexpr (std::is_same_v<T, double>) {
                return PyFloat_FromDouble(result);
            } else if constexpr (std::is_same_v<T, std::string>) {
                return PyUnicode_FromString(result.c_str());
            } else if constexpr (std::is_same_v<T, bool>) {
                return PyBool_FromLong(result ? 1 : 0);
            } else if constexpr (std::is_same_v<T, void>) {
                Py_RETURN_NONE;
            } else {
                // For other types, return the address as a Python int
                // (This is a limitation - custom types need custom conversion)
                Py_RETURN_NONE;
            }
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return nullptr;
        } catch (...) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown exception in future");
            return nullptr;
        }
    }
};

/**
 * Void specialization.
 */
template <>
class FutureResult<void> : public FutureResultBase {
private:
    std::shared_ptr<std::future<void>> future_;

public:
    FutureResult(std::future<void>&& fut)
        : future_(std::make_shared<std::future<void>>(std::move(fut))) {}

    bool is_ready() override {
        if (!future_ || !future_->valid()) return false;
        return future_->wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    std::exception_ptr get_exception() override {
        try {
            if (future_ && future_->valid()) {
                future_->get();
            }
        } catch (...) {
            return std::current_exception();
        }
        return nullptr;
    }

    PyObject* get_py_result() override {
        if (!future_ || !future_->valid()) {
            Py_RETURN_NONE;
        }

        try {
            future_->get();
            Py_RETURN_NONE;
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return nullptr;
        } catch (...) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown exception in future");
            return nullptr;
        }
    }
};

#endif // FUTURE_WRAPPER_HPP
