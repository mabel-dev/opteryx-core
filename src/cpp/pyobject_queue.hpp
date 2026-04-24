/**
 * Lock-free queue for Python objects using moodycamel::ReaderWriterQueue.
 *
 * Handles PyObject* reference counting correctly:
 * - enqueue: INCREFs the object (queue holds a reference)
 * - dequeue: transfers ownership (caller is responsible for DECREF)
 * - destructor: DECREFs all remaining items
 *
 * Single-producer single-consumer (SPSC). Safe for one producer thread and
 * one consumer thread. Do not use with multiple producers or consumers.
 */

#pragma once

#include <cstddef>
#include <Python.h>
#include "../../../third_party/moodycamel/readerwriterqueue.h"

class PyObjectQueue {
 private:
    moodycamel::ReaderWriterQueue<PyObject*> queue_;
    size_t capacity_;

 public:
    explicit PyObjectQueue(size_t capacity = 256)
        : queue_(capacity), capacity_(capacity) {}

    ~PyObjectQueue() {
        // Drain remaining items, releasing references
        PyObject* item = nullptr;
        while (queue_.try_dequeue(item)) {
            Py_XDECREF(item);
        }
    }

    /**
     * Non-blocking enqueue. INCREFs item on success.
     * Returns true if enqueued, false if queue is full.
     * Must be called with GIL held.
     */
    bool try_enqueue(PyObject* item) {
        Py_XINCREF(item);
        if (queue_.try_enqueue(item)) {
            return true;
        }
        Py_XDECREF(item);
        return false;
    }

    /**
     * Non-blocking dequeue. Transfers ownership to caller (caller must DECREF).
     * Returns the item, or NULL if the queue is empty.
     */
    PyObject* try_dequeue() {
        PyObject* item = nullptr;
        if (queue_.try_dequeue(item)) {
            return item;
        }
        return nullptr;
    }

    /** Approximate number of items currently in the queue. */
    size_t size_approx() const {
        return queue_.size_approx();
    }

    size_t capacity() const {
        return capacity_;
    }
};
