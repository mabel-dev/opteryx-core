// AppendBuffer<T> — a growable buffer of trivially copyable T over
// draken_malloc'd bytes. THE buffer type for appended/decoded POD data in
// draken, rugo and the engine (it replaced rugo's ScratchBuffer and
// std::vector on those paths).
//
// It differs from std::vector exactly where std::vector costs:
//   * no value-initialisation: growth never writes the new slots. There is NO
//     `resize` — the two ways to grow are explicit: `resize_uninit(n)` (new
//     slots uninitialised, for the caller to overwrite) and `resize_fill(n, v)`
//     (new slots = v). std::vector::resize's silent zero-fill of bytes the next
//     memcpy overwrites was the measured cost (GROUP BY key store, 2026-10-06);
//   * growth through draken_realloc: the allocator may extend the block in
//     place or remap its pages instead of allocate + copy + free;
//   * growth is out of line, so the append fast path is one compare and a bump.
// Every other member has std::vector's semantics, under std::vector's name.
//
// T must be trivially copyable: elements move by realloc, never by constructor.
// Freed with draken_free. Move-only — a copy is a deliberate `assign`.
#ifndef DRAKEN_CORE_APPEND_BUFFER_H
#define DRAKEN_CORE_APPEND_BUFFER_H

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <initializer_list>
#include <new>
#include <type_traits>

#include "alloc.h"

namespace draken {

template <typename T>
class AppendBuffer {
    static_assert(std::is_trivially_copyable_v<T>,
                  "AppendBuffer moves elements by realloc: T must be trivially copyable");

  public:
    using value_type = T;

    AppendBuffer() noexcept = default;
    ~AppendBuffer() { draken_free(data_); }

    AppendBuffer(const AppendBuffer&) = delete;
    AppendBuffer& operator=(const AppendBuffer&) = delete;

    AppendBuffer(AppendBuffer&& o) noexcept : data_(o.data_), size_(o.size_), cap_(o.cap_) {
        o.data_ = nullptr; o.size_ = 0; o.cap_ = 0;
    }
    AppendBuffer& operator=(AppendBuffer&& o) noexcept {
        if (this != &o) {
            draken_free(data_);
            data_ = o.data_; size_ = o.size_; cap_ = o.cap_;
            o.data_ = nullptr; o.size_ = 0; o.cap_ = 0;
        }
        return *this;
    }

    // Append n uninitialised slots; returns a pointer to the first. The pointer
    // is valid until the next growth (growth may move the block).
    inline T* extend(size_t n) {
        if (cap_ - size_ < n) grow(size_ + n);
        T* p = data_ + size_;
        size_ += n;
        return p;
    }

    inline void push_back(const T& v) {
        if (size_ == cap_) grow(size_ + 1);
        data_[size_++] = v;
    }

    inline void append(const T* p, size_t n) {
        if (n != 0) std::memcpy(static_cast<void*>(extend(n)), p, n * sizeof(T));
    }

    // Replace the contents with [first, last).
    void assign(const T* first, const T* last) {
        size_ = 0;
        append(first, static_cast<size_t>(last - first));
    }

    AppendBuffer& operator=(std::initializer_list<T> il) {
        assign(il.begin(), il.end());
        return *this;
    }

    // Size to n. Slots past the old size are UNINITIALISED; shrinking keeps capacity.
    inline void resize_uninit(size_t n) {
        if (n > cap_) grow(n);
        size_ = n;
    }

    // Size to n. Slots past the old size are set to v; shrinking keeps capacity.
    void resize_fill(size_t n, const T& v) {
        const size_t old = size_;
        resize_uninit(n);
        if (n > old) std::fill(data_ + old, data_ + n, v);
    }

    // Capacity to at least n (exactly n when it grows), contents unchanged.
    void reserve(size_t n) {
        if (n > cap_) realloc_to(n);
    }

    void clear() noexcept { size_ = 0; }

    const T* data() const noexcept { return data_; }
    T* data() noexcept { return data_; }
    size_t size() const noexcept { return size_; }
    size_t capacity() const noexcept { return cap_; }
    bool empty() const noexcept { return size_ == 0; }
    const T& operator[](size_t i) const noexcept { return data_[i]; }
    T& operator[](size_t i) noexcept { return data_[i]; }
    const T& back() const noexcept { return data_[size_ - 1]; }
    T& back() noexcept { return data_[size_ - 1]; }
    const T* begin() const noexcept { return data_; }
    const T* end() const noexcept { return data_ + size_; }
    T* begin() noexcept { return data_; }
    T* end() noexcept { return data_ + size_; }

  private:
    // Geometric: at least double, at least what is needed, never below a floor
    // that keeps the first few appends from each reallocating.
    [[gnu::noinline]] void grow(size_t need) {
        static constexpr size_t kMinBytes = 256;
        size_t cap = cap_ * 2;
        if (cap < need) cap = need;
        if (cap * sizeof(T) < kMinBytes) cap = (kMinBytes + sizeof(T) - 1) / sizeof(T);
        realloc_to(cap);
    }

    void realloc_to(size_t cap) {
        if (size_ == 0) {
            // Nothing to keep. realloc would copy the whole old block — stale
            // contents of a cleared, reused buffer — into the new one (measured:
            // +7% cache misses on x86, ClickBench Q02, 2026-10-06).
            draken_free(data_);
            data_ = nullptr;
            cap_ = 0;
            T* p = static_cast<T*>(draken_malloc(cap * sizeof(T)));
            if (p == nullptr) throw std::bad_alloc();
            data_ = p;
            cap_ = cap;
            return;
        }
        T* p = static_cast<T*>(draken_realloc(data_, cap * sizeof(T)));
        if (p == nullptr) throw std::bad_alloc();
        data_ = p;
        cap_ = cap;
    }

    T* data_ = nullptr;
    size_t size_ = 0;
    size_t cap_ = 0;
};

}  // namespace draken

#endif  // DRAKEN_CORE_APPEND_BUFFER_H
