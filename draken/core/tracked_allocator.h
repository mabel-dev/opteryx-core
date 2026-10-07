// STL allocators that charge the process memory account (mem_account.h).
//
// For the containers that hold peak query memory and do not go through
// draken_malloc — the GROUP BY partition arrays and the carchar index
// (docs/C7_PAGE_CACHE_DESIGN.md §21). Requests of at least
// DRAKEN_MEM_ACCOUNT_MIN bytes are charged on allocate and uncharged on
// deallocate; the STL passes the same n to both, so the pair is exact.
//
// Two flavours, because swapping a container's allocator must not change what
// its elements hold:
//   TrackedAllocator<T>        — std::allocator semantics: resize() value-
//                                initialises (zero for trivial T).
//   TrackedUninitAllocator<T>  — default-initialises on no-argument construct
//                                (no zero fill), for containers that already
//                                used an uninitialised allocator (carchar).
#pragma once

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>
#include <vector>

#include "mem_account.h"

namespace draken {

template <typename T>
struct TrackedAllocator {
    using value_type = T;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::true_type;

    TrackedAllocator() noexcept = default;
    template <typename U>
    TrackedAllocator(const TrackedAllocator<U>&) noexcept {}

    T* allocate(std::size_t n) {
        const std::size_t bytes = n * sizeof(T);
        T* p = std::allocator<T>{}.allocate(n);
        if (static_cast<int64_t>(bytes) >= DRAKEN_MEM_ACCOUNT_MIN)
            draken_mem_charge(static_cast<int64_t>(bytes));
        return p;
    }

    void deallocate(T* p, std::size_t n) noexcept {
        const std::size_t bytes = n * sizeof(T);
        if (static_cast<int64_t>(bytes) >= DRAKEN_MEM_ACCOUNT_MIN)
            draken_mem_uncharge(static_cast<int64_t>(bytes));
        std::allocator<T>{}.deallocate(p, n);
    }

    template <typename U>
    bool operator==(const TrackedAllocator<U>&) const noexcept { return true; }
    template <typename U>
    bool operator!=(const TrackedAllocator<U>&) const noexcept { return false; }
};

template <typename T>
struct TrackedUninitAllocator : TrackedAllocator<T> {
    using value_type = T;
    template <typename U>
    struct rebind {
        using other = TrackedUninitAllocator<U>;
    };

    TrackedUninitAllocator() noexcept = default;
    template <typename U>
    TrackedUninitAllocator(const TrackedUninitAllocator<U>&) noexcept {}

    template <typename U>
    void construct(U* ptr) noexcept(std::is_nothrow_default_constructible_v<U>) {
        ::new (static_cast<void*>(ptr)) U;   // default-init: no fill for trivial U
    }

    template <typename U, typename... Args>
    void construct(U* ptr, Args&&... args) {
        ::new (static_cast<void*>(ptr)) U(std::forward<Args>(args)...);
    }
};

template <typename T>
using tracked_vector = std::vector<T, TrackedAllocator<T>>;

}  // namespace draken
