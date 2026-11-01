// Replaces the grpc/absl/gpr headers the vendored ALTS record layer used
// (see README.md). Only what these files reference is defined.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <type_traits>
#include <vector>

#include <sys/uio.h>  // struct iovec

// gsec.h declares its own struct iovec unless the platform's is in use.
#ifndef GRPC_EVENT_ENGINE_POSIX
#define GRPC_EVENT_ENGINE_POSIX
#endif

// grpc/status.h — values are the gRPC status codes.
typedef enum {
    GRPC_STATUS_OK = 0,
    GRPC_STATUS_INVALID_ARGUMENT = 3,
    GRPC_STATUS_FAILED_PRECONDITION = 9,
    GRPC_STATUS_INTERNAL = 13,
} grpc_status_code;

// grpc/support/alloc.h
inline void* gpr_malloc(size_t n) {
    void* p = std::malloc(n);
    if (p == nullptr) std::abort();
    return p;
}
inline void* gpr_zalloc(size_t n) {
    void* p = std::calloc(1, n);
    if (p == nullptr) std::abort();
    return p;
}
inline void gpr_free(void* p) { std::free(p); }

namespace grpc_alts {

// absl::Span, reduced to the members the vendored files use.
template <typename T>
class Span {
public:
    Span() = default;
    Span(T* p, size_t n) : p_(p), n_(n) {}
    template <typename C, typename = std::enable_if_t<std::is_convertible_v<decltype(std::declval<C&>().data()), T*>>>
    Span(C&& c) : p_(c.data()), n_(c.size()) {}  // NOLINT: implicit, as absl::Span
    T* data() const { return p_; }
    size_t size() const { return n_; }
    T* begin() const { return p_; }
    T* end() const { return p_ + n_; }

private:
    T* p_ = nullptr;
    size_t n_ = 0;
};

template <typename T>
Span<T> MakeSpan(std::vector<T>& v) { return Span<T>(v.data(), v.size()); }

}  // namespace grpc_alts
