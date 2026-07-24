// draken/core/trace_bridge.cpp — the ONE compiled home of the shared
// execution tracer's state (see trace_bridge_c.h for why this file exists at
// all, and docs/EXECUTION_TRACING_DESIGN.md for the tracing design).
//
// Implements draken/core/trace.hpp's primitives against real state living
// HERE, in draken.draken_native (draken_native.so — loaded RTLD_GLOBAL by
// draken/__init__.py), and exposes them via extern "C" for every other .so
// (src/cpp/engine/trace.hpp, rugo/src/parquet/io_pipeline.hpp) to call
// through instead of each compiling — and getting its own independent copy
// of — draken_trace::g_trace_enabled / trace_registry() / etc.

#include "core/trace_bridge_c.h"
#include "core/trace.hpp"
#include "core/alloc.h"

#include <cstdio>
#include <cstring>
#include <random>
#include <string>

#include <ifaddrs.h>
#include <net/if.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <net/if_dl.h>
#else
#include <linux/if_packet.h>
#endif

#if defined(__aarch64__) || defined(__arm64__)
#define DRAKEN_TRACE_ARCH "aarch64"
#elif defined(__x86_64__) || defined(__amd64__)
#define DRAKEN_TRACE_ARCH "x86_64"
#elif defined(__riscv)
#define DRAKEN_TRACE_ARCH "riscv"
#else
#define DRAKEN_TRACE_ARCH "unknown"
#endif

namespace {

// Mirrors Python's uuid.getnode(): the 48-bit MAC address of a network
// interface, formatted as hex. Local-only (getifaddrs is a syscall, not a
// network round trip) so it's identical in cost everywhere — dev laptop,
// Cloud Run, CI — unlike a hostname, which Cloud Run's gVisor sandbox always
// reports as "localhost".
uint64_t first_interface_mac() {
    ifaddrs* ifap = nullptr;
    if (getifaddrs(&ifap) != 0) return 0;

    uint64_t node = 0;
    for (ifaddrs* ifa = ifap; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr || (ifa->ifa_flags & IFF_LOOPBACK)) continue;

        const unsigned char* mac = nullptr;
#if defined(__APPLE__)
        if (ifa->ifa_addr->sa_family != AF_LINK) continue;
        auto* sdl = reinterpret_cast<sockaddr_dl*>(ifa->ifa_addr);
        if (sdl->sdl_alen != 6) continue;
        mac = reinterpret_cast<const unsigned char*>(LLADDR(sdl));
#else
        if (ifa->ifa_addr->sa_family != AF_PACKET) continue;
        auto* sll = reinterpret_cast<sockaddr_ll*>(ifa->ifa_addr);
        if (sll->sll_halen != 6) continue;
        mac = sll->sll_addr;
#endif
        bool all_zero = true;
        for (int i = 0; i < 6; ++i) {
            if (mac[i] != 0) { all_zero = false; break; }
        }
        if (all_zero) continue;  // some virtual interfaces report a zero MAC

        for (int i = 0; i < 6; ++i) node = (node << 8) | mac[i];
        break;
    }
    freeifaddrs(ifap);
    return node;
}

// uuid.getnode()'s own fallback when no real MAC is found: a random 48-bit
// number with the multicast bit set, per RFC 4122, so it can never collide
// with (and be mistaken for) a real hardware address.
uint64_t random_node_id() {
    std::random_device rd;
    std::uniform_int_distribution<uint64_t> dist(0, (1ULL << 48) - 1);
    return dist(rd) | 0x010000000000ULL;
}

std::string node_id_hex() {
    uint64_t node = first_interface_mac();
    if (node == 0) node = random_node_id();
    char hex[16];
    std::snprintf(hex, sizeof(hex), "%llx", static_cast<unsigned long long>(node));
    return hex;
}

}  // namespace

extern "C" {

void draken_trace_set_enabled(int on) {
    draken_trace::trace_set_enabled(on != 0);
}

int draken_trace_enabled(void) {
    return draken_trace::trace_enabled() ? 1 : 0;
}

uint32_t draken_trace_start_query(void) {
    return draken_trace::trace_start_query();
}

uint32_t draken_trace_current_query_seq(void) {
    return draken_trace::trace_current_query_seq();
}

uint32_t draken_trace_next_corr_id(void) {
    return draken_trace::trace_next_corr_id();
}

uint64_t draken_trace_now_ns(void) {
    return draken_trace::trace_now_ns();
}

const char* draken_trace_host_info(void) {
    static const std::string info =
        std::string("arch=") + DRAKEN_TRACE_ARCH + ";host=" + node_id_hex();
    return info.c_str();
}

void draken_trace_record(uint16_t category, uint32_t node_id, uint32_t corr_id,
                          uint32_t rg_idx, uint16_t worker_id,
                          uint64_t t_start_ns, uint64_t t_end_ns,
                          uint32_t rows, uint32_t bytes, uint32_t detail,
                          uint32_t file_id) {
    draken_trace::trace_record(category, node_id, corr_id, rg_idx,
                                static_cast<int>(worker_id), t_start_ns, t_end_ns,
                                rows, bytes, detail, file_id);
}

uint32_t draken_trace_intern_file(const char* path, size_t len) {
    if (path == nullptr) return 0;
    return draken_trace::trace_intern_file(std::string(path, len));
}

DrakenFileSymbolC* draken_trace_drain_file_symbols(size_t* out_count) {
    std::vector<std::pair<uint32_t, std::string>> syms = draken_trace::trace_file_symbols();
    if (out_count) *out_count = 0;
    if (syms.empty()) return nullptr;
    auto* out = static_cast<DrakenFileSymbolC*>(
        draken_malloc(syms.size() * sizeof(DrakenFileSymbolC)));
    if (out == nullptr) return nullptr;
    for (size_t i = 0; i < syms.size(); ++i) {
        out[i].file_id = syms[i].first;
        const std::string& p = syms[i].second;
        char* buf = static_cast<char*>(draken_malloc(p.size() + 1));
        if (buf == nullptr) {
            // Unwind what's been allocated so far rather than leak on partial failure.
            for (size_t j = 0; j < i; ++j) draken_free(out[j].path);
            draken_free(out);
            return nullptr;
        }
        std::memcpy(buf, p.data(), p.size());
        buf[p.size()] = '\0';
        out[i].path = buf;
    }
    if (out_count) *out_count = syms.size();
    return out;
}

DrakenTraceSpanC* draken_trace_drain(uint32_t query_seq, size_t* out_count, int* out_truncated) {
    static_assert(sizeof(DrakenTraceSpanC) == sizeof(draken_trace::TraceSpan),
                  "bridge span layout must match draken_trace::TraceSpan exactly");
    draken_trace::DrainResult r = draken_trace::trace_drain(query_seq);
    if (out_truncated) *out_truncated = r.truncated ? 1 : 0;
    if (r.spans.empty()) {
        if (out_count) *out_count = 0;
        return nullptr;
    }
    auto* out = static_cast<DrakenTraceSpanC*>(
        draken_malloc(r.spans.size() * sizeof(DrakenTraceSpanC)));
    if (out == nullptr) {
        if (out_count) *out_count = 0;
        return nullptr;
    }
    std::memcpy(out, r.spans.data(), r.spans.size() * sizeof(DrakenTraceSpanC));
    if (out_count) *out_count = r.spans.size();
    return out;
}

}  // extern "C"
