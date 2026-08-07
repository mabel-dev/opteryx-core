#pragma once
// Minimal C++ test harness — no framework, no dependency.
//
// A failing check prints the file, line, expression and both values, then keeps
// going so one run reports every failure rather than only the first. main()
// returns non-zero if anything failed, which is what CTest reads.

#include <cstdint>
#include <cstdio>
#include <string>

namespace skene_test {

inline int g_failures = 0;
inline int g_checks = 0;

inline void report(const char* file, int line, const char* expr,
                   const std::string& detail) {
    ++g_failures;
    std::fprintf(stderr, "FAIL %s:%d\n  %s\n", file, line, expr);
    if (!detail.empty()) std::fprintf(stderr, "  %s\n", detail.c_str());
}

inline int summary(const char* suite) {
    std::fprintf(stderr, "%s: %d checks, %d failures\n", suite, g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

}  // namespace skene_test

#define CHECK(cond)                                                        \
    do {                                                                   \
        ++::skene_test::g_checks;                                          \
        if (!(cond)) ::skene_test::report(__FILE__, __LINE__, #cond, "");  \
    } while (0)

#define CHECK_EQ(a, b)                                                     \
    do {                                                                   \
        ++::skene_test::g_checks;                                          \
        auto _a = (a);                                                     \
        auto _b = (b);                                                     \
        if (!(_a == _b)) {                                                 \
            ::skene_test::report(__FILE__, __LINE__, #a " == " #b,         \
                "left=" + std::to_string(_a) + " right=" + std::to_string(_b)); \
        }                                                                  \
    } while (0)
