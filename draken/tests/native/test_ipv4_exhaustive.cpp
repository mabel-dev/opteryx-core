// Exhaustive check of draken/core/ipv4.h. Takes a couple of minutes, so it is a
// dev tool run on demand, not part of `make q` — the pytest twin next door
// (test_ipv4_paths.py) covers the same rules on a sample.
//
//   c++ -O2 -std=c++17 -I<repo root> draken/tests/native/test_ipv4_exhaustive.cpp \
//       -o /tmp/ipv4_exhaustive && /tmp/ipv4_exhaustive
//
// Two things need exhausting, and neither is reachable from Python at this size:
//
//   1. format() / text_length() over ALL 2^32 addresses. The width rule is
//      written twice (thresholds in text_length, a table in format) because the
//      two spellings measure differently on x86 and ARM, so nothing but a full
//      sweep proves they cannot disagree. The rendered text is parsed back in
//      the same pass, which also pins the round trip.
//
//   2. parse()'s vector path against detail::parse_scalar over a large
//      structured corpus. The scalar path is the reference implementation — it
//      is the strictness rules written the obvious way — and the vector path is
//      only allowed to be faster, never different, on ANY input, accepted or
//      rejected.

#include "draken/core/ipv4.h"

#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>

namespace {

long long g_cases = 0;
long long g_failures = 0;

void report(const char* what, const std::string& input) {
    if (++g_failures <= 20)
        std::fprintf(stderr, "FAIL %s on '%s'\n", what, input.c_str());
}

// One input through both parse paths. They must agree bit for bit.
void check_parse(const std::string& s) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(s.data());
    const uint32_t n = static_cast<uint32_t>(s.size());
    uint32_t vector_value = 0xAAAAAAAAu;
    uint32_t scalar_value = 0xBBBBBBBBu;
    const bool vector_ok = draken::ipv4::parse(p, n, &vector_value);
    // The scalar reference has the same [7, 15] precondition parse() applies
    // before it reaches either path.
    const bool scalar_ok =
        (n - draken::ipv4::MIN_TEXT_LENGTH
             <= draken::ipv4::MAX_TEXT_LENGTH - draken::ipv4::MIN_TEXT_LENGTH)
        && draken::ipv4::detail::parse_scalar(p, n, &scalar_value);
    ++g_cases;
    if (vector_ok != scalar_ok || (vector_ok && vector_value != scalar_value))
        report("vector/scalar parse disagree", s);
}

}  // namespace

int main() {
    // ---- 1. every address, rendered, measured and parsed back ----------------
    std::fprintf(stderr, "sweeping all 2^32 addresses...\n");
    for (uint64_t v = 0; v <= 0xFFFFFFFFull; ++v) {
        const uint32_t value = static_cast<uint32_t>(v);
        char text[draken::ipv4::FORMAT_SCRATCH_BYTES];
        const uint32_t written = draken::ipv4::format(value, text);
        if (written != draken::ipv4::text_length(value)) {
            report("text_length != format", std::string(text, written));
            continue;
        }
        if (written < draken::ipv4::MIN_TEXT_LENGTH || written > draken::ipv4::MAX_TEXT_LENGTH) {
            report("rendered width out of range", std::string(text, written));
            continue;
        }
        uint32_t back = 0u;
        if (!draken::ipv4::parse(reinterpret_cast<const uint8_t*>(text), written, &back)
            || back != value)
            report("round trip", std::string(text, written));
    }

    // ---- 2. vector vs scalar parse over a structured corpus -----------------
    std::fprintf(stderr, "differential parse corpus...\n");
    std::mt19937 rng(20260822);
    for (int i = 0; i < 2000000; ++i) {
        char text[draken::ipv4::FORMAT_SCRATCH_BYTES];
        const uint32_t n = draken::ipv4::format(static_cast<uint32_t>(rng()), text);
        check_parse(std::string(text, n));
    }

    // Every octet-value combination that sits on a width or range boundary.
    const int edge[] = {0, 1, 9, 10, 11, 99, 100, 101, 199, 200, 249, 255, 256, 300, 999};
    char scratch[64];
    for (int a : edge) for (int b : edge) for (int c : edge) for (int d : edge) {
        std::snprintf(scratch, sizeof(scratch), "%d.%d.%d.%d", a, b, c, d);
        check_parse(scratch);
    }

    // Mutations of valid text: substitution, insertion and deletion at every
    // position, which is where dot counts and octet widths go wrong.
    const char* seeds[] = {"192.168.1.1", "0.0.0.0", "255.255.255.255", "1.2.3.4",
                           "10.0.0.1", "9.99.199.255", "100.10.1.0", "01.2.3.4",
                           "1.2.3", "1.2.3.4.5", "1..2.3", "1.2.3.4 ", "10.1"};
    for (const char* seed : seeds) {
        const std::string base(seed);
        for (size_t i = 0; i < base.size(); ++i) {
            for (int ch = 0; ch < 256; ++ch) {
                std::string m = base;
                m[i] = static_cast<char>(ch);
                check_parse(m);
            }
            std::string deleted = base;
            deleted.erase(i, 1);
            check_parse(deleted);
        }
        for (size_t i = 0; i <= base.size(); ++i)
            for (const char* ins : {".", "0", "9", " ", "/", "\t"})
                check_parse(std::string(base).insert(i, ins));
    }

    // Random byte soup at every length either path will look at.
    const char alphabet[] = "0123456789..;/ a";
    for (int i = 0; i < 4000000; ++i) {
        std::string s;
        const int len = static_cast<int>(rng() % 21u);
        for (int k = 0; k < len; ++k) s.push_back(alphabet[rng() % (sizeof(alphabet) - 1)]);
        check_parse(s);
    }

    std::fprintf(stderr, "ipv4 exhaustive: 2^32 render/measure/round-trip + %lld parse cases, %lld failures\n",
                 g_cases, g_failures);
    return g_failures == 0 ? 0 : 1;
}
