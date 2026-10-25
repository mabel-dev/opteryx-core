// Reference gate for the text the CSV / JSONL / Avro writers print for a double or
// float (fmt_double / fmt_float / wr_double / wr_float / wr_double_sci in
// value_format.hpp; digits from dragonbox).
//
// The oracle is independent of dragonbox and of value_format.hpp's layout code:
// the C++ standard library's std::to_chars (shortest round-trip, scientific) gives
// the digits, and ref_layout below, a separate and deliberately plain
// implementation of Python's repr() layout rules, turns them into text. Nothing the
// code under test produced is trusted as the reference. (to_chars was itself
// checked byte-for-byte against ryu over 301M doubles and floats before ryu's
// shortest-digit code was removed.)
//
//   float_text_test              default sample (runs inside `make q`)
//   float_text_test --exhaustive every one of the 2^32 float32 bit patterns, on
//                                all cores, plus a far larger double sample
//
// Built and run by `make float-text-test`.

#include <atomic>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "interop/value_format.hpp"

using namespace rugo_text;

namespace {

struct Mix {  // splitmix64
  uint64_t s;
  uint64_t next() {
    uint64_t z = (s += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
  }
};

std::atomic<uint64_t> g_checked{0};
std::atomic<uint64_t> g_bad{0};

void report(const char *what, uint64_t bits, const std::string &want, const std::string &got) {
  if (g_bad.fetch_add(1) < 10)
    std::fprintf(stderr, "MISMATCH %s bits=0x%llx want='%s' got='%s'\n", what,
                 (unsigned long long)bits, want.c_str(), got.c_str());
}

// Python-style layout of shortest digits: fixed for -4 <= exp < 16, otherwise
// scientific with a lowercase e and a signed exponent of at least two digits.
std::string ref_layout(bool neg, const std::string &digits, int exp) {
  const int nd = (int)digits.size();
  std::string out = neg ? "-" : "";
  if (exp >= -4 && exp < 16) {
    if (exp < 0) {
      out += "0." + std::string(-exp - 1, '0') + digits;
    } else if (nd <= exp + 1) {
      out += digits + std::string(exp + 1 - nd, '0') + ".0";
    } else {
      out += digits.substr(0, exp + 1) + "." + digits.substr(exp + 1);
    }
  } else {
    out += digits.substr(0, 1);
    if (nd > 1) out += "." + digits.substr(1);
    char e[16];
    std::snprintf(e, sizeof e, "e%c%02d", exp < 0 ? '-' : '+', exp < 0 ? -exp : exp);
    out += e;
  }
  return out;
}

// "[-]d[.ddd]e[+-]XX" (std::to_chars, scientific) -> sign, digit run, exponent.
// Digits keep no trailing zeros except the lone "0" of zero.
struct Sci { bool neg; std::string digits; int exp; };
Sci parse_sci(const char *first, const char *last) {
  Sci r{false, "", 0};
  const char *p = first;
  if (*p == '-') { r.neg = true; p++; }
  for (; p < last && *p != 'e'; p++)
    if (*p != '.') r.digits.push_back(*p);
  r.exp = std::atoi(p + 1);
  return r;
}

template <class T>
Sci oracle_sci(T v) {
  char t[48];
  const auto res = std::to_chars(t, t + sizeof t, v, std::chars_format::scientific);
  if (res.ec != std::errc()) { std::fprintf(stderr, "to_chars failed\n"); std::abort(); }
  *res.ptr = '\0';  // to_chars does not terminate; parse_sci reads the exponent with atoi
  return parse_sci(t, res.ptr);
}

template <class T>
std::string oracle_text(T v) {
  const Sci s = oracle_sci(v);
  return ref_layout(s.neg, s.digits, s.exp);
}

// The scientific text CAST prints beyond fixed notation: "[-]D[.DDD]E[-]X".
std::string oracle_sci_text(double v) {
  const Sci s = oracle_sci(v);
  std::string out = s.neg ? "-" : "";
  out += s.digits.substr(0, 1);
  if (s.digits.size() > 1) out += "." + s.digits.substr(1);
  out += "E" + std::to_string(s.exp);
  return out;
}

void check_double(double v) {
  if (!std::isfinite(v)) return;
  g_checked.fetch_add(1, std::memory_order_relaxed);
  uint64_t bits;
  std::memcpy(&bits, &v, 8);
  const std::string want = oracle_text(v);
  std::string got;
  fmt_double(got, v);
  if (got != want) { report("double fmt_double", bits, want, got); return; }
  char buf[80];
  char *end = wr_double(buf, v);
  const std::string inplace(buf, end - buf);
  if (inplace != want) report("double wr_double", bits, want, inplace);
  if (v != 0.0) {
    const std::string want_sci = oracle_sci_text(v);
    const std::string sci(buf, wr_double_sci(buf, v) - buf);
    if (sci != want_sci) report("double wr_double_sci", bits, want_sci, sci);
  }
}

void check_float(float v) {
  if (!std::isfinite(v)) return;
  g_checked.fetch_add(1, std::memory_order_relaxed);
  uint32_t bits;
  std::memcpy(&bits, &v, 4);
  const std::string want = oracle_text(v);
  std::string got;
  fmt_float(got, v);
  if (got != want) { report("float fmt_float", bits, want, got); return; }
  char buf[80];
  char *end = wr_float(buf, v);
  const std::string inplace(buf, end - buf);
  if (inplace != want) report("float wr_float", bits, want, inplace);
}

// NaN and the infinities have no decimal text; every caller renders them as
// null / empty first, so reaching the formatters with one must fail loudly.
void check_non_finite_throws() {
  char buf[80];
  const double bad[] = {std::nan(""), HUGE_VAL, -HUGE_VAL};
  for (double v : bad) {
    int thrown = 0;
    std::string s;
    try { fmt_double(s, v); } catch (const std::domain_error &) { thrown++; }
    try { wr_double(buf, v); } catch (const std::domain_error &) { thrown++; }
    try { wr_double_sci(buf, v); } catch (const std::domain_error &) { thrown++; }
    try { fmt_float(s, (float)v); } catch (const std::domain_error &) { thrown++; }
    try { wr_float(buf, (float)v); } catch (const std::domain_error &) { thrown++; }
    if (thrown != 5) report("non-finite did not throw", 0, "5 throws", std::to_string(thrown));
  }
}

double from_bits(uint64_t b) { double d; std::memcpy(&d, &b, 8); return d; }
float from_bits32(uint32_t b) { float f; std::memcpy(&f, &b, 4); return f; }

void structured_doubles() {
  // Every binary exponent, at the mantissa corners and a spread between.
  Mix r{11};
  for (uint64_t e = 0; e < 0x7FF; e++)
    for (uint64_t m : {0ull, 1ull, 2ull, 0xFFFFFFFFFFFFFull, 0xFFFFFFFFFFFFEull, 0x8000000000000ull,
                       r.next() & 0xFFFFFFFFFFFFFull, r.next() & 0xFFFFFFFFFFFFFull})
      for (uint64_t sign : {0ull, 1ull})
        check_double(from_bits((sign << 63) | (e << 52) | m));
  // Every whole number up to 2^20, and a run across the 2^53 boundary where the
  // small-integer path hands over to dragonbox.
  for (int64_t i = -(1 << 20); i <= (1 << 20); i++) check_double((double)i);
  for (int64_t i = (1ll << 53) - 4096; i <= (1ll << 53) + 4096; i++) check_double((double)i);
  for (int p = 0; p < 63; p++)
    for (int64_t d = -3; d <= 3; d++) check_double((double)(((int64_t)1 << p) + d));
  // Powers of ten and their neighbours (the shortest-digit boundary cases).
  for (int p = -323; p <= 308; p++) {
    char t[32];
    std::snprintf(t, sizeof t, "1e%d", p);
    const double v = std::strtod(t, nullptr);
    uint64_t b;
    std::memcpy(&b, &v, 8);
    for (int d = -3; d <= 3; d++) check_double(from_bits(b + d));
  }
  // x.5, x.25 and small decimals.
  for (int i = 0; i < 200000; i++) {
    check_double(i + 0.5);
    check_double(i + 0.25);
    check_double(i / 100.0);
    check_double(i / 1000.0);
  }
}

void sampled_doubles(uint64_t n) {
  Mix r{12345};
  for (uint64_t i = 0; i < n; i++) {
    check_double(from_bits(r.next()));  // any finite bit pattern
    const double u = (double)(r.next() >> 11) * (1.0 / 9007199254740992.0);
    check_double(u * 1000);                         // full-precision
    check_double(std::round(u * 100000) / 1000);    // 3-decimal (rugo.dev's floats)
    check_double(std::round(u * 1e7) / 100);        // 2-decimal prices
    check_double((double)(int64_t)(r.next() >> (11 + (r.next() % 52))));  // whole numbers, any width
    check_double(u * 1e-300);
    check_double(u * 1e300);
  }
}

void floats(uint64_t first, uint64_t last, uint64_t stride) {
  for (uint64_t b = first; b < last; b += stride) check_float(from_bits32((uint32_t)b));
}

}  // namespace

int main(int argc, char **argv) {
  const bool exhaustive = argc > 1 && std::strcmp(argv[1], "--exhaustive") == 0;

  check_non_finite_throws();
  structured_doubles();
  sampled_doubles(exhaustive ? 100000000ull : 1000000ull);

  if (exhaustive) {
    const unsigned workers = std::max(1u, std::thread::hardware_concurrency());
    const uint64_t total = 1ull << 32;
    std::vector<std::thread> pool;
    for (unsigned w = 0; w < workers; w++)
      pool.emplace_back([=] { floats(total * w / workers, total * (w + 1) / workers, 1); });
    for (std::thread &t : pool) t.join();
  } else {
    // Prime stride: every exponent and mantissa-bit position, ~8M patterns.
    floats(0, 1ull << 32, 521);
    // All whole floats up to 2^24 and every float exponent's corners.
    for (int64_t i = -(1 << 24); i <= (1 << 24); i += 3) check_float((float)i);
    for (uint32_t e = 0; e < 0xFF; e++)
      for (uint32_t m : {0u, 1u, 2u, 0x7FFFFFu, 0x7FFFFEu, 0x400000u})
        for (uint32_t s : {0u, 1u}) check_float(from_bits32((s << 31) | (e << 23) | m));
    Mix r{99};
    for (int i = 0; i < 2000000; i++) {
      const float u = (float)(r.next() >> 40) * (1.0f / 16777216.0f);
      check_float(u * 1000);
      check_float(std::round(u * 100000) / 1000);
    }
  }

  std::printf("float_text_test: %llu values checked, %llu mismatches%s\n",
              (unsigned long long)g_checked.load(), (unsigned long long)g_bad.load(),
              exhaustive ? " (exhaustive float32)" : "");
  return g_bad.load() == 0 ? 0 : 1;
}
