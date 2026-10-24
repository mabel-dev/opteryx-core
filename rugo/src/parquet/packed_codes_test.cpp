// packed_codes_test.cpp — DecodeRLEBitPackedCodesPacked against the two-pass
// reference it replaces on all-present pages (docs/PARQUET_PACKED_CODES_ONE_PASS_DESIGN.md).
//
// Two references per case: the values the stream generator wrote (ground truth,
// every bit width), and DecodeRLEBitPackedIndicesNoPrefix into int32 followed by
// the old per-row narrow store (the two-pass path, bit widths <= 31 — at 32 its
// RLE mask `(1U << bit_width) - 1` is an undefined shift, a pre-existing defect
// reported separately). Every case must give byte-identical packed codes, the
// same max code, and the same success/failure verdict.
//
// Coverage: bit widths 0..32, code widths 1/2/4, streams mixing RLE and
// bit-packed runs (partial final groups, runs longer than one 512-value tile,
// num_values cutting a run short), truncated streams, and an invalid code width.

#include "decode_encodings.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <vector>

namespace {

int g_failures = 0;

#define CHECK(cond, ...)                                  \
  do {                                                    \
    if (!(cond)) {                                        \
      std::fprintf(stderr, "FAIL %s:%d: ", __FILE__, __LINE__); \
      std::fprintf(stderr, __VA_ARGS__);                  \
      std::fprintf(stderr, "\n");                         \
      ++g_failures;                                       \
    }                                                     \
  } while (0)

void put_varint(std::vector<uint8_t>& out, uint32_t v) {
  while (v >= 0x80) {
    out.push_back((uint8_t)(v | 0x80));
    v >>= 7;
  }
  out.push_back((uint8_t)v);
}

void put_rle(std::vector<uint8_t>& out, uint32_t count, uint32_t value, int bit_width) {
  put_varint(out, count << 1);
  const int bytes = (bit_width + 7) / 8;
  for (int i = 0; i < bytes; i++) out.push_back((uint8_t)(value >> (8 * i)));
}

// Bit-packed run of `groups` groups of 8, LSB-first.
void put_bitpacked(std::vector<uint8_t>& out, const std::vector<uint32_t>& vals, int bit_width) {
  const uint32_t groups = (uint32_t)(vals.size() / 8);
  put_varint(out, (groups << 1) | 1);
  const size_t nbytes = ((size_t)groups * 8 * bit_width + 7) / 8;
  const size_t base = out.size();
  out.resize(base + nbytes, 0);
  size_t bit = 0;
  for (uint32_t v : vals) {
    for (int b = 0; b < bit_width; b++, bit++) {
      if ((v >> b) & 1u) out[base + bit / 8] |= (uint8_t)(1u << (bit % 8));
    }
  }
}

void store_ref(std::vector<uint8_t>& dst, size_t i, int32_t code, uint8_t width) {
  switch (width) {
    case 1: dst[i] = (uint8_t)code; break;
    case 2: { uint16_t v = (uint16_t)code; std::memcpy(&dst[i * 2], &v, 2); break; }
    case 4: { uint32_t v = (uint32_t)code; std::memcpy(&dst[i * 4], &v, 4); break; }
  }
}

// Builds a random hybrid stream of exactly `n` encoded values (bit-packed runs
// padded to whole groups, so the stream may encode more than n).
std::vector<uint8_t> make_stream(std::mt19937_64& rng, int bit_width, int32_t n,
                                 std::vector<uint32_t>& truth) {
  std::vector<uint8_t> out;
  truth.clear();
  const uint64_t mask = bit_width == 32 ? 0xFFFFFFFFull : ((1ull << bit_width) - 1);
  int32_t written = 0;
  while (written < n) {
    if (rng() % 2 == 0) {
      const uint32_t count = 1 + (uint32_t)(rng() % 3000);
      const uint32_t value = (uint32_t)(rng() & mask);
      put_rle(out, count, value, bit_width);
      truth.insert(truth.end(), count, value);
      written += (int32_t)count;
    } else {
      const uint32_t groups = 1 + (uint32_t)(rng() % 150);  // up to 1200 values: spans tiles
      std::vector<uint32_t> vals(groups * 8);
      for (auto& v : vals) v = (uint32_t)(rng() & mask);
      put_bitpacked(out, vals, bit_width);
      truth.insert(truth.end(), vals.begin(), vals.end());
      written += (int32_t)vals.size();
    }
  }
  return out;
}

enum class Expect { Succeed, Fail, Either };

// `truth` holds the values the stream encodes (a prefix of them for a truncated
// stream); a successful decode must reproduce its first n.
void compare_case(const std::vector<uint8_t>& stream, int32_t n, int bit_width, uint8_t width,
                  const std::vector<uint32_t>& truth, Expect expect, const char* label) {
  std::vector<uint8_t> got((size_t)n * width + 16, 0xAB);
  uint32_t got_max = 12345;
  const int32_t got_rc = DecodeRLEBitPackedCodesPacked(stream.data(), stream.size(), n,
                                                       bit_width, got.data(), width, &got_max);
  CHECK(got.size() >= 16 &&
            std::memcmp(got.data() + (size_t)n * width, std::vector<uint8_t>(16, 0xAB).data(), 16) == 0,
        "%s bw=%d cw=%d n=%d: wrote past the end", label, bit_width, width, n);

  if (expect == Expect::Fail)
    CHECK(got_rc == -1, "%s bw=%d cw=%d n=%d: short stream accepted", label, bit_width, width, n);
  if (expect == Expect::Succeed)
    CHECK(got_rc == n, "%s bw=%d cw=%d n=%d: rc=%d", label, bit_width, width, n, got_rc);
  if (got_rc == n) {
    std::vector<uint8_t> want((size_t)n * width, 0);
    uint32_t want_max = 0;
    for (int32_t i = 0; i < n; i++) {
      store_ref(want, (size_t)i, (int32_t)truth[i], width);
      if (truth[i] > want_max) want_max = truth[i];
    }
    {
      CHECK(got_max == want_max, "%s bw=%d cw=%d n=%d: max %u want %u", label, bit_width,
            width, n, got_max, want_max);
      CHECK(std::memcmp(got.data(), want.data(), (size_t)n * width) == 0,
            "%s bw=%d cw=%d n=%d: packed codes differ from the stream's values", label,
            bit_width, width, n);
    }
  }

  if (bit_width > 31) return;
  draken::AppendBuffer<int32_t> idx;
  const int32_t ref_rc = DecodeRLEBitPackedIndicesNoPrefix(stream.data(), stream.size(), n,
                                                           bit_width, idx);
  CHECK((got_rc == n) == (ref_rc == n), "%s bw=%d cw=%d n=%d: verdict new=%d two-pass=%d",
        label, bit_width, width, n, got_rc, ref_rc);
  if (ref_rc != n || got_rc != n) return;
  std::vector<uint8_t> ref((size_t)n * width, 0);
  uint32_t ref_max = 0;
  for (int32_t i = 0; i < n; i++) {
    store_ref(ref, (size_t)i, idx[i], width);
    if ((uint32_t)idx[i] > ref_max) ref_max = (uint32_t)idx[i];
  }
  CHECK(got_max == ref_max, "%s bw=%d cw=%d n=%d: max new=%u two-pass=%u", label, bit_width,
        width, n, got_max, ref_max);
  CHECK(std::memcmp(got.data(), ref.data(), (size_t)n * width) == 0,
        "%s bw=%d cw=%d n=%d: packed codes differ from the two-pass path", label, bit_width,
        width, n);
}

}  // namespace

int main() {
  std::mt19937_64 rng(0x5eed1234);
  const uint8_t widths[] = {1, 2, 4};

  for (int bw = 1; bw <= 32; bw++) {
    for (uint8_t cw : widths) {
      for (int rep = 0; rep < 12; rep++) {
        const int32_t n = 1 + (int32_t)(rng() % 20000);
        std::vector<uint32_t> truth;
        const auto stream = make_stream(rng, bw, n, truth);
        compare_case(stream, n, bw, cw, truth, Expect::Succeed, "random");
        // num_values short of the stream: the decode stops mid-run.
        if (n > 9)
          compare_case(stream, n - 1 - (int32_t)(rng() % 8), bw, cw, truth, Expect::Succeed, "cut");
        // Stream cut in half: it may or may not still hold n values (a long
        // leading RLE run can), so the verdict must match the two-pass path and a
        // success must still be the true prefix.
        if (stream.size() > 8) {
          std::vector<uint8_t> half(stream.begin(), stream.begin() + stream.size() / 2);
          compare_case(half, n, bw, cw, truth, Expect::Either, "halved");
        }
        // One bit-packed run one byte short of its payload: must refuse.
        {
          std::vector<uint32_t> vals(8 * (1 + rng() % 40));
          const uint64_t mask = bw == 32 ? 0xFFFFFFFFull : ((1ull << bw) - 1);
          for (auto& v : vals) v = (uint32_t)(rng() & mask);
          std::vector<uint8_t> bp;
          put_bitpacked(bp, vals, bw);
          bp.pop_back();
          compare_case(bp, (int32_t)vals.size(), bw, cw, vals, Expect::Fail, "short");
        }
      }
    }
  }

  // bit_width 0: single-entry dictionary, every code 0.
  for (uint8_t cw : widths) {
    std::vector<uint8_t> got(100 * cw + 16, 0xAB);
    uint32_t mx = 7;
    const int32_t rc = DecodeRLEBitPackedCodesPacked(nullptr, 0, 100, 0, got.data(), cw, &mx);
    CHECK(rc == 100 && mx == 0, "bw=0 cw=%d rc=%d max=%u", cw, rc, mx);
    bool zero = true;
    for (int i = 0; i < 100 * cw; i++) zero = zero && got[i] == 0;
    CHECK(zero, "bw=0 cw=%d: codes not zero", cw);
    CHECK(got[100 * cw] == 0xAB, "bw=0 cw=%d: wrote past the end", cw);
  }

  // Invalid code width and bit width are refused.
  {
    std::vector<uint8_t> stream;
    put_rle(stream, 10, 3, 4);
    std::vector<uint8_t> got(64, 0);
    uint32_t mx = 0;
    CHECK(DecodeRLEBitPackedCodesPacked(stream.data(), stream.size(), 10, 4, got.data(), 3, &mx) == -1,
          "code width 3 accepted");
    CHECK(DecodeRLEBitPackedCodesPacked(stream.data(), stream.size(), 10, 33, got.data(), 1, &mx) == -1,
          "bit width 33 accepted");
  }

  // ── DecodeRLEBitPackedVerdicts (docs/PARQUET_RUN_DOMAIN_PREDICATE_DESIGN.md §11) ──
  // Reference: the stream's values looked up in the pass table one by one. The
  // verdict walk must give byte-identical verdicts, the same success verdict,
  // count every value in exactly one of runs_filled / codes_unpacked, and never
  // write past num_values.
  // Random streams at bit widths 0..20 (a pass table sized to the max code; wider
  // widths would need gigabyte tables — they take the small-value streams below).
  for (int bw = 0; bw <= 32; bw++) {
    for (int rep = 0; rep < (bw == 0 ? 1 : 10); rep++) {
      const int32_t n = 1 + (int32_t)(rng() % 20000);
      std::vector<uint32_t> truth;
      std::vector<uint8_t> stream;
      if (bw == 0) {
        truth.assign((size_t)n, 0);
      } else if (bw <= 20) {
        stream = make_stream(rng, bw, n, truth);
      } else {
        // Wide width, small codes: RLE runs and bit-packed groups of values < 4096.
        int32_t written = 0;
        while (written < n) {
          if (rng() % 2 == 0) {
            const uint32_t count = 1 + (uint32_t)(rng() % 3000), value = (uint32_t)(rng() % 4096);
            put_rle(stream, count, value, bw);
            truth.insert(truth.end(), count, value);
            written += (int32_t)count;
          } else {
            std::vector<uint32_t> vals(8 * (1 + rng() % 150));
            for (auto& v : vals) v = (uint32_t)(rng() % 4096);
            put_bitpacked(stream, vals, bw);
            truth.insert(truth.end(), vals.begin(), vals.end());
            written += (int32_t)vals.size();
          }
        }
      }
      // A pass table covering every code the DECODE reads (the first n — a
      // bit-packed run's group padding past n is never read), with a random
      // verdict per entry.
      uint32_t max_code = 0;
      for (int32_t i = 0; i < n; i++) max_code = truth[(size_t)i] > max_code ? truth[(size_t)i] : max_code;
      const uint32_t nent = max_code + 1 + (uint32_t)(rng() % 3);
      std::vector<uint8_t> pass(nent);
      for (auto& p : pass) p = (uint8_t)(rng() & 1);
      std::vector<uint8_t> got((size_t)n + 16, 0xAB);
      uint32_t bad = 99;
      long long runs = 0, unpacked = 0;
      const int32_t rc = DecodeRLEBitPackedVerdicts(stream.data(), stream.size(), n, bw,
                                                    pass.data(), nent, got.data(), &bad,
                                                    &runs, &unpacked);
      CHECK(rc == n, "verdicts bw=%d n=%d: rc=%d", bw, n, rc);
      CHECK(runs + unpacked == n, "verdicts bw=%d n=%d: runs %lld + unpacked %lld != n", bw, n,
            runs, unpacked);
      CHECK(std::memcmp(got.data() + n, std::vector<uint8_t>(16, 0xAB).data(), 16) == 0,
            "verdicts bw=%d n=%d: wrote past the end", bw, n);
      bool same = true;
      for (int32_t i = 0; i < n && same; i++) same = got[(size_t)i] == pass[truth[(size_t)i]];
      CHECK(same, "verdicts bw=%d n=%d: verdicts differ from the per-value lookup", bw, n);

      if (bw == 0) continue;
      // num_values short of the stream: stops mid-run, prefix identical.
      if (n > 9) {
        const int32_t m = n - 1 - (int32_t)(rng() % 8);
        std::vector<uint8_t> got2((size_t)m + 16, 0xAB);
        long long r2 = 0, u2 = 0;
        const int32_t rc2 = DecodeRLEBitPackedVerdicts(stream.data(), stream.size(), m, bw,
                                                       pass.data(), nent, got2.data(), &bad,
                                                       &r2, &u2);
        CHECK(rc2 == m && r2 + u2 == m, "verdicts cut bw=%d m=%d: rc=%d", bw, m, rc2);
        CHECK(std::memcmp(got2.data(), got.data(), (size_t)m) == 0 && got2[(size_t)m] == 0xAB,
              "verdicts cut bw=%d m=%d: prefix differs or overran", bw, m);
      }
      // A code at or above the table size is refused and named.
      {
        const uint32_t small = max_code;   // the max code itself is now out of range
        if (small > 0) {
          const int32_t rc3 = DecodeRLEBitPackedVerdicts(stream.data(), stream.size(), n, bw,
                                                         pass.data(), small, got.data(), &bad,
                                                         &runs, &unpacked);
          CHECK(rc3 == -2 && bad >= small, "verdicts bw=%d: out-of-range code rc=%d bad=%u",
                bw, rc3, bad);
        }
      }
      // A bit-packed run one byte short of its payload: refused.
      {
        std::vector<uint32_t> vals(8 * (1 + rng() % 40));
        const uint64_t mask = bw == 32 ? 0xFFFFFFFFull : ((1ull << bw) - 1);
        for (auto& v : vals) v = (uint32_t)(rng() & mask);
        std::vector<uint8_t> bp;
        put_bitpacked(bp, vals, bw);
        bp.pop_back();
        uint32_t vmax = 0;
        for (uint32_t v : vals) vmax = v > vmax ? v : vmax;
        std::vector<uint8_t> big_pass((size_t)vmax + 1, 1);
        std::vector<uint8_t> got3(vals.size() + 16, 0xAB);
        const int32_t rc4 = DecodeRLEBitPackedVerdicts(bp.data(), bp.size(), (int32_t)vals.size(), bw,
                                                       big_pass.data(), (uint32_t)big_pass.size(),
                                                       got3.data(), &bad, &runs, &unpacked);
        CHECK(bw == 32 || rc4 == -1, "verdicts short bw=%d: rc=%d (short stream accepted)", bw, rc4);
      }
    }
  }
  // A zero-length RLE run is malformed, never absorbed.
  {
    std::vector<uint8_t> stream;
    put_rle(stream, 0, 1, 4);
    put_rle(stream, 10, 1, 4);
    uint8_t pass[2] = {0, 1};
    std::vector<uint8_t> got(10, 0);
    uint32_t bad = 0;
    long long r = 0, u = 0;
    CHECK(DecodeRLEBitPackedVerdicts(stream.data(), stream.size(), 10, 4, pass, 2, got.data(),
                                     &bad, &r, &u) == -1,
          "verdicts: zero-length RLE run accepted");
  }

  if (g_failures) {
    std::fprintf(stderr, "packed codes: %d failure(s)\n", g_failures);
    return 1;
  }
  std::printf("packed codes: all checks passed\n");
  return 0;
}
