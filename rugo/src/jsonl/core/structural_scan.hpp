#ifndef _JSONL_STRUCTURAL_SCAN_HPP_
#define _JSONL_STRUCTURAL_SCAN_HPP_

#include <cstddef>
#include <cstdint>

namespace rugo::_jsonl {

// Masked structural index (simdjson stage-1 style). Writes the ABSOLUTE position
// (`base` + offset) of every byte of [data, data + length) that the document map needs,
// in ascending order, and returns how many were written:
//
//   * { } [ ] : ,  OUTSIDE a string
//   * every real (unescaped) delimiter quote — a string's opening and closing quote
//   * every newline, inside a string or not
//
// In-string structurals, backslashes and escaped quotes are never written, so the
// consumer sees clean structure: the entry after a string's opening quote is its closing
// quote (or a newline), and a container is bounded by depth alone. A position's byte is
// the marker's kind — the index stores positions only.
//
// A line is a record: string state never crosses a newline. A malformed line that ends
// inside a string (odd quote count, or a raw 0x0A inside a string value — invalid JSON,
// RFC 8259 requires the two bytes '\'+'n'; the JSONBench Bluesky dump has records split
// this way, tests/performance/jsonbench/README.md "Known data-quality defect") must not
// invert the quote parity of every following line, so each newline closes any open
// string and is always written. Escape and string state start clear, so `data` must
// begin at a line start (0, or one past a newline).
//
// `out` must have room for `length + 64` entries: blocks of 64 bytes write up to 16
// entries unconditionally past the count (simdjson's branch-light flatten); only the
// first `return value` entries are meaningful.
//
// NEON (AArch64) and AVX2+PCLMUL (x86-64, the -march=haswell floor) process 64-byte
// blocks: compare to bitmasks, escapes by the odd-backslash-run carry, in-string runs by
// carry-less-multiply prefix XOR. Other targets take the scalar loop, same output.
size_t scan_structural_index(const uint8_t* data, size_t length, uint32_t base, uint32_t* out);

}  // namespace rugo::_jsonl

#endif  // _JSONL_STRUCTURAL_SCAN_HPP_
