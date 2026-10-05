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

// scan_structural_index resumed mid-line: `state` carries the escape and string state in
// and out ({0, 0} at a line start), so a line indexed piece by piece writes exactly the
// entries one call over the whole line would. Every piece but a line's last must be a
// multiple of 64 bytes (the SIMD tail block is padded, which would swallow a trailing
// escape).
size_t scan_structural_index_cont(const uint8_t* data, size_t length, uint32_t base, uint32_t* out,
                                  uint64_t state[2]);

// The early-exit tail check (build_columns, interpreter.hpp). [data, data + length) is the
// UNREAD rest of a record's line — from just after the value that finished the record to
// the buffer end — entered OUTSIDE a string with `depth` brackets open. One pass finds the
// line's end (*nl_off: the first newline, or `length` when there is none; always set) and
// returns true iff, over [0, *nl_off), outside strings (the same escape rule as the index),
// the brackets bring the depth to 0 exactly once, at the last bracket (*close_off), and the
// line ends outside a string. Nothing else is validated: member grammar and scalar tokens
// past the wanted columns are not read.
bool check_line_tail(const uint8_t* data, size_t length, int depth, size_t* close_off, size_t* nl_off);

}  // namespace rugo::_jsonl

#endif  // _JSONL_STRUCTURAL_SCAN_HPP_
