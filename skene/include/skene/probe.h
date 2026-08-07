#pragma once
// skene/probe.h — identify a file without being able to read it.
//
// THIS INTERFACE IS FROZEN FOREVER.
//
// A build reads two versions (kVersion and kVersion-1) and migrates across one
// hop. Stepping a file forward several versions means running successively
// newer RETAINED binaries — and an operator can only pick the right binary if
// *any* build, however old or new, can answer "what version is this file?".
//
// So probe_version() reads magic and version and nothing else. It succeeds for
// versions this build cannot read, including versions that do not exist yet. It
// never interprets a footer, never follows an offset, never allocates. The first
// 8 bytes of FileHead (magic, version) can therefore never change layout — that
// is the price of a working migration chain, and it is worth paying.
//
// Everything past those 8 bytes is fair game for a version bump.

#include <cstddef>
#include <cstdint>

#include "skene/status.h"

namespace skene {

// Minimum bytes probe_version() needs. Deliberately tiny: a caller reading a
// remote object can probe from the first range request's prefix.
inline constexpr size_t kProbeBytes = 8u;

// Reads the format version from the head of a .skene file.
//
// Returns kNotSkene if the magic is absent (an unrelated or front-truncated
// object), kTruncated if fewer than kProbeBytes are supplied. Otherwise OK and
// *out_version is set — EVEN IF this build cannot read that version. Callers
// decide what to do about it; that is the whole point.
Status probe_version(const void* head, size_t head_bytes, uint16_t* out_version);

// Human-readable statement of what this build supports, for --version output
// and for error messages. Operators route files to binaries by this string, so
// it names the write version and the full readable range.
//
//   "skene format: writes v3, reads v2..v3"
const char* supported_versions_string();

// Actionable advice for a file this build cannot read, written into `out`.
//
// A build reads exactly two versions and writes exactly one. There is no mode
// that writes an older version — that would put two writers in one binary and
// make the chain ambiguous about what a file at version X actually contains.
// So moving a file forward several versions is several runs of several
// binaries, one hop each:
//
//   binary vX migrates (X-1) -> X
//
// which means a file at version F reaches N by running v(F+1), v(F+2), … v(N)
// in order. Refusing without saying THAT leaves an operator to work the chain
// out from release dates, so this composes the exact next step:
//
//   "file is v1; this build reads v3..v4. Run the skene binary that writes v2
//    first, then v3, then v4."
//
// For a file from the FUTURE there is no chain to walk — the advice is to
// upgrade, because no retained older binary will ever read it.
void migration_advice(uint16_t file_version, char* out, size_t out_bytes);

}  // namespace skene
