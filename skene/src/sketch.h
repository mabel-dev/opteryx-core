#pragma once
// Internal: the per-file KMV sketch v3 stores for each column node
// (FORMAT.md §8.1) — draken's Vector.hash() family, built on the write side.
//
// Its own translation unit because draken's hash lives behind draken's ops
// table (draken/ops/hash.h), which pulls in every kernel header; nothing else in
// the writer needs them.

#include "skene/format.h"
#include "skene/status.h"

// draken — imported, never copied.
#include "core/buffers.h"
#include "core/kmv_sketch.h"

namespace skene {

using FileSketch = draken::KmvSketch<kSketchK, draken::KmvHashFamily::kDrakenVectorHash>;

// True when `v`'s rows can be hashed by draken's Vector.hash(): the type has a
// hash kernel (every scalar type; not ARRAY, NULL or VECTOR_FP16) and, for the
// string family, the payloads were not elided — a length-only column's bytes
// are not there to hash. Decided by type and elision only, so it is the same
// answer for every row group of a column node.
bool sketch_supported(const DrakenVector& v);

// Adds the Vector.hash() of every row of `v` — a null row contributes the
// null hash, once, exactly as the catalog's sketches do. Precondition:
// sketch_supported(v).
Status sketch_add_rows(const DrakenVector& v, FileSketch* sketch);

}  // namespace skene
