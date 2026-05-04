#pragma once
//
// Comparison kernels for TimestampVector.
//
// Timestamps are stored as int64_t (microseconds since Unix epoch by default).
// The comparison semantics are identical to Int64Vector, so we re-use the
// _int64_compare.hpp templates verbatim — only the namespace differs so Cython
// can reference them with distinct extern-from declarations.
//
// op codes match timestamp_vector.pyx convention (and int64_vector.pyx):
//   0=eq  1=ne  2=gt  3=ge  4=lt  5=le
//

#include "draken/vectors/_int64_compare.hpp"

namespace draken { namespace timestamp_cmp {

using draken::int64_cmp::bit_fill_range;
using draken::int64_cmp::dispatch_compare_once;
using draken::int64_cmp::dispatch_scalar_nonnull;
using draken::int64_cmp::dispatch_scalar_branchless;
using draken::int64_cmp::dispatch_scalar_branching;
using draken::int64_cmp::dispatch_vector_nonnull;
using draken::int64_cmp::dispatch_vector_one_null_branchless;
using draken::int64_cmp::dispatch_vector_one_null_branching;
using draken::int64_cmp::dispatch_vector_both_null_branchless;
using draken::int64_cmp::dispatch_vector_both_null_branching;

}}  // namespace draken::timestamp_cmp
