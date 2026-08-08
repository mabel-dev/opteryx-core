// Memory ceilings for the buffering ("holistic") aggregates.
//
// These live in their own dependency-free header for a LAYERING reason, not a
// tidiness one. The values must be readable from `opteryx/variables.py`, which
// sits BELOW the engine in the import graph (`_operators` -> `expression` ->
// `models` -> `execution_context` -> `variables`), so `variables` cannot import
// the engine bundle back without a cycle. A header with no includes and no
// dependants can be pulled into a small extension that `variables` CAN import.
//
// Why they are reported to users at all: MEDIAN and ARRAY_AGG retain every input
// value until finalize and fail loud on these budgets. `SHOW VARIABLES` carries
// them so an author can see the line before hitting it — a limit nobody can
// discover is a limit that only ever shows up as a surprise. The variables are
// SERVER-owned and unsettable; these are compile-time constants, and a session
// that appeared to change one would be lying.
//
// Each aggregate gets its OWN budget rather than sharing one counter (architect's
// ruling): a failure then names the aggregate that actually overspent, at the
// cost of the real ceiling being the sum across aggregates in one query.
//
// Only the VALUES live here. The atomic counters that charge against them stay
// with the states that own them (median_budget_used in _agg_kernels.hpp,
// array_agg_budget_used in native_group_sinks.hpp) — they are deliberately
// per-shared-object statics, and moving them into a header this widely included
// would spread them across extensions that never append.

#pragma once

#include <cstdint>

namespace opteryx { namespace agg_budgets {

// MEDIAN — buffers every non-null value as a double until finalize.
constexpr int64_t kMedianBytes = 512LL * 1024 * 1024;   // 512MB, all groups

// ARRAY_AGG — buffers every input row (NULLs included) as a list element.
constexpr int64_t kArrayAggBytes = 512LL * 1024 * 1024;   // 512MB, all groups

// CIDR_AGG needs TWO budgets, and they cannot be derived from one another.
//
// The COLLECTION state is a Roaring bitmap, which dedups on insert, so it grows
// with DISTINCT addresses rather than rows and is bounded by construction at
// ~512MB per set (65536 containers x 8KB) however many rows arrive. The budget
// below bounds the TOTAL across groups, which is what is actually unbounded.
//
// The EMIT is bounded by neither. The worst-case input is HALF density — every
// even address, where no /31 is ever complete so nothing folds — which is 2^31
// distinct /32 blocks, about 36GiB of text, produced from a state sitting at
// exactly the 512MB collection ceiling. Adding the odd addresses collapses that
// same output to a single "0.0.0.0/0". So a satisfied collection budget says
// nothing whatever about the output fitting, and uniform-random input at high
// density lands within ~1.4x of the degenerate case rather than safely away
// from it. Emit charges as it builds and fails loud; it cannot size up front
// because the size is not a function of anything known before the walk.
constexpr int64_t kCidrAggStateBytes = 512LL * 1024 * 1024;   // 512MB, all groups
constexpr int64_t kCidrAggEmitBytes  = 512LL * 1024 * 1024;   // 512MB of CIDR text

}} // namespace opteryx::agg_budgets
