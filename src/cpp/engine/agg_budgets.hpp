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
//
// TWO figures, because a single ceiling has to be wrong in one direction or the
// other. Holding 2GB open for a query that needs 10MB is not a use of a shared
// process's memory anyone would defend; refusing at 256MB a query that would
// finish in 300MB is not either. So 256MB is what a query STARTS entitled to,
// and the ceiling doubles on measured demand up to 2GB before the query is
// refused. Almost nothing reaches the second step: the budget is charged only
// by MEDIAN, and in the benchmark suite exactly one query in 193 uses it.
//
// The escalation is on MEASUREMENT, never on a plan-time estimate. That is the
// same rule as before and for the same reason: what a buffering aggregate
// retains depends on properties no planner statistic carries, and the group-by
// cardinality estimator falls back to input_rows/2 per unknown key — on h2o g6
// that predicts 47.7GB against a true 1.2GB, so an estimate in front of this
// would refuse working queries 39x over.
constexpr int64_t kMedianFloorBytes = 256LL * 1024 * 1024;    // where every query starts
constexpr int64_t kMedianBytes      = 2048LL * 1024 * 1024;   // hard ceiling, after escalation

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

// MERGE INTO's address set: which target rows the statement has acted on. Held
// until the commit, because the commit is atomic — the appends and the
// row-deletes land in one snapshot, so neither half can be flushed early.
//
// SEPARATE from the CIDR budget rather than sharing it. They are unrelated
// workloads that can run in the same process, and a shared counter makes each
// one's ceiling depend on what the other happens to be doing — a merge would
// fail for reasons in someone else's query, and the message could not honestly
// say why. Sized the same because the underlying structure is the same (roaring
// over dense values) and 512MB of it addresses far more rows than a merge that
// size would take to write.
constexpr int64_t kMergeAddressStateBytes = 512LL * 1024 * 1024;

}} // namespace opteryx::agg_budgets
