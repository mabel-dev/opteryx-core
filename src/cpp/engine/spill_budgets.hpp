// Spill thresholds for buffered morsel accumulation (pipeline_buffers.hpp).
//
// Same layering argument as agg_budgets.hpp: the values must be readable from
// `opteryx/variables.py`, which sits BELOW the engine in the import graph, so
// they live in a dependency-free header a tiny extension can include.
//
// Why they are reported to users at all: a spilling query is deliberately a
// VISIBLE thing — spill converts an OOM into latency, and a threshold nobody
// can discover makes that a mystery slowdown instead of an explained one.
// The variables are SERVER-owned and unsettable; these are compile-time
// constants, and a session that appeared to change one would be lying.
//
// The two figures are a trigger and a ceiling, not a ladder (contrast MEDIAN's
// escalation in agg_budgets.hpp — that shape exists to avoid REFUSING a working
// query, and spill removes refusal, so it does not transfer):
//
//   - kSpillFlushBytes (512MB): the flush trigger. Chosen to match the size of
//     a typical skene-written file, so spill files run the writer at the shape
//     it is measured and tuned at (architect, 2026-08-27; revised down from
//     ~0.8GB). Below this, accumulation is purely resident — an eight-row CTE
//     never touches disk.
//   - kSpillCeilingBytes (1GB): the hard ceiling on outstanding (unflushed)
//     bytes while spill is CONFIGURED. The trigger-to-ceiling gap is consumed,
//     not spare: the encode working set, arrival overshoot at morsel
//     granularity, and room for a second unit to accumulate while the first is
//     being written. Exceeding it is a loud error naming the operator.
//
// When spill is NOT configured (no spill root), neither figure is enforced and
// buffered accumulation is unbounded — exactly the pre-spill behaviour. A
// budget whose overflow has nowhere to go could only refuse, and refusal here
// would be a regression, not a protection.

#pragma once

#include <cstdint>

namespace opteryx { namespace spill_budgets {

constexpr int64_t kSpillFlushBytes   = 512LL * 1024 * 1024;   // flush trigger
constexpr int64_t kSpillCeilingBytes = 1024LL * 1024 * 1024;  // hard ceiling

// A Writer handle splices its lock-free local batch into the buffer's shared
// pile once it holds this much. One mutex touch per 32MB keeps the append path
// effectively lock-free while ensuring the shared pile — the only pile a flush
// can take — holds nearly all outstanding bytes when the trigger fires.
constexpr int64_t kSpillSpliceBytes = 32LL * 1024 * 1024;

}}  // namespace opteryx::spill_budgets
