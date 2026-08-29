# GROUP BY Exchange — AWS Validation Brief

**Date:** 2026-08-29 · **From:** local exchange prototyping session (18-core Apple Silicon)
**For:** the thread running AWS experiments (c8g.8xlarge, 32 vCPU, or larger)

## What this validates

GROUP BY is the only thing preventing the engine from using a large machine
(measured 2026-08-28: non-agg queries scale 3.3–4.5x @ 74–92% occupancy at DOP 16;
GROUP BY queries 2.4–3.2x @ 43–66%, with a serial-looking tail that is CONSTANT in
absolute ms from DOP 2→16). This session established why, and built two candidate
fixes now in the working tree. Both need a >18-core box to prove; that is this job.

**Root cause found locally:** the tail is `GroupBySink::finalize`'s cross-worker
MERGE. Its width used to be hardcoded `min(16, hw-2)` (fix A below), but more
fundamentally its WORK grows with worker count (W workers × N groups each to
reconcile) while its width also grows with W — so the tail is ~constant wall at
every DOP and never shrinks. It is an Amdahl term by construction.

## What is in the tree (all in `src/cpp/engine/`, build with `make compile`)

1. **A — merge width widening** (`operator.hpp`, `executor.hpp`,
   `native_group_sinks.hpp`): finalize's thread count is now
   `max(min(16, hw-2), exec_dop)` — can only widen. On an 18-core box this is a
   provable no-op; on 32+ vCPU at DOP 30 it lifts a hard 16-thread cap.
2. **Emit fixes** (`native_group_sinks.hpp`): bulk memcpy lanes/keys, shared global
   identity selections, windowed string-arena copy. Proven: routed-mode finalize
   (pure emit) fell ~720ms → 33–75ms. Masked in base mode by the merge.
3. **The exchange** — `OPTERYX_GB_ROUTED=1`, default OFF: ownership-routed grouped
   aggregation. Producers materialize per-owner partial-aggregate batches (dict
   morsels: one tuple per DISTINCT; dense: per row, count=1) while the morsel is
   hot; owners fold dense batches into tables only they write. **No merge exists**;
   finalize is pure emit (5–80ms on every local query).
4. **Phase timers** — `OPTERYX_GB_FINALIZE_PROF=1`: prints
   `[gb-finalize-prof] threads=N merge=…ms keys=…ms lanes=…ms` (thread-time sums)
   to stderr once per finalize.
5. **Probe** — `OPTERYX_GB_MERGE_THREADS=<n>`: forces the merge width (base arm
   only); `=16` reproduces the pre-A behaviour for A's before/after.

## ⛔ Scope limit — read before running anything

`OPTERYX_GB_ROUTED=1` covers **only `COUNT(*) GROUP BY` shapes** and FAILS LOUD
(RuntimeError) on any other aggregate. Do NOT run the full ClickBench suite under
the flag — run only the queries below. The error is by design (no silent fallback).

## Local baseline (DOP 16, medians of 3 rounds, subprocess-isolated, ms)

Dataset `scratch.hits_skene` (11 GB, ~99M rows). Queries:

    gb-low    SELECT RegionID, COUNT(*) … GROUP BY RegionID              (9,009 groups)
    gb-mid    SELECT SearchPhrase, COUNT(*) … GROUP BY SearchPhrase     (5.97M groups)
    gb-high   SELECT URL, COUNT(*) … GROUP BY URL                       (18.2M groups)
    gb-vhigh  SELECT WatchID, ClientIP, COUNT(*) … GROUP BY both        (99.0M groups)

| query | wall base | wall exch | fin base | fin exch | totCPU base | totCPU exch |
|---|---|---|---|---|---|---|
| gb-low | 81 | 80 | 0.3 | 0.1 | ~180 | ~175 |
| gb-mid | 304 | 246 | 37 | 5 | ~2770 | ~1760 |
| gb-high | 1108 | 1062 | 209 | 80 | ~10400 | ~8400 |
| gb-vhigh | 1330 | 1349 | 681 | 39 | ~19750 | ~19450 |

totCPU = operator `cpu_time` + finalize thread-time (prof lines). At DOP 16 the
exchange is equal-or-better on every metric; vhigh is a wash because both arms do
the same reconciliation work in different (fully parallel) phases.

## The hypotheses to test (in priority order)

**H1 — the exchange's advantage grows with DOP.** Base's tail is constant wall at
every DOP; the exchange has no tail. Run base vs `OPTERYX_GB_ROUTED=1` on the four
queries at DOP 8 / 16 / 24 / 30. Predicted: base `finalize_time` stays ~constant
(vhigh ~650ms-class); exchange stays 5–80ms; exchange wall advantage ≈ the tail's
share of wall, growing with DOP. If base's tail instead SHRINKS with DOP (A doing
more than predicted), say so — that weakens the case for the exchange.

**H2 — A alone does not shrink the tail.** Base arm only, DOP 30:
`OPTERYX_GB_MERGE_THREADS=16` (pre-A) vs unset (A active, width 30). Predicted from
local Amdahl fits: ≤1.2x on finalize, far less on wall. Use the phase timers — the
merge/keys/lanes split tells you whether extra width went anywhere.

**H3 — occupancy.** occupancy = Σ`cpu_time` / (wall × dop) from
`session._telemetry._reading["native_op_stats"]` + `native_engine_dop`. The 2026-08-28
sweep measured gb-vhigh at 43% @ DOP 16. Does the exchange raise it at DOP 30?

## Methodology — hard-won traps, do not skip

- **One subprocess per measurement, env set at spawn.** `OPTERYX_GB_DICT` is a
  process-lifetime static latch in C++ (first grouped query freezes it);
  in-process env flipping silently ran every arm as the first arm's config and
  VOIDED three full sweeps locally. `OPTERYX_GB_ROUTED` was de-latched (per-query
  read) but isolate anyway. Pattern: `subprocess.run([python, "-c", child], env=…)`,
  warm run inside the child, measure the second run.
- **Set DOP via `config.MAX_EXECUTION_WORKERS = n` in the child and ASSERT the
  telemetry `native_engine_dop == n`.** The knob has silently done nothing before.
- **`merge_time` == `finalize_time` in telemetry** (same value duplicated) — never
  sum them.
- **`cpu_time` EXCLUDES finalize** (D4 ruling, wall-only bracket). Cross-arm CPU
  comparisons must add finalize thread-time from the prof lines or they are
  asymmetric — this produced a false "exchange is 2.3x CPU" reading locally.
- **Interleave arms and rotate order per round** (thermal drift; local noise floor
  3.9% median / 14.7% max). Medians of ≥3 rounds.
- **Pin correctness on the AWS dataset first.** Run in the BASE arm, pin the
  results, then require the exchange arm to match exactly:
  `SELECT COUNT(*), SUM(c), MIN(c), MAX(c), SUM(c*c) FROM (<gb query with COUNT(*) AS c>) AS t`
  (`SUM(c*c)` catches any single wrong per-group count). Do not reuse the local
  pinned tuples — different dataset.

## Decision this feeds

If H1 holds at DOP 30: the exchange design is validated for generalization
(mergeable aggregates as lane partials; holistic as a raw-row lane) and becomes
the grouped strategy candidate. If H1 fails: the exchange stays a flag, and the
DOP ceiling needs a different explanation — bring back the phase-timer splits and
occupancy numbers either way, not just walls.

Known open residue (do not re-derive): at count=1 (nothing compresses, gb-vhigh)
the exchange pays batch materialization + big-owner-table probes ≈ what base pays
in its merge; levers identified but unexplored are flat-stride batch buffers
(replacing per-tuple `append_row`) and probe locality.
