# GROUP BY sink-side probe: experiment design

Status: DRAFT for the architect (2026-10-27). Nothing built. Nothing in `main` changes
until a stage below passes its decision rule.

## 1. Why

ClickBench Q35 (`GROUP BY ClientIP, ClientIP-1, -2, -3`) is 1.12x DuckDB 2.0.0 and Q32
1.14-1.28x (interleaved, `make clickbench-interleave`). Everywhere else group-by is level or
ahead. Samply (Mac, 2026-10-27): Q35's `CarcharIndex::find_or_insert_id` is 34% of work
samples, sink side 66% / merge 25%; telemetry `probe_s` is 83% of Pass A+B+C (about 26 ns/row,
thread-summed). Q32 is merge-dominated with no hotspot and is out of scope here (ruled "no
open lever" 2026-10-27). Both engines saturate ~16 cores; the gap is cycles, not parallelism.

The profile locates the cost. It does not prove the mechanism. This design separates
"measure" from "change" so nothing is built on a guess.

## 2. Effect size needed

Probe is ~34% of Q35 CPU. To close the 12% gap by the probe alone the probe must get ~35%
cheaper; a 15% probe gain is ~5% on the query (about the noise floor). So:

- microbench gate: >= 20% faster probe on the Q35-shaped stream, else stop;
- engine gate: Q35 ratio < 0.95 B/A with controls inside their band.

## 3. Rules of engagement (from the standing rulings)

- Develop in a PRIVATE bench tree (a copy of the repo, not a git worktree), never untested
  edits in `main`; land only after validation. Scratchpad trees die on reboot, so the tree
  and results live in a named directory the user picks.
- A/B only, strictly interleaved, arm order alternating; no A/A arms. Noise floor comes
  from the controls inside every A/B.
- Baseline before the first edit; prove the knob moves the mechanism metric before spending
  on the outcome metric.
- Heavy runs only when the machine is free; quote durations from a measured rate.
- Mac only until x86 is available (no x86 access as of 2026-10-27). Per-arch results are
  owed before anything is called landed.
- Hash identity stays 64-bit. A variant that weakens it is NOT tested without a ruling.

## 4. Stage 1 - measure (no behaviour change)

**M1. Local dedup ratio per flush window.** At `maybe_flush` the engine already holds
`groups` and `rows_since_flush`. Log `groups / rows_since_flush` per flush per worker (one
line per flush, no per-row cost) behind a private-tree env flag. Queries: Q35, Q32, Q15, Q09,
Q36 (the shape that regressed at raw threshold 0.3).
Decision: if Q35 windows are mostly > 0.7 distinct the local table barely dedups and the
local probe is largely wasted work -> go to Stage 2c. If mostly < 0.4 the probe is productive
and only its cost matters -> Stage 2a/2b.

**M2. Probe cost vs table size.** Reuse the `gb_merge_poc` approach (real `CarcharIndex`,
real hash stream). Sweep 1k..16M entries x hit ratio {0, 50, 90}% and report ns/probe. Shows
where the L1/L2/SLC/DRAM cliffs are on this machine and where a 65,536-entry window x 16
workers (~22 MB) sits.

**M3. Real stream.** Read the real `ClientIP` / `WatchID` columns (rugo, not pyarrow), apply
the engine's hash, keep the row order. Every Stage 2 arm replays this stream, so clustering
is real.

## 5. Stage 2 - layout microbenchmarks (private POC, not the engine)

Arms, all with identical checksums (group count and sum of ids), min of 3, order rotated:

- A  current `CarcharIndex` (control byte + `{hash, id}` slot).
- B1 fused 8-byte entry (16-bit salt + 48-bit id) PLUS a hash array to confirm identity
  (correctness-preserving; shows what the dependent miss costs).
- B2 co-located state: slot `{hash, count}` so the COUNT(*) lane lives in the entry
  (Pivot / ClickHouse shape). Only meaningful for COUNT-shaped aggregates.
- C  two-phase batched probe: load a batch of independent control groups, then slots
  (no prefetch instruction; prefetch is banned).

Streams: Q35-shaped (real order), Q32-shaped (near-unique), and the cache-resident 9k-group
shape as the DISCRIMINATOR - it must stay flat, otherwise the gain is not the mechanism
claimed (the H16 measurement used exactly this test).

Decision: an arm proceeds only if >= 20% faster on the Q35 stream, flat (+-3%) on the 9k
shape, and not slower on the Q32 stream. Otherwise it is killed and recorded.

## 6. Stage 2c - raw-mode / local-probe policy (only if M1 says so)

Candidates change WHEN a worker skips the local probe, not the table. The standing
constraint is Q36/Q17/Q18: raw threshold 0.3 regressed them +26..29%, because a flush window
cannot see global dedup. Any candidate must be A/B'd on Q36, Q17, Q18 first, then Q35.

## 7. Stage 3 - engine A/B (survivors only)

- Harness: `dev/ab_bench.py` (A = snapshot of `main`, B = private tree; only the `.so` under
  test differs, md5-checked), >= 11 interleaved rounds, alternating arm order.
- Targets: Q35, Q32, Q08, Q09. Group-by controls: Q15, Q16, Q18. Non-group-by controls:
  Q20, Q25, Q29. Q01 is NOT a clean control for allocation changes.
- Regression guards: TPC-H SF10 (Q01, Q18 plus the join queries if the shared header is
  touched), H2O g1-g10 via `dev/groupby_baseline.py --compare`, distinct (`CarcharSet`) and
  join users if `carchar_common.hpp` / `carchar_index.hpp` change.
- Mechanism metric: `get_groupby_telemetry()['probe_s']` must drop by roughly what Stage 2
  predicted; wall is the outcome metric. Wall moving without `probe_s` moving means the
  change is not the mechanism.
- Correctness: answer digests identical to A on >= 14 GROUP BY queries (NULL keys, strings,
  multi-key, COUNT DISTINCT, AVG/SUM, ordered LIMIT), ASan build for any layout change.
- Sign-off for landing: `make q` and `make slt` (engine change), `make dt/st/rt` if a native
  file is deleted or renamed.

## 8. Stage 4 - versus DuckDB

`make clickbench-interleave QUERIES=8,9,15,16,18,32,35 LOOPS=6` (12 pairs per query). Report
the per-query ratio with its CI, not the harness PASS/FAIL (a small sample fails the 1.05
bound regardless).

## 9. Out of scope / needs a ruling

- Weakening the 64-bit hash identity (8-byte salt+id entry without a confirm).
- Changing the lane model (co-located state beyond COUNT).
- Any change to `kGBFlushEntries` (swept 2026-10-04, 65,536 best or tied on both arches).
- Q32's merge (ruled no open lever).

## 10. Order and stop rules

M1 and M2 first (hours). Each later stage runs only if the previous gate passed. A killed arm
is recorded with its numbers, not left disabled.
