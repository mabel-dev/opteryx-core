# GROUP BY sink-side probe: experiment design

Status: DRAFT for the architect (2026-10-27). Nothing built. Nothing in `main` changes
until a stage below passes its decision rule.

## 1. Why

ClickBench Q35 (`GROUP BY ClientIP, ClientIP-1, -2, -3`) is 1.12x DuckDB 2.0.0 and Q32
1.14-1.28x (interleaved, `make clickbench-interleave`). Everywhere else group-by is level or
ahead. Samply (Mac, 2026-10-27): Q35's `CarcharIndex::find_or_insert_id` is 34% of work
samples, sink side 66% / merge 25%; telemetry `probe_s` is 83% of Pass A+B+C (its timers are
not CPU time: 2.6 s summed against 1.9 s of total process CPU - see section 11). Q32 is merge-dominated with no hotspot and is out of scope here (ruled "no
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

## 11. Stage 1 results (2026-10-28, Mac M5 Pro, offline POC; nothing in the engine changed)

Harness: private `probe.cpp` (real `CarcharIndex`) replaying the REAL `ClientIP` stream (99,997,497
rows, 1,526 morsels, dumped in scan arrival order) with the engine's local-sink shape. Hash is
splitmix64(ClientIP) - same identity as Q35's composite hash, not the engine's exact function.
M1 was answered offline from the real stream instead of a private engine build (the data is the
same; the engine's dynamic morsel dealing differs from the harness's round-robin).

**M1 - local dedup is productive.** Per-window distinct ratio (groups / rows) at 65,536-group
windows: 1 worker - 278 windows, none above 0.6, median ~0.2-0.3; 16 workers - 282 windows, none
above 0.6. Raw mode (>0.9) never triggers and would be wrong for Q35. Local groups total 20.6M
against 9.76M global distinct, so the merge sees 2.11x the distinct groups.

**M2/M3 - the probe is cheap on this stream.** Hit-only ns/probe by table size (1 thread):
1k-16k entries 1.3-1.5; 65k 2.2; 262k 3.0; 1M 13.1; 4M 20.0; 16M 24.2. Real-stream replay:
5.6 ns/row with 1 worker, 8.7 ns/row with 16 workers (thread time). The window table (<=65,536
entries) sits in the L2-resident region.

**Cross-check.** Samply: `find_or_insert_id` is 34% of work samples = ~0.37 s of ~1.09 s work
CPU per run = ~3.7 ns/row, the same order as the replay. The earlier "26 ns/row" came from
telemetry `probe_s`, which is not CPU time and is withdrawn.

**Q35 CPU timeline (same sampler both engines).** Opteryx 1,904 CPU-ms, 135 ms wall, 14.1 busy
cores; DuckDB 2.0.0 1,784 CPU-ms, 119 ms wall, 15.0 cores. The 16 ms gap is roughly half extra CPU
(+6.7%) and half parallelism: Opteryx is at 4.6 cores in the first 10 ms (DuckDB 10.7) and dips to
7.2 cores around 20 ms and 90 ms.

**Verdict against section 2's gates.** The layout hypothesis (probe misses two cache lines) is
NOT supported: the probe is already near L2 speed. Even a 30% better probe is ~5-8% of Q35 CPU,
at best the CPU half of the gap. Stage 2 layout arms are therefore NOT justified yet. The new
leads are (a) the ramp-up and mid-run dips, (b) the merge's 2.11x multiplier. Next measurements,
before any layout work: a 2 ms timeline correlated with phases (query start, flush, merge) and the
split of Q35's remaining CPU outside the probe.
