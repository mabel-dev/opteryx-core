# What to borrow from Velox — ranked plan

Status: M1/M2 measured; item 1 LANDED in main 2026-10-29 with tail fix R1', the costly-match and the large-build exceptions (uncommitted), make q + make slt green in main.
Date: 2026-10-23
Basis: Velox's vector/evaluation model (from knowledge, not re-read for this doc) checked
against the opteryx tree as of today. Every "opteryx does X" claim below cites the code read.
Impact numbers are HYPOTHESES until the measurement step attached to each item has run.

## Summary

| Rank | Borrow | Velox name | Expected impact | Confidence | First step |
|---|---|---|---|---|---|
| 1 | Build-side key set filters the probe **scan** before decode | dynamic filter pushdown (HashBuild → TableScan) | high on star joins (SSB, TPC-H, TPC-DS), zero elsewhere | medium — needs M1 | measure |
| 2 | Narrow later AND terms to survivors, cheaply, in runtime-measured order | ConjunctExpr + SelectivityVector + adaptive conjunct reorder | medium on multi-predicate WHERE | low-medium — needs M2 | measure |
| 3 | One shared survivor list per filter, not one per column | shared dictionary indices across wrapped columns | small, certain (bytes and allocations on every filter) | high | build |
| — | Per-dictionary result memoization | `evalWithMemo` | ~0 | high | drop |
| — | Result vector buffer reuse | `prepareForReuse` | unknown, likely low | — | only on a new profile |
| — | Known-ASCII flag | `StringVector::isAscii` | 0 today | high | park until NVARCHAR Unicode ops exist |

## 1. Build-side key filter into the probe scan (rank 1)

**What Velox does.** When a hash join's build finishes, Velox pushes a filter on the probe
key (a range, an exact value set, or a bloom) into the probe-side TableScan. The scan
applies it while decoding the key column, so rejected rows are never decoded for the other
projected columns and never reach any operator between the scan and the join. Velox
disables a pushed filter at runtime if it stops rejecting rows.

**What opteryx has.**
- `src/cpp/engine/runtime_bound.hpp`: a runtime ordinal **min/max** bound captured on the
  build side and consumed by the probe scan's **row-group zone map**. It prunes whole row
  groups only when the build key range is narrow relative to the probe file's layout. A
  dimension filter like `c_region = 'AMERICA'` yields keys spread across the whole range,
  so it prunes nothing.
- The plumbing that makes a row-level version cheap to add already exists: the build→probe
  handoff point in `Engine::run()` (same place the runtime bounds are copied), and row
  admission + selective decode in `native_parquet_scan_source.hpp` ("WHICH ROWS of each row
  group the scan may decode at all"; DeleteAdmission uses it).

**What was already ruled out, and why this is different.** A build-side bloom prefilter
inside the join probe loop (`native_join2.hpp`) was built and measured net negative
(JOB +5.6%, 2026-08-07): a miss in the native CSR probe costs ~5-13 ns, so skipping lookups
saves almost nothing. This proposal saves something else: **decode** of every other
projected probe-side column for rejected rows, plus all work by operators between scan and
join (filters, projections, exchange/repartition). It is placed in the scan, before decode,
not in the probe loop. If M1 shows that saving is small, this item dies the same death.

**Proposed shape.**
- Build side publishes, per inner/semi join key: the existing min/max bound, plus an exact
  key set when the build has at most 65,536 distinct keys (hash set over draken ordinals —
  the same ordinal space the runtime bound uses), else a blocked bloom.
- Probe scan: evaluated on the key column first; survivors become the row admission for
  the remaining columns (existing selective decode). Rows are still probed by the join, so
  the filter only has to be a superset: a false positive costs a probe, never a wrong row.
- Disable per scan when the measured rejection rate over the first N row groups is below a
  threshold. This is a perf gate on a superset filter: it cannot change the answer.
- Not for LEFT/FULL probe sides, anti joins, or keys without an ordinal.

**M1 — prove it exists before building.** SSB SF10, TPC-H SF10, TPC-DS SF1, rugo parquet
(RUGOONLY), after "machine's free". Per inner join, from existing join telemetry
(`probe_rows`, misses) and scan telemetry: probe rows read vs probe rows that match, and the
decode + downstream time spent on probe columns. Ceiling = time spent on rows that do not
match. **Build only if** the ceiling is at least 5% of the SSB suite or 3% of TPC-H/TPC-DS.
Report the ceiling either way.

## 2. Survivor narrowing for AND terms, in measured order (rank 2)

**What Velox does.** An AND evaluates its terms in sequence. After each term, the active
rows (a SelectivityVector, i.e. a bitmask) shrink to the rows still true, and the next term
runs only on those. Narrowing costs nothing because kernels honour the mask in place. Velox
also times each term and its rejection rate and reorders terms at runtime so cheap,
selective ones run first.

**What opteryx has.**
- `PredicateOrderingStrategy` orders conjuncts at plan time from estimated cost and
  selectivity. The 2026-07-16 audit found the estimates unreliable (ANALYZE row counts,
  key-name resolution, divergent default selectivity tables).
- BC_LAZY (2026-09-29) narrows a later AND term to the rows earlier terms admitted, but
  only for terms that **can raise** (functions, casts, arithmetic), for correctness. Terms
  that cannot raise run over every row. Narrowing is by **gather** of every loaded column,
  and always-on narrowing was measured at TPC-H Q06 2.4x slower, suite +9.6%. That is
  exactly the cost Velox's mask avoids.
- Side effect: `cheap AND LIKE` already narrows LIKE to survivors, because LIKE is
  conservatively "can raise". `LIKE AND cheap` does not, so plan-time order matters.

**Proposed shape**, only after M2 shows a ceiling:
- (a) Cheaper narrowing: narrow by a composed selection view (4 bytes/row of codes, the
  lazy-filter representation already used between operators) instead of gathering values.
  The 2026-09-29 trap (a narrowed view misread as dense by `_ensure_dense_bitmap_c`'s
  `data_length == length` test) is an open-coded shape test, which §11 forbids anyway. The
  fix is to make that reader use the canonical predicates, not to keep gathering.
- (b) Runtime order: per filter operator, time each top-level term and count its
  rejections over the first morsels, then fix the order for the rest of the scan. The
  answer is order-independent for terms that cannot raise. Terms that can raise keep their
  guarded position relative to the terms that protect them.
- (c) A narrowing policy: narrow before a term when the survivor fraction is below a
  measured threshold, else run over all rows (the popcount fast path noted on 2026-09-29).

**M2 — prove it exists.** For every TPC-H, TPC-DS and ClickBench query with ≥ 2 top-level
conjuncts: time per conjunct and survivor fraction (instrumented build in a private bench
tree, MUTPROBE), and the time with the best static permutation forced. Ceiling = current
minus best. **Build (b) only if** the summed ceiling is at least 3% of a suite; build (a)
only if the narrowing cost is what makes the best order lose.

## 3. One shared survivor list per filter (rank 3)

**What Velox does.** A filter wraps every surviving column in a dictionary that shares one
indices buffer.

**What opteryx does.** `cxx_mask` (`draken/morsels/cxx_morsel_ops.cpp`) derives the
survivor list once, then `lazy_select_column` allocates and writes a **separate** codes
buffer per column, `codes[j] = selection[idx[j]]`. Every column from the same scan morsel
has the same `selection` pointer (the global identity permutation), so those buffers are
byte-identical copies. A 30-column morsel of 65,536 rows filtered to half writes 29 × 128 KB
of duplicate codes and makes 29 extra allocations.

**Proposed shape.** In `cxx_mask`, memoize the composed codes **by source selection
pointer**: columns whose `selection` pointer is equal share one codes buffer, held by a
shared owner. This is not shape dispatch: it keys on the input pointer, not on dense/dict,
and the result is identical by construction. Dict-shaped columns, whose selections are
their own codes, still get their own buffers. It needs `VectorOwner`'s codes slot to accept
a shared buffer. That is an ownership change, not a `DrakenVector` layout change, so the
frozen ABI is untouched.

**Measure.** Interleaved A/B on ClickBench (filter-heavy, wide) and TPC-H SF10, after
"machine's free", per arch. Kept only if it is not slower (SLOWDITCH).

## Dropped or parked

- **Per-dictionary memoization** — dropped. The parquet Source builds one morsel per row
  group (`native_parquet_scan_source.hpp`), and a parquet dictionary is per column chunk, so
  a dictionary never reaches a second morsel. Velox gains because its readers cut stripes
  into small batches that share one dictionary. Content-keyed memoization would only hit on
  low-cardinality columns, where kernels already work on a handful of values.
- **Result buffer reuse** — not planned. The 2026-07-13 profile put allocator time at 0.6%
  and ruled out pooling without a new profile. The 2026-10-09 re-measure put it higher (7.2% of
  busy samples on M5; on x86 most of the cost is kernel page faults) but attributed it to
  GROUP BY partition growth, not expression outputs. Revisit only if a
  profile attributes material allocator time to kernel outputs.
- **Known-ASCII flag** — parked. VARCHAR already has ASCII semantics by type. The flag
  only pays once NVARCHAR has Unicode-aware operations, which do not exist yet. When they
  do, it fits the `flags` byte as an "absent = don't know" hint with no layout change.

## Order of work

1. Item 3 build + A/B (small, certain, independent).
2. M1 and M2 in one measurement session (both are telemetry reads on the same suites).
3. Item 1 if M1 passes its gate; item 2 (a)/(b)/(c) in the order M2's numbers justify.
4. x86 confirmation for anything kept (ARCHPERF).

Each step ends on `make q`; anything kept ends on `make slt`.

## Results — M1 and M2, measured 2026-10-29 (ARM, M-series; x86 owed)

Private bench tree built from main as of 2026-10-29 with measurement-only probes
(`scratch/velox_borrow/apply_probes.py`; nothing landed). One query at a time,
EXPLAIN ANALYZE, warm run discarded. Raw results: `scratch/velox_borrow/results_2026_10_29/`.
Shares are of suite wall time, each query's CPU share weighted by its wall time.

**M1 — build-side key filter into the probe scan. PASSES the gate on every suite, even at
the tight bound.**

| Suite | Tight | Middle | Upper | Gate |
|---|---|---|---|---|
| SSB SF10 | 17.9% | 26.5% | 46.6% | 5% |
| TPC-H SF10 | 5.7% | 7.8% | 12.7% | 3% |
| TPC-DS SF1 | 14.5% | 16.8% | 18.8% | 3% |

- Tight = scan decode + operators below the join, for rows a join later rejects. Middle adds
  the join's match work (gather/emit + the matching share of the probe loop) on rows a join
  ABOVE it rejects. Upper adds that join's whole CPU.
- Where it comes from: SSB Q1.x is decode of lineorder rows the date join rejects (the
  runtime min/max bound already cut Q1.1's scan from 60M to 7.9M rows; the rest falls
  inside the bound). SSB Q4.x and TPC-H Q05/Q07 are match work in the first fact join on
  rows a later join drops (Q4.1: 12.3M rows match the first join, 0.98M survive the chain).
  TPC-H Q19 rejects 98.9% of lineitem at its one join.
- Not counted: the filter's own cost (one key-set lookup per scanned row) against the join
  hash and miss work it removes for the same rows, assumed to cancel; 5 TPC-H and 13 TPC-DS
  joins that run inside another operator (excluded, so the ceiling is understated there).

**M2 — narrowed, runtime-ordered AND terms. FAILS the gate everywhere.**

| Suite | Narrowing (today's order) | Reordering on top | Total | Gate |
|---|---|---|---|---|
| TPC-H SF10 | 0.8% | 0.6% | 1.4% | 3% |
| TPC-DS SF1 | 0.2% | 0.2% | 0.4% | 3% |
| ClickBench | 0.0% | 0.0% | 0.0% | 3% |

- Upper bounds (free narrowing). All AND-term evaluation in the VM totals 199 ms (TPC-H),
  85 ms (TPC-DS) and 9.6 ms (ClickBench) of thread time: there is little to save.
- ClickBench predicates are settled in the scan (zone maps, range settlement, page search);
  the VM only sees undecided rows. M2 does NOT cover predicate-column DECODE: every
  predicate column is decoded for every undecided row before any term runs. Narrowing that
  decode (Velox's column-by-column filter in the reader) is a separate, unmeasured hypothesis.

**Consequence for the order of work:** item 2 is dropped as specified. Item 1 is the
build, subject to D1. Item 3 is unchanged (small, certain, independent).

## Item 1 build — design as measured (D1 approved 2026-10-29)

Built and measured in a private tree, then LANDED in main on the architect's go
(2026-10-29; uncommitted). Landed from `scratch/velox_borrow/key_filter.patch`; main
rebuilt, `make q` 530/530 and `make slt` green there. Extends the runtime min/max join bound
(docs/RUNTIME_MINMAX_FILTER_DESIGN.md): same eligibility, same barrier, same publish
point, same switch (`disable_runtime_minmax_join_filter`).

- **What the filter holds:** the build side's 64-bit key hashes — the draken hash the join
  itself treats as key identity — so it admits a superset of the join's matches. Built in
  `Join2BuildSink::finalize` from the queued hash chunks (keyed rows only): an exact set up
  to 65,536 keyed rows, a split-block bloom (~16 bits/key) up to 16M, none above.
  `src/cpp/engine/runtime_key_filter.hpp`.
- **Eligibility:** the runtime bound's, plus a single-column key on a native parquet probe
  scan, BOOL refused.
- **Applied AFTER decode, not as a decode mask.** The first build used the key bits as the
  mask the other columns decode under; profiling TPC-DS Q96 showed row-masked decode on
  narrow integer columns costs MORE than decoding everything (per-value appends), plus
  re-decodes when a mask narrowed. So every column decodes exactly as before; the decode
  worker tests the key columns over the rows the result emits and returns a
  `MorselRef::key_mask`; the Source drops those rows with the existing lazy-selection
  filter before any operator sees them. The saving is the joins' probe and emit work on
  rows a join would drop — which M1's middle estimate said was the bulk of the ceiling.
- **Several filters on one scan narrow:** smallest build first (a proxy for most
  selective), and each later filter hashes only the rows still alive (fixed-width keys are
  gathered densely before hashing). Without this, SSB's three-filter scans hashed every
  row three times and went 27% slower.
- A key column that is also a pushed-predicate column has the predicate's full length;
  its bits are compacted onto the predicate's survivors before joining the mask.
- Not armed under range settlement; self-disables after 1M rows tested if it keeps >95%.
- **Telemetry:** `key_filter_rows_in` / `key_filter_rows_out` in `io_scan_diagnostics`.

## Item 1 — results (2026-10-29, ARM; x86 owed)

**Correctness.** `make q` 530/530 and `make slt` (every group) green in the private tree.
26 integration/unit tests (`tests/integration/test_runtime_key_filter.py`,
`tests/unit/operators/test_runtime_key_filter_absent_key.py`): on/off result identity,
refused modes test no row, settlement does not arm, both filter shapes fire, a
schema-evolved probe, and a predicate key beside a second key. The off/on oracle compares
FULL results across TPC-H SF1 (22), SSB SF1 (13), TPC-DS SF1 (99) and SF0.01 (99): no
mismatch except TPC-DS Q75, whose float sums differ run-to-run within the BASELINE itself
(~1e-14 relative), same magnitude across trees, every non-float value identical.

⛔ An earlier oracle run reported "0 mismatches" while every query had errored in both
trees on result extraction — it compared nothing. The oracle now reports how many queries
produced a result and declares itself invalid below 90%. The re-run found two real bugs
(a predicate key mixing lengths, and a crash it caused in the join); both are fixed and
covered by tests.

**Timing**, interleaved A/B, 9 rounds, both trees built identically, sum of medians:

| Suite | B/A total | Geomean | Slower >3% | Faster >3% |
|---|---|---|---|---|
| SSB SF10 | 0.893 | 0.914 | 3 of 13 | 6 of 13 |
| TPC-H SF10 | 0.954 | 0.966 | 3 of 22 | 10 of 22 |
| TPC-DS SF1 | 1.000 | 1.005 | 28 of 99 | 16 of 99 |
| ClickBench subset (no joins, control) | ~1.00 | — | within ±5%, large queries ±2% | |

- Best: SSB Q4.2 0.62, Q3.4 0.72, Q4.1 0.82; TPC-H Q05 0.84, Q21 0.87, Q10 0.89;
  TPC-DS Q80 0.73, Q64 0.75.
- Worst: TPC-DS Q91 1.17, Q62 1.13, Q74 1.12; SSB Q3.1 1.15; TPC-H Q07 1.10. These are
  single-join or already-selective-first-join shapes where the scan's test duplicates work
  the join does cheaply.
- ⛔ TPC-H Q06 (no join) reads 0.91 in every run, including with identical builds. Not
  explained; the ClickBench control shows no general offset between the trees. Read the
  TPC-H total with that in mind.

## Item 1 — x86 results (2026-10-29)

Intel i5-8500 (6 cores, AVX2, gcc 12, `-march=haswell`), the same baseline and key-filter
sources as the ARM run, both built on the box, the Mac's own rugo-v2 datasets shipped
there, 9 interleaved rounds. Raw: `scratch/velox_borrow/results_x86_2026_10_29/`.

Correctness on x86: the full-result oracle over TPC-DS SF1 compares all 99 queries; the
only difference is Q75's baseline float noise, as on ARM. The filter fired on 93 queries
and kept 14.7% of the rows it tested (ARM: 14.9%).

| Suite (x86) | B/A total | Geomean | Slower >3% | Faster >3% |
|---|---|---|---|---|
| SSB SF10 | 0.917 | 0.960 | 6 of 13 | 5 of 13 |
| TPC-H SF10 | 0.948 | 0.967 | 4 of 22 | 8 of 22 |
| TPC-DS SF1 | 1.007 | 1.029 | 51 of 99 | 11 of 99 |

- TPC-H controls are clean on x86 (Q01 0.993, Q06 1.011): the ARM Q06 reading of 0.91 is
  specific to the Mac runs.
- Best: TPC-DS Q80 0.54, Q64 0.63; SSB Q4.2 0.62; TPC-H Q21 0.79, Q05 0.81.
- Worst: TPC-DS Q62 1.36, Q21 1.35, Q91 1.23, Q96 1.18; SSB Q2.3 1.22; TPC-H Q08 1.13.
- The TPC-DS tail is heavier on x86 than ARM (51 slower vs 28). Suite totals still net
  out: faster on SSB and TPC-H, flat on TPC-DS.

## Item 1 — tail fix R1' (2026-10-29, LANDED in main, uncommitted)

**Diagnosis** (per-operator CPU, baseline vs filter, TPC-DS losers vs winners):

| Query | Scan CPU | Join CPU | Net |
|---|---|---|---|
| Q62 | +19.8 ms | first join -4.7 ms, the rest flat | +15 ms |
| Q21 | +4.5 ms | first join -1.4 ms | +3 ms |
| Q91 | +10 ms | -2 ms | +8 ms |
| Q96 | +12.6 ms | first join -12.8 ms | ~0 |
| Q80 | +36 ms | LEFT OUTER 153 -> 3 ms | -185 ms |

1. A filter from the FIRST join over the scan's rows only duplicates that join: a row it
   rejects, the join rejects itself at the same price (~4.5 ns: a hash and a miss). The
   saving exists only for rows an EARLIER join or operator would have worked on before a
   LATER join drops them (Q80: inner-join filters ahead of a LEFT OUTER join).
2. Filter ORDER matters: "smallest build first" put unfiltered small dimensions
   (warehouse, ship mode, web site in Q62) first, each a full hash pass that rejects
   nothing, and the 1M-row self-disable never triggered on a 719k-row scan.
3. But the first join's filter is valuable as the cheap first stage IN FRONT of later
   filters (SSB Q2.3: dropping it took 1.02 -> 1.45, because the supplier filter then
   hashed every row instead of the part filter's 0.1% survivors).

**Rule R1' (proposed):**
- Each filter records its join's POSITION over the scan's rows (join probes already in the
  probe pipeline when it is wired).
- The scan tests filters in join order (the optimizer's order, selective first), not by
  build size.
- A scan whose ONLY published filter is position 0 arms nothing (pure duplication).
- The self-disable checks after 64k rows tested, not 1M.

Patch against main as landed: `scratch/velox_borrow/key_filter_rule_r1prime.patch`
(dry-run clean). Correctness: only drops or reorders filters (mask AND is
order-independent); full-result oracle clean on TPC-H SF1, SSB SF1, TPC-DS SF1 (Q75 float
noise only); the 22 result/refusal tests pass. The 4 positive controls use SINGLE-join
shapes, which R1' deliberately disarms — they must move to multi-join shapes if adopted.

**Measured (Mac, three interleaved arms, 9 rounds; raw in
`scratch/velox_borrow/results_rule_2026_10_29/`):**

| Suite | Current / base | R1' / base | Slower >3%: current -> R1' | Worst under R1' |
|---|---|---|---|---|
| SSB SF10 | 0.891 | 0.900 | 1 -> 0 | Q3.1 1.02 |
| TPC-H SF10 | 0.962 | 0.967 | 4 -> 2 | Q04 1.08 |
| TPC-DS SF1 | 0.994 | 0.979 | 22 -> 8 | Q74 1.16 |

Wins kept: TPC-DS Q80 0.72, TPC-H Q05 0.83, Q07 1.10 -> 0.85. Wins reduced: TPC-H Q21
0.87 -> 1.02, SSB Q4.2 0.61 -> 0.73. Q21's first join is a semi join with a RESIDUAL
condition, whose match work is not a cheap miss — a candidate refinement is to keep a lone
first-join filter when its join carries a residual. Not measured.

An intermediate rule R1 (never arm the first join's filter) was also measured: better
totals (TPC-DS 0.981) but SSB Q2.3 1.45 and Q3.3 1.17 — finding 3 above.

Landed on the architect's go (2026-10-29) with the positive controls moved to two-join
shapes and a new `test_lone_first_join_filter_does_not_arm`; main rebuilt, 72 join-filter
tests + `make q` 530/530 + `make slt` green there.

**x86 for R1'** (i5-8500, same box and datasets as the first x86 run, baseline vs the
R1' sources, 9 rounds; raw in `scratch/velox_borrow/results_x86_rule_2026_10_29/`). Oracle
on TPC-DS SF1: all 99 match (Q75 float noise only); filter fired on 67 queries.

| Suite (x86) | Before R1' / base | R1' / base | Slower >3%: before -> R1' | Worst under R1' |
|---|---|---|---|---|
| SSB SF10 | 0.917 | 0.897 | 6 -> 3 | Q2.3 1.23 |
| TPC-H SF10 | 0.948 | 0.986 | 4 -> 3 | Q02 1.06 |
| TPC-DS SF1 | 1.007 | 0.978 | 51 -> 20 | Q99 1.11 |

TPC-H controls clean (Q01 1.001, Q06 1.007). The tail is fixed on x86 too (51 -> 20
slower, worst 1.36 -> 1.11) and SSB improved; TPC-H gave back most of its gain (only Q05
0.80 and Q07 0.83 remain clearly faster; Q21 was 0.79 before R1'). SSB Q2.3 stays 1.23 on
x86. Both point at the same refinement: keep a lone first-join filter when its join's
match work is not a cheap miss (a residual / semi-anti condition).

## Item 1 — refinement: costly-match exception (2026-10-29, LANDED in main, uncommitted)

**Rule:** a lone first-join filter still arms when its join's match costs more than a
hash miss — a SEMI join, or any residual condition (`costly_match`, decided in
`_compile_join`). Patch against main as landed: `scratch/velox_borrow/key_filter_costly_match.patch`
(code only, tests unchanged). Landed on the architect's go; main rebuilt, 72 join-filter
tests + `make q` 530/530 + `make slt` green there. Correctness: 30 key filter tests pass;
full-result oracle clean on TPC-H, SSB and TPC-DS SF1 (Q75 float noise only); the filter
now fires on 9 TPC-H queries (R1': 7) and 68 TPC-DS (R1': 67).

Measured, three interleaved arms (baseline, R1', refinement), 9 rounds; raw in
`scratch/velox_borrow/results_refinement_2026_10_29/`:

| Suite | Mac: refinement / R1' | x86: refinement / R1' |
|---|---|---|
| SSB SF10 | 1.004 | 1.005 |
| TPC-H SF10 | 0.988 (slower >3%: 5 -> 1) | 0.994 |
| TPC-DS SF1 | 0.986 (noise, see below) | 1.003 |

- The one real effect: TPC-H **Q04** (EXISTS semi join) — Mac 1.05 -> 0.99, x86 1.03 -> 0.84.
  Nothing else moves beyond noise on x86.
- TPC-H **Q21 is NOT recovered** (Mac 1.04 -> 1.01, x86 1.02 -> 1.03; it was 0.79-0.87
  before R1'). Its pre-R1' win was not from its semi join's residual.
- Untested hypothesis for Q21 and Q03 (both lost their pre-R1' wins on x86): a lone
  first-join filter is NOT pure duplication when the join's build is large. The join then
  probes a large hash table with a cache miss per row, while the filter (exact set or
  bloom, a fraction of the size) stays cache-resident. Q03's lineitem filter sits on a
  plain inner join to ~1.5M orders.
- ⛔ TPC-DS small-query counts are noise-dominated between runs: R1' read 8 slower in one
  Mac run and 32 in the next. Compare TPC-DS on totals, and on the same run's arms only.

## Item 1 — large-build exception (2026-10-29, LANDED in main, uncommitted)

**Hypothesis (confirmed):** a lone first-join filter is not pure duplication when the
join's build is LARGE. The join then probes a hash table that misses cache on most rows,
while the filter (exact set or bloom, a fraction of the size) stays resident.

**Rule:** a lone first-join filter also arms when its build has at least
`RuntimeKeyFilter::kLargeBuildKeys` = 262,144 keyed rows. Patch against main as landed:
`scratch/velox_borrow/key_filter_large_build.patch`, landed on the architect's go (main
rebuilt; 74 join-filter tests + `make q` 530/530 + `make slt` green there): the constant, the
condition in `NativeParquetScanSource::arm_key_filters`, and a new test that a lone
first-join filter with a ~960k-key build arms (32 key filter tests pass). Full-result
oracle clean on TPC-H SF1 and TPC-DS SF1 (Q75 float noise only).

**Measured** with the threshold as an experiment-only environment knob in one tree
(arms differ only in the setting), four interleaved arms — baseline, current, 256k, 1M —
9 rounds; raw in `scratch/velox_borrow/results_large_build_2026_10_29/`:

| Suite | Mac: current / 256k / 1M vs base | x86: current / 256k / 1M vs base |
|---|---|---|
| SSB SF10 | 0.905 / 0.911 / 0.907 | 0.902 / 0.901 / 0.901 |
| TPC-H SF10 | 0.958 / **0.934** / 0.949 | 0.979 / **0.935** / 0.962 |
| TPC-DS SF1 | 0.993 / 1.006 / 1.007 (arm bias, below) | 0.976 / 0.977 / 0.976 |

- TPC-H at 256k vs baseline: **Q21** Mac 0.83, x86 0.76 (current: 1.02 on both);
  **Q03** Mac 0.94, x86 0.86. On x86, 256k vs current: 4 faster, 0 slower; Q16 0.85,
  Q12 0.89. Controls clean on x86 (Q01 0.999, Q06 0.991).
- 1M misses Q21 (its lineitem builds sit between the two thresholds); 256k wins on both.
- TPC-DS: only 7 queries change arming at 256k (Q04 Q11 Q18 Q22 Q57 Q72 Q74). On x86 they
  read 1.00 except Q18 1.06; the 92 unaffected read 1.001. On the Mac the 92 UNAFFECTED
  queries read 1.016 in the threshold arms — an arm bias from the environment-knob arms,
  not the rule (identical code path); the 7 affected sit within it. SSB unchanged.

## Decisions requested

- **D1** Item 1 sits next to the ruled-out join bloom. Is a scan-side, pre-decode key filter
  a different enough mechanism to measure (M1)? Recommended: yes.
- **D2** Item 2(a) changes BC_LAZY's narrowing from gather to selection view, reversing the
  2026-09-29 "narrow by GATHER, never by view" rule by fixing the reader that forced it.
  Allowed to measure? Recommended: yes, behind M2.
- **D3** Item 3's shared codes buffer changes `VectorOwner` ownership of the codes slot.
  Approve? Recommended: yes.
