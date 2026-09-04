# Compaction — Engine-Owned Execution

**Status: DELIVERED 2026-09-04 (engine side). See §6.2 for what is still open.**

All fourteen decisions are settled and recorded in §10. §1 describes the state
BEFORE the change and is kept as the record of what was measured and why it
moved; §6.2 describes what shipped.

Ratified:

- The engine owns the `OPTIMIZE TABLE` implementation end to end. It does not
  hand execution back to the catalog.
- Selection becomes a new **optimizer strategy**, carrying most of the planning
  code out of the catalog.
- `EXPLAIN OPTIMIZE TABLE <name>` is the preflight, rendered in the existing
  EXPLAIN shape.
- **One** commit path, not two, with whole-file retirement surfaced in telemetry.
- The pass bound stays a **byte** budget, at **2.1x max file size** — the ratio
  already in use.
- This lands **before** external sort.
- `DatasetCompactor` is deleted from the catalog. We do not keep two of them.
- Pushdown strategies decline a compaction plan through the existing
  `should_i_run` gate.

---

## 1. What exists today (verified, 2026-09-04)

| Fact | Source |
|---|---|
| The engine parses `OPTIMIZE TABLE` and builds a `LogicalPlanStepType.OptimizeRelation` node. | [`logical_planner.py:5991`](../opteryx/planner/logical_planner/logical_planner.py) |
| Execution is then handed wholesale to a library: `DatasetCompactor(dataset, …).compact(dry_run=False)`. The engine builds no plan and moves no data. | [`opteryx_connector.py:1286-1299`](../opteryx/connectors/opteryx_connector.py) |
| `opteryx-catalog` does **not** depend on `opteryx-core`. | `opteryx-catalog/pyproject.toml` |
| So compaction reimplements scan, sort and write in Python, with no access to the engine. | `opteryx_catalog/catalog/compaction.py`, 2,580 lines |
| The catalog has **zero internal callers** of `DatasetCompactor`. | grep across `opteryx_catalog/` |
| Every real caller is outside: the engine, and xb500 twice, both `dry_run=True` probes. | `trigger_compaction.py:365`, `recommend.py:351` |
| `merge.pyx` already carries an explicit `file_paths` parameter, writes via `connector.write_morsel`, and commits through `merge_commit`. | [`merge.pyx:97,153,277`](../opteryx/operators/merge/merge.pyx) |
| `merge_commit` already **drops a fully-deleted file from the manifest outright**. | [`dataset.py:2058`](../../opteryx-catalog/opteryx_catalog/catalog/dataset.py) |
| The scan takes its file list from a `Manifest` returned by `get_dataset_metadata()`. | [`opteryx_connector.py:658`](../opteryx/connectors/opteryx_connector.py) |
| `SortSink` appends every morsel and sorts the lot in `finalize`. No bound, no spill. | [`native_sort.hpp`](../src/cpp/engine/native_sort.hpp) |
| `SpillStore` exists — native, GIL-free, orphan sweep on startup. Only consumer today is `pipeline_buffers.hpp`. | [`spill_store.hpp`](../src/cpp/engine/spill_store.hpp) |
| Optimizer strategies get a `CopyOnWritePlan`; every change must go through a graph op or it is invisible to change detection. | `optimizer/strategies/optimization_strategy.py` |

### 1.1 The measured failure

Measured, not estimated, on
`gs://opteryx_data/opteryx/test/nyc_taxicab_2021/data/18cf730107ef1eab-99f004065bb3-30.parquet`
(395,177,689 bytes, 20,010,456 rows, 77 row groups, 20 columns).

| Measurement | Result |
|---|---|
| rugo full read, all 20 columns | 65.86s |
| pyarrow full read, same file | 0.57s |
| ratio | **115x** |
| one group's predicate read (3-day window) | 64.57s, **77 of 77 row groups scanned**, 275,095 rows returned |
| groups (30,904,427 rows / 272,000) | ~114 |
| extrapolated, one of two files | 7,297s (2 hours) |

Two independent causes multiply.

**Read amplification is 114x.** The streaming executor re-reads its whole
selection once per output row group — deliberate, and documented as read
amplification paid to avoid a measured ~21x write amplification. It relies on
pruning to stay cheap, and pruning cannot engage: candidate pruning is per *file*
against manifest min/max, one nyc file carries dirty timestamps spanning
2002–2098 and the other sits inside it, and within a surviving file there is no
row-group saving either (77 of 77, measured).

**Each read is ~13x slower than it should be.** Nine of twenty columns are
DECIMAL stored as `FIXED_LEN_BYTE_ARRAY`. Controlled pair:

| column | physical | width | encodings | compression | uncompressed/rg | read |
|---|---|---|---|---|---|---|
| `pickup_datetime` | INT64 | 8 | PLAIN, RLE | ZSTD | 2.1 MB | 0.48s |
| `fare_amount` | FLBA, Decimal(10,0) | 8 | PLAIN, RLE | ZSTD | 2.1 MB | 6.95s |

Identical in every respect that should matter. Strings are innocent: all six read
in 1.63s together. **Tracked separately, not part of this design.**

### 1.2 What it costs in production

- 11 requests/day run to the 1200s Cloud Run timeout and are killed. ≈ **$46/month**.
- Cloud Tasks is `maxAttempts: 1` and the worker has no timeout path, so each
  leaves its job stranded in `EXECUTING` — 35 accumulated on 2026-09-03.
- Cleanup only runs on a caught exception, and a timeout is not one. Five orphans
  of **exactly 199,816,606 bytes** sit in the nyc data directory, written on three
  different days: byte-identical progress every run, discarded every run.
- nyc has been attempted 60 times in 7 days with zero successes.

---

## 2. Why this moves

**It is a contract violation.** §2 says execution is native and Python on the
execution path is interim debt. `_execute_compaction_streaming_inner` is a Python
per-window drive loop over data, unnoticed because it is in another repo.

**Selection is planning.** Choosing which files to merge is reasoning over
statistics. §1 puts planning with the engine, whose planner already does this far
better than the compaction rules can.

**Nothing in the catalog wants it.** Zero internal callers.

The dependency direction only works this way. The catalog cannot depend on the
engine without a cycle; moving compaction *into* the engine resolves the layering.

---

## 3. Architecture

### 3.1 Shape

```
CompactionCommit      retire selected files, add new ones, one snapshot
  └── Sort            by the dataset's sort key
        └── Scan      over the selected files only
```

Two nodes exist unchanged. The third has a working sibling in `merge.pyx`.

### 3.2 Selection is a new optimizer strategy

**Ratified (D-1).** The `_select_*` rules — brute compaction, brute merge,
sort-aware merge, consolidation, delete debt, overlap decluster, binpack — move
out of the catalog and become an optimizer strategy that expands an
`OptimizeRelation` node into the subtree above. That is most of the planning code
in `compaction.py`.

It stays Python, per §1.

Three traps this inherits, all documented on the optimizer:

**Mutation contract.** Every plan change must go through a graph op — a
`plan[nid] = node` write-back, or add/remove node/edge. A pure in-place node edit
is invisible to change detection *and* fails to materialize the COW copy. Two
existing strategies have already been bitten by this.

**Column narrowing is a data-loss trap.** Compaction rewrites whole rows.
Projection pushdown narrowing this scan to the sort column would silently drop
every other column from the rewritten files, and the row-count balance check
would not catch it because the row counts would still match.

**Resolved (D-10).** Every strategy already has a `should_i_run(plan)` gate
([`optimization_strategy.py:322`](../opteryx/planner/optimizer/strategies/optimization_strategy.py),
called at [`optimizer/__init__.py:368`](../opteryx/planner/optimizer/__init__.py)).
A plan containing a compaction node returns False from the pushdown strategies,
so they do not run at all rather than running with an exemption carved into them.

That is broader than exempting one scan, and deliberately so. A compaction plan
is scan, sort, commit — there is no user projection or predicate anywhere in it,
so pushdown has nothing legitimate to do and disabling it wholesale costs
nothing. It also fails safe: a strategy that never runs cannot narrow anything
by accident.

It still wants a test that fails loudly if a future strategy forgets its gate.

**Resolved (D-11): no ordering constraint.** The strategy constructs a subtree
from an `OptimizeRelation` node using manifest statistics directly. It reads
nothing another strategy produces and produces nothing another strategy needs, so
it declares no provides/requires tokens and sits anywhere in the order.

Also note `_scan_base_stats` memoizes `RelationStatistics` keyed on
`(node.uuid, id(schema), id(manifest))`, and the cached object is **shared** —
the strategy must treat it as immutable.

### 3.3 Scanning the selected files

I called this a "pinned scan" in the first draft. That was a bad name for
something that is not new, and it is dropped.

The scan already takes its files from a `Manifest`. Compaction needs it to read a
*subset* — the files the strategy selected — so the strategy hands down a
Manifest containing only those entries. Same object, fewer rows in it. The reader
is still chosen from `FileEntry.file_format`, so `_scan_reader_for_manifest` is
untouched, and the binder already narrows a scan's schema in place, so narrowing
its manifest is the same shape of operation rather than a new mode.

**Resolves D-2.** There is no new scan capability. There is a narrowed manifest.

### 3.4 One commit path

**Ratified (D-3): one path.** My first draft proposed a second entry point and
was wrong to. `merge_commit` already documents that a file whose every row ends up
deleted is dropped from the manifest outright, so whole-file retirement is
*already* the semantics — compaction does not need different behaviour.

The only real objection was cost of expression: saying "retire this file" through
`positions` means naming every ordinal, which for nyc is 30.9M integers to express
a two-file intent. That argues for a cheaper way to say "all of it" on the
existing path, not for a parallel path.

It is not compaction-specific either. A MERGE that rewrites every row of a file
has the same problem today.

**Resolved (D-12):** a `retired_files` argument alongside `positions`, not a
sentinel buried in the ordinals. The two kinds of retirement stay visibly
distinct, and retirement becomes something the commit can *report* rather than
something inferred by noticing a file's ordinals happened to cover every row.

Retired files are exposed in telemetry (§8).

The sink itself mirrors `merge.pyx`: accumulate written `FileEntry` objects,
commit once, nothing observable until it commits.

### 3.5 The row-count balance check

**Resolved (D-4): noise.** It moves with the rest of the execution into the
engine sink and gets no special treatment. It survives; it just is not an
architectural question.

---

## 4. The pass bound

**Ratified (D-5): it was always a byte budget, and it stays one.**

My first draft argued for switching to a row bound. That was wrong, and wrong for
an instructive reason: it imported a constraint from the broken design. Under the
current executor, cost is `groups x bytes` and groups is `rows / 272,000`, so rows
drive cost and a row bound follows. Removing the re-read removes the group count
entirely. What is left is one read and one sort, and what has to be bounded is
**resident bytes**. That is a byte budget. The catalog's own comment calling for a
row bound was reasoning about the same broken executor.

The real problem is not the unit, it is that the current byte figure is not
trustworthy. `uncompressed_size_in_bytes` is a sum of `sys.getsizeof` estimates.
On the nyc file it is close — 4.17 GB estimated against 4.41 GB actually
materialized. On pypi it reads 11.97 GB for three files that are 52 MB on disk,
and whether that is a genuine in-memory footprint or an overstatement depends on
something this design should not guess at: whether dictionary-encoded string
columns stay dict-shaped in memory (§11 says the shape is preserved, which would
make the estimate far too high) or are densified.

**Resolved (D-13): the budget is 2.1x max file size**, which is the ratio already
in use — `DECLUSTER_MAX_COMBINED_MB` 8704 over `MAX_SIZE_MB` 4198 is 2.073. Two
target-sized files plus headroom, which is exactly the motivating decluster case:
two overlapping files combining and splitting back into two disjoint ones.

Expressing it as a multiple of max file size rather than an absolute is the part
worth keeping — it rescales on its own if the target file size ever moves.

Note `uncompressed_size_in_bytes` is `Optional[int]` and `None` for files written
before it was recorded, so the budget still needs a fallback for older files.

**Container sizing follows from this and does not currently fit.** At 2.1x a
4.1 GB max file the budget is 8.5 GB resident, and the old executor's
`PEAK_RAM_PER_BUDGET_BYTE` of 2.0 would put peak near 21 GiB once warmup and the
safety fraction are applied. The worker runs with 8 GiB. That 2.0 factor came
from `Morsel.combine` holding inputs and result together in the hold-everything
path, and the engine's sort sink does not necessarily have that shape, so the
multiplier should be **measured against the engine's sort** rather than inherited.
See §10.

**Ratified (D-6): this lands before external sort.** The lack of an external sort
has never caused a problem in practice; if it had, it would already be fixed. The
byte budget is what keeps the sort inside memory, so bounding the pass *is* the
protection, and spill later just relaxes the bound.

One property worth stating: once the multiplier is gone, memory buys pass size
which buys convergence speed, linearly. Today more memory means a larger selection
and quadratically more work per pass, which is why the answer to "should we add
memory" was "not yet".

---

## 5. EXPLAIN as preflight

**Ratified (D-7).** `EXPLAIN OPTIMIZE TABLE <name>` replaces the catalog's
`dry_run=True` probe, rendered in the **existing EXPLAIN shape** rather than a
bespoke result set, so it makes sense to consumers who already read EXPLAIN
output. A dataset with nothing to do returns an empty plan and the sweep skips it.

This is strictly better than what it replaces. The current probe runs the
catalog's selection code, which after this change is no longer the code that
decides. EXPLAIN asks the actual planner, so preflight and execution cannot
disagree.

---

## 6. What moves, what stays, what is deleted

**Moves to the engine.** Selection rules (as an optimizer strategy), plan
construction, execution.

**Stays in the catalog.** Sort-order normalisation (`normalize_sort_order`,
`resolve_sort_column`) — shared with the dataset code, not compaction-specific.
Manifest entry construction. Snapshot persistence. The commit primitive.

**Deleted (D-8, ratified).** `DatasetCompactor` goes from the catalog outright.
We do not keep two of them, and §8 of the contract says dead code rots the system
from the inside. xb500's two probe sites move to EXPLAIN in the same change.

That removes the whole execution half of `compaction.py` — the streaming
executor, group iterator, chunk-group computation, key-run scan, schema
reconciler and source cache — on the order of **twelve hundred lines**, replaced
by a plan builder and a sink.

Two hand-rolled steps disappear for free:

- Merge-on-read delete vectors are applied by hand at fetch time in the source
  cache. The engine's scan applies them by the same path as every other read.
- The source cache exists only to make repeated reads survivable. With one read
  there is nothing to cache.

---

## 6.1 Port verification (2026-09-04)

Selection has moved and is verified against the code it replaces. Both selectors
were given identical inputs — the same real manifests, the same schema, so the
same resolved sort column, and the same RNG seed — and compared on rule AND on
the exact file set chosen:

| dataset | sort column | catalog | engine | |
|---|---|---|---|---|
| nyc_taxicab_2021 | pickup_datetime | combine-split/overlap-decluster n=2 | same | match |
| gdelt_events | col_23 | combine/small-file-brute n=997 | same | match |
| pypi | col_2 | combine-split/overlap-decluster n=2 | same | match |
| github.events | col_13 | combine/small-file-brute n=381 | same | match |

⛔ The first version of this harness reported four mismatches and was WRONG: it
gave the catalog no schema, so the catalog could not resolve a sort column and
fell back to brute while the engine used the sort-aware rules. Two different
rules compared against each other is not a parity failure, it is a broken
experiment. Give both sides the same schema.

Note what this also confirms independently of the port: nyc_taxicab and pypi both
select `overlap-decluster`, which is the plan §1.1 measures at 114 reads of the
input. The selection is not what is wrong with them.

## 6.2 Delivered (2026-09-04)

The cutover has landed. `OPTIMIZE TABLE` is an engine plan.

| piece | where |
|---|---|
| desugar to `SELECT * FROM x [ORDER BY <cluster cols>]` + sink | `plan_optimize_table` |
| sink step type and binder visitor | `CompactionCommit`, `visit_compaction_commit` |
| file selection, all three rule families | `opteryx/planner/compaction/` |
| manifest narrowing and Order removal | `CompactionPlanningStrategy` |
| the sink | `operators/compaction_commit/compaction_commit.pyx` |
| retire-and-add commit | `Dataset.compaction_commit` (opteryx-catalog) |

Gates: `make q` 463, planner 197, storage 1072, sql 2099. All passing.

Three things worth recording about how it came out.

**The ORDER BY is emitted in SQL, not synthesized.** The desugar resolves the
relation's clustering columns and writes a real ORDER BY, so the binder resolves
it like any other. It is emitted unconditionally because which rule fires is not
known until the manifest is read, and the strategy REMOVES it for a brute plan,
which never sorts. Removing a node is routine; adding a correctly-bound one is
the half-bound trap.

**Capability is declared by overriding, not by a flag.** A connector that has not
overridden `Writable.compaction_commit` is refused at BIND time. That moved the
refusal earlier than the old path, which raised from the operator — so it now
arrives as a planning error rather than at execution, and
`test_optimize_is_not_refused_for_being_a_view` was updated to match. What it
pins, that the refusal names a missing capability rather than the target being a
view, is unchanged.

**The old path is gone**, not left beside the new one: the `OptimizeRelation`
step type, its physical mapping, its binder visitor, the operator action, and
`Writable.optimize_relation` with its connector override. D-8 said we do not have
two of them.

### Still outstanding

- `DatasetCompactor` is still in opteryx-catalog. Deleting it must land together
  with moving xb500's two `dry_run=True` probes to `EXPLAIN OPTIMIZE` (D-7),
  since removing it breaks them.
- No `maintenance_policy` reader, so the per-dataset delete-debt threshold
  override is ignored and every dataset gets the default.
- The container change to 16 GiB and the sort peak-multiplier measurement (D-14).

## 7. Failure and cleanup

The current failure mode is the thing to design out. A pass that does not finish
today leaks its output and strands its job.

- The commit is all-or-nothing, already optimistic-concurrency guarded by the
  connector's `_commit` wrapper.
- Nothing is observable until it commits, as with MERGE.

**Resolved (D-9): orphan reclamation is out of scope.** It stays with the
existing `detect-orphaned-storage` job.

Independently, the worker needs a time budget below the Cloud Run timeout that
marks a job FAILED rather than leaving it `EXECUTING`. That is a worker.opteryx
change, not covered here.

---

## 8. Observability

Compaction currently emits nothing until it commits — no billing event, no
telemetry, no progress. A run that dies at 1200s is indistinguishable in the logs
from one that never started.

As an engine plan it inherits per-operator telemetry for free: rows in and out,
bytes read, time per node. That is the difference between the investigation that
produced this document and reading a counter.

**Retired files are reported explicitly** (D-12). A compaction commit's whole
point is the swap, so the count and bytes of files retired — alongside those
added — are what says whether a pass made progress. Inferring it from a manifest
diff after the fact is how the current silence happened.

---

## 9. Scope boundary

Out of scope, deliberately:

- The rugo DECIMAL decode defect. Tracked separately, valuable on its own.
- External sort (§4).
- The worker's timeout handling and the sweep's resubmission loop. Both real,
  both cost money today, both in other repos.
- `DEDUPLICATE BY` and the other OPTIMIZE clauses the grammar currently rejects.

---

## 10. Decisions

### Settled 2026-09-04

| | Decision | Outcome |
|---|---|---|
| D-1 | Where selection lives | New optimizer strategy |
| D-2 | What a "pinned scan" is | Not a new capability — a narrowed manifest. Term dropped |
| D-3 | One commit path or two | **One.** `merge_commit` already drops all-deleted files |
| D-4 | Relocate the balance check | Noise. Moves with the rest |
| D-5 | Bound unit | **Bytes.** Always was. The row bound was imported from the broken design |
| D-6 | Sequencing vs external sort | **Before.** Lack of spill has never bitten us |
| D-7 | EXPLAIN output | Existing EXPLAIN shape, whatever consumers already read |
| D-8 | `DatasetCompactor` | Deleted from the catalog outright |
| D-9 | Orphan reclamation | Out of scope |
| D-10 | Pushdown exemption | `should_i_run` returns False on a plan containing a compaction node |
| D-11 | Strategy ordering | No constraint. Depends on nothing, provides nothing |
| D-12 | Retirement spelling | `retired_files` argument, surfaced in telemetry |
| D-13 | Byte budget | 2.1x max file size — the ratio already in use |

| D-14 | Container memory | **16 GiB.** The cost that forced it down is what this design removes |

### D-14 in detail

Memory was lowered because the worker was expensive to run. That expense is the
failure in §1.2 — hung passes holding an instance for 1200s, eleven times a day —
so removing it makes the larger container affordable again. Hung compactions alone
burn ~13,200 instance-seconds a day today; after this change a pass is seconds,
so the increase is charged against a fraction of the runtime it replaces.

16 GiB is also the right number for a second reason: Cloud Run caps 4 vCPU at
16 GiB, so it is the largest container that does **not** force the move to
8 vCPU. vCPU is about $0.000024/vCPU-s against memory's $0.0000025/GiB-s, so
staying under that tier keeps this a memory delta of a few dollars a month rather
than a doubling of the vCPU line across all worker traffic.

`CONTAINER_RAM_MB` must be set to 16384 to match. It already reads 16384 today
regardless of the container, which is exactly why selection over-commits by 2x
against the current 8 GiB — the constant becomes correct rather than changing.

**This gives the sort a pass/fail threshold rather than an open question.** With
a 2.1x budget of 8.5 GB (7.92 GiB) and 16 GiB available:

```
(16 x 0.85) - 0.75 warmup = 12.85 GiB usable
12.85 / 7.92                = 1.62x
```

So the engine sort's peak multiplier must come in at or below **1.62x** for the
ratified budget to fit. The old executor's 2.0 does not, but that figure came
from `Morsel.combine` holding inputs and result together in the path being
deleted, and a sort sink need not have that shape.

Measure it. If it lands at or under 1.62x, both D-13 and D-14 stand as ratified.
If it does not, memory is fixed at 16 GiB, so the budget is what gives — and
that is a decision to bring back, not to make silently.

### Also flagged, not decisions

- `SORT_SPILL_DESIGN.md` §1 is stale on spill machinery and should be corrected;
  some of its open decisions may already be settled by the morsel spill work.
