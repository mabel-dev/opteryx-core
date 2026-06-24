# `ANALYZE … FOR COLUMNS` — Statistics Sidecars: Design & Status

**Status:** IMPLEMENTED (local filesystem), `make q` 190/190. Bloom filters were
explicitly cut by the architect — see "Decision log" below. What shipped:
column-scoped NDV (KMV) statistics via `ANALYZE … FOR COLUMNS`, and removal via
`DROP STATISTICS ON t [FOR COLUMNS …]`.
**Date:** 2026-06-24.
**Related:** [`docs/NDV_STATISTICS_DESIGN.md`](NDV_STATISTICS_DESIGN.md) (WP-8, sidecar generation/freshness).

## Decision log (what changed from the original design)

- **Blooms cut.** The architect chose to skip bloom filters entirely. Blooms in
  this codebase are *embedded in the Parquet file* (read via `bloom_offset` in
  the footer), not sidecars — there is no sidecar-bloom reader. Adding them
  would have meant either a new scan-side consumer (sidecar path) or rewriting
  user data (embedded path). Out of scope.
- **Correctness landmine gone with them.** The remaining artifacts (NDV/KMV,
  and prospectively histograms) are *advisory* — used only for cost estimation.
  A stale stat yields a worse plan, never a wrong answer. So the §7
  version-binding discipline is **not required**; the loader's existing
  field-id staleness check is sufficient.
- **Scope: local filesystem.** The local FS exposes no write/delete API, so
  sidecars are written via `os` on the local paths (they *are* local files).
  Remote/object-store writes are a later increment; an unsupported backend
  fails loudly (`UnsupportedSyntaxError`).
- **Bare `ANALYZE TABLE t` analyzes all columns** (the old path was dead —
  `action` was never set, `refresh_manifest` did not exist). This supersedes
  the §8.1 "keep bare ANALYZE as manifest refresh" suggestion.

## What was built

- Format: the existing v1 `.stats.json` sidecar (`field_ids` = full schema,
  `min_k_hashes` = analyzed columns only). KMV core promoted to
  [`opteryx/utils/kmv.py`](../opteryx/utils/kmv.py) (stdlib BLAKE2b, no pyarrow).
- Orchestration: [`opteryx/operators/table_management/_analyze.py`](../opteryx/operators/table_management/_analyze.py)
  — native read via `rugo.read_parquet`, per-file KMV, atomic sidecar write;
  and the drop path (column-scoped edit or whole-file delete, idempotent).
- Wiring: `plan_analyze_query` reads `columns` + sets `action`; `DROP STATISTICS`
  recognized in the planner pre-parse layer (`_intercept_drop_statistics`) and
  routed through the same Analyze/Table-Management node by `action`.
- Tests: [`tests/storage/test_analyze_statistics.py`](../tests/storage/test_analyze_statistics.py)
  (7 cases — lifecycle, exact NDV, idempotent drop, loud failures).

The sections below are the original design discussion, retained for context.

---

---

## TL;DR

The read-side machinery that skips data — **bloom row-group pruning, NDV estimation,
zone-map / IN / BETWEEN pruning, histogram-driven selectivity** — already exists and
runs in the scan and planner today. What is missing is a **documented, user-invoked
producer** for the one artifact production files don't already carry: **bloom filters
on high-entropy equality columns**, plus a productionized path for NDV/histogram
sidecars.

This is therefore **~80 % surfacing existing capability, ~20 % new code.** The new code
is a per-column artifact builder behind `ANALYZE … FOR COLUMNS`, a sidecar manifest, and
— the only part that must be perfect — **version-binding so a stale sidecar can never
produce a wrong answer.**

**No hints.** `ANALYZE … FOR COLUMNS cols` means *"build whatever skip structures suit
these columns; the engine decides per column from measured data shape."* Artifact
selection is data-driven, not user-specified. (Hint syntax was evaluated and rejected —
see [§9](#9-rejected-alternatives).)

**Decision needed from the architect:** see [§8](#8-open-decisions).

---

## 1. Current state (verified)

### 1.1 The statement already parses and dispatches

- `sqlparser` 0.62 + `OpteryxDialect` parse `ANALYZE TABLE t FOR COLUMNS a, b` into
  `Statement::Analyze { table_name, for_columns: true, columns: [a, b], … }`. **No Rust
  change required** — the column list already reaches Python.
- `"Analyze"` is wired in `QUERY_BUILDERS`
  ([`logical_planner.py:1940`](../opteryx/planner/logical_planner/logical_planner.py)).
- `plan_analyze_query`
  ([`logical_planner.py:1795`](../opteryx/planner/logical_planner/logical_planner.py))
  builds a `LogicalPlanStepType.Analyze` node **but currently reads only `table_name`
  and ignores `columns`.**
- Physical: `Analyze` → **Table Management** node
  ([`physical_planner/__init__.py:242`](../opteryx/planner/physical_planner/__init__.py)),
  whose `analyze_table` action today just calls `connector.refresh_manifest("system")`
  ([`table_management.pyx:51`](../opteryx/operators/table_management/table_management.pyx)).

So the spine exists end-to-end. The work is to **thread `columns` through and replace the
manifest-refresh stub with the artifact builder.**

### 1.2 The consumers already exist (the "undocumented capability")

| Consumer | State | Where |
|---|---|---|
| **Bloom row-group pruning** | Live: `Eq`/`InList` on int32/int64/byte_array, LOCAL files, fail-open. **No producer for files lacking blooms.** | scan / `pool_reader` |
| **NDV / KMV** | Estimator + sidecar **loader** work end-to-end; only a `dev/` script produces. | `cost_estimation/`, `statistics_refresh.py` |
| **Zone-map / IN / BETWEEN pruning** | Live in the scan (why Q41 is 61 ms not 600 ms). | `pool_reader`, `predicates` |
| **Histogram → selectivity** | Consumer exists. | `cost_estimation/selectivity.py` |

`ANALYZE … FOR COLUMNS` is the front door that *feeds* these. The dominant new artifact
is the **bloom sidecar**; NDV/histogram productionization reuses the WP-8 path.

---

## 2. Goal & non-goals

**Goal.** A user-invoked, explicit, documented command that builds skip-structure
sidecars for named columns, bound to an exact data version, consumed automatically by
the existing scan/planner consumers, fail-open on any staleness.

**Non-goals.**
- No automatic/lazy build on query miss — **explicit only** (§1 "no hidden behaviour").
- No per-statement artifact-kind selection — engine decides (§9).
- Does **not** address the count-distinct cluster (Q09–Q12, single-threaded agg CPU) or
  Q40 (weak filter, large residual). This targets **needle queries** — Q20, and the
  `RefererHash`/`URLHash` legs of Q41/Q42. See [§10](#10-what-this-does-not-fix).

---

## 3. Surface syntax

```sql
ANALYZE TABLE hits FOR COLUMNS UserID, RefererHash;
```

- `TABLE` keyword required (current builder already enforces this).
- `FOR COLUMNS <list>` — the columns to analyze. Omitting it = whole-table refresh
  (current behaviour; preserved).
- **No options, no hints.** The artifact set per column is decided in §4.

Bare `ANALYZE TABLE hits` retains today's manifest-refresh semantics.

---

### 3.1 Dropping sidecars (the inverse)

A delete path is required — the inverse of build. **There is no native parse for it**
(verified): `DROP STATISTICS hits` is a parse error (`ObjectType` has no `Statistics`),
and `ALTER TABLE hits DROP STATISTICS` **misparses** — sqlparser reads `STATISTICS` as a
*column name* in a `DropColumn` op, i.e. "drop a column called STATISTICS." A clean
failure would be fine; a silent misparse into a destructive op is not. `AlterTable` is
not dispatched in Opteryx regardless.

Therefore the drop path is **tier-C**: recognized in `do_sql_rewrite` *before* sqlparser,
parsed in Python into a native node, dispatched directly. Intercepting it in the rewrite
layer also **prevents** the dangerous ALTER-misparse from ever reaching the parser.

```sql
DROP STATISTICS ON hits;                                  -- all sidecars for the table
DROP STATISTICS ON hits FOR COLUMNS UserID, RefererHash;  -- specific columns
```

**Drop is the safe direction.** Removing a sidecar reverts the scan to a full read —
fail-open, **no correctness risk**, no version-binding to get right (the inverse of the
stale-bloom landmine in §7). Semantics:

- Removes the named sidecar artifacts (or all, if no column list); idempotent —
  dropping absent stats is a success, not an error.
- Does **not** touch the data file (sidecars are separate objects, §6).
- Honest reporting: return the count of artifacts actually removed.

This is the only statement in the design that needs the tier-C pre-parse hook; build
(`ANALYZE … FOR COLUMNS`) parses natively and does not.

## 4. Artifact-selection rule (the "engine decides" policy)

The decision is **data-shape-driven and measured during the analyze scan itself** — no
guessing about future queries. For each named column, the scan computes NDV (KMV) and
per-row-group min/max span, then:

| Measured shape | Decision |
|---|---|
| High NDV **and** per-row-group min/max spans most of the domain (hash-distributed — `UserID`, `RefererHash`, `URLHash`) | **Build bloom sidecar** — the only skip mechanism that can help these |
| Already zone-map-prunable (clustered/ranged — `EventDate`, `CounterID`) | **Skip bloom** — zone maps already prune; bloom would be wasted |
| Low cardinality | **Skip bloom** — cheap to scan; bloom adds no pruning |
| **Any** column | **Always emit NDV + histogram** — small, cheap, broad optimizer benefit |
| Non-Parquet source (CSV/JSONL, no native stats) | **Add zone maps** as well |

The bloom trigger condition — *high NDV + wide per-RG min/max* — is exactly the shape we
measured on `UserID`/`RefererHash`: every row group's min/max spanned the full int64
range, so zone maps prune nothing and a per-row-group bloom is the only lever. The rule
reads the same statistics the scan already reads; it is not a heuristic guess.

**Granularity: per row group**, not per file — that is where the pruning win is for a
point lookup on an unclustered column (a given value lives in few row groups).

---

## 5. End-to-end flow

```
ANALYZE TABLE hits FOR COLUMNS UserID, RefererHash
   │
   ├─ do_sql_rewrite()            unchanged (no hint extraction needed)
   │
   ├─ sqloxide.parse_sql          → Statement::Analyze{ columns:[UserID,RefererHash] }
   │
   ├─ plan_analyze_query          *** EXTEND: read `columns`, attach to logical node ***
   │        → LogicalPlanStepType.Analyze{ table_name, columns }
   │
   ├─ physical: Table Management (analyze_table)
   │        *** EXTEND: if columns present → run artifact builder; else manifest refresh ***
   │
   ├─ artifact builder (NEW)
   │        • scan named columns once (off-GIL where possible)
   │        • measure NDV (KMV) + per-RG min/max span
   │        • apply §4 rule → {bloom?, ndv, histogram, zonemap?}
   │        • write sidecar + version-binding (§6)
   │        → NonTabularResult
   │
   └─ later queries: scan-side consumers (bloom prune / NDV / zonemap)
            load sidecar, check version-binding, use or fail-open (§7)
```

The only new component is the **artifact builder**; the loaders and pruners already exist.

---

## 6. Sidecar format & placement

- **Separate sidecar object**, never a Parquet rewrite. Rewriting embeds native blooms
  but touches the data file (expensive; impossible on read-only/remote buckets). A
  sidecar leaves source data untouched — required for GCS/immutable stores.
- **Placement:** sibling to the data file by convention (`hits_0.parquet` →
  `hits_0.parquet.opt`, or an `_opteryx/` sibling dir). Reuse the WP-8 NDV-sidecar
  manifest rather than inventing a second registry.
- **Contents (per data file):**
  - `binding`: exact data-file identity — etag / content-hash (preferred) or size+mtime.
  - `row_groups[]`: per-row-group bloom blocks for selected columns.
  - `columns{}`: NDV (KMV sketch), histogram, optional zone maps (non-Parquet).
  - `built_at`, `analyzer_version`, `column_set`.

---

## 7. Version-binding & correctness (the part that must be perfect)

A bloom is **fail-open by nature** for false positives (a wasted read), but a **stale
bloom that misses a newly-added value is a false negative → dropped rows → wrong count.**
Therefore:

1. Every sidecar is bound to an **exact data-file version** (etag / content-hash).
2. On scan, if the binding does **not match exactly**, the sidecar is **ignored** —
   fail-open to full scan. No heuristics, no grace window.
3. **"Possibly stale" == "absent."** Never prune on a sidecar that cannot be proven
   current.

This is the same posture the bloom-read path already takes when a file has no blooms; the
builder's job is only to **stamp the binding at write time** so the scan can verify it.

---

## 8. Open decisions

1. **Column scope default** — `FOR COLUMNS` is explicit; should bare `ANALYZE TABLE t`
   (no column list) also build skip structures via the §4 rule across all columns, or
   keep today's manifest-only refresh? (Recommend: keep bare ANALYZE as manifest refresh;
   skip structures require an explicit column list.)
2. **Overload `ANALYZE` vs new `OPTIMIZE` verb** — recommend `ANALYZE` (parses today,
   builder exists, `columns` is an honest field). Naming only; engineering is identical.
3. **Sidecar placement convention** — sibling file vs `_opteryx/` dir vs central
   manifest, and the remote-object naming. Tie to WP-8.
4. **Binding primitive** — etag/content-hash vs size+mtime, per storage backend.
5. **Build cost ceiling** — ANALYZE is a full column scan; acceptable as an explicit
   admin op? (WP-8 measured ~1.5 s / 30 KB for a 33 MB ClickBench file for NDV alone.)
6. **Drop surface** — `DROP STATISTICS ON t [FOR COLUMNS …]` (recommended, §3.1) is
   tier-C only. Confirm the verb/spelling and whether a non-SQL admin API
   (`session.drop_statistics(t, columns=…)`) should exist alongside it for tooling.

---

## 9. Rejected alternatives

- **Hint syntax on `ANALYZE`.** Tested: `WITH (…)`, `OPTIONS (…)`, `USING` are hard parse
  errors; `/*+ … */` parses but sqlparser **discards** it (hints attach only to `SELECT`).
  Carrying artifact options would require reading the comment in `do_sql_rewrite` or
  session `SET`. **Dropped** in favour of the data-driven §4 rule — no options to carry.
- **`OPTIMIZE TABLE … ZORDER BY (cols)`.** Parses today (dialect flag already on), but
  `zorder` means physical clustering — reading it as "columns to index" is a semantic
  bend, and `OptimizeTable` has no honest slot for options. ANALYZE's `columns` is the
  honest field.
- **`Dialect::parse_statement` hand-rolled grammar.** Can accept any input syntax, but
  must return an existing `Statement` variant — no honest home for options. Unnecessary
  once §4 removes the need for options.

---

## 10. What this does NOT fix

- **Q09–Q12 / Q14 (`COUNT(DISTINCT UserID)`)** — single-threaded aggregation CPU; lever
  is multi-core, not data-skipping. `UserID` is high-NDV everywhere, so even the bloom
  helps only point lookups, not the distinct counts.
- **Q40** — filter too weak (`CounterID=62` matches a large slice); cost is the residual
  + 5-column GROUP BY. A compute/parallelism problem.
- **Benchmark-methodology note.** ClickBench is conventionally run without custom
  secondary indexes. An `ANALYZE` pass improves real point-lookup latency unambiguously,
  but whether the resulting number is a fair ClickBench comparison vs the DuckDB baseline
  is a conscious judgment — this serves **production needle-query latency**, which is the
  right target for it.

---

## 11. Scope summary

| Component | State | Work |
|---|---|---|
| Parse `ANALYZE … FOR COLUMNS` | exists | none |
| `plan_analyze_query` reads `columns` | ignores today | **small extend** |
| Logical `Analyze` node carries columns | partial | **small extend** |
| Physical Table Management `analyze_table` | manifest stub | **extend: dispatch to builder** |
| Artifact builder (scan → measure → decide → write) | — | **new** |
| Sidecar format + manifest | partial (WP-8 NDV) | **extend** |
| Version-binding | — | **new, must be exact** |
| Bloom writer (per-RG) | — | **new** |
| Scan-side consumers (bloom/NDV/zonemap) | **exist** | none |
| `DROP STATISTICS …` pre-parse (tier-C) | — | **new** (only statement needing the hook) |
| Sidecar deletion + count reporting | — | **new, low-risk (fail-open)** |

The risk is concentrated in **version-binding** (§7), not in artifact construction or
deletion. Drop is fail-open and carries no correctness risk.
