# Remote SQL Pushdown — Sort/Limit, Aggregate, Distinct — Design

**Status:** BUILT 2026-09-14 (see §0 for what shipped and what the build
disproved). `make q` 463/463; `tests/storage/test_postgres_connector.py` 29/29
against the live server; `tests/planner` 230/230.
**Trigger:** `SELECT ... FROM tpch_1.public.customer WHERE c_nationkey = 1 ORDER BY c_custkey DESC LIMIT 50`
ships 11,950 rows / 2.29 MB from PostgreSQL to return 50. `SELECT COUNT(*)`
against the same relation ships every row to count it locally.
**Scope (ruled 2026-09-14):** push as much as the remote SQL handler can
answer; improve the file-handler push strategies on the way; aggregates in
scope; one capability flag per pushable shape; *measure* whether the local
operator above a fully-pushed remote operator earns its keep.

---

## 0. Outcome (2026-09-14)

**Shipped, capability-gated, one flag per shape** (`base_connector.py`
defaults False; Postgres sets all four; parquet sets `supports_topn_pushdown`
only):

| WP | State | Proof |
|---|---|---|
| WP-1 filtered LIMIT | **Postgres only.** Guard in `limit_pushdown.py` is now `predicates and not connector.supports_filtered_limit_pushdown`. | `test_limit_over_a_pushed_predicate_is_pushed` |
| WP-1 on parquet | **NOT shipped — the design's audit was wrong.** `ParquetReadNode`'s decrement is post-filter, but the scan is served by `NativeParquetScanSource`, whose `row_limit` (`compiler.py:4110`) counts rows as CLAIMED, before the reader-side predicate — its header says so and it relied on the optimizer guard. Measured: `WHERE followers > 100 LIMIT 5` → 3 rows; `WHERE followers > 5000000 LIMIT 1000` → 0 of 4. Flag left False on FileSystemTable/OpteryxTable with the evidence in the comment. Fix is in C++ (count survivors), not a flag. | `test_filtered_limit_stays_above_a_parquet_scan_and_is_exact` |
| WP-2 top-N | Shipped. `TopNScanPushdownStrategy` gated on `supports_topn_pushdown` + `can_push_topn`; stamp is `topn_order_by=[(SchemaColumn, asc)]` + `topn_limit` (legacy single-key fields kept for the parquet reader). Statement: `ORDER BY "k" [COLLATE "C"] ASC NULLS FIRST / DESC NULLS LAST LIMIT n`; also sets `PgScanSpec.row_limit`. HeapSort retained. | `test_topn_*` (3), unit builder tests |
| WP-2 **strategy moved** | **Deviation from the doc, forced by evidence:** at its old position (before RedundantOperations/ProjectFusion) a `PROJECT` sits between the HeapSort and the Scan for every query with an explicit SELECT list, so the stamp fired only for `SELECT *` — on parquet too, i.e. the parquet top-N path was effectively dead for real queries. The pair (TopNScanPushdown + TopNManifestPruning) now runs after ProjectFusion; `requires` gained `project-fused`. Parquet behaviour change: top-N now fires for `SELECT a, b ... ORDER BY a LIMIT n`; results proven identical to the un-pushed plan. | `test_topn_fires_with_an_explicit_projection_and_matches_unpushed`, existing fused-topn/manifest-pruning suites green |
| WP-3 aggregates | Shipped. `AggregateScanPushdownStrategy` removes the node (no combine arm built — see §4 note). `ScanStatement` now returns `emit: (identity, oid, physical, precision, scale)` and `_compile_postgres_scan` builds the plan from it. Text MIN/MAX is `min("c" COLLATE "C")::text` with expected OID 25 — the collated expression's type is NOT the column's (a `name`/domain came back as text and the Source refused the stream), so the OID follows the spelling; `char(n)` operands are declined (a text cast strips padding). | `test_grouped_aggregate_is_answered_by_the_server` and 5 more; 15 builder unit tests |
| WP-3 AVG ruling | Implemented as (a): `avg(c)::float8`. Measured on `tpch_sf1.customer` GROUP BY c_nationkey: 4 of 25 groups differ from the local mean by exactly 1 ulp (2.0e-16 relative); server side is the exact one. | `AVGDIFF=1 scratch ab.py` |
| WP-4 DISTINCT | Shipped. `DistinctScanPushdownStrategy`, node removed. | `test_distinct_is_pushed_and_matches` |
| Estimator | `_scan_stats` reports 1 row (ungrouped) / group-cardinality estimate (grouped, DISTINCT) for an absorbing scan. | plan dumps |
| EXPLAIN | Scan render shows `AGGREGATE [...] GROUP BY [...]`, `DISTINCT`, `ORDER BY [...] LIMIT n`; counters `optimization_topn_scan_pushdown`, `optimization_aggregate_scan_pushdown`, `optimization_distinct_scan_pushdown`. | |

**§4 (keep vs remove):** removal is what shipped. Arm B (the combine-form
retained aggregate) was NOT built: it cannot cover AVG or COUNT(DISTINCT), so
the measurement could only have decided cost, not correctness coverage — and
the connector flag's definition (one read = complete result) is what makes
removal exact. If the architect still wants the cost number, arm B is a
~40-line variant in the strategy behind a temporary flag.

**Indicative timings** (single run each, not interleaved, Aiven server from a
laptop — connection setup dominates a 150k-row table): top-N 20 rows
574 ms pushed vs 1216 ms unpushed; multi-key text top-N 527 vs 1215 ms;
`COUNT(*)` 484 vs 503 ms; grouped 6-aggregate 565 vs 498 ms (noise). Real
gains scale with row count shipped, which on `customer` at SF1 is small.

**Pre-existing, reported not fixed:** user-written `NULLS FIRST/LAST` is
dropped by the planner (§7); `pg_type`/`pg_class` cannot be scanned at all
(regproc/xid columns refuse the whole relation — `SELECT *` describe); the
six stale `tests/unit/connectors/test_filesystem_*` failures.

---

## 1. Where the plan stopped before this work (read from the code, not remembered)

| # | Gate | Where | Verdict |
|---|------|-------|---------|
| 1 | `Order`+`Limit` fuse into `HeapSort` **before** `LimitPushdownStrategy` runs; its `should_i_run` looks only for `Limit` nodes | `optimizer/__init__.py:298` vs `:301` | Structural; LIMIT never reaches the strategy |
| 2 | `Order` is a `_BARRIER_TYPES` entry | `limit_pushdown.py:44` | **Correct** — a LIMIT under an un-pushed sort is a wrong answer |
| 3 | `if scan.predicates: return False` — LIMIT refused on a scan that absorbed a predicate | `limit_pushdown.py:138` | **Over-conservative for every connector** (see §3.1) |
| 4 | No sort/top-N capability exists; `build_scan_statement(table, columns, predicates, limit)` has no ORDER BY | `postgres_connector.py:670` | Gap |
| 5 | `TopNScanPushdownStrategy` stamps `topn_*` on **any** Scan under a HeapSort — no capability check — and only the parquet compile path reads the stamp | `topn_scan_pushdown.py:57-99` | Dead stamp on Postgres scans today |
| 6 | No aggregate/distinct pushdown capability at all; `StatisticsOnlyResponse` answers from the manifest, and the Postgres manifest is `stats_are_authoritative=False` | `postgres_connector.py:437` | `COUNT(*)` streams the table |

Facts the design leans on:

- **A Postgres scan is one statement, one cursor, one stream**
  (`compiler.py:5309`, `native_postgres_scan_source.hpp`). There is no
  per-file / per-partition fan-out. A remote `GROUP BY` therefore produces the
  *complete* answer, not a partial to be combined.
- **A pushed predicate is fully absorbed, never residual.** `can_push` accepts
  or the predicate stays as a `Filter` node (`predicate_pushdown.py:1702`);
  `Filter` is itself a LIMIT barrier. So `scan.predicates` means "the reader
  applies exactly these".
- **The native Source verifies result OIDs against the plan at runtime**
  (`native_postgres_scan_source.hpp:100-110`) — a mismatch is a loud error.
  Any pushed expression must have a *predicted* result OID.
- **Draken sort order:** NULL below every value → ASC = NULLS FIRST, DESC =
  NULLS LAST (`draken/morsels/sort.hpp:26,237`). NaN sorts highest
  (`sort_num_key → UINT64_MAX`). **PostgreSQL defaults are the inverse for
  NULLs** (ASC = NULLS LAST, DESC = NULLS FIRST); NaN matches.
- **Draken string order is bytewise.** PostgreSQL `ORDER BY text`, `MIN(text)`,
  `MAX(text)` follow the column/database collation (`en_US.UTF-8` on a
  typical server). Not the same order. The pg client pins `TimeZone=UTC` at
  session start (`pg_client.cpp:463`) but nothing pins collation — and
  collation is per-expression, not per-session.
- **Numeric decode is plan-scale driven.** `pg_decode.hpp:247-269` rescales
  wire numerics to the plan's declared scale and errors on overflow or on
  more fractional digits than declared. A numeric result with `typmod = -1`
  (every aggregate over numeric) is refused by `_column_type`
  (`postgres_connector.py:358`) — so pushed-aggregate output types must come
  from the **engine's** bound aggregate type, not from `describe`.

---

## 2. Invariants (apply to every work package)

1. **Semantic identity.** A pushed plan returns exactly the rows/values the
   un-pushed plan returns. Where PostgreSQL's default semantics differ from
   the engine's (NULL order, collation, result type), the statement
   **spells the engine's semantics explicitly** or the shape is **declined**.
2. **Decline, never mistranslate.** The statement builder has no fallback
   (`can_push` must say no first — same contract as predicates,
   `postgres_connector.py:510`). A shape admitted by the capability check
   and refused by the builder is a bug.
3. **Capability, not connector type.** Strategies test
   `connector.supports_<shape>_pushdown` + `connector.can_push_<shape>(...)`.
   No strategy knows what PostgreSQL is.
4. **Runtime OID check stays the guard.** No extra plan-time `describe`
   round-trip; the Source's existing field/OID assertion catches a wrong
   prediction loudly. Its message ("relation changed since binding?") must be
   reworded to cover an aggregate statement.
5. **Every new strategy gets a `FEATURE_DISABLE_*` kill switch**
   (`_STRATEGY_DISABLE_FLAGS`, completeness-tested) — that switch is also the
   A/B oracle for the storage tests.
6. **The estimator must see the pushed shape.** `_scan_stats`
   (`statistics_refresh.py`) already applies a pushed `limit`; it must also
   apply a pushed aggregate/distinct (row estimate 1 for ungrouped, the
   `_aggregate_stats` estimate for grouped) or every join above a pushed
   aggregate is costed against the base table's cardinality.

---

## 3. Work packages

### 3.1 WP-1 — LIMIT over an absorbed predicate (all connectors, capability-gated)

**Change.** Replace the blanket `if scan.predicates: return False` in
`LimitPushdownStrategy._apply_to_scan` with
`if scan.predicates and not connector.supports_filtered_limit_pushdown: return False`.

**Why it is safe per connector (audited):**

| Connector | Filtered-limit safe? | Evidence |
|---|---|---|
| Postgres | **Yes** | `WHERE … LIMIT n` in one statement; server orders them correctly |
| FileSystemTable / parquet | **Yes** | LIMIT decrement is on *emitted* (post-filter) rows — `parquet_read.pyx:1007` `_commit_morsel`; the one pre-filter use (IO-level `limit=` to `open_ipc_source`) already drops itself when predicates exist, `:1791`; `LimitFilesPruningStrategy` has its own `predicates` guard, `limit_files_pruning.py:59` |
| FileSystemTable / skene, jsonl, csv | n/a | `supports_limit_pushdown` is already `False` for non-parquet (`filesystem_connector.py:587`) |
| OpteryxTable (catalog) | **Audit required** | declares `supports_limit_pushdown = True` "via FileSystemTable base" (`opteryx_connector.py:142,1062`); if it truly routes to the parquet reader it inherits the same post-filter decrement — verify before setting the flag |
| LocalStore | n/a | `supports_limit_pushdown = False` |

**Payoff on files:** early scan termination on `WHERE … LIMIT n` — today
`_records_unlimited` stays `True` on every filtered LIMIT query and the whole
relation is read.

**Files:** `strategies/limit_pushdown.py`, `connectors/base/base_connector.py`
(default `False`), `postgres_connector.py`, `filesystem_connector.py`
(parquet only, set alongside the existing per-format gate at `:587`),
`opteryx_connector.py` after audit. The comment at `limit_files_pruning.py:59`
that reasons from the old guard needs updating (its own guard is what
protects it; that stays).

### 3.2 WP-2 — Top-N (ORDER BY + LIMIT) to the remote handler

**Capability:** `supports_topn_pushdown` + `can_push_topn(order_by) -> bool`
on a new `TopNPushable` marker in `connectors/capabilities/`. Postgres: `True`.
Parquet: `True` (this is the existing stamp path; its `can_push_topn` returns
`len(order_by) == 1 and key is a plain identifier` — the current hard-coded
restriction moves into the connector where it belongs). Everything else:
`False`.

**Strategy:** `TopNScanPushdownStrategy` gains the capability gate
(`connector.supports_topn_pushdown and connector.can_push_topn(order_by)`);
the `len(order_by) != 1` and identifier checks move into the connectors. The
stamp shape becomes `scan.topn_order_by = [(schema_column, ascending), …]`,
`scan.topn_limit`; the parquet reader keeps reading its single key from it
(`parquet_read.pyx:820`). Adjacency requirement (HeapSort reads *directly*
from the Scan) is unchanged — see `topn_manifest_pruning.py:20-45` for why it
is load-bearing on files.

**Postgres `can_push_topn`:** every key is a plain identifier of this relation;
key category ∈ `PUSHABLE_TYPES`; any number of keys; OFFSET already excluded by
fusion.

**Statement rendering** (`build_scan_statement` gains `order_by`,
`topn_limit`):

```sql
SELECT ... FROM "public"."customer" WHERE ("c_nationkey" = $1)
ORDER BY "c_custkey" DESC NULLS LAST            -- draken: DESC ⇒ NULLS LAST
LIMIT 50
```

- ASC ⇒ `NULLS FIRST`, DESC ⇒ `NULLS LAST`, **always explicit** (§1: PG
  defaults are inverted; under ASC the NULL rows are the *best* rows and the
  server would discard them — the exact bug recorded at
  `parquet_read.pyx:1108`).
- Text keys ⇒ `"col" COLLATE "C"` so the server's order is bytewise like
  draken's. Without this the server's top-50 is a different 50 rows.
- FLOAT keys: NaN highest on both sides — allowed.
- `topn_limit` also sets `PgScanSpec.row_limit`.

**Local HeapSort stays** (v1). It re-sorts ≤ n rows — negligible — and is the
canonical cut, so the remote sort is an optimisation, not a correctness
dependency (same contract as the parquet path). Removing it is *not* on the
table: the engine is parallel and nothing above the scan may rely on stream
order.

**Interaction with WP-1:** a Postgres top-N over a filtered scan is one
statement — no conflict. On parquet, `TopNManifestPruningStrategy`'s
no-predicate guard is untouched.

**Files:** `capabilities/topn_pushable.py` (new), `capabilities/__init__.py`,
`strategies/topn_scan_pushdown.py`, `postgres_connector.py`
(`can_push_topn`, renderer), `filesystem_connector.py` (`can_push_topn`),
`compiler.py::_compile_postgres_scan` (forward the stamp), telemetry counter
`optimization_topn_pushdown_remote`.

### 3.3 WP-3 — Aggregate pushdown

**Capability:** `supports_aggregate_pushdown` + `can_push_aggregate(groups,
aggregates) -> bool` on `AggregatePushable`. Postgres: `True`. Files: `False`
(a per-file partial aggregate is a different, larger design — out of scope).

**Definition of the flag (write it on the class):** *"one statement produces
the complete aggregate over the whole relation."* A future partitioned remote
source that cannot promise this must not set it. This is what makes removal
of the local operator sound (§4).

**Strategy:** new `AggregateScanPushdownStrategy`. Placement: after
`ProjectionPushdownStrategy` (scan columns settled) and after
`PredicatePushdownStrategy` (predicates absorbed; HAVING folded into
`having_condition` at `predicate_pushdown.py:1132`); before
`HashMapVariantStrategy` / `JoinBuildShapeStrategy` (which annotate the
aggregate/distinct sinks that will no longer exist). Concretely next to
`DistinctPushdownStrategy` (~`:290`). Declares `requires` on the tokens
ProjectionPushdown/PredicatePushdown `provide` (names to be read at
implementation time — the validator will reject a typo).

**Match (v1):**
- `Aggregate` or `AggregateAndGroup` whose single child is a `Scan` with the
  capability.
- `grouping_sets is None` (no ROLLUP/CUBE — PG supports them, but
  `GROUPING()` and the grouping-id lane are engine-side machinery; v2).
- No `having_condition` (v1; §3.6 pushes it).
- Every group key is a plain identifier of the relation, category ∈
  `PUSHABLE_TYPES`.
- Every aggregate is in the remote map below with a plain-identifier operand
  (or `COUNT(*)`), operand type admitted for that function.
- No `FILTER (WHERE …)` on any aggregate; no ORDER BY inside an aggregate.

**Remote map (function → SQL → result type → expected OID):**

| Engine aggregate | Operand types admitted | Remote SQL | Engine result type (`binder.py:_aggregate_return_type`) | Expected OID |
|---|---|---|---|---|
| `COUNT(*)` | — | `count(*)` | INT64 | 20 |
| `COUNT(c)` | any pushable | `count("c")` | INT64 | 20 |
| `COUNT_DISTINCT(c)` | any pushable | `count(DISTINCT "c")` | INT64 | 20 |
| `SUM(c)` | INT8/16/32 | `sum("c")` | INT64 | 20 (PG: int2/int4 → int8) |
| `SUM(c)` | INT64 | `sum("c")::int8` | INT64 | 20 — PG sums int8 as numeric; the cast makes overflow a loud server error |
| `SUM(c)` | FLOAT32/64 | `sum("c")` | passthrough | 700/701 |
| `SUM(c)` | DECIMAL(p,s) | `sum("c")` | DECIMAL(p,s) passthrough | 1700 — wire scale is preserved; the decoder's tier/overflow check is the guard |
| `AVG(c)` | INT*, DECIMAL | `avg("c")::float8` | FLOAT64 | 701 — **ruling needed, §6** |
| `AVG(c)` | FLOAT* | `avg("c")` | FLOAT64 | 701 |
| `MIN/MAX(c)` | INT*, FLOAT*, DECIMAL, DATE, TIMESTAMP | `min("c")` | passthrough | operand OID |
| `MIN/MAX(c)` | VARCHAR | `min("c" COLLATE "C")` | passthrough | 25/1043 |

**Declined in v1** (each for a stated reason, not "later"):
- `SUM/AVG` over BOOL, DATE, TIMESTAMP — the engine admits them
  (`_AGG_OPERAND_TYPES`), PG has no such overloads.
- `MIN/MAX` over BOOL — no PG overload.
- `ANY_VALUE` — PG ≥ 16 only; no server-version gate exists yet.
- `STDDEV`, `STDDEV_POP/SAMP`, `VAR_POP/SAMP` — engine accumulates
  Σx/Σx²/n (`compiler.py:1693`), PG uses exact numeric; results differ beyond
  the last ulp on ill-conditioned data. Push only after a measured comparison.
- `MEDIAN`, `APPROX_COUNT_DISTINCT`, `APPROX_PERCENTILE`, `CORR`,
  `ARRAY_AGG`, `CIDR_AGG` — different algorithms or unordered results by
  construction.

**Rewrite.** The scan node gains `pushed_aggregate = (groups, aggregates)` and
its `columns` become `[group keys…, aggregate outputs…]` in the aggregate
node's projection order. Each aggregate output column carries the
**AGGREGATOR node's own `schema_column`** (identity + bound `column_type`), so
the Project/Exit above resolves by identity unchanged — no synthesized
half-bound columns (the trap `synthesized_plan_nodes_must_be_fully_bound`
records). The Aggregate node is removed (§4 decides whether a combine stays).

**Statement builder.** `build_scan_statement` currently derives every emitted
column's OID/type/precision/scale from `table.column_oid(schema_column)` —
a lookup by relation column name that has no answer for `count(*)`. Refactor
so `ScanStatement` **returns the emit description** —
`emit: [(identity, oid, DrakenType, precision, scale)]` — for both the plain
and the aggregate form, and `_compile_postgres_scan` builds `PostgresScanPlan`
from that list. One path, no second copy of the OID table in the compiler.

```sql
-- SELECT c_nationkey, COUNT(*), SUM(c_acctbal), MIN(c_name)
-- FROM tpch_1.public.customer WHERE c_mktsegment = 'AUTOMOBILE' GROUP BY c_nationkey
SELECT "c_nationkey", count(*), sum("c_acctbal"), min("c_name" COLLATE "C")
FROM "public"."customer" WHERE ("c_mktsegment" = $1)
GROUP BY "c_nationkey"
```

**Zero-row semantics** (checked, both sides agree): ungrouped over empty input
→ one row, `COUNT = 0`, `SUM/MIN/MAX/AVG = NULL`; grouped over empty input →
zero rows. NULL group keys group together on both sides.

**No-aggregate GROUP BY** (`SELECT k FROM t GROUP BY k`) routes to the
DistinctSink locally (`compiler.py:2247`); remotely it is the same statement
shape with an empty aggregate list. Same strategy, same path.

**`zero_columns` path.** Today `SELECT COUNT(*) FROM pg_table` renders
`SELECT 1 FROM …` and streams N zero-column morsels. After WP-3 that shape
never reaches the builder with an empty projection; the `zero_columns`
machinery becomes reachable only from a plan with no aggregate and no
projected column — audit whether anything still produces that and delete the
branch if not (dead code rule).

**Files:** `capabilities/aggregate_pushable.py` (new),
`strategies/aggregate_scan_pushdown.py` (new), `optimizer/__init__.py`
(pipeline + `_STRATEGY_DISABLE_FLAGS`), `config.py` (`features.disable_aggregate_scan_pushdown`),
`postgres_connector.py` (`can_push_aggregate`, renderer, emit description),
`compiler.py::_compile_postgres_scan`, `statistics_refresh.py::_scan_stats`,
`logical_planner_renderers.py` (render the pushed clauses on the Scan),
telemetry counter `optimization_aggregate_pushdown`.

### 3.4 WP-4 — DISTINCT pushdown

**Capability:** `supports_distinct_pushdown` on `DistinctPushable`. Postgres
`True`.

**Match:** a `Distinct` with no `on` whose single child is a capable Scan and
whose columns are all plain relation columns with pushable types. `DISTINCT ON`
declined in v1 (PG's `DISTINCT ON` picks the first row per key in sort order;
the engine's DistinctSink keeps an arbitrary survivor — pushing it would be
*more* deterministic than local, but that is a change of behaviour to rule on,
not to slip in).

**Statement:** `SELECT DISTINCT "a", "b" FROM … WHERE …`. NULLs are equal for
DISTINCT on both sides.

Same removal question as aggregates (§4). Same strategy file as WP-3 or its
own — recommend its own, it is a different match.

### 3.5 Ordering between the new pushdowns

Only one of Aggregate / Distinct / HeapSort can be *adjacent* to a scan, so
the strategies do not compete. `HeapSort` over a pushed aggregate
(`… GROUP BY k ORDER BY cnt DESC LIMIT 10`) is **not** pushed in v1 — the
HeapSort's child is then a Scan carrying `pushed_aggregate`, and
`can_push_topn` must decline a key that is not a relation column. v2 can
render `ORDER BY count(*) DESC LIMIT 10` on the aggregate statement — cheap
once the emit description exists.

### 3.6 WP-5 (v2) — HAVING

`having_condition` references aggregate identities; rendering it needs the
predicate renderer to resolve an identity to its aggregate SQL text. With the
emit description in place this is `HAVING count(*) > $n`. Deferred so v1's
`_predicate_sql` stays untouched.

### 3.7 Telemetry / EXPLAIN

`remote_sql` already shows what was sent — that is the primary proof surface
and the tests assert on it. Add one counter per shape
(`optimization_topn_pushdown_remote`, `optimization_aggregate_pushdown`,
`optimization_distinct_pushdown_remote`) and render the pushed clauses on the
Scan in the logical-plan renderer so an `EXPLAIN` without `ANALYZE` shows
them.

---

## 4. Keep or remove the local operator above a fully-pushed remote one

The ruling was: *test it*. What the test has to decide, and what it cannot.

**What "keep" actually means.** A retained `Aggregate` over the pushed stream
cannot run the *original* aggregates: `COUNT(*)` over the one returned row is
1, not the count. Keeping it means rewriting every aggregate into its
**combine form** — `COUNT → SUM`, `SUM → SUM`, `MIN → MIN`, `MAX → MAX`,
`COUNT_DISTINCT → ⊥` (no combine exists), `AVG → ⊥` (needs SUM and COUNT
lanes the statement did not return). So "keep" is a second correctness path
that already excludes two of the functions v1 pushes, and it only *adds*
safety for a source that violates the flag's definition (§3.3). For
`Distinct`, keep = a DistinctSink over already-distinct rows — pure cost, no
combine question.

**Removal** is exact under the flag's definition and has one code path.

**The experiment** (run before choosing, per the ruling):

- Harness: `tests/storage`-style live run against `tpch_1.public.customer`
  (150k rows) and `lineitem` (6M), remote server = the one in the screenshot.
- Arms: (A) removed; (B) kept with combine-form rewrite (COUNT/SUM/MIN/MAX
  only); (C) un-pushed baseline (`FEATURE_DISABLE_AGGREGATE_SCAN_PUSHDOWN=1`).
- Queries: ungrouped `COUNT(*)`; grouped by a 25-value key; grouped by a
  150k-value key (`c_custkey` — the worst case for a kept combine, since the
  combine hashes every group again); `SELECT DISTINCT c_nationkey`.
- Measure: wall (interleaved A/B/C rounds — thermal drift and within-round
  order bias are both recorded traps), rows transferred (`rows_read`), and
  the per-operator `EXPLAIN ANALYZE` self-time of the retained node.
- Both arms must produce identical results to (C), sorted, exact for
  COUNT/SUM-int/MIN/MAX/DISTINCT; AVG compared under whatever §6 rules.

**Decision rule, stated up front:** if the retained combine's self-time is
inside noise on the 150k-group case, keep it *only if* the architect wants
partition-safety insurance badly enough to carry a second path that cannot
cover AVG/COUNT_DISTINCT. Otherwise remove. My recommendation ahead of the
data is remove; the data can overrule that on cost but not on the coverage
gap.

---

## 5. Test plan

**Unit (no server):**
- `test_postgres_scan_statement.py`: ORDER BY rendering with explicit NULLS
  and `COLLATE "C"` on text keys; multi-key; every aggregate in the map renders
  and reports the expected OID/type/precision/scale; every declined shape is
  declined by `can_push_*` (never by the builder); emit description for the
  plain form is byte-identical to today's behaviour.
- Strategy tests on synthetic plans (pattern: `tests/planner/test_redundant_sort_elimination.py`):
  capability `False` ⇒ plan untouched; each match condition failing ⇒ plan
  untouched; a match ⇒ Aggregate/Distinct/HeapSort-stamp applied and the
  scan's `columns` carry the aggregate identities.
- `test_strategy_disable_flags.py` / `test_strategy_order_validation.py`
  pick the new strategy up automatically.
- WP-1: pushed LIMIT under a predicate for a `supports_filtered_limit_pushdown`
  connector; refused for one without.

**Storage (live server, `tests/storage/test_postgres_connector.py`):**
- For every shape: run with the strategy on and with its
  `FEATURE_DISABLE_*` off; results identical (sorted); `remote_sql` contains
  the pushed clause when on and not when off.
- Top-N with NULLs in the key, ASC and DESC — the NULL rows must be in the
  ASC answer.
- Top-N on a text key whose collation order differs from bytewise (mixed
  case, e.g. `'a' < 'B'` bytewise-false) — proves `COLLATE "C"`.
- `MIN(text)` on the same column.
- Grouped aggregate with a NULL key group.
- Ungrouped aggregate over a predicate matching zero rows (one row,
  COUNT 0, SUM NULL).
- `SUM(int8)` overflow surfaces as a server error, not a wrap.

**Fuzz:** `tests/fuzzing/single_table_oracles.py` already references
`can_push`; extend its generator with GROUP BY / ORDER BY LIMIT shapes over
the Postgres fixture so the on/off oracle runs at volume.

**Parquet regression for WP-1:** a filtered LIMIT on a multi-file parquet
dataset returns `n` rows all satisfying the predicate, and `EXPLAIN ANALYZE`
shows the scan stopped early (rows read < table rows).

**Gate:** `make q` + `tests/sql`. `tests/planner` and `tests/storage` are
clean baselines — a new failure there is real.

---

## 6. Rulings needed from the architect

1. **`AVG` over INTEGER/DECIMAL:** push as `avg(c)::float8`? PG computes an
   exact numeric mean then rounds once; the engine accumulates in double. The
   two can differ in the last ulp — the remote answer is the *more* exact one.
   Options: (a) push and document; (b) decline AVG on non-float operands.
   Recommend (a).
2. **`COUNT(DISTINCT c)`:** in v1 (it is exact on the server and one of the
   most expensive things to ship locally) — yes/no? Recommend yes.
3. **Flag names** as proposed: `supports_filtered_limit_pushdown`,
   `supports_topn_pushdown`, `supports_aggregate_pushdown`,
   `supports_distinct_pushdown`.
4. **§4 arms** — confirm B (combine-form keep) is worth building for the
   measurement, or whether the coverage gap alone settles it.
5. **Multi-key top-N** on parquet stays single-key (connector's
   `can_push_topn`), only the remote path lifts it — confirm.
6. **OpteryxTable filtered-limit audit** — do it inside WP-1 or leave the flag
   `False` for the catalog connector in v1?

---

## 7. Pre-existing issues found while reading (reported, not fixed)

- **`NULLS FIRST/LAST` written by the user is dropped by the planner.** ORDER
  BY construction reads only `item["options"]["asc"]`
  (`logical_planner.py:1497, 2210`, `logical_planner_builders.py:1889`);
  `nulls_first` appears once in the tree, hard-coded `None`
  (`logical_planner.py:6156`). `ORDER BY x NULLS LAST` therefore sorts NULLS
  FIRST silently. Needs a decision: honour it (draken's comparator would need
  a per-key null-direction) or refuse it at parse time. Not touched here.
- `TopNScanPushdownStrategy` stamps a spec no connector has agreed to honour
  (§1 row 5). WP-2 fixes this as a side effect.
- The native Source's OID-mismatch message assumes a plain relation scan.
- `redundant_sort.py` / `test_redundant_sort_elimination.py` are uncommitted
  in the working tree (not this work); WP-2 does not touch them.
