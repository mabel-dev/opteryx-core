# Recursive CTE (Fixpoint) Design

**Status:** DELIVERED (all phases, 2026-08-27) — architect rulings: span-jump in
`Engine::run()`, `WITH RECURSIVE` as the only SQL surface, `UNION` and
`UNION ALL` both supported, semi-naive evaluation as the only mode.

**Implementation deltas from the proposal as written** (each below in place):
1. The control step runs at the TOP of the span (entered before every pass),
   not after `last` — the anchor then seeds the loop through the identical
   path as every iteration (anchor pipelines append into DELTA; no prologue).
2. `UNION` dedup is 64-bit draken row-hash identity (`cxx_hash_c`) — the SAME
   sanctioned contract native DISTINCT runs on (DistinctSink) — not
   hash-plus-value-confirm.
3. Anchor/term type unification is a LOUD BIND-TIME REJECTION naming the
   column and the cast to write — no automatic widening. Honest and explicit;
   auto-widening can be layered on later if wanted.
4. The recursive legs ride the existing shared-CTE rail end to end: they are
   ordinary `shared_ctes` entries (anchor immediately before term) and the
   self-reference is an ordinary `MaterializedCteRef`; only the compiler
   swaps its buffer to WORKING. `plan.recursive_ctes` is pure metadata.

Phase 4 (delivered): EXPLAIN renders each recursive CTE as its own section —
`RECURSIVE CTE <name>` with ANCHOR and RECURSIVE TERM subtrees — and under
ANALYZE the header carries the fixpoint readings (`UNION, 3 iterations,
4 distinct rows, ceiling 1000`) from the engine's per-LoopSpan harvest
(`collect_loop_stats`, surfaced as `telemetry._reading["recursive_loop_stats"]`).
The LoopSpan carries the CTE's declared name, so the ceiling error names it too.
Fixed in passing (it blocked the phase): EXPLAIN ANALYZE's inner run copies the
plan and `Graph.copy()` drops instance attributes — `shared_ctes` /
`recursive_ctes` are now re-carried onto the copy, which also repairs EXPLAIN
ANALYZE for ordinary shared CTEs. Ruled 2026-08-27: result order without
ORDER BY is UNSPECIFIED (the engine-wide UNION ALL contract); no
iteration-order determinism will be added.

Tests: `tests/unit/execution/test_engine_loop_span.py` (hand-built NativePlan:
loop mechanics, ceiling, empty anchor) and `tests/sql/test_recursive_cte.py`
(the SQL rail: traversal on DAGs and cyclic graphs, UNION/UNION ALL, fibonacci
carried state, EXPLAIN structure and readings, every rejection by message
form).

**Motivation:** graph traversal (reachability, hierarchy expansion) is a
fixpoint problem. The engine has never had a fixpoint primitive, so recursive
CTEs are rejected at logical planning today
(`opteryx/planner/logical_planner/logical_planner.py`, `extract_ctes`).

---

## 1. Semantics being implemented

```sql
WITH RECURSIVE r (cols...) AS (
    <anchor term>          -- no reference to r
    UNION [ALL]
    <recursive term>       -- exactly one reference to r
)
SELECT ... FROM r ...
```

Evaluation (semi-naive, the only mode):

1. Run the **anchor**. Its output is the first slice of RESULT and iteration
   0's WORKING table (the frontier).
2. Run the **recursive term** with the self-reference bound to WORKING —
   never to the accumulated RESULT. Output is DELTA.
3. `UNION`: DELTA := DELTA minus rows already in RESULT (persistent visited
   set). `UNION ALL`: DELTA unchanged.
4. If DELTA is empty → done; RESULT is the CTE's value. Otherwise append
   DELTA to RESULT, WORKING := DELTA, go to 2.
5. An iteration ceiling bounds step 4 (see §6). Hitting it is a loud error,
   never truncation.

Semi-naive is not an optimization to add later: WORKING **is** the previous
delta by construction, which is what makes graph traversal a frontier
expansion (BFS) instead of quadratic re-expansion. On a cyclic graph,
`UNION` terminates via the visited set; `UNION ALL` does not terminate and
is stopped by the ceiling — that is the correct, standard behaviour.

---

## 2. Engine: span-jump in `Engine::run()`

`Engine::run()` (src/cpp/engine/engine.hpp) executes `pipelines` strictly
one at a time, in creation order, on the driver thread. The fixpoint is a
**backward jump over a contiguous span of pipelines**, decided between
pipelines on the driver thread — the same place the runtime min/max join
filter publishes, with the same no-atomics justification.

### 2.1 Loop descriptor

A new engine-level structure, filled by the compiler:

```
struct LoopSpan {
    size_t first;          // index of first pipeline in the recursive term
    size_t last;           // index of the loop-control point (the delta sink pipeline)
    int    working_buf;    // buffer id the recursive term's leaf reads
    int    delta_buf;      // buffer id the recursive term's sink appends to
    int    result_buf;     // buffer id consumers of the CTE read
    bool   distinct;       // UNION (true) / UNION ALL (false)
    int    visited_set;    // engine-owned row-hash set id, -1 for UNION ALL
    uint32_t max_iterations;
    uint32_t iterations_run;   // telemetry, filled by run()
};
std::vector<LoopSpan> loops;   // v1: at most one per Engine (see §7 nesting)
```

`run()`'s main loop gains one check after each pipeline completes: if
`pipeline_index == loop.last`, execute the loop-control step:

```
control step (driver thread, between pipelines — no concurrency):
    if distinct: delta_buf ← delta_buf \ visited_set   (see §4)
    if delta_buf row count == 0:
        release working_buf, delta_buf
        pipeline_index = loop.last + 1        // fall through, loop done
    else:
        append delta_buf's morsels to result_buf      (pointer moves, no copy)
        swap: working_buf's contents ← delta_buf's    (delta becomes the frontier)
        delta_buf reset to empty
        if ++iters > max_iterations: err = RecursionCeiling (loud, named)
        pipeline_index = loop.first           // jump back
```

Nothing about `run_pipeline`, workers, sinks, or sources changes. Each
re-entry into the span constructs fresh `GlobalSourceState` per pipeline —
exactly what already happens when N `CteRef` pipelines re-read one buffer —
so re-running a `BufferSource` pipeline over the swapped WORKING buffer is
the existing mechanism, not a new one.

### 2.2 Buffer lifetime — the one real trap

`run()` frees each buffer after its **last consumer by pipeline index**
(`last_consumer[]`). A backward jump breaks that: a buffer read at index
*i* inside the span would be released on the first pass and read again on
the second. Rule:

> **No buffer whose `reads_buffer` consumer lies inside `[first, last]` may
> be released until the loop exits.** Liveness treats the span as ONE
> pipeline at index `last`.

Concretely: when computing `last_consumer`, any consumer index inside a
loop span is recorded as the span's `last`. WORKING/DELTA are released by
the control step at loop exit; RESULT is released by the normal rule (its
consumers are all after the span).

This also covers non-CTE buffers read inside the span (a join build buffer
feeding the recursive term, a shared CTE the recursive term references):
they stay alive for the whole loop. Correct, and the memory cost is what
the semantics require.

### 2.3 Buffer mechanics

`MorselBuffer` (pipeline_buffers.hpp) already supports append (per-worker
`Writer`), claim-based read, `release()`, and spill. Additions:

- **move-append** (RESULT ← DELTA): splice DELTA's resident morsel pointers
  into RESULT — shared_ptr moves, no data copy. If DELTA has spilled, its
  spill units transfer ownership the same way.
- **swap/reset** (WORKING ⇄ DELTA): swap the internal piles, then clear the
  new DELTA. Both are driver-thread-only operations between pipelines; no
  locking beyond what MorselBuffer already holds.
- Spill applies to RESULT (the buffer that grows). WORKING/DELTA are one
  frontier each — they use the same MorselBuffer type and inherit spill for
  free, but the expected case is resident.

### 2.4 Early exit and errors

The existing between-pipelines checks (`out_q->closed()`, `err.code != 0`)
already run between iterations because the control step sits between
pipelines. LIMIT short-circuit and cursor-drop therefore stop the loop
promptly with no new code. The recursion ceiling raises through `err` /
`errslot` like every other native error, naming the CTE and the ceiling
value, and telling the user how to raise it.

---

## 3. Compiler (`managers/execution/compiler.py`)

Today `compile_to_native` compiles `plan.shared_ctes` bodies first, each
into pipelines ending in `set_buffer_append_sink(pipeline, buf)`, and
`CteRefNode` lowers to a pipeline reading that buffer (`cte_buffers`).

A recursive CTE compiles as:

1. **Anchor body** → pipelines appending to RESULT, then a driver-side
   copy of the anchor's output into WORKING (compile-emitted, part of the
   loop prologue; for `UNION` the anchor's rows also seed the visited set —
   §4).
2. **Recursive term body** → pipelines. Its self-reference leaf is a
   `BufferSource` over WORKING (a `RecursiveRefNode`, sibling of
   `CteRefNode` — like `CteRefNode` it never executes; it is compiler
   configuration). The body's head gets `set_buffer_append_sink` into
   DELTA.
3. `nplan.add_loop_span(first, last, working, delta, result, distinct,
   max_iterations)` — the one new NativePlan builder method.
4. Every reference to the CTE in the main query lowers exactly like a
   `CteRefNode` today: a `BufferSource` pipeline over RESULT with the
   reference's own column identities. Multiple references cost nothing new.

Column layout contract: anchor layout defines RESULT/WORKING/DELTA layout.
The compiler asserts the recursive term's output layout matches positionally
(types already unified by the binder, §5) — a mismatch here is an internal
error, not a user error.

Recursive CTEs are **forced** into the materialized path regardless of
reference count — `shared_cte.py`'s multiply-referenced heuristic does not
apply; there is no inline-expansion alternative for a recursive body.

---

## 4. UNION: the visited set

`UNION` needs "rows already in RESULT" membership that persists across
iterations. Design:

- An engine-owned row-hash set keyed on the full row (all CTE columns),
  using the existing native row-hash machinery (`compute_row_hashes` /
  carchar's hash-set substrate — `opteryx/compiled/structures/carchar_set.pyx`
  and the C++ it fronts).
- Seeded from the anchor's output **with the anchor itself deduplicated**
  (`UNION` semantics dedupe the anchor too).
- The control step probes DELTA against the set, keeps only novel rows
  (building a selection, not copying data where a morsel survives intact),
  and inserts the survivors.
- Hash-equal is not equal: probes confirm on value comparison, same as
  every other hash structure in the engine. No parity shortcuts
  (feedback ruling: hash-no-parity).

The set lives in the `LoopSpan`, is driver-thread-only (control step), and
is released at loop exit. v1 sizing: grow-on-demand with the standard
presizing rules; it holds every distinct row of the CTE, which is the same
order of memory as RESULT itself.

Running dedup in the control step (serial, driver thread) rather than as a
pipeline operator is deliberate: it needs the cross-iteration set, it runs
once per iteration on frontier-sized data, and it keeps the recursive term's
pipelines completely ordinary. If profiling ever shows the serial probe as
the wall, parallelizing it is an isolated change inside the control step.

---

## 5. Planner and binder

### 5.1 `extract_ctes` (logical_planner.py)

The blanket `RECURSIVE` rejection is replaced by structural validation. The
parser already delivers `with.recursive: true` and `cte_tables`; each
recursive CTE's query body must be a set operation. Split it at the
**topmost** `UNION` / `UNION ALL` into anchor and recursive term. Reject,
loudly and specifically, at this stage or at bind:

- CTE under `WITH RECURSIVE` whose body is not `anchor UNION [ALL] term`
  (a CTE in the list that doesn't reference itself is fine — it plans as an
  ordinary CTE; `RECURSIVE` is permission, not obligation).
- Self-reference in the **anchor** term.
- Zero or multiple self-references in the recursive term (v1: exactly one).
- Mutual recursion (CTE A references B which references A).
- Self-reference under an aggregate, window, `ORDER BY`/`LIMIT` sub-scope,
  or on the null-padded side of an outer join within the recursive term.
- Nested recursive CTEs (a recursive CTE whose body contains another
  `WITH RECURSIVE`) — v1 supports one loop span per plan (§7).

Each rejection names the construct and the CTE. No silent acceptance of a
shape the engine will compute wrongly.

### 5.2 Binding the self-reference

Two-phase bind of the recursive CTE:

1. Bind the **anchor** alone → its schema (with the CTE's declared column
   aliases applied) is the CTE's schema.
2. Synthesize the working-table relation with that schema and bind the
   **recursive term** against it. The self-reference resolves to a
   `RecursiveRefNode` leaf carrying the CTE key and the working schema.
3. Unify each column's type across anchor and recursive term with the
   standard `find_compatible_type` rules. Widening is applied as an
   **explicit cast on the term that needs it** — visible in EXPLAIN, never
   silent. If unification widens the anchor's type, the anchor is re-bound
   /cast to the unified schema (the working table's schema is the unified
   one; iteration 1's frontier must already be in final types).

### 5.3 Relation resolver

The resolver's cycle detection (relation_resolver/`__init__.py`) stays
exactly as strict as today with ONE carve-out: a self-reference inside a
declared recursive CTE resolves to the `RecursiveRefNode` leaf instead of
splicing the body — so no cycle exists in the plan graph, and the
termination machinery (path tagging, MAX_EXPANSION_DEPTH) is untouched.
View cycles and undeclared self-references stay fatal with the current
error.

### 5.4 Logical plan shape

One new logical node pair mirroring the shared-CTE architecture: the
recursive CTE body is carried off to the side (like `shared_ctes`) as an
(anchor plan, recursive-term plan, distinct flag, ceiling) bundle keyed by
CTE key; references in the main plan are leaf nodes. The optimizer treats
`RecursiveRefNode` like `CteRefNode`: a leaf with a schema. Predicate
pushdown does **not** push into the recursive body in v1 — pushing a filter
inside a fixpoint changes which rows recur and is a correctness trap
(filtering the frontier ≠ filtering the result). Filters land above the
reference. Revisit only with a proof obligation, per the
eligibility-gates-must-be-provably-safe ruling.

Cost estimation v1: the estimator has no fixpoint model. Estimate the CTE's
cardinality as anchor-estimate × a fixed small multiplier, and mark the
estimate lingo accordingly (it is an *estimate*, per the stat-lingo
contract). Honest and crude beats fabricated precision.

---

## 6. Guards and configuration

- `MAX_RECURSION_ITERATIONS` — engine config, no prefix on the env var
  (config convention), default **1000** iterations. Exceeding it:
  `RecursionCeilingError` naming the CTE, the ceiling, and the config knob.
  Never truncation, never a warning-and-partial-result.
- RESULT buffer spills under the standard MorselBuffer contract; the
  visited set does not spill in v1 — if it exhausts memory that is a real
  OOM on a real workload and we want to see it, not mask it.
- Telemetry: iterations run, rows per iteration (first/last/total), visited
  set size, per-iteration wall time — through the existing pipeline
  telemetry rows (each re-run of the span produces pipeline readings;
  loop-aware aggregation groups them by span). Telemetry, not trace;
  trace spans stay in the trace channel.

---

## 7. Explicitly out of scope (v1)

| Deferred | Why |
|---|---|
| Nested / mutual recursion | one loop span per engine; span-jump nesting needs a loop stack — rejected at bind until wanted |
| Aggregates/windows in recursive term | non-standard, semantics contested across engines |
| Predicate/projection pushdown into the body | correctness trap; needs per-case proof |
| Graph-syntax sugar (SQL/PGQ `MATCH`) | surface ruling: `WITH RECURSIVE` only; sugar can lower onto this later |
| Depth/path pseudo-columns | expressible in plain SQL (`depth + 1`, array append) on top of this primitive |
| Parallel visited-set probe | control step is serial by design; revisit only on profile evidence |

---

## 8. Test obligations

SLT + regression coverage before the feature is "done":

- Transitive closure on a DAG (`UNION ALL`) — exact rows, exact counts.
- Reachability on a **cyclic** graph (`UNION`) — terminates, exact visited
  set; same graph under `UNION ALL` → clean ceiling error.
- Anchor-only fixpoint (recursive term immediately empty) and empty anchor
  (zero iterations; empty result with correct schema — the courtesy-morsel
  path must produce the CTE's schema).
- Type widening across anchor/recursive term (INT anchor, DOUBLE step;
  VARCHAR/decimal cases per the find-compatible-type memory).
- Multiple references to one recursive CTE; recursive CTE joined to
  ordinary CTEs; recursive CTE under LIMIT (early-exit stops iterating).
- Every §5.1 rejection: asserted error message forms (slt-error contract).
- NULL keys in the visited set (NULL-row dedup follows `UNION` semantics:
  NULLs compare equal for dedup).
- Depth-column pattern (`SELECT n+1 ... WHERE n < k`) — the idiom users
  will actually write for bounded traversal.
- Ceiling telemetry visible; `EXPLAIN` renders the loop structure.

## 9. Build order

1. **Engine**: `LoopSpan`, control step, loop-aware buffer liveness,
   move-append/swap on MorselBuffer. Hand-built NativePlan test (no SQL)
   proving fixpoint, swap, release, ceiling. `UNION ALL` only.
2. **Compiler + planner**: `extract_ctes` split, two-phase bind,
   `RecursiveRefNode`, resolver carve-out, `add_loop_span` emission.
   End-to-end `UNION ALL` SQL green.
3. **Visited set**: `UNION` dedup in the control step; cyclic-graph tests.
4. **Guards + telemetry + EXPLAIN**, full SLT matrix, docs.

Each phase lands behind `make q` green; no phase leaves a half-wired path
reachable from SQL (a shape phase N doesn't support yet keeps a loud
bind-time rejection naming what's missing — the cutover posture).
