# ClickBench Harness Design — Measure the Engine, Not the Harness

Status: **proposed** — decision points in §6 are the architect's.
Scope: `tests/performance/clickbench/` (local dev loop) and the upstream
`ClickHouse/ClickBench` `opteryx/` entry (published result).

---

## 1. The problem

We run two ClickBench harnesses and neither measures only the engine.

| Harness | Command | What it feeds |
|---|---|---|
| Local | `make clickbench` | The daily optimisation loop; "did that change help?" |
| Upstream | ClickBench `opteryx/` entry | The published ×11.14 on the public chart |

Both charge engine time for work the engine does not do in production, and
neither proves the answers were right. A harness that inflates our number wastes
optimisation effort chasing its own overhead; a harness that cannot tell a wrong
answer from a right one can report a speedup that is actually a correctness
regression.

**The governing principle: every millisecond on the clock must be attributable
to parse, bind, optimise, plan, execute, or morsel consumption. Nothing else.**

---

## 2. What each harness currently charges us for

### 2.1 Local runner (`tests/performance/clickbench/opteryx/runner.py`)

```python
start = time.monotonic_ns()
session = opteryx.session()                       # <-- inside the clock
for _ in session.execute_to_morsels(statement):
    pass
elapsed_ms = (time.monotonic_ns() - start) / 1e6
```

1. **`opteryx.session()` is inside the timed region.** Session construction is
   charged to every query, on every iteration. In production a session is built
   once and reused across queries.
2. **A fresh session per iteration discards every warm engine cache** — plan
   cache, footer/metadata cache, catalog state. "Iteration 2" and "iteration 3"
   are therefore not warm engine runs; only the OS page cache is warm. The
   iteration count buys us noise reduction and nothing else.
3. **The timing runner validates nothing.** Result checking lives in a separate
   battery (§4.1) that defaults to the `tiny` dataset and asserts only a
   `VERIFIED` subset. Nothing connects the two: at full scale, a change that
   makes a query return zero rows still scores as a large speedup here.
4. **`pytest.skip` on `UnsupportedSyntaxError`** (line 105-107) turns an
   unrunnable query into a silent pass in the pytest path. Note also that
   `test_sql_battery` is parametrised with the raw `{DATASET}` placeholder
   unsubstituted — see §9.
5. **Iteration ordering concentrates thermal drift.** All iterations of Q1 run
   consecutively, then all of Q2. Drift over the suite therefore lands unevenly
   on specific queries instead of being spread across all of them — the same
   class of error already recorded against A/B benchmarking.
6. **Baseline comparison is not interleaved.** DuckDB times come from a stored
   JSON captured in a different session at a different thermal state. Comparing
   a live run to a frozen baseline is exactly the A/B shape we have already
   ruled unsafe.
7. **`--iterations` defaults to 2** while its help text says 3 and the table
   header prints three iteration columns.
8. **`--variant skene` resolves to the parquet dataset** (§1 defect report).
   `Dataset.FULL_SPLIT_SKENE` is dead.
9. **No provenance recorded.** No git SHA, build flags, core count, dataset
   identity, or thermal state is captured with the numbers.

### 2.2 Upstream entry (`ClickBench/opteryx/query`)

```python
start = timeit.default_timer()
res = opteryx.query(query)
for method in ("arrow", "to_arrow_table", "fetchall"):
    fn = getattr(res, method, None)
    if callable(fn):
        try:
            out = fn()
            break
        except Exception:
            continue
end = timeit.default_timer()
```

1. **A full PyArrow table is materialised inside the timed region.** Our peers
   print results from their own memory; we build a second columnar copy in a
   foreign library that the engine is otherwise banned from touching. This is
   pure harness tax and it is unique to us among the Parquet-mode entries.
2. **The measured code path is chosen by a `getattr` ladder with a silent
   `except: continue`.** Which API gets timed depends on what happens not to
   raise. Both patterns are banned by the engineering contract, and here they
   also mean we cannot state what was measured.
3. **`opteryx.query()` rather than a session + morsel loop** — a different entry
   point from the one the local runner measures, so the two harnesses are not
   measuring the same thing.
4. Correct as-is: the timer starts *after* `import opteryx`, so interpreter and
   import cost are excluded. Keep this.
5. Correct as-is: `BENCH_RESTARTABLE=no` with no-op `start`/`stop` matches how
   DuckDB, Hyper, DataFusion and chDB are classified. We are not being treated
   worse than our peer group.

---

## 3. The timing contract

One contract, obeyed by both harnesses, stated in the upstream entry's README so
the published number is self-describing.

> **A query timing is the wall-clock interval from submitting SQL to an
> already-constructed session, until the final morsel of the result has been
> consumed.**

**Inside the clock:** parse, rewrite, bind, optimise, physical planning, plan
handoff to the native engine, execution, morsel production and consumption.

**Outside the clock:** process start, module import, session construction,
result formatting or serialisation for display, GC invoked by the harness,
dataset staging.

Two consequences worth stating explicitly:

- **Morsel consumption is inside the clock.** We do not stop the timer at plan
  handoff or at first morsel. A lazily-executing engine must pay for its
  laziness.
- **Rendering is outside the clock.** The engine's output contract is morsels;
  turning morsels into TSV for stdout is presentation. The upstream `query`
  script must still print results — it just prints them after the timer stops.
  This is a small asymmetry in our favour versus DuckDB's `.timer`, which
  includes console rendering. **It must be declared in the entry README**, not
  left for a reader to discover.

---

## 4. Changes

### 4.1 Correctness gate — it already exists; the gap is coverage, not machinery

An earlier draft of this document proposed building a golden-result gate from
scratch. That was wrong: `tests/integration/sql_battery/test_battery_clickbench_results.py`
already does it — DuckDB-generated goldens via `dev/clickbench/generate_golden.py`,
order-insensitive and float-tolerant comparison, keyed `qNN` against the same
`STATEMENTS` list the timing runner owns. 34 queries pass today.

The real gaps are:

- **It defaults to the `tiny` dataset.** Only the tiny golden is checked in. A
  query can be correct on `testdata/clickbench_tiny` and wrong on the 100M-row
  `scratch/hits` — different row-group counts, different dictionary/dense
  transitions, different spill behaviour. The timing runner reports numbers at
  full scale for queries whose answers have only ever been checked at toy scale.
- **Only the `VERIFIED` subset is asserted.** Whatever sits in `EXCLUDED` is
  timed by `make clickbench` and checked by nothing.
- **The two harnesses never meet.** Nothing makes a timing run depend on the
  correctness run having passed for the same dataset.

Work required, in order:

1. Generate and check in the **full-dataset golden** for `scratch/hits`.
2. Shrink `EXCLUDED` to zero, or record per query why it cannot be verified.
3. Have the timing runner refuse to publish a headline for any query not in
   `VERIFIED` for the dataset it just ran against — report the timing marked
   UNVERIFIED and exclude it from the sum.

Step 3 is the one that closes the hole: a timing whose answer was never checked
should not silently contribute to a total anyone quotes.

### 4.2 Timing contract implementation

- **Local:** hoist `opteryx.session()` out of the timed region. Construct one
  session for the whole battery; the loop times only
  `execute_to_morsels(...)` drain. If per-query session isolation is wanted for
  other reasons, construct the session before `start = monotonic_ns()`.
- **Upstream:** replace `res.arrow()` and the `getattr` ladder with the same
  session + `execute_to_morsels` drain the local runner uses. Format for stdout
  after the timer stops. One pinned API, no probing, no `try/except` around the
  measured call.
- Both harnesses then measure the same code path, so a local improvement is
  predictive of the published number.

### 4.3 Ordering and statistics

- **Interleave iterations round-robin**: round 1 runs Q1…Q43, round 2 runs
  Q1…Q43, and so on. Thermal drift then applies roughly equally to every query
  instead of concentrating on whichever queries ran during the ramp.
- **Report min, median and spread per query.** Flag any query whose
  (max−min)/min exceeds a threshold (suggest 15%) as unstable — an unstable
  query's min is not a usable signal.
- **Raise default iterations to 3** and make the help text, default, and table
  width agree.
- **A/B against another build must be interleaved**, alternating arms within
  each round, never live-run-versus-stored-JSON. The stored DuckDB baseline
  stays for orientation only and must be labelled as such in the output — it is
  not an A/B result.

### 4.4 Provenance

Every run emits a header and writes it into the results JSON: git SHA, dirty
flag, Opteryx version, build flags (LTO/PGO), Python version and GIL state,
resolved dataset enum *and* its on-disk path, physical core count, and the
allocator preload in use. A number without this is not reproducible and should
not be quoted.

### 4.5 Fix the variant wiring

`VARIANT_DATASETS["skene"]` must resolve to `Dataset.FULL_SPLIT_SKENE`, and the
runner must assert the resolved dataset path exists and is non-empty before
running, failing loudly if not. A variant that silently falls back to another
dataset is the failure mode we just found; the assert is what stops it
recurring.

---

## 5. Code organisation

One implementation of the timing contract, not two.

- Move the driver — timing contract, interleaving, validation, statistics,
  provenance — into `dev/clickbench/driver.py`. `dev/` is not packaged and never
  imported by production code, which is correct for benchmark tooling.
- `tests/performance/clickbench/opteryx/runner.py` becomes a thin front-end:
  the query list, dataset selection, and terminal formatting.
- The upstream entry's `install` script fetches the same `driver.py` from a
  pinned tag of this repo. The published entry then runs the same measurement
  code as the local loop, and there is no second implementation to drift.
- If vendoring proves necessary instead of fetching, a test in this repo must
  diff the vendored copy against `dev/clickbench/driver.py` and fail on
  divergence.

---

## 6. Decision points for the architect

### 6.1 Process posture upstream — measure before choosing

ClickBench supports two shapes for an entry, both legitimate and both already in
use by systems on the chart:

- **A — embedded (status quo).** `BENCH_RESTARTABLE=no`; a fresh process per
  try. Matches DuckDB, Hyper, DataFusion, chDB. Every try pays cold plan cache
  and cold footer cache.
- **B — in-process server.** A long-lived process holding a session, with a thin
  `query` client, as polars/pandas/daft already do. Our data is Parquet on disk,
  so we would be `BENCH_RESTARTABLE=yes` + `BENCH_DURABLE=yes`: the harness
  restarts us before try 1 (genuinely cold) and tries 2–3 hit a warm process —
  exactly the treatment ClickHouse, Umbra and Postgres receive.

B is closer to how Opteryx is actually deployed (a long-lived service), and it
measures engine steady state rather than per-process re-initialisation.

**But the size of the prize is unknown, so it is not yet a decision.** Phase 0
below measures what per-process re-init actually costs across the 43 queries. If
it is under a few percent, B adds a moving part for nothing and A stays. If it
is material, B is on the table — and if adopted, it must be declared in the
entry README, and `start` must launch a real service and nothing else. No
pre-warming, no query-result caching, no touching the dataset during `start`.

### 6.2 Rendering exclusion

§3 excludes result rendering from the clock. Confirm that is the posture we
publish, given DuckDB's `.timer` appears to include it.

### 6.3 Full-scale golden

DuckDB-as-oracle is already settled by the existing battery. The open question is
whether to check in a full-dataset golden for `scratch/hits` (§4.1) — it is a
large artefact and it pins us to a specific dataset build. The alternative is
generating it on demand into `scratch/`, which keeps the repo small but means
the gate only runs where the dataset exists.

---

## 7. Phasing

| Phase | Work | Status |
|---|---|---|
| 1 | Local harness: variant wiring (§4.5), session off the clock (§4.2), interleaving + statistics (§4.3), provenance (§4.4), dead `--warm` branch removed | **done** — `make clickbench`, `clickbench-skene`, `clickbench-profile` |
| 2 | Full-scale golden and UNVERIFIED gating (§4.1) | not started — needs §6.3 |
| 3 | Upstream entry: timing contract, drop PyArrow materialisation and the `getattr` ladder (§4.2) | not started |
| 4 | Driver extracted to `dev/clickbench/` so both harnesses share one implementation (§5) | not started — only worth doing alongside phase 3 |
| 5 | Process-posture decision (§6.1) and upstream PR | not started — needs the phase 3 measurement first |

Phase 2 will make the suite look worse. That is the point: the current green
includes queries whose answers have only been checked at toy scale.

---

## 8. Expected effect on the published number

Phase 0 measures this rather than assuming it, but for scale: the PyArrow
materialisation is the only tax we carry that our Parquet-mode peers do not.
Across the four engines that appear on the chart in both native and Parquet
mode, the format switch alone costs 2.4–3.1×, which places Opteryx's ×11.14
Parquet result in the same band as a ×3.6–4.6 native engine. The honest gap to
DuckDB-on-Parquet (×5.41) — our nearest architectural peer — is 2.1×.

Removing harness tax will not close that gap. It will stop us optimising against
a number that includes work the engine does not do.

---

## 9. Found while doing this, not fixed — needs direction

**`test_sql_battery` runs with the dataset placeholder unsubstituted.** The
`@pytest.mark.parametrize` on `STATEMENTS` passes the raw statement text, so the
pytest path executes `SELECT COUNT(*) FROM {DATASET};` literally rather than
against any dataset. Combined with the `pytest.skip` handlers above it, this
function cannot presently be doing what its name claims. It is not on the
`make clickbench` path so it was left untouched; deciding whether to fix it or
delete it is a separate call. `test_battery_clickbench_results.py` imports only
`STATEMENTS` from this module and is unaffected either way.
