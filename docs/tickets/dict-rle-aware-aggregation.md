# Ticket: Dict- and RLE-aware group-by + aggregation for StringVector

## Problem

Today, when a `GROUP BY` or aggregation runs over a dict-encoded or RLE-encoded
`StringVector`, the vector is materialized to a flat representation **before**
the operator sees it. For ClickBench's URL column this is the dominant cost:
the dictionary already *is* most of the structure a hash group-by would build.
We are throwing away pre-computed information, then rebuilding it.

Concretely, every access path in `draken/vectors/string_vector.pyx` on a dict
vector with `ptr.data == NULL` calls `_materialize_dict_string(self)` first.
Examples: lines 630, 674, 716, 729, 764, 833, 1385–1387. The grouped/ungrouped
aggregate operators consume the materialized vector with no awareness that
encoding existed.

This ticket: teach group-by and the aggregation kernels to consume
dict-encoded and RLE-encoded `StringVector`s **without materialization**, on
the encoded representation directly.

## Why this matters

- URL column in ClickBench is ~3.3s read; a meaningful slice is StringVector
  materialization for dict-encoded chunks.
- Materialization is wasted work for grouping: the dict already enumerates
  unique values; we only need the per-group counters. The codes act as
  pre-computed group ids.
- Same logic applies to RLE: the run-lengths already partition the vector.

## Key insight (from the user)

> A dictionary is the first half of what we need to do to build the
> group-by structure. Materializing it just to re-discover the same uniqueness
> is wasted effort.
>
> - **COUNT(col)**: per dict code, the count is the number of references to
>   that code. Just walk the codes and tally.
> - **COUNT(*) per group**: same as above — group key = dict code.
> - **MIN/MAX(string col)**: compute once per unique dict value, then the
>   per-group answer is just a lookup.
> - **SUM/AVG of `f(col)`** where `f` is a function of the value (e.g. a cast,
>   a length): evaluate `f` on each unique dict value once, then for each
>   group `sum = f(value) * occurrence_count`.
> - **DISTINCT col**: it's the dict (minus codes that never appear).
>
> RLE is the same shape, with run-lengths in place of code-occurrence counts.

## Scope

In scope:
- Grouped hash aggregation: `opteryx/operators/grouped_aggregate_hashed/` —
  particularly the key-construction path and the per-group accumulators in
  `_collectors_*.pxi`.
- Ungrouped aggregation kernels in `opteryx/operators/aggregate/`:
  `ungrouped_agg_count.pyx`, `ungrouped_agg_min_max.pyx`,
  `ungrouped_agg_sum.pyx`, `ungrouped_agg_count_distinct.pyx`,
  `ungrouped_agg_any_value.pyx`.
- New fast paths in `draken/vectors/string_vector.pyx` that expose iteration
  over (code, count) pairs for dict and (run_value_index, run_length) pairs
  for RLE without materializing.

Out of scope (explicitly):
- Numeric vector dict/RLE handling (URL is the target).
- Changing the on-disk parquet representation.
- Changing the materialization path itself — leave it as the fallback.
- JOIN keys built from dict-encoded vectors (separate ticket).

## Approach

### 1. Draken: expose encoded-form access

In `draken/vectors/string_vector.pyx`, add (or surface, if already private)
typed accessors that the operators can use without going through the
`ptr.data == NULL` → `_materialize_dict_string` branch:

For `DRAKEN_ENCODING_DICTIONARY`:
- `dict_size()` — number of unique values
- `dict_value(i)` — the i-th unique value (zero-copy view)
- `dict_codes_ptr()`, `dict_code_width()`, `length()` — for walking codes
  (codes are already stored as packed 1/2/4-byte ints — see the audit notes
  on `_dict_codes`, `_dict_values`, `_dict_code_width`).
- `dict_code_counts()` — returns a length-`dict_size()` int64 array of
  occurrence counts. Compute once on demand and cache on the vector.

For `DRAKEN_ENCODING_RLE`:
- `rle_run_count()`
- `rle_value(i)` and `rle_run_length(i)` for each run
- (Or a single zero-copy view over the runs array.)

These must be `nogil`-callable and not allocate beyond the cache for
`dict_code_counts()`.

### 2. Aggregation kernels: encoded fast paths

Each kernel in `opteryx/operators/aggregate/ungrouped_agg_*.pyx` gets a
branch at entry:

```cython
if vec._encoding == DRAKEN_ENCODING_DICTIONARY and vec.ptr.data == NULL:
    return _agg_X_dict(vec)
if vec._encoding == DRAKEN_ENCODING_RLE:
    return _agg_X_rle(vec)
# fall through to existing flat path
```

Implementations:

- **count** (non-null): `length() - null_count()`. No traversal needed.
- **count_distinct**: number of dict codes that appear at least once
  (i.e. `(dict_code_counts() > 0).sum()`). For RLE, number of distinct
  run values that appear at least once.
- **min/max**: scan `dict_value(i)` for `i in 0..dict_size()` where the code
  is referenced; use the existing string compare. O(dict_size), not O(rows).
- **sum / avg of `f(col)`** where the kernel currently does `sum += f(v)`
  per row: precompute `f(dict_value(i))` once, then
  `sum += f_i * dict_code_counts()[i]`. For pure SUM of a string column this
  doesn't apply, but for SUM(LENGTH(col)) and similar, the planner currently
  evaluates the function per row — this fast path collapses that to one
  evaluation per unique value. **Note**: this requires the function-evaluation
  layer to expose a "evaluate on dict values, return a derived dict" path; if
  that's not available yet, scope this kernel-level optimization to the
  count/min/max/distinct cases and leave a `# TODO: dict-aware sum` marker.
- **any_value**: return `dict_value(first_referenced_code)`.

### 3. Grouped hash aggregation: encoded fast paths

In `opteryx/operators/grouped_aggregate_hashed/_engine.pxi` and
`_key_store.pxi`, when the **only** group key is a dict-encoded
`StringVector`:

- Use the dict codes directly as group ids — skip hashing strings entirely.
- Allocate the per-group accumulator array sized to `dict_size()` instead of
  using the hash key store at all. Walk the codes once, dispatching to the
  collector for each row's code.
- For COUNT, this collapses to incrementing `counts[code]` in a tight loop
  over the codes array — no string ops, no hash ops.
- For MIN/MAX of another column, you still need the other column's value, but
  the group lookup is `code` directly.

When the group key is a multi-column key that **includes** a dict-encoded
vector, hash the code (1/2/4 bytes) instead of the string. This is a smaller
change in the hash-builder.

For RLE keys: walk runs, emit `(run_value, run_length)` pairs into the
collector once per run instead of once per row. Same accumulator structure.

### 4. Tests

- Add unit tests in `draken/tests/` for the new accessors: empty dict, dict
  with unreferenced codes, dict with nulls, RLE with single run, RLE with
  many short runs.
- Add operator tests verifying that for a dict-encoded URL-like column, the
  results of COUNT, COUNT DISTINCT, MIN, MAX, GROUP BY are **bit-identical**
  to the materialized path. Run the same query twice — once forcing
  materialization (existing path), once on the encoded vector — and assert
  equality.
- Run `make q` — must be 100% pass.
- Run `make clickbench` — capture before/after timings on URL queries
  (Q15, Q20, Q22, Q28, Q29). Expect material reduction; report the numbers.

## Constraints (from CLAUDE.md — do not break)

- **No Python in hot paths.** All new code must be `cdef`/`cpdef` Cython,
  typed, no `object`. Release the GIL where possible.
- **No fallback duplication.** The encoded path is in addition to the
  materialized path; do not delete or duplicate the materialized path.
- **Fail fast.** If a precondition is violated (e.g. dict code out of range),
  abort — do not silently fall back.
- **No PyArrow.** The build will fail if introduced.
- **No speculative refactors.** Changes should be additive: new fast paths,
  encoding checks at kernel entry. Do not restructure the operators beyond
  what this ticket needs.
- **Do not commit.**

## Files (verify before editing — line numbers drift)

- `draken/vectors/string_vector.pyx` — encoding flags at lines 33–34;
  materialization branches around 630, 674, 716, 729, 764, 833, 1385–1387.
- `draken/core/buffers.pxd` — encoding constants.
- `opteryx/operators/aggregate/ungrouped_agg_count.pyx`
- `opteryx/operators/aggregate/ungrouped_agg_min_max.pyx`
- `opteryx/operators/aggregate/ungrouped_agg_count_distinct.pyx`
- `opteryx/operators/aggregate/ungrouped_agg_any_value.pyx`
- `opteryx/operators/aggregate/ungrouped_agg_sum.pyx`
- `opteryx/operators/grouped_aggregate_hashed/_engine.pxi`
- `opteryx/operators/grouped_aggregate_hashed/_key_store.pxi`
- `opteryx/operators/grouped_aggregate_hashed/_collectors_*.pxi`

## Suggested order of implementation

1. Draken accessors + tests (smallest, isolated).
2. Ungrouped COUNT / COUNT DISTINCT / MIN / MAX / ANY_VALUE on dict.
3. Grouped hash aggregation: dict-only single key (the URL `GROUP BY` case).
4. RLE variants of (2) and (3).
5. Multi-key hash group-by with one dict component (hash the code).
6. SUM-of-function-of-dict-value, only if the function-evaluation layer can
   evaluate on the dict values. Otherwise leave a TODO and stop.

Land each step as a separate change; verify `make q` passes after each.

## Definition of done

- All five steps above implemented or explicitly deferred with reason.
- `make q` passes.
- `make clickbench` URL queries (Q15, Q20, Q22, Q28, Q29) measured before
  and after; results reported in the PR description.
- No new PyArrow imports. No Python objects in the new hot paths.
- Materialized path still works (covered by existing tests).
