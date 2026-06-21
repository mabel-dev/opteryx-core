# Dictionary-Aware Integer Filter — Design

**Status:** Draft for architect review
**Author:** scan/decode investigation, 2026-06-21
**Trigger:** ClickBench Q20 is our worst query vs DuckDB (17.5× on the multi-thread
baseline; 3× vs single-thread DuckDB).

```sql
-- Q20
SELECT UserID FROM hits WHERE UserID = 435090932899640449;
```

---

## 1. Problem & measurement

Q20 is a high-selectivity point lookup: 76,083,093 rows scanned → **4 matches**.

Telemetry (warm, local mmap):

| metric | value |
|---|---|
| `parquet_rows_before_filter` | 76,083,093 |
| `parquet_rows_after_filter` | 4 |
| `parquet_row_groups_pruned` | 0 / 231 |
| `time_parquet_decode_columns_ns` | ~345 ms (summed across scan workers) |
| wall (warm) | ~105 ms |
| DuckDB multi-thread / single-thread | 6 ms / 35 ms |

**Min/max pruning cannot help.** UserID is random: every row group's
`[min,max]` spans nearly the full int64 range, so the needle is inside all of
them. Our `Eq` row-group pruning (`pool_reader.pyx:615`) correctly prunes zero.
There are **no bloom filters and no page index** in the files. The cost is
entirely *decode of the UserID column + the equality compare*.

**The column is `PLAIN_DICTIONARY` + SNAPPY.** Measured: median ~55K distinct
UserIDs per 336K-row group (dict ratio ~0.2); in 32 sampled row groups, **zero**
contained the needle (the user lives in ~4 of the 231 row groups).

---

## 2. Root cause — we already have a dict-aware filter; the scan destroys the dictionary

The dict-aware int64 scalar comparison kernel **already exists and is wired**:
`draken/ops/int64_compare.h:439` (`compare_scalar_impl`). For a Dict-shaped
operand it compares only the `data_length` *unique* values and scatters the
result through `selection` — O(unique) + O(n/8) instead of O(n). The constant /
dict / dict-cross fast paths are all present (`int64_compare.h:436`, `:439`,
`:569`).

The problem is upstream, in the **scan**:

- `rugo/src/parquet/io_pipeline.hpp:177`
  ```cpp
  if (!d.dict_indices.empty() || !d.dict_codes_array.empty()) return DK_POOL;
  ```
  Numeric dictionary columns are sent to the pool path and **materialized
  Dense** before reaching the engine.

- For **non-nullable** dict columns (UserID), decode takes the `rle_path`
  (`decode_column.cpp:712`, body at `:1164`): it *resolves* each dict code to its
  value, producing `rle_int64_values` + `rle_run_lengths`. The per-row codes are
  discarded; this also routes to `DK_POOL` (`io_pipeline.hpp:178`).

So when the filter runs, `draken_is_dict(&v)` is false → the dict fast path
never fires → we compare all 76M dense values.

**Contrast with strings, which already do the right thing:**
`io_pipeline.hpp:173` hands byte_array dictionaries to the engine as
`DK_VARCHAR_DICT` (Dict shape, zero-copy). There is simply no numeric
equivalent.

---

## 3. The building blocks already exist

This is glue, not new machinery:

| Layer | Exists today |
|---|---|
| Decode (nullable dict) | `dict_int64_values` + `dict_codes_array` / `dict_indices` (`decode.hpp:18,45,16`) |
| Transport struct | `ColumnOut.data` / `.codes` / `.data_length` (`io_pipeline.hpp:91`), used by `DK_VARCHAR_DICT` |
| Build helper precedent | `build_direct_string_dict` (`io_pipeline.hpp:266`) |
| Consumer wrap precedent | `_wrap_string_dict_direct` (`pool_reader.pyx:115`) |
| Draken ownership entry | `draken_vector_own_dict_i64(data, data_length, codes, length, validity)` (`draken_native.cpp:2242`, `_vector_shim.pyx:269`) |
| Filter kernel | dict-aware `compare_scalar_impl` (`int64_compare.h:439`) |

The only missing piece is a `DK_INT64_DICT` direct kind plus the decode-mode
choice that keeps the dictionary instead of resolving/expanding it.

---

## 4. Design — Phase 1: preserve numeric dict shape into the engine

Mirror the existing `DK_VARCHAR_DICT` path for numeric columns. End state: a
dictionary-encoded numeric **filter** column reaches pass-1 as a §11 Dict-shaped
`DrakenVector` (`data` = unique values, `selection` = per-row codes,
`data_length` < `length`), and the existing dict-aware kernel fires
automatically.

### 4.1 New direct kind

`io_pipeline.hpp:67` — add `DK_INT64_DICT = 8` (and, if we extend to float,
`DK_FLOAT64_DICT`, `DK_FLOAT32_DICT`). Sync the `.pxd` enum the consumer reads.

Scope decision (architect): **start with int64/int32→int64 only.** Float dict
columns are rare as filter keys; defer unless measurement justifies.

### 4.2 Decode mode: keep the dictionary for filter columns

This is the crux. The decoder currently picks the *cheapest materialization*,
which is the wrong shape for us:

- non-nullable dict → `rle_path` resolves codes to values (`rle_int64_values`)
- nullable dict → `dict_codes_array` (keeps codes + dict — already usable)
- otherwise → dense `int64_values`

**Proposal:** add a `prefer_dict` flag to `DecodeColumnFromChunk`
(`decode.hpp:108`). When set, the decoder must produce **dictionary + per-row
codes** (`dict_int64_values` + `dict_codes_array` or `dict_indices`) and must
**not** take `rle_path` or dense expansion for dict-encoded pages. When the page
is *not* dict-encoded (PLAIN), `prefer_dict` is a no-op and the column decodes
dense as today (correct: a plain page has no dictionary to exploit).

The scan sets `prefer_dict=true` only for **pass-1 filter columns** that the
predicate references with an `=` / `IN` (these are known at scan setup via
`_sp_pass1_column_names`). Projection-only columns keep today's behaviour.

> Open question for the architect — two alternatives to the `prefer_dict` flag:
> (a) **flag (recommended):** plumb a per-column "prefer dict" bool through the
>     decode call; smallest blast radius, dict shape only where it pays.
> (b) **build dict from RLE post-hoc:** keep `rle_path`, then re-intern
>     `rle_int64_values` runs into a dict + codes. Avoids touching the decode
>     gate but is O(n) code writes + a dedup hash map per column — partially
>     defeats the point and adds a second representation to maintain.
> Recommendation: (a).

### 4.3 Build helper

Add `build_direct_int64_dict(decoded, alloc, free, out)` (mirror
`build_direct_string_dict`, simpler — no arena/slots):

- `out.data` = `draken_alloc` copy of `dict_int64_values` (the unique values)
- `out.data_length` = dict size
- `out.codes` = `draken_alloc` `uint32_t[n]`, widened from `dict_codes_array`
  (`code_width` 1/2/4) or from `dict_indices` (scatter through validity for the
  nullable-compact case, exactly as the string version does at
  `io_pipeline.hpp:292–309`)
- `out.validity` = copy of `valid_bits` (or NULL)
- `out.length` = `n`

Wire it in the worker dispatch at `io_pipeline.hpp:791` alongside the existing
`DK_VARCHAR_DICT` branch.

### 4.4 Consumer wrap

`pool_reader.pyx:_wrap_direct` (`:144`): add a `dk == 8` branch that calls
`draken_vector_own_dict_i64(data, data_length, codes, length, validity)`,
taking ownership via `morsel_take_direct` + the codes via a take that nulls the
`ColumnOut.codes` slot (mirror `_wrap_string_dict_direct`). `MorselRef`'s
destructor already frees `codes` for abandoned columns (`io_pipeline.hpp:123`).

### 4.5 Logical-type gate

`io_pipeline.hpp:788` already forces non-"safe" logical types to the pool path.
**DATE / TIMESTAMP / int-backed DECIMAL dict columns must stay pool** in Phase 1
(they are reinterpreted downstream; a dict-shaped reinterpret is a separate
proof — same reasoning as the WP-6b fixed-width gate, and the historic tpch Q01
decimal trap). Only `lt.empty()` / `int64` / `int32` dict columns take
`DK_INT64_DICT`.

### 4.6 Correctness invariants (§11)

- The dict path must produce **bit-identical** filter results to the dense path.
  The uniform access contract `data[selection[i]]` is already the kernel's
  correctness path; the dict fast path is the agreed targeted optimization
  (`int64_compare.h:439`) and is proven equal to the gather path for the same
  data.
- Codes are `uint32_t` (draken selection width, per §11). `code_width` 1/2/4 in
  the parquet stream all widen to `uint32_t`.
- Nullable: validity bitmap is carried verbatim; null rows get code 0 and are
  masked by `validity`, never by a sentinel value (same as the string path).
- A Dict-shaped vector handed to *any other* operator (projection, the output
  of Q20 is the column itself) must already work via the uniform path — verify
  the output morsel materializes correctly (it is the same column the filter
  read).

### 4.7 Expected result

Removes dense expansion + cuts the compare from 76M → ~unique-per-RG. The data
pages are still decoded (we still read every code). Estimate: Q20 ~105 ms →
~50–70 ms. **Does not fully reach single-thread DuckDB (35 ms)** because we still
touch every data page. That is Phase 2.

---

## 5. Design — Phase 2 (optional, composable): dictionary-membership row-group pruning

For `col = const` / `col IN (...)` on a dict-encoded column: load the (small)
dictionary page first and check whether **any** needle is present. If absent,
**skip the entire row group's data pages** — exact, strictly stronger than
min/max. This is the actual DuckDB algorithm for Q20 and would skip ~227/231 row
groups, getting us into the single-digit-ms range.

- Fits the existing `_rg_passes_predicates_native` seam (`pool_reader.pyx:592`),
  which already special-cases `Eq` / `In`.
- Cost: read + decode the dictionary page per candidate row group (small;
  ~110 KB region observed). For local mmap this is cheap; for remote IO it is an
  extra small range read — gate on a selective equality/IN predicate.
- Narrower than Phase 1 (only `=`/`IN` on dict columns) but the bigger Q20 win.

Phase 1 and Phase 2 compose: prune by membership first, hand survivors to the
engine in Dict shape. **Phase 2 is a separate design doc once Phase 1 lands and
is measured.**

---

## 6. Telemetry

- Add a counter `parquet_dict_filter_columns` (columns handed over as
  `DK_INT64_DICT`) so we can confirm the path engaged.
- Phase 2: `parquet_row_groups_pruned_by_dict`.

---

## 7. Test plan

- `make q` (190), tpch (22), clickbench (43) all green — no regressions.
- New regression: a dict-encoded non-nullable int64 column equality filter
  returns identical rows to the dense path (oracle: pyarrow / current output).
- Nullable dict int64 equality (codes + validity) — exercise null rows.
- `IN (...)` on a dict int64 column (dict-cross / scalar-per-needle).
- Group-by / join on a dict int64 key still correct (the shape now reaches those
  operators too — uniform path must hold).
- Abandonment / LIMIT early-exit: `MorselRef` frees `codes` with no leak/double
  free (ASAN spot check, mirror the string-dict abandon stress).
- Q20 timing before/after (architect drives the benchmark; one quiet run).

---

## 8. Risks & rollback

- **Blast radius:** dict shape now reaches every operator for filter columns,
  not just the compare. Mitigated by the uniform-access contract; the string
  dict path already proves the pattern end-to-end.
- **Decode-gate change** (`prefer_dict`) is the riskiest piece: it changes which
  representation a column decodes into. Strictly opt-in per filter column;
  off → byte-identical to today.
- **Rollback:** `direct_kind_for` returning `DK_POOL` for numeric dicts (i.e.
  not setting `prefer_dict`) restores current behaviour with one line.

---

## 8b. Outcome (implemented 2026-06-21)

Both phases landed for int32/int64 (decode-level, parallelism preserved).

| stage | Q20 wall | scan decode (summed) |
|---|---|---|
| baseline | ~105 ms | ~337 ms |
| Phase 1 (dict-shape + dict-aware compare) | ~95 ms | ~337 ms (decode-bound; P1 optimizes the *compare*) |
| Phase 2 (membership skip, constant marker) | ~72 ms | ~30 ms |
| Phase 2b (empty-row-group skip) | **~49 ms** | — |

- DuckDB: 6 ms (multi-thread baseline) / 35 ms (single-thread). We went 3.0× → 2.05× vs single-thread; the decode root-cause is gone (11× faster).
- Verified: `make q` 190/190, tpch 22/22, pyarrow oracle EXACT (UserID =, IN, absent, present; RegionID), new regression test
  `tests/unit/connectors/parquet_io/test_dict_int_filter.py`.

**Phase 1 was nearly invisible on Q20** because Q20 is decode-bound — its value is for GROUP BY / DISTINCT / JOIN on dict int keys (dict-aware compare), not point lookups. **Phase 2 delivered the Q20 win** by skipping data-page decode for the ~227/231 row groups whose dictionary lacks the needle.

**Attribution of the post-Phase-2 cost (worker sweep):** `PARQUET_LOCAL_IO_WORKERS` 1→16 barely moved the all-skip query (74.6→65.5 ms, flat) → the cost was **serial per-row-group consumer overhead** (wrap→filter→morsel ×231), not parallel I/O. So **Phase 2b** landed: the worker flags an all-filtered row group (`MorselRef.empty_filtered`/`empty_rows`) and stops decoding its remaining columns; the consumer (`_single_pass_next`/`_run_pass1`, `next_vectors` returns a `vectors=None` sentinel) skips it with no wrap/filter/morsel. Q20 72→**49 ms**.

**Remaining gap (~49 ms vs COUNT(*) floor ~16 ms):** the consumer still *receives* all 231 worker results and each worker still mmaps + decodes that RG's dictionary page to probe membership. Removing that needs a **membership index that avoids reading dict pages** (bloom filter / sidecar — reuse the NDV sidecar infra), since the worker sweep already showed pruning-level (serial dict reads in planning) wouldn't help. This is the plateau for index-free membership skip — deferred.

**Roll-out — TEMPORAL landed (2026-06-21):** date/timestamp dict columns now reach
the engine compressed, reusing the entire `DK_INT64_DICT` path; only coercion
needed to be shape-preserving:
- TIMESTAMP: coercion already used `vector_retag_int64_as_timestamp64` (zero-copy,
  shape-preserving) — just relaxed the `prefer_dict` gate to include `timestamp*`.
- DATE: rewrote `vector_reinterpret_as_date32` to be shape-preserving (convert only
  the `data_length` dictionary values, copy the codes; dense/constant unchanged),
  then relaxed the gate to include `date*`.
- Verified: dict-encoded date32 + timestamp range filters vs direct computation
  (`test_dict_int_filter.py`), make q 190 / tpch 22. NB: ClickBench's
  EventDate/EventTime are stored as *plain ints* (no logical annotation) so they
  were already covered by the int path — this rollout helps datasets with real
  temporal logical types.

Also cleaned up: `DK_INT64_CONST` + `build_direct_int64_const` +
`draken_vector_own_constant_i64` were removed — Phase 2b's `empty_filtered` break
fires before `direct_kind_for`, so the constant marker became unreachable.

**Roll-out — FLOAT + STRINGS landed (2026-06-21):**
- FLOAT: `DK_FLOAT64_DICT`/`DK_FLOAT32_DICT` direct kinds + `build_direct_float_dict`
  + `draken_vector_own_dict_f64`/`_f32`; gate extended to float physical types;
  decode `rle_path` bypass extended to float dict modes; added the constant→scalar
  reduction (`FpReversedOp`) to `fp_compare_vector_impl`. Phase 2 membership-skip
  stays int-only (float equality membership out of scope). No widening (float32 stays
  float32). Verified: float64 dict range+equality correct; float32 dict data-integrity
  round-trip correct.
- STRINGS: Phase 1 was already done (`DK_VARCHAR_DICT`). The gap was that
  `str_compare_vector` lacked the constant→scalar reduction, so `str_col <op>
  'literal'` (dict vs constant) fell to O(n). Added it (`str_swap_op`). Now dict
  string filters (`= 'x'`, `<> ''`, ordered) hit the dict fast path. Verified.

**Found a PRE-EXISTING bug (flagged, not fixed):** comparing a float32 column to a
float64 *literal* returns wrong results — the kernel reads the double literal's
bytes as a float (`fp_compare_*_impl<float>` over a FLOAT64 constant). Non-dict
float32 has it too; it needs a type-promotion fix in the planner or compare
dispatch. Spawned as a separate task.

**Done:** int32/int64, date/timestamp, float32/float64, strings all reach the engine
dict-compressed with dict-aware comparisons. **DECIMAL** is the only type still on
the pool path (the int-backed reinterpret trap — intentional).

## 8c. Decode-skip generalized beyond equality (2026-06-21)

The Phase-2 decode-skip was equality/IN-only. Generalized it from "int needle
membership" to **"evaluate any pushed per-value conjunct against the dictionary;
skip the data pages if no unique value satisfies it."** The skip is valid for any
per-value predicate (every row's value is a dictionary value), so:

- **`=` / `IN`** — int *and* string membership (string equality skip is new).
- **`LIKE` rewrites** — `_STARTS_WITH` (prefix), `_ENDS_WITH` (suffix), `InStr`
  (contains). These are the highest-value targets: min/max can't prune `LIKE` at
  all, so the dictionary is the *only* lever, and ClickBench leans on `URL`/`Title`
  `LIKE` heavily. (Case-insensitive `_CI_*`/`IInStr` deferred — need case folding.)
- **One-sided ranges** (`<`,`>`,`<=`,`>=`) intentionally NOT pushed to decode-skip:
  "any dict value > X" ≡ footer max, already pruned for free by min/max. `BETWEEN`
  interior-gap pruning is a possible future add.

Mechanism: unified `DictSkipPredicate {kind, int_vals, str_vals}` (decode.hpp); the
worker evaluates it against the just-decoded dictionary (int membership, or
string membership/prefix/suffix/contains over `string_dict_arena`) and reuses the
Phase-2b `empty_filtered` skip. `extract_predicate_stats` gained
`_try_extract_str_func` for the `_STARTS_WITH`/`_ENDS_WITH`/`InStr` FUNCTION nodes
(they ARE pushed to the scan — `can_push` passes boolean functions). The pipeline
channel generalized: `add_int_needles` + `add_str_pred(col, kind, patterns)`.

Verified: `LIKE 'absent%'` on real `hits.Title` decodes ~218ms (dictionary pages
only) vs ~848ms for a matching prefix — most row groups skipped. make q 190 / tpch
22 + regression cases (string `=`, `LIKE` prefix/contains, absent-everywhere).

Implementation: `DK_INT64_DICT` (=8) and `DK_INT64_CONST` (=9) direct kinds mirror `DK_VARCHAR_DICT`; `prefer_dict` decode flag keeps the dictionary (bypasses rle resolve-to-values) for int dict columns; `eq_needles_` map on the pipeline carries pushed Eq/IN needles; `draken_vector_own_constant_i64` is the all-non-match marker exit.

## 9. Decision log (architect, 2026-06-21)

- **Scope:** int32/int64 only. Other types (float, then date/timestamp/decimal)
  are a follow-up roll-out once the int path works.
- **RLE → compressed, not either/or:** the unified vector model has no RLE; an
  RLE-decoded column is *already* being converted — today to dense. The target
  encoding is **`compressed`** (= §11 Dict shape, `data_length < length`). So
  `prefer_dict` (keep the dictionary at decode) and "build dict from RLE" are not
  mutually exclusive — both are routes to the same compressed vector. We keep the
  dictionary at decode for int columns (cleanest, smallest dict) and stop
  resolving codes to values for them.
- **Coverage:** apply to *all* int32/int64 dict columns (filter and projection),
  not only pass-1 filter columns. `prefer_dict` is a no-op on plain pages.
- **Phase 2 now:** dictionary-membership row-group pruning lands in the same
  effort.
- **DATE/TIMESTAMP/int-DECIMAL stay pool** (logical-type gate) for now — physical
  int only.
- **Direction:** make it work for ints end-to-end (both phases), then roll the
  same shape out to other physical types.
