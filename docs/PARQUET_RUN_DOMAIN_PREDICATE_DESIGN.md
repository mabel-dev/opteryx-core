# Run-domain predicate evaluation for dictionary-encoded columns

Status: **ON HOLD — step 0 (2026-10-17) contradicted the premise; see §10. Not built.**
Date: 2026-10-17. Follows `docs/PARQUET_PACKED_CODES_ONE_PASS_DESIGN.md` (ruled D; one-pass
kept on both arches, 3d reverted). This is the "count over runs" step of that ruling, written
up as the general mechanism it turns out to need.
Trigger: ClickBench Q1 (`COUNT(*) WHERE AdvEngineID <> 0`) and Q7 (the same filter,
`GROUP BY AdvEngineID`). On c6a (0.9.164) Q1 was 0.086 s against Polars' 0.008 s.

## 0. Summary

A pushed predicate on a dictionary-encoded column is evaluated **per row** today:
1. the worker decodes the column's codes;
2. it widens them to a `uint32` selection;
3. the pass-1 kernel evaluates the dictionary once per entry and then scatters that answer
   back to every row.

Q1 does this for ~71M rows to keep 630k.

The proposal: for every pushed conjunct that reads a single column, take its per-entry
answer (a pass table of `dict_size` bits, plus one bit for NULL), then build that
conjunct's survivor bitmask **straight from the RLE/bit-packed code stream**.
- An RLE run sets or clears a range of bits in one operation.
- A bit-packed group of 8 is unpacked and looked up in the pass table.

There are no per-row codes, no widened selection and no per-row kernel pass. Pages of the
same chunk that are not dictionary-encoded are evaluated as they are today, into the same
positional mask.

Downstream nothing changes. The mask feeds the existing selective decode and survivor
materialisation, and a scan that emits no column (Q1) is answered by the mask's popcount.

This brings back the option D-4 of `docs/PARQUET_SELECTIVE_DECODE_DESIGN.md` set aside
("codes → mask during unpack"). Under that design's governing rule D-4 is a speed choice,
settled by measurement. §1 is why its 2026-09-28 ceiling estimate no longer holds.

## 1. The measured problem (2026-10-17, after the one-pass change)

Mac (M5 Pro), `scratch.hits_rugo_262k`, warm, single process:

| query | wall | process CPU | rugo decoder CPU (`rle_s` + `val_expand_s`, summed) |
|---|---|---|---|
| Q1 | 16.3 ms | ~200 ms | ~9 ms |
| Q7 | 17.0 ms | ~205 ms | ~6.4 ms |

x86 (i5-8500, 6 cores): Q1 109 ms wall, ~596 ms process CPU.

The decoder is now about 5% of Q1's CPU. By the code path, nearly all the rest is per-row
work over ~71M rows downstream of it:
- `expand_packed_codes` widening 1-byte codes to a `uint32` selection (~284 MB written);
- the pass-1 kernel's per-row scatter from per-entry results to the mask;
- the morsel handling around both.

**That split is inferred, not measured.** The engine `.so` files are linked `-Wl,-x`, so
attributing those ~190 ms needs a symbolised native profile (dSYMs from the `.o` files).
That profile is step 0 of the build (§8).

D-4's ceiling was sized on Q23's decoder time (`rle` + `val_expand` = 0.25 thread-s). It did
not count the per-row work after the decoder, which is what this removes.

## 2. What exists (reused, not replaced)

- **Worker prefilter** (`docs/PARQUET_SELECTIVE_DECODE_DESIGN.md` §2.1, shipped 2026-09-28):
  - `pass1_run_predicate` (io_pipeline.hpp ~502) builds DrakenVector views of the predicate
    columns and runs the injected c-native program into `result.survivor_mask` (1 bit/row).
  - `popcount == 0` → `empty_filtered`; otherwise projected columns are selectively decoded
    by the mask.
- **Dictionary views**: the pass-1 kernels already evaluate a dict-shaped view once per
  entry. The per-entry answer this needs is exactly that evaluation, run over a
  dictionary-only view.
- **Hybrid stream walkers**: `DecodeRLEBitPackedCodesPacked` (one pass, just kept),
  `DecodeRLEBitPackedIndicesToRuns`, and the per-arch group unpackers.
- **Single-run level probe**: `LevelStreamIsSingleRunOf` (decode_encodings.cpp ~698) answers
  "this page has no nulls" from the def-level header alone.
- **Zero-column morsels**: `CxxMorsel::zero_col_rows` and `count_only_morsel`
  (native_parquet_scan_source.hpp ~1455) carry a row count with no columns into COUNT(*).
- **Kernel data errors**: rc 96 carries a message naming the offending value
  (kernel_data_error_channel). It is reused for per-entry errors (§4).

## 3. Proposal

### 3.1 Plan time: mark single-column conjuncts

The compiler already splits the scan's pushed predicate into conjuncts for the worker
prefilter. It stamps every conjunct that **references exactly one column**, together with
its program, on the scan, the way it stamps `topn_boundary_key`. Nothing else restricts the
marking:
- any column type that parquet can dictionary-encode (integers, dates, timestamps,
  decimals, floats, strings);
- any c-native program over that column (comparisons, `IN`, `LIKE`, casts, arithmetic,
  functions, `IS [NOT] NULL`, `COALESCE`, `CASE`).

A conjunct over two or more columns cannot be answered per dictionary entry. It stays in
the existing whole-conjunction program, which runs over the views as today. Its mask is
ANDed with the run-domain masks.

### 3.2 Worker: evaluate a marked conjunct by page

For a marked conjunct `p` on column `c`, per row group, in the worker that runs the prefilter:

```
if c's chunk has a dictionary page:
  pass[k]   = p(dict[k])          for k in [0, dict_size)   -- one program run over a dict-only view
  pass_null = p(NULL)                                       -- one run over a single NULL
  err[k], err_null: set where that evaluation raised (§4)
  short-circuits: no entry passes and not pass_null  → conjunct mask all zero
                  every entry passes and (pass_null or c has no null) → all one
for each data page of c:
  dictionary-encoded page:
    rows positional if the def levels are one run of max_def (LevelStreamIsSingleRunOf),
    else expand def levels: a null row gets pass_null
    walk the hybrid stream of present values:
      RLE run (code k, count n): fill n row bits with pass[k]
      bit-packed group of 8:     unpack 8 codes (per-arch unpackers);
                                 byte = OR of pass[code_i] << i
      any code with err[k] set:  raise (§4)
  PLAIN / DELTA page (mixed chunk, or no dictionary):
    decode the page's values and run p over them, writing that page's slice of the mask
survivor_mask = AND of the run-domain conjunct masks and the multi-column program's mask
```

The mask is positional, so a chunk can mix both kinds of page. A dictionary that spilled to
PLAIN mid-chunk is handled page by page.

### 3.3 Downstream: nothing new

- The survivor mask has the same shape and meaning as today. Selective decode of projected
  columns, survivor materialisation and `empty_filtered` are unchanged.
- A scan whose read set is only predicate columns (Q1) is answered by the mask's popcount.
  Step 0 checks whether today's prefilter already emits a zero-column morsel for this
  shape. If not, emitting `zero_col_rows = popcount(mask)` is part of this work.
- A conjunct column that is also projected (Q7's `GROUP BY AdvEngineID`) is materialised
  from its codes for survivors only. Q7 keeps 630k rows.
- A length-only string column (the `<> ''` rewrite, `LENGTH(...)`) is evaluated per entry
  over the dictionary's lengths, which the length-only decode already has.

## 4. Semantics

- **NULL**: `pass_null` is the program's own answer on NULL. So `IS NULL`, `COALESCE` and
  `CASE` keep their meaning, and NULL-rejecting predicates give 0 for null rows, as the
  row-wise kernel does under WHERE.
- **Errors**: evaluating a dictionary visits values that no row of this chunk may carry, so
  an entry that raises must not raise by itself. Each entry's evaluation that raises sets
  `err[k]` and keeps the kernel's message. The conjunct raises only when a row actually
  carries code `k` (or is null with `err_null` set), with that message. That is the row
  the row-wise path would have raised on.
  - The row-wise program evaluates the whole conjunction on every row, so a row that
    another conjunct rejects still raises today. This matches that.
  - It also matches the BC_LAZY guarded-operand rule: guarded branches inside `p` run only
    for the entries that reach them, as in any evaluation.
- **Unreferenced dictionary entries**: their answers and errors are never read.
- **Codes out of range**: the walker keeps the one-pass bound (max code below `dict_size`)
  and fails loud with the page and the code (§7).
- **Volatile programs** (`RANDOM()`, ...): evaluated per row in the row-wise path; never
  marked. That is a correctness exclusion, the only one.

## 5. Expected effect (stated before building; a hypothesis until step 0)

- **Q1 / Q7**: the run walk is bounded above by today's ~9 ms of decoder CPU. If step 0
  confirms that the per-row widening and scatter are most of the ~190 ms that remains,
  that work goes. Q1 on the Mac would be about fixed cost (~4 ms: planning, compile,
  teardown) plus the walk: roughly 16 ms → 5–6 ms. Q7 is similar. x86 (109 ms) would
  shrink in proportion.
- **String filters over 100M rows**: Q12–Q14 (`SearchPhrase <> ''`), Q21/Q22 (`URL` /
  `Title` LIKE alongside `SearchPhrase <> ''`). The same per-row scatter goes; how much that
  is, step 0 measures.
- **Q36–Q42** (`CounterID = 62`, `IsRefresh = 0`, `DontCountHits = 0`,
  `TraficSourceID IN (-1, 6)`, `EventDate` ranges): all single-column conjuncts. Only ~14
  row groups are read, so the absolute gain is small.
- **Large dictionaries** (URL, Title: millions of entries per shard): per-entry evaluation
  is what the view path already does, so it adds no work. The deleted F ("kernel dictionary
  probe") lost because it added a second dictionary pass. This has none.

## 6. Interactions

| path | interaction |
|---|---|
| page search (LIKE on raw pages, byte_array) | drops pages before decode, as now; surviving pages go through §3.2 |
| page index / page-jump pruning | page skipping unchanged; skipped pages contribute no mask bits |
| selective decode of projected columns | consumes the same mask |
| top-N boundary, runtime join bounds | row-group skipping before the worker, unchanged |
| multi-column conjuncts | whole-conjunction program over views, ANDed in |
| standalone rugo | the walker is rugo code; the marking is opteryx planning. Standalone callers never set it |
| LIST columns | not dictionary-walked (repetition levels); their conjuncts stay in the program |

## 7. Fail-loud

The walker sets `error_message` on:
- a short stream;
- a code at or above `dict_size`;
- a page whose row count disagrees with its def levels.

The wording matches the one-pass decoder (its doc §9). A per-entry evaluation error reaches
the caller through the kernel data-error channel with the kernel's own message (§4). No bare
`return`s are added.

## 8. Build and verification plan

### 8.1 Performance questions (default built, and the measurement that would overturn it)

| question | default | overturned by |
|---|---|---|
| D-4: codes → mask during unpack | built (this proposal) | step 0 showing the per-row work is not the cost, or the A/B below |
| per-page vs per-chunk choice of path | per page (§3.2) | an A/B against per-chunk on mixed-encoding fixtures, if per-page costs anything measurable |
| §11 encoding specialisation | built; the mask is identical by construction and asserted equal | any result-hash difference is a bug, never a trade-off |
| short-circuits (all-pass / all-fail) | on | leave-one-out in the ablation |

### 8.2 Steps

0. **Profile first.** Symbolised native profile (samply, dSYMs from the `.o` files) of Q1,
   Q7 and Q12, hit path. It attributes the ~190 ms. If the per-row widening and scatter are
   not the bulk of it, stop and report, because §5 would be wrong.
1. Baseline arms built from one tree, the two .so pairs only (pool_reader, rugo_native).
2. **Result equality** on every query (hash of all values). Fixtures:
   - nullable multi-page pages with and without nulls;
   - a mixed dict+PLAIN chunk;
   - `IS NULL` / `COALESCE` conjuncts;
   - a conjunct that raises on an unreferenced dictionary entry (must not raise);
   - one that raises on a referenced entry (must raise with the value).
3. **C++ test** of the walker against the row-wise reference, under ASan/UBSan, clang and
   GCC. Covers: bit widths 0-32, int and string dictionaries, NULL pages, RLE runs crossing
   64-bit mask words, truncated streams.
4. **Build all, then ablate** (leave-one-out of the short-circuits and the per-page mixing).
   Interleaved ABBA, 12 rounds, Mac and x86.
   - Keep rule, fixed before the first number: Q1 and Q7 median wall ≤ 0.90 with the new
     build winning ≥ 10 of 12 on both arches.
   - No guard median worse than 1.00. Guards: q12–q14, q15, q19, q21, q22, q36, q41, the
     20-int GROUP BY OS.
   - Otherwise the change is reverted.
5. `make q`, then `make slt`.

## 9. Decisions requested

None outstanding. Scope is maximal (§3.1), and every remaining choice is a performance
question in §8.1. The one exclusion, volatile programs, is a correctness rule, not a scope
choice.

## 10. Step 0 result (2026-10-17, Mac, samply + dSYM, hit path)

Q1, 8 s warm loop (464 runs, 17.2 ms/run). Leaf work samples (kernel waits excluded):
`draken_native` 81%, `_operators` 5.8%, malloc 4.2%, platform 4.1%, pool_reader 2.0%,
rugo_native 0.1%. Inside `draken_native` the hot frames are `draken_numeric_cmp`
(function_kernels.cpp ~1781-1788) with `fk_read_dec` / `fk_cmp_i128`. That is the generic
mixed-type compare, which per row switches on the operand type, widens to int128 and
switches on the operator. The per-row widening and scatter this proposal targets are not
in the top frames. **The premise of §1 is wrong.**

Why it routes there: `compiled_expression.pyx` (~2097-2112) re-types the literal `0` to
the column's physical type (INT16) on the node (`_coerce_literal_physical`). But the
mixed-type check (~2169-2181) compares `left_type.physical` with `right_type.physical`,
and `right_type` is still the literal's INT64. So INT16 vs int-literal is treated as a
type mismatch and goes to `draken_numeric_cmp` instead of the same-type
`draken_compare_dv`. This applies to every narrow integer column compared with an integer
literal.

Probe: the same query with `CAST(0 AS INT16)` runs `fixed_int_compare_vector<short>`.
- CPU work per run falls from ~154 to ~57 sample-ms (2.7x less).
- Wall moves only 15.7 → 13.9 ms. After the fix, Q1's wall is not CPU-bound: about 3.7
  cores are busy on average out of 16, and idle samples dominate.

Consequence: run-domain evaluation would cut CPU, but it cannot meet a wall-time keep rule
on Q1 until the wall is understood. On hold. The next steps are (a) the literal-routing
fix and (b) Q1's critical path.
