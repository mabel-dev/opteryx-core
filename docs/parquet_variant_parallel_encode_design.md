# Design: encode a VARIANT row group in parallel

> Status: design only, 2026-11-22. Nothing here is built. Decisions for the architect are
> in §6. Background: `docs/tickets/perf-variant-columns-review.md` R4 and R5.

## 1. The problem

Every VARIANT write (INSERT, CTAS, OPTIMIZE, the JSONBench load) turns each row's JSON
text into Variant binary and splits it into the column's shredded leaves. That work runs
on the **calling thread**, one row after another, while the writer pool sits idle.

Measured on `OPTIMIZE TABLE` (local-disk catalog, 3 files of 1M JSONBench rows into 1,
5.77 s window; R5 in the ticket):

- The calling thread was on-CPU for 3.38 s of the 5.77 s. About 2.9 s of that is the
  VARIANT encode: `Shredder::pair` 1.29, `Shredder::typed` 0.22, `RowEncoder` (object,
  sorted_members, stable_sort, value, metadata) about 0.6, a vector `emplace_back` 0.22,
  `classify_number` 0.07.
- The whole compaction used about 2.4 of 18 cores.

## 2. How a row group is written today [read]

Per morsel (one row group: up to 65,536 rows in the streaming writer):

1. **Conversion, on the calling thread** (`parquet_writer.pxi`, the morsel-to-`ColumnInput`
   loop). For a VARIANT column, inside `with nogil`:
   - `VariantColumn::append` calls `Shredder::encode` over every row
     (`variant_encode.hpp:427`). Per row: a yyjson parse, a walk that collects the row's
     field names, the `metadata` string, then a second walk that writes every leaf.
   - Then, per leaf, it builds the `StrSlice` array and the validity bitmap
     (`variant_columns.hpp:77-93`).
2. **Encode, on the writer pool** (`add_row_group` → `encode_row_group`): one task per
   column chunk. The caller waits for all of them.

Steps 1 and 2 do not overlap. Step 1 is serial; step 2 is parallel across the ~110 leaves.

## 3. Why the rows can be split

Each row's encoding is a function of that row and the file's `ShredType` only:

- `metadata` is per row: the row's own field names (`RowEncoder::keys`, cleared by
  `begin_row`).
- `Shredder::pairs_` is the fixed plan built from the `ShredType`.
- `RowEncoder::scratch` / `members` are reused buffers; they carry no meaning between rows.
- `ReadPool` is sized per call to the longest document it will parse.
- Every per-row vector in a `ShredLeaf` (`def`, `i64`, `i128`, `boolean`, `offset`) holds
  exactly one entry per row (`variant_encode.hpp:384-386`).

So encoding rows `[a, b)` with a Shredder of the same `ShredType` gives exactly the
entries rows `[a, b)` get from encoding everything, with `offset` relative to that chunk's
own `bytes`. The file written is byte-identical to today's.

## 4. The change

**`variant_columns.hpp` — `VariantColumn::append`:**

1. Split `[0, rows)` into `K` chunks (§6 D-a: `ceil(rows / 4096)` capped at the pool
   width; one chunk = today's path).
2. One `Shredder` per chunk, all built from the column's `ShredType`. They are kept as
   members because the `ColumnInput`s point into their buffers until the write returns,
   as today's single `shredder_` is.
3. Run `encode` for each chunk as a task on `rugo_writer::pool()`. Wait for every task
   (they write members), then rethrow the error of the **lowest-numbered chunk** that
   failed, so the error is the one a serial encode would have raised first.
4. Build each leaf's `ColumnInput` from the chunks:
   - `def`, `i64`, `i128`, `boolean`: concatenated in chunk order into one array per leaf
     (they are what `ColumnInput` points at, and it needs one contiguous array).
   - `StrSlice` per row: points into that row's chunk's `bytes`, so the byte payloads
     are never copied.
   - Validity bits: built from the concatenated `def`, as today.

**`variant_encode.hpp` — `Shredder::encode`:** takes the chunk's first row number, so
"invalid JSON at row N" names the row in the morsel, as today. No other change.

**The 4 GiB check:** today `encode` refuses a leaf whose `bytes` pass `UINT32_MAX` in one
row group. With one buffer per chunk, each chunk's offsets stay in range on their own, but
the check is kept on the leaf's total, so what is refused does not change.

**Nothing else moves.** `infer_variant` (once per file), the `ColumnInput` shape, the
encode on the pool and the footer are unchanged.

**Pool use is safe:** `_writer_pool.hpp` requires that no writer task submits to the pool
and waits. The conversion runs on the calling thread, not in a pool task, as
`encode_row_group` already does when it submits the column tasks.

## 5. Tests and measurement

**Tests** (in `variant_encode_test.cpp`, run by `make variant-encode-test` under
ASan/UBSan, part of `make q`):

- The same documents encoded as 1 chunk and as K chunks give identical leaves. Cover a
  row count that is not a multiple of the chunk size, SQL NULL rows, missing fields,
  residual objects, arrays, ints/decimals/doubles, and a type mismatch that falls to
  `value`.
- An invalid JSON document in a later chunk is reported with its morsel row number, and
  when two chunks fail the lower row is the one reported.

**Byte identity on real data:** write the 10 JSONBench shards with the old build and the
new one and compare the files' hashes. Equal hashes also mean every read benchmark is
unaffected.

**Speed:**

- The compaction harness from R5 (3 arms, fresh process per cell, quiet-machine gate):
  variant before vs after. **[hypothesis]** about 125 ms per row group today (about 63 ms
  encode on the caller plus about 50 ms waiting for the pool) falls to about 60 ms, so
  OPTIMIZE goes from 5.5 s to about 3 s.
- The JSONBench load (`tests/performance/jsonbench/variant/load.py`, 24.4 s for 10M rows
  when last recorded): same code path.

Sign-off: `make q`, `make slt`, `make rt`.

## 6. Decisions for the architect

- **D-a, chunk size.** Fixed: 4,096 rows per chunk, at most the pool width. No setting,
  in line with D2's "no setting" for shredding. *Recommend.* The size only trades task
  overhead (a `Shredder` and a `ReadPool` per chunk) against balance; it never changes the
  output.
- **D-b, concatenate vs write in place.** Writing each chunk's per-row entries straight
  into shared arrays at its row offset avoids the concatenation, but changes how
  `ShredLeaf` stores data. The concatenation copies about 9 bytes per row per leaf at
  worst (`def` plus an `int64`), small next to the encode. *Recommend: concatenate; move
  to in-place only if the concatenation shows in the profile afterwards.*
- **D-c, inference stays serial.** `infer` could also be split, but merging the per-chunk
  `Seen` trees does not give the same answer once an object has more than 1,024 distinct
  names (`kShredTracked`: the first names seen win). It runs once per file. *Recommend:
  leave it.*
- **D-d, not in scope: overlapping row groups.** The caller still waits for the pool to
  encode row group N before converting N+1. Overlapping them touches every column type
  and the streaming writer's buffer lifetimes. *Recommend: not now; measure what this
  change leaves first.*
- **Noted, not in scope:** after this change the next bound is expected to be zstd level 7
  under the `storage` profile (5.6 cpu-s in the same profile). That level is a policy
  choice, not part of this design.
