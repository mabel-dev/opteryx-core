# Vector index — ANN access path over catalog tables

**Status:** PROPOSED. Nothing has been built or measured. Decisions for the architect are in §14.
**Date:** 2026-09-30
**Replaces:** an externally drafted HLD, "Object-Store Vector Index for Opteryx and Hadro". That
draft was written with only partial knowledge of the platform. §1 reviews it against the code;
the rest of this document redesigns from what actually exists.

---

## 0. Summary

The first requirement is the one the external draft leaves implicit.
`ORDER BY COSINE_DISTANCE(emb, q) LIMIT k` has one correct answer, and today's brute-force plan
returns it. An ANN index returns a *different* answer: it is approximate by construction, and
exact re-ranking of the candidates does not restore the rows it missed. An approximate search
therefore has to be **asked for explicitly in SQL**. The optimizer must never substitute it
silently (§7).

Given that, the design follows the platform's existing shapes rather than adding a second storage
system:

- **One index artifact per data file.** Each artifact is an immutable sidecar referenced from that
  file's manifest row, following the precedent of the per-file delete vectors
  (`opteryx_catalog/catalog/deletes.py`).
  - Row identity is `(data file, row ordinal)`, the address the catalog already uses for deletes
    and MERGE. It is stable for exactly as long as the file exists, and the sidecar dies with the
    file.
  - The snapshot → manifest → file → sidecar chain therefore gives consistency, time travel,
    rollback and expiry without extra machinery.
  - No separate "vector manifest", generation matching, LSM or tombstones are needed. Tombstones
    are the existing delete vectors.
- **Filter first, then search, then fetch.** Execution is a new native `Source` shaped like the
  existing `LatmatScanSource`:
  1. Pass 1 decodes the predicate columns into a survivor mask.
  2. The ANN search per file is *filtered by that mask and the delete bitmap during graph
     traversal*, which avoids an overfetch loop.
  3. A global candidate top-k is formed, and exact distances are computed with the SQL kernel.
  4. Pass 2 does a masked decode of the projected columns through the existing
     `submit_row_group(..., row_mask)`.
- **The index is not a "standalone library" in a new repo.** Its native parts go where
  standalone native code already lives:
  - distance kernels in **draken**
  - the index file format and search in **draken or rugo** (see D-3)
  - both ship in both wheels and are Python-free.

  Planning glue goes in `opteryx/planner`, the execution `Source` in `src/cpp/engine`, and
  lifecycle in `opteryx-catalog`.
- **S3 Vectors compatibility and Hadro are re-scoped** (§11). Hadro is a stateless, read-only S3
  emulator with no concept of a dataset, so it cannot be the "dataset-aware" builder the draft
  describes.

**Phase 0 prerequisite.** An embedding column cannot survive a catalog write today (§3).

---

## 1. Review of the external draft

| # | Draft claim | What the code shows | Consequence |
|---|---|---|---|
| 1 | "Hadro knows Parquet fragments, row groups, statistics, partitioning, file generations, physical row locations; can run `CREATE VECTOR INDEX`." | Hadro is ~2.7k lines of pure-Python FastAPI. It is a **read-only** S3 emulator/gateway. A catch-all route answers PUT/DELETE/PATCH with 405 (`hadro/src/hadro/app.py:40-44`), and `StorageBackend` has no write methods (`storage/base.py:1-50`). Every request addresses one object key. It has no tables, manifests, snapshots, partitions, catalog, or opteryx dependency. Row-group pruning happens inside rugo. | The entire "Hadro-native" section (draft §§20-21, Phase 4) has no foundation. Dataset awareness lives in **opteryx-catalog** (`Metastore`/`OpteryxCatalog`). See §11. |
| 2 | "Parquet remains authoritative for the embedding column." | The only vector type is `DRAKEN_VECTOR_FP16` (`draken/core/buffers.h:74`). rugo's writer emits it as `LIST<DOUBLE>`, lossily (`rugo/src/parquet/parquet_writer.pxi:1162-1175`), and it reads back as `ARRAY<FLOAT64>`, which `COSINE_SIMILARITY` refuses. rugo has no FIXED_SIZE_LIST and decodes FLBA only as DECIMAL (`decode_column.cpp:642-649`). The catalog write path drops logical types. | In a catalog table, the embedding *cannot be stored as an embedding today*. This becomes Phase 0 (§3, D-1). |
| 3 | "`float32`, 1-4096 dims, cosine or Euclidean" (S3 Vectors model). | Draken has **fp16 only**. `VECTOR(n, base_type)` raises for anything else (`opteryx/types/logical_type.py:238-249`). Cosine is the only metric (`draken/ops/kernels/function_vector_distance.cpp`). No L2, dot product or `VECTOR_DISTANCE` exists. | Either fp32 is added engine-wide or S3-style compatibility is lossy (D-2). Euclidean needs new kernels. |
| 4 | "Introduce a stable logical `row_id` independent of physical location." | No row id exists. Rows are addressed as `(file, ordinal)` by delete vectors (`deletes.py:40`) and MERGE (`$merge_file`, `$merge_ordinal`, `opteryx/operators/merge/merge.pyx:16-37`). Files are immutable, and compaction retires and replaces them. | A stable row id would be an engine-wide change to every write path, and it isn't needed. A **per-file** index keeps `(file, ordinal)` valid for the index's whole lifetime (§5). |
| 5 | "Vector manifest records `dataset_generation`; queries must not mix generations." | The catalog already has snapshots. The head pointer is `current-snapshot-id`, and rollback can move it *backwards*, so `max(id)` is wrong. Manifests are per-snapshot, cumulative, one row per file. | Tying the index to files (not snapshots) makes a mismatch impossible by construction. No generation protocol is needed. |
| 6 | "LSM segments, tombstones, background compaction of vector segments." | Delete vectors (per-file bitmaps referenced from the manifest) already exist. `OPTIMIZE TABLE` (`docs/COMPACTION_ENGINE_EXECUTION_DESIGN.md`) already rewrites files. | Tombstones = delete vectors, and index compaction = rebuilding the sidecar when a file is rewritten. No parallel lifecycle is needed. |
| 7 | "Duplicate filterable metadata into the index; S3-style metadata filters." | The filter columns *are* table columns, with manifest zone maps/KMV/histograms, row-group stats, and in-file blooms. Latmat already decodes predicate columns into a survivor set. | Don't duplicate them. Filter with the engine and pass the survivor mask into the ANN traversal (§8). |
| 8 | "Optimizer recognises `ORDER BY VECTOR_DISTANCE … LIMIT` as an access path." | This would change query answers silently, which violates "correctness is non-negotiable" and "no hidden behaviour". | Approximate search must be explicit in SQL (§7, D-4). |
| 9 | "Adaptive overfetch loop in the executor (K, 2K, 4K…)." | usearch 2.21.4 is vendored and has `filtered_search(vector, k, predicate)` (`third_party/usearch/include/usearch/index_dense.hpp:774-779`), which evaluates a predicate during traversal. | No re-query loop. The residual predicate is evaluated *before* the search, and its mask becomes the traversal predicate (§8). |
| 10 | "Cache ANN files in local memory → NVMe → GCS." | Prod is a Cloud Run worker with **8 GiB** and no NVMe. Local disk there is memory-backed. GCS GETs cost 110-150 ms each. HNSW is random-access and cannot be range-read efficiently. | Cold-query cost and the memory budget decide the algorithm choice (§9, D-5). This is the main risk in the whole design. |
| 11 | "New standalone library with api/, storage/, adapters/ for Opteryx/Hadro/S3." | The repo already has the standalone shape: draken and rugo are Python-free C++ and ship in both wheels (`build_common.py`). A Python adapter layer over them would put Python on the execution path. | Native parts go in draken/rugo and nothing else gets a new library (§4, D-3). |
| 12 | "Manifest publication is the atomic commit." | This is broadly true: data → manifest → snapshot doc → CAS on `current-snapshot-id` (`opteryx_catalog.py:8590`). But the CAS check runs in a read-only transaction (`_refuse_if_pointer_moved`, `:8563-8588`), and the `doc_ref.set()` at `:8662` sits outside it, leaving a window. | The index adds no new commit protocol. It rides the existing one, and the race is reported separately (§15). |

What the draft gets right, and this design keeps:
- ANN structures are derived and disposable.
- Objects are written before the commit that references them.
- Orphans are GC'd.
- Relational pruning happens before vector search.
- ANN is candidate generation, and exact distances decide the final order.

---

## 2. What exists today

| Piece | Where | State |
|---|---|---|
| `VECTOR(n)` type | `draken/core/buffers.h:74`, `draken/logical_type.h:86,111,180-199` | fp16 only. Contiguous `uint16` rows, stride `dim`. Dimension lives only on the interned LogicalType (the frozen 40-byte `DrakenVector` has no room for it). |
| `CAST(arr AS VECTOR(n))` | `function_vector_distance.cpp:537-626` | Native. Dimension 1..65535. |
| `COSINE_SIMILARITY` / `COSINE_DISTANCE` / `MATCH…AGAINST` / `EMBED` | `function_vector_distance.cpp`, `draken/ops/vector_cosine.h:35-115` | Native C ABI kernels, but a **scalar** loop (fp16→fp32→double) with no SIMD. |
| `EMBED` default provider | `opteryx/types/vectors/embedding_capability.py` | Static hashed lexical projection, 256-d, **not semantic**. MiniLM (ONNX, local) requires `OPTERYX_BUILD_EMBEDDINGS=1` plus a model dir. |
| usearch 2.21.4 (HNSW) + SimSIMD + fp16 | `third_party/usearch/` | Vendored. `index_dense_gt` supports f16 scalars, `filtered_search`, `save`/`load`, and `view(memory_mapped_file_t)` (MAP_SHARED, **path-only** in this version), plus `exclude_vectors` serialization. |
| `UsearchIndex` binding | `src/cpp/usearch_native.cpp` | f32 buffers only. No save, load or view. Called **only** by `tests/unit/core/test_usearch_cpp.py`. |
| Vector Top-K plan flag | `operator_fusion.py:42-63`, `plan_steps.pyx:5875-5917` | Sets `vector_topk_candidate`, but **nothing reads it** at runtime; it affects telemetry and EXPLAIN text only. |
| Latmat two-pass scan | `src/cpp/engine/native_latmat_scan_source.hpp` | Pass 1 builds a survivor set, `reduce_to_topn` (the draken `SortKeyCmp`) reduces it, pass 2 does a masked decode. Locator is `LatmatRowGroup{path, rg_idx, positions}`. The mask is **one byte per row-group row**. |
| Masked decode | `rugo/src/parquet/io_pipeline.hpp:3808` `submit_row_group(..., row_mask)` | Skips pages via the page index. Skene has **no** row-mask read (whole RG decode + gather). |
| Scan selection / handoff | `opteryx/managers/execution/compiler.py` `_compile_scan` (`:4473`) → `NativePlan.set_*_scan_source` → `native_plan_execute` | This is the "last Python". Unsupported shapes raise at compile time; there is no fallback. |
| Manifest | `opteryx-catalog`, `<loc>/metadata/manifest-<snap>-<nonce>.parquet` | One row per file: bounds, KMV, histograms, null counts, `delete_file_path`, …. The format is touched at **four** sites (manifest_io writer+reader, `FileEntry.from_datafile`, catalog `write_parquet_manifest`, `ParquetManifestEntry`). |
| Delete vectors | `opteryx_catalog/catalog/deletes.py`, `MOR_DELETES_DESIGN.md` | Per-snapshot parquet of per-file bitmaps, referenced per manifest row. Protected from deep-clean and expiry. **This is the precedent.** |
| Bind parameters | `opteryx/planner/ast_rewriter/__init__.py:37-138` | `?` and `:name`, substituted as literals. **Lists/arrays raise**, so a query vector cannot be a parameter today. |
| Catalog metastore | `opteryx_catalog/opteryx_catalog.py:924,964` | `OpteryxCatalog(Metastore)` on Firestore. `save_dataset_metadata` rewrites the whole doc with `set()`, so unknown fields are erased. That's why tags live in a subcollection. *(Memory records the live catalog as Postgres. The code in this checkout is Firestore. Flagged in §15.)* |

---

## 3. Phase 0 — an embedding must survive a catalog write

Nothing below matters until a catalog table can hold a `VECTOR(n)` column and read it back as
`VECTOR(n)`. Today:

- Parquet writes `VECTOR_FP16` as `LIST<DOUBLE>`: 4× the bytes, lossy on the way through fp32, and
  read back as `ARRAY<FLOAT64>`.
- The catalog write path drops logical types.
- Skene preserves `VECTOR_FP16` + dimension (`skene/src/writer.cpp:139-141`), but nothing in SQL
  or the catalog writes skene.

Options (D-1):

| Option | On-disk | Read cost | Interop | Work |
|---|---|---|---|---|
| **A. FLBA** | `FIXED_LEN_BYTE_ARRAY(2·dim)` with a rugo key-value annotation `opteryx.vector=fp16:<dim>` | Zero-copy into the `VECTOR_FP16` data buffer (same bytes, same stride) | Foreign readers see opaque binary | rugo FLBA non-decimal decode (currently throws); writer; declared-type plumbing through the catalog |
| **B. `LIST<FLOAT>`** | Standard 3-level list, fp32 | List decode (a "not a primary decode target" path) plus fp32→fp16 pack on every read | Any Parquet reader | List decode performance; declared-type cast on read |
| **C. Skene data files** | Native | Best | Opteryx-only | Catalog writes skene; skene needs a row-mask read for pass 2 |

Recommendation: **A**, if the engine is the only consumer that matters. It is the only option
where the stored bytes are the in-memory bytes. **B** if external readers matter. Either way, the
catalog's logical-type drop must be fixed, and that fix is Phase 0 regardless.

---

## 4. Where the code lives

Planning is Python and execution is native (CLAUDE.md §1/§2). Draken and rugo must run without
Python.

| Layer | Home | Contents |
|---|---|---|
| Distance kernels | **draken** (`draken/ops/`) | SIMD cosine (and, if D-2 adds it, L2/IP) over `VECTOR_FP16`, targeting NEON/AVX2 via `SIMD_STATIC_SELECT`. Needed anyway for exact re-ranking, and it makes the brute-force baseline honest (§12). SimSIMD is already vendored; D-6 decides whether draken calls it or owns its kernels. |
| Index file format + build + search | **draken or rugo** (D-3) | A C++ `VectorIndexFile` (writer and reader) wrapping usearch (or an IVF implementation, D-5). Its inputs are a `DrakenVector` of `VECTOR_FP16` plus an optional validity mask; it outputs `(ordinal, approx_distance)`. It has no Python and no opteryx dependency, so it ships in the standalone `rugo` wheel as well. Moving usearch into that extension's `sources=` must follow `docs/VENDORED_LIBRARY_RULE.md` (`make check-symbols`). |
| Execution | **`src/cpp/engine/`** | `NativeVectorIndexScanSource`, a sibling of `LatmatScanSource` that reuses `NativeScanColumnBuilder`, the rugo IO pipeline, `submit_row_group(..., row_mask)`, and draken `SortKeyCmp` / `TopNSink`. |
| Index build at write | **native sink** (`DataFileStream` path) or a native maintenance operator (D-7) | Builds the sidecar from the vectors the sink already holds. |
| Planning | `opteryx/planner` | Binding the explicit SQL form, an optimizer strategy stamping the scan, and a `_compile_scan` branch. |
| Lifecycle / metadata | **opteryx-catalog** | Index definitions, a new manifest column, deep-clean/expiry protection, and a catalog commit when sidecars are added. |

The existing dead or broken pieces (`vector_topk_candidate`, `vector_search_native.cpp`,
`vector_ranking.py`, `UsearchIndex`'s f32-only binding) are **not** extended. They are listed in
§15 for the architect to rule on.

---

## 5. The index artifact

### 5.1 Granularity: one sidecar per (data file, index)

- Each file's sidecar indexes that file's rows. The key is the **row ordinal within the file**
  (`uint32`, the same address space as delete vectors).
- The sidecar is written once, never modified, and lives exactly as long as its data file.
- Converting an ordinal to a latmat locator is arithmetic over the footer's row-group row counts:
  `ordinal → (rg_idx, offset)`.

Why per file:
- **No row-id problem.** `(file, ordinal)` cannot go stale while the file exists, and the sidecar
  cannot outlive the file.
- **No consistency protocol.** The manifest that lists a file also lists its sidecar, so any
  snapshot, including a time-travel snapshot or one restored by rollback, sees a consistent pair.
- **Pruning comes free.** Manifest pruning, row-group pruning and the Top-N boundary remove
  *files* before any sidecar is opened. The draft's "partition-aware indexes" (§27) happen
  automatically.
- **Incremental by construction.** An append adds files and therefore sidecars. Nothing global is
  rebuilt.

Cost:
- A query searches one index per surviving file and merges the results (k per file → global k).
- Catalog files are streamed at up to ~4 GB (`DataFileStream`), so file counts per table are
  modest. Small appends, however, produce small files. For those, a sidecar costs more than an
  exact scan of the file.
- Therefore **files under a row threshold get no sidecar and are always searched exactly** (D-8).
  That is a defined rule, visible in EXPLAIN and telemetry. It is not a fallback: the exact result
  for those rows is a superset of what approximate search could return.

### 5.2 Contents

```text
<location>/index/<index_name>/<data-file-stem>.vidx
```

- A small fixed header: magic, format version, index-definition id, dimension, metric, scalar kind,
  algorithm and parameters, the data file's path, size and row count (a binding check), and
  CRC/length of the body.
- The body is the serialized usearch `index_dense_gt<uint32_t>` (or IVF blocks, D-5).
- Vectors are stored **in** the sidecar, as fp16, or as i8 if D-5 picks quantization.
  - HNSW needs random access to vectors during traversal. Reading them from the data file would
    mean decoding the whole embedding column, which defeats the index.
  - This duplication is the draft's "Variant A" without the extra `vector-data.parquet`. The data
    file already *is* the reference representation, so a third copy buys nothing.

### 5.3 Referencing from the manifest

- Add one manifest column: `vector_index_paths: list<struct{index_id, path, size}>`, one entry per
  index that covers the file.
- It is optional on read (like `distinct_counts`), so old manifests are simply "no sidecars".
- It must be added at all **four** manifest sites (§2).

Index **definitions** (name, column, dimension, metric, algorithm and parameters, row threshold,
owner, created-at) live in a new dataset subcollection `indexes`, following `tags`. They must not
live on the dataset document, because `save_dataset_metadata`'s `set()` would erase them.

### 5.4 Lifecycle, mapped onto existing commits

| Event | What happens to sidecars |
|---|---|
| `CREATE INDEX` on an existing table | A maintenance operation builds sidecars for every live file above the threshold and commits a snapshot that changes only the manifest's `vector_index_paths`. It is modelled on `refresh_manifest` (operation type e.g. `index-build`), with the same CAS. Queries before that commit see no sidecars. |
| `INSERT` / `append` / CTAS | Either the sink builds sidecars for its new files before commit (synchronous), or the files land unindexed and a later maintenance commit adds them (D-7). |
| `DELETE` / `UPDATE` / `MERGE` | Delete vectors mark ordinals. The search predicate excludes them (§8). New files from MERGE follow the append rule. |
| `OPTIMIZE TABLE` | Output files are new, so they need new sidecars (same rule as append). Retired files' sidecars retire with them. |
| Rollback / `VERSION AS OF` / tags | Nothing extra. The older manifest references the older sidecars. |
| Expiry / deep clean | Sidecars must be recognised as **referenced** artifacts, exactly like `deletes-*.parquet`. Otherwise deep clean will quarantine them. Ownership rules (`ownership.py`) apply unchanged because they sit under the dataset's location. |
| `DROP INDEX` | Removes the definition and commits a manifest without that index's entries. Files are reclaimed by normal expiry, so older snapshots keep working. |
| Schema change to the indexed column | The sidecar header's definition id and dimension make a mismatch detectable. Plan-time use of a mismatched sidecar is **refused**, never skipped. |

---

## 6. Row identity

This needs no new design; it has been decided by §5.1. `(file, ordinal)` is the address. The
draft's "stable logical row_id" is unnecessary, because nothing outlives the file it points into.

If a future feature needs row identity across compaction, for example an external system holding
S3-Vectors-style keys, that is a **user key column** (§11), not an engine row id.

---

## 7. SQL surface and semantics

Rule: **an approximate result is only produced by syntax that says "approximate".**
`ORDER BY COSINE_DISTANCE(...) LIMIT k` stays exact forever, whether or not an index exists.

Candidate spellings (D-4). All of them must be parse-checked before being ruled
(`parse_sql(do_sql_rewrite(s), "mysql")`).

1. **A table function**, BigQuery-style:
   `SELECT … FROM VECTOR_SEARCH(docs, emb, <query>, k => 20) AS v JOIN …` or with a projection.
   This is explicit and composes, but a table-valued function whose scan is another relation is a
   new binder shape.
2. **A distinct distance function:** `ORDER BY APPROX_COSINE_DISTANCE(emb, <query>) LIMIT 20`.
   - It is only valid as the sole ORDER BY key over a table scan with a LIMIT, and is refused
     anywhere else.
   - It returns the **exact** distance value. "Approx" describes which rows are admitted, never
     the number reported.
   - It fits the existing HeapSort/`OperatorFusion` recognition, which already pattern-matches this
     shape.
3. **A clause:** `ORDER BY COSINE_DISTANCE(...) LIMIT 20 WITH INDEX docs_emb_idx`, via the
   dialect/aside parser. It is maximally explicit, but it is new grammar.

Common rules:
- The query vector must be a plan-time constant: a `CAST([...] AS VECTOR(n))` literal, a bound
  parameter, or `EMBED('literal')`. Bind parameters must learn to accept a list for this, since
  `_build_literal_node` raises today (§2).
- Dimension and metric must match an index definition on that column, or the statement is
  **refused** with a message naming the index and the mismatch.
- The approximate form over a table with **no** index is refused. It does not fall back to exact
  search, because the user asked for an index path.
- Recall/cost knob: `expansion_search` (HNSW `ef`) or `nprobe` (IVF), as a session variable and/or
  a statement option (D-9).
- DDL:
  `CREATE INDEX name ON table USING HNSW (col) WITH (metric='cosine', …)` /
  `DROP INDEX name ON table`. This is sqlparser's `CreateIndex` shape, which DuckDB's vss extension
  also uses. It must be parse-checked. Governance matches other dataset DDL: owner/WRITE grant,
  and egress rules as for writes.

---

## 8. Execution

One native `Source`, planned by `_compile_scan` when the scan carries an index stamp:

```text
per surviving data file (after manifest + row-group pruning):
  pass 1  decode predicate columns (existing NativeScanColumnBuilder / Pass1Pred)
          → survivor mask M_f (bit per file ordinal), AND NOT delete-bitmap D_f
  search  if file has a sidecar and popcount(M_f) ≥ τ:
              filtered_search(q, k', pred = M_f[ordinal])     -- filter DURING traversal
          else:
              exact SIMD distance over survivors of M_f       -- small / unindexed / very selective
          → candidates (ordinal, approx_dist), k' per file
merge     global candidate set across files (k' × files, bounded)
pass 2a   masked decode of the EMBEDDING column for candidates (submit_row_group(..., row_mask))
          exact distance via the SQL kernel → the reported value and the ordering key
reduce    draken SortKeyCmp / TopNSink to k   (same comparator as every other ORDER BY)
pass 2b   masked decode of remaining projected columns for the final k
```

Why this shape:
- **No overfetch loop.** Residual predicates are evaluated *before* the search. The mask becomes
  usearch's traversal predicate, so the graph walk only yields admissible rows. This covers every
  predicate the engine can evaluate natively, not just an S3-style metadata-filter subset.
- **Selective filters.** HNSW recall degrades when very few nodes are admissible, and brute force
  over a few survivors is cheap. So below a survivor threshold τ, the exact path is used for that
  file. The rule is deterministic, native and per-file, and both arms are correct under the
  "approximate" contract. τ is a measured constant, not a guess (§12).
- **The distance the user sees is always the SQL kernel's value.** usearch's SimSIMD fp16 cosine
  and draken's double-accumulated cosine differ in the last bits. Ordering by the kernel value
  means ties and NULL handling come from the same comparator `TopNSink` uses (the `reduce_to_topn`
  lesson).
- **Pass 2 reuses masked decode**, so only candidate rows' pages are decoded. For skene tables,
  pass 2 is a whole-RG decode plus gather until skene gets a row-mask read.
- **Plan-time checks, not runtime surprises.** A sidecar whose header disagrees with the manifest
  (path, size, row count, definition id) fails the query loudly.
- **GIL-free.** The entire loop is native. Python only stamps the plan.

k' (per-file candidates) defaults to k and is raised by the recall knob. Because the final order
is taken over a union of per-file top-k' sets, the result equals a global approximate top-k at the
same per-file recall.

---

## 9. Remote IO, memory, and the algorithm choice

This is the part of the draft that was hand-waved, and it decides whether the feature is usable in
production.

Facts:
- Prod workers are Cloud Run with 8 GiB. Local disk is memory-backed.
- A GCS GET costs ~110-150 ms, and scan waves are the floor.
- The vendored `memory_mapped_file_t` takes a **path**, not a buffer, so `view()` needs a local
  file. `load()` from a stream puts the whole index on the heap.
- HNSW's traversal is random-access across the whole structure, so it cannot be served by range
  reads.

Sizing example: 1M rows × 384-d fp16 = 768 MB of vectors, plus a graph of roughly 64 B/row at
M=16 (~64 MB). HNSW over that table needs **~830 MB resident** per query unless it is cached warm,
and a cold query must download all of it first.

| Algorithm | Cold query IO | Warm resident | Fits object storage | Code |
|---|---|---|---|---|
| **HNSW (usearch)**, fp16 vectors | Whole sidecar per surviving file | Whole sidecar | Poorly: only with a warm local cache | Vendored |
| **HNSW, i8-quantized** + exact re-rank from the data file | ~½ of fp16 | ~½ | Same shape, half the bytes | Vendored (usearch i8) |
| **IVF-flat** (centroids + per-list contiguous blocks, fp16) | Centroids (KB) + `nprobe` range GETs | Centroids only | **Yes**: matches the coalesced range-fetch pipeline | New code (small: k-means build + block layout) |
| **Exact SIMD brute force** (no index) | Embedding column of surviving RGs | None | Yes (today's path) | Kernels only |

Recommendation: **measure before choosing (D-5).**
1. Build the SIMD exact kernel first. It is needed anyway.
2. Measure cold and warm latency at 100k / 1M / 10M rows, locally and against GCS, before
   committing to HNSW.

If exact SIMD brute force over a manifest- and filter-pruned file set is already inside the latency
budget for realistic table sizes, the index is not worth its lifecycle cost. The draft's §27
insight, that pruning matters more than the ANN algorithm, points the same way.

Caching:
- Sidecars are immutable, and their path encodes their identity (files are immutable = law), so a
  path-keyed cache is correct.
- There is no data-block cache today. The manifest and footer caches explicitly do not cache data
  files.
- A sidecar cache is therefore a new budgeted component. Its budget must come out of the same
  8 GiB as query execution, so it needs a hard byte cap with LRU-K eviction, like the footer
  caches.
- This is a design item in its own right. It is not assumed to exist.

---

## 10. Build path

- **Where:** a native C++ builder in the index library (§4). Its input is a `VECTOR_FP16`
  `DrakenVector` stream in file-ordinal order, plus the delete bitmap if one exists (deleted rows
  are *not added*, so rebuilding after heavy deletes shrinks the index). Its output is the sidecar
  bytes, uploaded through the existing FileIO before the commit that references them.
- **Cost:** HNSW construction is CPU-heavy, roughly minutes for millions of rows even
  multi-threaded. Doing it synchronously inside every INSERT/MERGE/OPTIMIZE sink ties write latency
  to index build (D-7):
  - (a) **Synchronous in the sink.** The index is always complete, and write latency and cost go up.
  - (b) **Asynchronous maintenance commit** (a catalog task / `REFRESH INDEX`). Writes are
    unaffected, and freshly written files are searched exactly until indexed, under the §5.1 rule.
    EXPLAIN and telemetry report indexed vs exact file counts.
  - (c) **Hybrid.** Synchronous for OPTIMIZE (already a heavy maintenance write), asynchronous for
    INSERT/MERGE.

  Recommendation: (c). It keeps small writes cheap and makes compacted files, where the bulk of
  rows end up, immediately indexed.
- **Bloom-style trap to avoid:** blooms were written for months with no remote reader
  (`bloom_probe_is_local_path_only`). The index must not ship its writer before its production
  (GCS) reader is proven.

---

## 11. S3 Vectors compatibility and Hadro, re-scoped

### 11.1 What S3 Vectors maps to

| S3 Vectors | Opteryx equivalent | Gap |
|---|---|---|
| Vector bucket | Workspace / collection | None conceptually. Governance is richer (grants, egress, billing). |
| Index (name, dim, metric, dtype, metadata config) | A catalog table with a key column, a `VECTOR(n)` column and metadata columns, plus a vector index definition | `float32` vs fp16 (D-2). Non-filterable metadata is simply columns. |
| `PutVectors` (upsert by key) | `MERGE INTO … ON key` | The catalog has no unique constraint (informational keys only), so upsert-by-key semantics rely on MERGE. Many tiny puts → many tiny files, which needs batching or relies on OPTIMIZE. |
| `GetVectors` / `ListVectors` / `DeleteVectors` | `SELECT … WHERE key IN …` / scan with pagination / `DELETE WHERE key IN …` | Pagination tokens need a stable order, i.e. ORDER BY key. |
| `QueryVectors` (topK, filter, returnDistance/Metadata) | The explicit approximate SQL form (§7) with a WHERE clause | S3's JSON filter language must be translated to a SQL predicate. It is a strict subset of what the engine can filter on. |

So S3 Vectors compatibility is **a thin translation façade onto SQL and the catalog**. It is not a
separate storage library, and it adds nothing to the storage design. It belongs wherever an HTTP
API that can reach the catalog and engine is hosted (the opteryx service tier). It is Phase 4 at
the earliest and optional (D-10).

### 11.2 Hadro

Hadro can't host the full API: it has no write path, no catalog, and no engine. What it *could*
plausibly do, consistent with its per-object S3 Select model, is **single-object vector search**:
"top-k nearest rows in this one Parquet object using its `.vidx` sidecar". It would use the same
native index reader shipped in the `rugo` wheel. That is only possible if the index library lives
in draken/rugo (D-3), and it is only worth doing if someone needs it. It is not on the critical
path, and the draft's premise that Hadro builds indexes from dataset knowledge is withdrawn.

---

## 12. Measurement plan (baseline before the first edit)

1. **Baseline today's exact path.** Measure `ORDER BY COSINE_DISTANCE(emb, q) LIMIT 20` at
   100k / 1M / 10M rows × 384-d. This needs a skene fixture, because Parquet cannot hold the
   column yet (Phase 0). Report local and GCS, cold and warm, with DOP pinned (thread-seconds
   inflate with DOP), in interleaved ABBA.
2. **SIMD exact kernel.** Same matrix. This gives the ceiling the index must beat.
3. **Index prototypes.** usearch HNSW fp16 / i8, and IVF-flat if D-5 keeps it open: build time,
   sidecar bytes, cold/warm p50/p95, recall@k versus exact on the same queries.
4. **Filter-selectivity sweep.** 100%, 10%, 1%, 0.1% survivors, to fix τ (§8) from data.
5. **Decision gate.** If (2) is within budget for the realistic table sizes, stop at (2). Per
   "perf work that measures slower is deleted", the index ships only where it wins.

---

## 13. Phasing

| Phase | Deliverable | Gate |
|---|---|---|
| **0** | `VECTOR(n)` survives catalog write and read (D-1). The catalog stops dropping logical types. Bind parameters accept a list for a query vector. | Round-trip test through a catalog table. `make q`. |
| **1** | SIMD fp16 cosine (plus L2/IP if D-2) in draken. The measurement matrix (§12 steps 1-2). | ABBA numbers. Decision gate. |
| **2** | Index library (format, build, search) in draken/rugo. Prototype measurements (§12 steps 3-4). | Recall and latency against the gate. D-5 ruled. |
| **3** | Catalog: `indexes` subcollection, manifest column (four sites), deep-clean/expiry protection, `CREATE INDEX` / `DROP INDEX`, maintenance build commit. | Time travel, rollback, OPTIMIZE and expiry tests. |
| **4** | Planner form (D-4) + `NativeVectorIndexScanSource` + EXPLAIN/telemetry (indexed/exact file counts, candidates, recall knob). | `make q`. Exact-vs-approx recall tests. Delete-vector exclusion tests. |
| **5** | Sidecar cache (budgeted) for prod. The write-path build policy (D-7). | Prod-shaped GCS measurement. |
| **6 (optional)** | S3 Vectors façade (D-10). Hadro single-object search (D-3 dependent). | Only if there is a consumer. |

---

## 14. Decisions for the architect

| # | Decision | Options | Recommendation |
|---|---|---|---|
| D-1 | On-disk embedding representation in catalog tables | A FLBA + annotation / B `LIST<FLOAT>` / C skene data files | **A** (bytes on disk = bytes in memory). B if external readers matter. |
| D-2 | Vector element type and metrics | fp16 only + cosine / add fp32 `VECTOR` base type / add L2 & inner product | fp16 + cosine for v1. L2/IP kernels are cheap to add with the SIMD work. fp32 only if S3 compatibility (D-10) is pursued. |
| D-3 | Home of the index library | draken / rugo / `src/cpp` (opteryx-only) | **rugo** (it is a file format, and it keeps it in the standalone wheel), with distance kernels in draken. |
| D-4 | SQL spelling of "approximate" | `VECTOR_SEARCH` TVF / `APPROX_COSINE_DISTANCE` / `WITH INDEX` clause | `APPROX_COSINE_DISTANCE`: smallest binder change, explicit, and it matches existing fusion recognition. |
| D-5 | ANN algorithm | HNSW fp16 / HNSW i8 + re-rank / IVF-flat / none (SIMD brute force) | Decide from §12 measurements. Nothing is chosen before the numbers exist. |
| D-6 | SIMD distance: SimSIMD (vendored) vs draken-owned kernels | — | Draken-owned NEON/AVX2 via `SIMD_STATIC_SELECT`, consistent with the rest of draken. SimSIMD stays usearch-internal. |
| D-7 | When sidecars are built | sync in sink / async maintenance / hybrid | **Hybrid:** OPTIMIZE sync, INSERT/MERGE async. |
| D-8 | Minimum rows per file for a sidecar | fixed constant / measured / per-index option | Measured constant from §12, overridable per index. |
| D-9 | Recall knob exposure | session variable / statement option / index-definition default | Index-definition default, overridable by session variable. |
| D-10 | S3 Vectors façade | none / SQL-translation façade in the service tier | Defer. Revisit only with a consumer. |
| D-11 | Index definition as a catalog object | dataset subcollection `indexes` / new `ResourceType` | Subcollection. An index has no independent ownership or grants; it belongs to its table. |

---

## 15. Unrelated issues found during research (reported, not fixed)

1. **Catalog commit CAS window.** `_refuse_if_pointer_moved`
   (`opteryx_catalog.py:8563-8588`) checks the head pointer in a read-only transaction. The
   `doc_ref.set()` at `:8662` runs outside it, while the docstring claims one transaction.
2. **Dead vector plan flag.** `vector_topk_candidate` (`operator_fusion.py`, `plan_steps.pyx`) is
   set but never read at runtime. The docstring of `tests/unit/planner/test_vector_topk_plan.py`
   claims it keeps `TopNScanPushdownStrategy` off, but that strategy never reads it.
3. **Likely dead code.** `src/cpp/vector_search_native.cpp` (`exact_search_cosine`) is reached
   only from a fallback at `registrar/utility.pyx:121`. `opteryx/types/vectors/vector_ranking.py`
   has no engine caller.
4. **Broken dev tooling.** `dev/run_usearch_smoke.sh` compiles `dev/usearch_smoke.cpp`, which
   does not exist. The docstring of `dev/vendor_usearch.py` gives its own path as
   `tools/vendor_usearch.py`.
5. **Draken not declared as a dependency.** Hadro imports draken, but it is not declared in
   `pyproject.toml`; it presumably arrives transitively via the `rugo` wheel.
6. **Stale type docs?** `reference/types.json:941-961` says only literal arrays cast to `VECTOR`,
   but a column cast kernel exists.
7. **Memory vs code on the metastore.** Memory records the live catalog as Postgres (via
   `DATA_CATALOG_CONNECTION`). The `opteryx-catalog` code in this checkout is Firestore-only.
   This needs confirming before any catalog-side work in Phase 3.
