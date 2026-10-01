# Vector index — ANN access path over catalog tables

**Status:** PROPOSED. Nothing has been built or measured. Decisions for the architect are in §14.
**Date:** 2026-09-30, **rev 2** 2026-10-01 (architect direction: vectors live only in index files, not data files; index files are skene; ONNX cannot ship in the wheel, so embeddings come from an optional runtime-loaded provider whose weights are baked into the deployed image — §3, §9A). **rev 3** 2026-10-01: D-1 ruled (`VECTOR` is not a user-land concept; it exists only inside the index), D-12 approved (MIT licence verified), D-3 decided (§5.2).
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

- **Vectors never reach a data file.** `VECTOR(n)` exists only for the life of a query; table
  writes refuse it (§3). The vectors live **only in the index files**, which are skene files (they
  preserve `VECTOR_FP16` and its dimension). Because they are derived from table columns by
  a text column through a pinned provider, the index stays disposable and rebuildable (§9A).
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
  3. A global candidate top-k is formed. The graph's metric is the SQL kernel itself, so its
     distances are exact for every candidate it returns.
  4. Pass 2 does a masked decode of the projected columns through the existing
     `submit_row_group(..., row_mask)`.
- **The index is not a "standalone library" in a new repo.** Its native parts go where
  standalone native code already lives:
  - distance kernels in **draken**
  - ANN search in **draken** (usearch's core graph, vectors held in a skene file; D-3 decided)
  - both ship in both wheels and are Python-free.

  Planning glue goes in `opteryx/planner`, the execution `Source` in `src/cpp/engine`, and
  lifecycle in `opteryx-catalog`.
- **S3 Vectors compatibility and Hadro are re-scoped** (§11). Hadro is a stateless, read-only S3
  emulator with no concept of a dataset, so it cannot be the "dataset-aware" builder the draft
  describes.

**Phase 0 prerequisite.** The embedding provider hook (§9A) and the table-write refusal of
`VECTOR` (§3). Without a real provider, `EMBED` is a lexical hash and the index is only a
lexical index.

---

## 1. Review of the external draft

| # | Draft claim | What the code shows | Consequence |
|---|---|---|---|
| 1 | "Hadro knows Parquet fragments, row groups, statistics, partitioning, file generations, physical row locations; can run `CREATE VECTOR INDEX`." | Hadro is ~2.7k lines of pure-Python FastAPI. It is a **read-only** S3 emulator/gateway. A catch-all route answers PUT/DELETE/PATCH with 405 (`hadro/src/hadro/app.py:40-44`), and `StorageBackend` has no write methods (`storage/base.py:1-50`). Every request addresses one object key. It has no tables, manifests, snapshots, partitions, catalog, or opteryx dependency. Row-group pruning happens inside rugo. | The entire "Hadro-native" section (draft §§20-21, Phase 4) has no foundation. Dataset awareness lives in **opteryx-catalog** (`Metastore`/`OpteryxCatalog`). See §11. |
| 2 | "Parquet remains authoritative for the embedding column." | The only vector type is `DRAKEN_VECTOR_FP16` (`draken/core/buffers.h:74`). rugo's writer emits it as `LIST<DOUBLE>`, lossily (`rugo/src/parquet/parquet_writer.pxi:1162-1175`), and it reads back as `ARRAY<FLOAT64>`, which `COSINE_SIMILARITY` refuses. rugo has no FIXED_SIZE_LIST and decodes FLBA only as DECIMAL (`decode_column.cpp:642-649`). The catalog write path drops logical types. | **Superseded by architect direction (rev 2):** the embedding is *not* stored in data files at all. It lives only in skene index files, which preserve the type. `VECTOR` is refused on table write instead of being silently degraded (§3). |
| 3 | "`float32`, 1-4096 dims, cosine or Euclidean" (S3 Vectors model). | Draken has **fp16 only**. `VECTOR(n, base_type)` raises for anything else (`opteryx/types/logical_type.py:238-249`). Cosine is the only metric (`draken/ops/kernels/function_vector_distance.cpp`). No L2, dot product or `VECTOR_DISTANCE` exists. | Either fp32 is added engine-wide or S3-style compatibility is lossy (D-2). Euclidean needs new kernels. |
| 4 | "Introduce a stable logical `row_id` independent of physical location." | No row id exists. Rows are addressed as `(file, ordinal)` by delete vectors (`deletes.py:40`) and MERGE (`$merge_file`, `$merge_ordinal`, `opteryx/operators/merge/merge.pyx:16-37`). Files are immutable, and compaction retires and replaces them. | A stable row id would be an engine-wide change to every write path, and it isn't needed. A **per-file** index keeps `(file, ordinal)` valid for the index's whole lifetime (§5). |
| 5 | "Vector manifest records `dataset_generation`; queries must not mix generations." | The catalog already has snapshots. The head pointer is `current-snapshot-id`, and rollback can move it *backwards*, so `max(id)` is wrong. Manifests are per-snapshot, cumulative, one row per file. | Tying the index to files (not snapshots) makes a mismatch impossible by construction. No generation protocol is needed. |
| 6 | "LSM segments, tombstones, background compaction of vector segments." | Delete vectors (per-file bitmaps referenced from the manifest) already exist. `OPTIMIZE TABLE` (`docs/COMPACTION_ENGINE_EXECUTION_DESIGN.md`) already rewrites files. | Tombstones = delete vectors, and index compaction = rebuilding the sidecar when a file is rewritten. No parallel lifecycle is needed. |
| 7 | "Duplicate filterable metadata into the index; S3-style metadata filters." | The filter columns *are* table columns, with manifest zone maps/KMV/histograms, row-group stats, and in-file blooms. Latmat already decodes predicate columns into a survivor set. | Don't duplicate them. Filter with the engine and pass the survivor mask into the ANN traversal (§8). |
| 8 | "Optimizer recognises `ORDER BY VECTOR_DISTANCE … LIMIT` as an access path." | This would change query answers silently, which violates "correctness is non-negotiable" and "no hidden behaviour". | Approximate search must be explicit in SQL (§7, D-4). |
| 9 | "Adaptive overfetch loop in the executor (K, 2K, 4K…)." | usearch 2.21.4 is vendored and has `filtered_search(vector, k, predicate)` (`third_party/usearch/include/usearch/index_dense.hpp:774-779`), which evaluates a predicate during traversal. | No re-query loop. The residual predicate is evaluated *before* the search, and its mask becomes the traversal predicate (§8). |
| 10 | "Cache ANN files in local memory → NVMe → GCS." | Prod is a Cloud Run worker with **8 GiB** and no NVMe. Local disk there is memory-backed. GCS GETs cost 110-150 ms each. HNSW is random-access and cannot be range-read efficiently. | Cold-query cost and the memory budget decide the algorithm choice (§9, D-5). This is the main risk in the whole design. |
| 11 | "New standalone library with api/, storage/, adapters/ for Opteryx/Hadro/S3." | The repo already has the standalone shape: draken and rugo are Python-free C++ and ship in both wheels (`build_common.py`). A Python adapter layer over them would put Python on the execution path. | Native parts go in draken and skene, and nothing else gets a new library (§4, D-3). |
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

## 3. `VECTOR` is an index concept, not a user-land concept

**RULED (D-1, rev 3).** After this feature, `VECTOR` exists only inside the index. Users never
declare, cast to, store, project or receive one. The vectors live only in the index files (§5.2),
which use skene because it already preserves `VECTOR_FP16` and its mandatory dimension
descriptor (`skene/src/writer.cpp:139-141`).

What follows from the ruling. These are inferences from it, written in for confirmation:
- **Removed from the SQL surface:** the `VECTOR(n)` type spelling and `CAST(... AS VECTOR(n))`;
  `EMBED(...)` as a user-callable function; the `COSINE_SIMILARITY_VECTOR` /
  `COSINE_DISTANCE_VECTOR` overloads. Each is refused at bind with an error saying vectors are
  internal to indexes.
- **Kept:** the text overloads, `COSINE_SIMILARITY(text, text)` / `COSINE_DISTANCE(text, text)`,
  and `MATCH … AGAINST`. They embed internally and return FLOAT64, so no vector crosses into
  user land.
- **The query is text, not a vector.** The approximate form takes the query string and the index
  embeds it with its own pinned provider (§7, §9A). A user can no longer hand in a vector computed
  elsewhere.
- **Table writes can no longer meet a `VECTOR`,** because no user expression produces one. That
  removes today's silent degradation, where the Parquet writer turned it into `LIST<DOUBLE>`
  (4x the bytes, lossy via fp32, read back as `ARRAY<FLOAT64>`). A guard in the writers stays as
  an internal-invariant failure.
- **Bind parameters** need no list support. The query text is an ordinary string parameter.
- No rugo FLBA/LIST work, and no catalog logical-type plumbing, is needed for this feature.

## 4. Where the code lives

Planning is Python and execution is native (CLAUDE.md §1/§2). Draken and rugo must run without
Python.

| Layer | Home | Contents |
|---|---|---|
| Distance kernels | **draken** (`draken/ops/`) | SIMD cosine (and, if D-2 adds it, L2/IP) over `VECTOR_FP16`, targeting NEON/AVX2 via `SIMD_STATIC_SELECT`. It is the graph's metric functor (§5.2), and it makes the brute-force baseline honest (§12). SimSIMD is already vendored; D-6 decides whether draken calls it or owns its kernels. |
| Index files + build + search | **skene** (vectors file) + **draken** `ops/ann/` (graph and search), D-3 decided | Vectors are a skene file written and read with `skene::write_morsel` / `FileReader`; skene is its own extension (`build_common.py:1034`) and depends on draken alone. The graph is usearch's header-only core `index_gt`, with no Python and no opteryx dependency (§5.2). Any vendored source must follow `docs/VENDORED_LIBRARY_RULE.md` (`make check-symbols`). |
| Embedding provider hook | **draken** (kernel registration) + `opteryx/types/vectors` | A runtime-loaded provider; onnxruntime installed `--no-deps` (§9A). Registration already exists (`register_embedding_capability`). |
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

**DECIDED (D-3, rev 3).** Two immutable objects per (data file, index), both written before
the commit that references them:

```text
<location>/index/<index_name>/<data-file-stem>.vectors.skene   -- (ordinal UINT32, embedding VECTOR_FP16)
<location>/index/<index_name>/<data-file-stem>.graph           -- HNSW graph only, no vectors
```

- **Vectors: a skene file**, in ordinal order, so skene's dimension descriptor and checksums apply.
  It is the only place the vector exists (§3). It is still derived state: rebuildable from the
  source text column with the same provider and model (§9A).
- **Graph: usearch's core `index_gt`** (`third_party/usearch/include/usearch/index.hpp:1986`),
  not the `index_dense_gt` wrapper. Verified in the vendored 2.21.4:
  - `index_gt` stores only the graph. Its `add` and `search` (`index.hpp:2783`, `:3020`) take the
    **metric as a caller-supplied functor**, so distances are computed against vectors held
    outside it, here the skene column's fp16 buffer.
  - `search` also takes a **predicate** (the survivor and delete masks, §8).
  - `save_to_stream` / `load_from_stream` (`:3281`, `:3327`) serialise through byte callbacks, so
    the graph goes to any sink without a temporary file.
  - The `index_dense_gt` route is rejected. With `exclude_vectors`, its `view()` sets the vector
    count to 0 and then fails its own size check (`index_dense.hpp:1215-1236`, `:1288`), so it
    cannot run without its own copy of the vectors.
- The `.graph` object carries a small header ahead of the usearch stream: magic, format version,
  index-definition id, provider identity (§9A), dimension, metric, HNSW parameters, the data
  file's path, size and row count, the vectors file's path and size (binding checks), and the
  body's length and CRC. Any mismatch fails the query.
- **The code lives in draken** (`draken/ops/ann/`). It is a search over a `DrakenVector` of
  `VECTOR_FP16`, which is draken's domain, and draken is Python-free and ships in both wheels.
  usearch is header-only and already on draken's include path (`draken/core/fp16.h` uses its fp16
  library), so nothing new is compiled. `make check-symbols` still applies.
- **The metric functor is draken's own cosine kernel** (the SIMD one from Phase 1). The distances
  the search returns are therefore the SQL kernel's values, and no separate exact re-rank pass is
  needed (§8).
- For IVF (if D-5 measures it ahead), the vectors file is instead sorted by cluster, with one row
  group per cluster plus a centroid table, and no `.graph` object.

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
- The query is **text**: a string literal or a string bind parameter. The index embeds it with its
  own pinned provider. No vector appears in the statement (§3).
- The indexed column is a text column. The provider identity, dimension and metric come from the
  index definition. If the running provider's identity differs, the statement is **refused** with a
  message naming the index and the mismatch.
- The approximate form over a table with **no** index is refused. It does not fall back to exact
  search, because the user asked for an index path.
- Recall/cost knob: `expansion_search` (HNSW `ef`) or `nprobe` (IVF), as a session variable and/or
  a statement option (D-9).
- DDL:
  `CREATE INDEX name ON table USING HNSW (text_col) WITH (metric='cosine', …)` /
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
          → candidates (ordinal, distance), k' per file  (distance = draken kernel value)
merge     global candidate set across files (k' × files, bounded)
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
- **The distance the user sees is always the SQL kernel's value.** The graph's metric functor *is*
  the draken kernel (§5.2), so search distances need no re-rank. Ordering goes through the same
  comparator `TopNSink` uses, so ties and NULLs are handled as in every other ORDER BY (the
  `reduce_to_topn` lesson).
- **Pass 2b reuses masked decode** of the data file, so only the final rows' pages are decoded. For skene tables,
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
| **HNSW, i8-quantized vectors file** + exact re-rank (needs an fp16 copy too) | ~½ of fp16 | ~½ | Same shape, half the bytes | Vendored (usearch i8) |
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

## 9A. Embedding provider (runtime-loaded, weights in the image)

ONNX Runtime cannot ship in the wheel (it made the library too large to publish). The existing
MiniLM path links the ONNX SDK at build time (`src/cpp/minilm_native.cpp` includes
`<onnxruntime_cxx_api.h>`), so a pip extra cannot supply it. Direction:

- **Hook.** The provider loads ONNX Runtime **at runtime** from the `onnxruntime` pip package
  (shared library, stable C API via `OrtGetApiBase`). Nothing is linked at build time. The wrapper
  is rewritten against the C API, with only `onnxruntime_c_api.h` vendored (license and vendoring
  need agreement under the vendored-library rule).
- **No extra (rev 4).** onnxruntime's pip package declares numpy, protobuf and flatbuffers, so
  there is no `[embeddings]` extra. The image runs `pip install --no-deps "onnxruntime>=1.30,<2"`,
  which installs only the package and its native library; the engine never imports the package.
  Without it, installing the provider raises with that command. No fallback to the static hash.
- **Weights.** The user supplies the model. In the deployed image they are downloaded at
  **container build time**, with the checksum verified, into a fixed directory
  (`OPTERYX_MINILM_MODEL_DIR` today). Nothing downloads at query time, and an image carries one
  model. The engine ships no weights.
- **Locating the library.** The path to the shared library comes from the installed Python
  package, once, at provider registration. Execution stays native, so this is planning-phase
  Python, but it is a point for you to rule on.
- **Identity.** An index definition records provider name, model checksum, dimension and
  pooling/normalisation settings. `EMBED('literal')` in a query must resolve to the same
  identity or the statement is **refused**. A new image with a different model therefore refuses
  old indexes until they are rebuilt. It never scores them silently.
- **Why this fixes the build question.** Because the vectors are derived from a text column
  through a pinned provider, `CREATE INDEX … (body)` can rebuild the index at any time, so
  the sidecars stay disposable (§2.3 of the draft, kept).
- **APPROVED (D-12, rev 3). Verified 2026-10-01 against `onnxruntime` 1.30.0:**
  - Licence: **MIT** (the wheel's `onnxruntime/LICENSE` and its METADATA).
  - The wheel ships the shared library at `onnxruntime/capi/libonnxruntime.<ver>.dylib`/`.so`,
    which the hook loads.
  - Wheels on PyPI: Linux x86_64 and aarch64 for cp311-cp314 **and** cp314t (free-threaded);
    macOS arm64 cp311-cp314. **No macOS free-threaded wheel**, which affects dev only.
  - The pip package declares Python dependencies (including numpy); none are needed, hence
    `--no-deps`.
- **Still unverified:** the model's licence; image size and cold-start cost of loading the model on
  an 8 GiB worker; CPU embedding throughput, which decides how expensive the synchronous build in
  §10 would be.
- **Development.** The index mechanics are built and tested on the static hash provider, which is
  already native and deterministic. Real-embedding recall and latency measurements need a fixture
  of real embeddings generated offline under `dev/` with the A2 provider.

---

## 10. Build path

- **Where:** a native C++ builder in the index library (§4). Its input is the indexed expression
  (the indexed text column embedded by the §9A provider) evaluated to a `VECTOR_FP16` `DrakenVector` stream in
  file-ordinal order, plus the delete bitmap if one exists (deleted rows
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
| Index (name, dim, metric, dtype, metadata config) | A catalog table with a key column and metadata columns, plus an index definition. Vectors supplied by the caller (PutVectors) have **nowhere to live**: tables refuse `VECTOR` (§3), and index files are derived from a text column (§3). | `float32` vs fp16 (D-2). Non-filterable metadata is simply columns. |
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
in draken plus skene (D-3), and it is only worth doing if someone needs it. It is not on the critical
path, and the draft's premise that Hadro builds indexes from dataset knowledge is withdrawn.

---

## 12. Measurement plan (baseline before the first edit)

D-1 changes what "the exact baseline" means. In user land the only exact query is
`ORDER BY COSINE_DISTANCE(body, 'q') LIMIT k`, which embeds **every row on every query**. With a
real model that is not a viable baseline at any size. So the comparison is between index shapes,
all of which store vectors:

1. **Today's kernel, today's path.** Cost of `COSINE_DISTANCE(text, text)` per row, static
   provider and ONNX provider. This is the number that proves an index is needed.
2. **Vectors file only, exact scan** (no graph): SIMD cosine over the skene vectors file of every
   surviving data file. This is the floor the graph must beat, and it is also a valid shipping
   shape (D-5 "none").
3. **Vectors file + HNSW graph:** build time, `.graph` bytes, cold/warm p50/p95, recall@k against
   (2) on the same queries.
4. **Filter-selectivity sweep:** 100%, 10%, 1%, 0.1% survivors, to set τ (§8) and the D-8 row
   threshold from data.
5. Matrix: 100k / 1M / 10M rows, local and GCS, cold and warm, DOP pinned, interleaved A/B.
6. **Decision gate (D-5):** if (2) meets the latency budget at realistic sizes, ship (2) and do not
   build the graph path. Perf work that measures slower is deleted.

---

## 13. Build plan

Each step is a separate change, approved before the next starts, and none is complete until
`make q` passes. Blocking decisions are named at the step they block.

### Stage A: foundations (no index yet)

**DELIVERED 2026-10-01** (A0-A4, `make q` green, `make check-symbols` clean):
- **A0 baseline:** scalar fp16 cosine ≈ 0.79 ns/element (202 / 304 / 604 ns per row at
  256 / 384 / 768 dims, M-series). `ORDER BY COSINE_DISTANCE(t, 'quick brown fox') LIMIT 10`
  over 1M JSONL rows: ~1.65 s (static provider; embedding dominates).
- **A1:** `draken/ops/vector_cosine_row.h`, the one cosine definition: fp64 accumulation in 8
  fixed lanes, so NEON, AVX2 and scalar give **identical bits** (27,200 random pairs, 1-1100
  dims, wide exponent range; AVX2 verified under Rosetta; same result hash on both). 4.4-4.6x
  faster per row. End-to-end text query ~1.6 s (embedding still dominates). Results differ
  from the old kernel only in the last bits (summation order).
- **A2:** `src/cpp/minilm_native.cpp` rewritten against the ONNX Runtime C API, loading
  `libonnxruntime` at runtime from the `onnxruntime` package (headers vendored under
  `third_party/onnxruntime`, v1.30.0, MIT); onnxruntime installed `--no-deps` (no extra). Build-time
  SDK discovery deleted; the extension always builds. The provider identity
  (`minilm-l6-v2:<max_len>:sha256:<weights+vocab>`) is recorded on the capability. Verified:
  every refusal path, plus dlopen → C API v30 → CreateEnv → CreateSession against the real
  1.30.0 library. **Not yet verified: a real inference** (needs the model).
- **A3:** `VECTOR` refused on every type spelling (CAST, `::`, TRY_CAST, column
  declarations); `EMBED` removed as a SQL function; the vector overloads of `COSINE_*` and
  `draken_cast_array_to_vector` removed; skene relations carrying a VECTOR column refused at
  schema read. Text `COSINE_*` and `MATCH … AGAINST` unchanged.
- **A4:** deleted the Python embedding providers (`embeddings.py`, `vector_ranking.py`,
  `vector_types.py`, `vector_math.pyx`), the Python `COSINE_*`/`EMBED` implementations
  (proven unreachable by poisoning them across 239 tests and 10 SQL shapes), the
  `vector_topk_candidate` flag, `vector_search_native.cpp`, the f32 `UsearchIndex` binding
  and their tests, and `dev/run_usearch_smoke.sh`. The usearch headers stay vendored for B2.
  Kept: draken's column cosine kernel `cosine_sim_fp16` and its tests, which the B4 exact
  scan uses.

| Step | Work | Files | Gate |
|---|---|---|---|
| **A0** | Baseline the current cosine kernel and the `COSINE_DISTANCE(text, text)` path (§12 step 1), before any edit. | `scratch/` bench only | Numbers recorded |
| **A1** | SIMD fp16 cosine in draken: NEON / AVX2 / scalar via `SIMD_STATIC_SELECT`, replacing the scalar loop. ⚠ Changing from double to fp32 accumulation changes results in the last bits. Keeping double accumulation is the default unless you rule otherwise. | `draken/ops/vector_cosine.h`, `draken/simd/` | Same results as the scalar path (or as ruled); A/B speed-up |
| **A2** | ONNX provider hook (D-12): vendor `onnxruntime_c_api.h` under `third_party/onnxruntime/`, rewrite `minilm_native.cpp` against the C API and load the library at runtime, delete the build-time SDK discovery in `setup.py` (~1492), install onnxruntime `--no-deps` (no extra), and record provider identity (model sha256, dimension, pooling). Without it, the provider fails with an install instruction. Tests needing the model run in a CI job that installs it, and fail (do not skip) when it is missing. | `src/cpp/minilm_native.cpp`, `setup.py`, `pyproject.toml`, `opteryx/types/vectors/embedding_capability.py`, `third_party/` | `make check-symbols`; wheel builds with no ONNX present; provider round-trip test |
| **A3** | Remove `VECTOR` from user land (D-1): the binder refuses the `VECTOR(n)` type, `CAST … AS VECTOR`, user-called `EMBED`, and the vector overloads of `COSINE_*`. Internal kernels stay. Update the generators in `dev/`, not `reference/`. | `opteryx/planner/binder/`, `registrar/utility.pyx`, `dev/` generators, tests | Refusal tests; text overloads unchanged |
| **A4** | *(needs your approval)* Delete the dead vector code from §15: the `vector_topk_candidate` flag, `vector_search_native.cpp`, `vector_ranking.py`, the f32 `UsearchIndex` binding, and the broken usearch smoke script. A3 is the natural point to do it. | as listed in §15 | `make q` |

### Stage B: index files as a library, no SQL

**B1/B2 DELIVERED 2026-10-02.**
- **B1 needs no code:** skene's existing writer/reader already round-trips `VECTOR_FP16` with its
  dimension. Convention (supersedes the `ordinal` column in §5.2): the vectors file has ONE
  column `embedding VECTOR_FP16(dim)`, and row i is data-file ordinal i (null where the text is
  null). Deleted rows stay in the file; they are excluded at build and masked at search.
- **B2:** `draken/ops/ann/fp16_cosine_hnsw.h`, built on usearch `index_gt<double, uint32_t, uint32_t>`.
  - The graph holds no vectors. It reads the column through `data[selection[r]]`, mapping
    slot to row via the add callback.
  - The metric is `1 - clip(cosine_row_fp16)`, so the distances are bit-identical to the
    SQL kernel's.
  - Null, deleted, zero-magnitude and non-finite rows are outside the searchable domain on
    both paths.
  - Serialized as a 56-byte header (magic, version, metric, dimension, M, ef, row count,
    indexed count) + caller binding + body, with an XXH3 checksum. A wrong checksum, row
    count, dimension or magic is refused.
  - `HnswSearcher` takes an admitted mask, applied during traversal. `exact_topk` is the
    single-pass exact path.
  - Test/measurement binding: `opteryx.compiled.nanobind.vectors.ann_*`. Tests:
    `tests/unit/core/test_vector_ann.py`.

**B4 provisional measurements, 2026-10-02.** Synthetic clustered fp16 data, 384 dims,
M-series laptop, 6 P-core threads. Recall on synthetic data is NOT representative; real
embeddings are pending B3.

| rows | exact, 1 thread | exact, 6 threads | HNSW build (6 threads) | graph size | vectors size | HNSW ef=64 | HNSW ef=128 |
|---|---|---|---|---|---|---|---|
| 100k | 6.6 ms (66 ns/row) | 1.3 ms | 3.5 s | 14.5 MB | 77 MB | 0.20 ms, recall 1.00 | 0.23 ms, recall 1.00 |
| 1M | 67 ms (67 ns/row) | 12.1 ms | 95 s | 145 MB | 768 MB | 0.60 ms, recall 0.78 | 0.92 ms, recall 0.91 |

Filtered search (1M rows; HNSW filtered during traversal vs an exact scan of the admitted rows):

| admitted | HNSW filtered | exact over mask |
|---|---|---|
| 10% | 1.7 ms (recall 0.99) | 17.2 ms |
| 1% | 8.5 ms (recall 0.98) | 2.5 ms |
| 0.1% | 84 ms (recall 0.98) | 0.6 ms |

What this says:
1. **Compute:** the exact path is linear at ~67 ns per row per core. 10M rows on 32 prod
   workers is roughly 20 ms. HNSW saves this compute, but costs ~95 s of build per 1M rows
   and 144 bytes per row.
2. **Selective filters invert the choice.** Below roughly 1-10% admitted, the exact scan over
   the mask beats filtered HNSW by up to 140x. τ (§8) lies in that band. This confirms the
   per-file switch is required, not optional.
3. **IO is the real cost, and HNSW does not reduce it.** The graph reads vectors at random, so
   the whole vectors file (768 bytes per row; 768 MB per 1M rows) must be resident for either
   path. A cold query against GCS pays that transfer either way, and the 8 GiB worker cannot
   cache it for large tables. Only a layout that reads part of the vectors reduces IO:
   IVF clustered row groups (read the centroids, then `nprobe` row groups), optionally with
   i8 quantisation.

**Next measurement before ruling D-5:** an IVF-flat prototype (k-means at build, cluster-sorted
skene row groups), measuring bytes read and latency per query against exact and HNSW, on real
embeddings (B3).

| Step | Work | Gate |
|---|---|---|
| **B1** | Writer and reader for the skene vectors file: `(ordinal UINT32, embedding VECTOR_FP16)` from a `DrakenVector` plus a delete mask. | C++ round-trip tests |
| **B2** | `draken/ops/ann/`: build an `index_gt` graph whose metric is the A1 kernel, search with a predicate, and the `.graph` header with its binding checks, serialised through `save_to_stream` / `load_from_stream`. | C++ tests: recall against an exact scan, predicate filtering, header-mismatch refusal |
| **B3** | Real-embedding fixture: a `dev/` script uses the A2 provider to embed a text dataset at 100k / 1M / 10M rows into vectors files. | Fixture exists, with checksums |
| **B4** | **Measurement gate** (§12 steps 2-6), which rules **D-5**, τ and **D-8**. If the vectors-file exact scan wins, B2 is deleted. | D-5 ruled |

### Stage C: catalog lifecycle

| Step | Work | Gate |
|---|---|---|
| **C0** | *(needs your approval)* Fix the commit check-and-set race (§15 item 1) first, because every index commit relies on it. | Catalog race test |
| **C1** | opteryx-catalog: `indexes` subcollection (D-11), `vector_index_paths` manifest column at all four sites, deep-clean and expiry treating sidecars as referenced, and an `index-build` commit (like `refresh_manifest`). | Time-travel, rollback, expiry and deep-clean tests |
| **C2** | `CREATE INDEX … USING HNSW (text_col) WITH (…)` / `DROP INDEX`: parse-check, binder, governance (owner/WRITE grant, egress), plus a native build operator (scan text → embed → B1/B2 → upload → C1 commit). | DDL tests on a local catalog |
| **C3** | Write-path build policy (**D-7**, needs ruling): OPTIMIZE builds sidecars synchronously; INSERT/MERGE leave files unindexed until a maintenance build. | OPTIMIZE/MERGE lifecycle tests |

### Stage D: query path

| Step | Work | Gate |
|---|---|---|
| **D1** | The approximate SQL form (**D-4**, needs ruling; recommended `APPROX_COSINE_DISTANCE(text_col, 'query')`), the refusal rules in §7, the recall knob (**D-9**), and an optimizer strategy that stamps the scan. | Binder and refusal tests |
| **D2** | `NativeVectorIndexScanSource` in `src/cpp/engine/`: pass-1 mask, per-file filtered search (or exact below τ), merge, `TopNSink`, then pass-2 masked decode. Plus a `_compile_scan` branch and EXPLAIN/telemetry (indexed vs exact file counts, candidates). | Recall tests, delete-vector exclusion, filter tests, `make q` |
| **D3** | Budgeted sidecar cache for GCS (§9), then a prod-shaped measurement. | GCS cold/warm numbers |

**Outside this repo:** the deployment image runs `pip install --no-deps "onnxruntime>=1.30,<2"` and downloads the
model at container build with its checksum verified (§9A).

**Optional, only with a consumer:** S3 Vectors façade (D-10) and Hadro single-object search.

---

## 14. Decisions for the architect

| # | Decision | Options | Recommendation |
|---|---|---|---|
| D-1 | `VECTOR` in user land | — | **RULED rev 3:** not a user-land concept. It exists only inside the index (§3). |
| D-2 | Vector element type and metrics | fp16 only + cosine / add fp32 `VECTOR` base type / add L2 & inner product | fp16 + cosine for v1. L2/IP kernels are cheap to add with the SIMD work. fp32 only if S3 compatibility (D-10) is pursued. |
| D-3 | Home of the ANN code and the graph format | — | **DECIDED rev 3:** draken `ops/ann/`, usearch core `index_gt` (graph-only), vectors in a skene file, graph in a separate `.graph` object (§5.2). |
| D-4 | SQL spelling of "approximate" | `VECTOR_SEARCH` TVF / `APPROX_COSINE_DISTANCE` / `WITH INDEX` clause | `APPROX_COSINE_DISTANCE`: smallest binder change, explicit, and it matches existing fusion recognition. |
| D-5 | ANN algorithm | HNSW fp16 / HNSW i8 + re-rank / IVF-flat / none (SIMD brute force) | Decide from §12 measurements. Nothing is chosen before the numbers exist. |
| D-6 | SIMD distance: SimSIMD (vendored) vs draken-owned kernels | — | Draken-owned NEON/AVX2 via `SIMD_STATIC_SELECT`, consistent with the rest of draken. SimSIMD stays usearch-internal. |
| D-7 | When sidecars are built | sync in sink / async maintenance / hybrid | **Hybrid:** OPTIMIZE sync, INSERT/MERGE async. |
| D-8 | Minimum rows per file for a sidecar | fixed constant / measured / per-index option | Measured constant from §12, overridable per index. |
| D-9 | Recall knob exposure | session variable / statement option / index-definition default | Index-definition default, overridable by session variable. |
| D-10 | S3 Vectors façade | none / SQL-translation façade in the service tier | Defer. Revisit only with a consumer. |
| D-12 | Embedding provider | — | **APPROVED rev 3:** runtime-loaded ONNX Runtime C API (onnxruntime installed `--no-deps`), weights baked into the image at container build. MIT licence verified (§9A). |
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
8. **Leftover ONNX/MiniLM code.** The ONNX Runtime SDK discovery (`setup.py` ~1492), the
   `src/cpp/minilm_native.cpp` build, and the comment-only `embeddings` extra in `pyproject.toml`
   remain after ONNX was dropped from the wheel. §9A's C-API rewrite would replace them. Until
   then they are dead weight, and I have not touched them.
