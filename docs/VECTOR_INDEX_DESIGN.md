# Vector index — ANN access path over catalog tables

**Status:** IN BUILD. Stages A and B delivered, Stage C in progress (§13). Open decisions are in §14.
**Date:** 2026-09-30, **rev 2** 2026-10-01 (architect direction: vectors live only in index files, not data files; index files are skene; ONNX cannot ship in the wheel, so embeddings come from an optional runtime-loaded provider whose weights are baked into the deployed image — §3, §9A). **rev 3** 2026-10-01: D-1 ruled (`VECTOR` is not a user-land concept; it exists only inside the index), D-12 approved (MIT licence verified), D-3 decided (§5.2). **rev 5** 2026-10-02: D-5 ruled (IVF-flat) and D-7 ruled (per-index sync/async, default async, compaction never re-embeds); storage accounting and billing (§5.5), compaction carry (§5.6), GC sizing (§5.4) and index discovery (§7A) designed; stale HNSW/usearch text corrected. **rev 6** 2026-10-02: D-13 ruled (index storage charged at logical bytes), D-14 ruled (compaction never embeds; compaction and index builds never run at the same time on a table, enforced by a maintenance lease, §5.7), D-15 ruled (follow sqlparser: `SHOW INDEXES FROM t`), D-16 ruled (`REFRESH INDEX`, fired by every commit that adds data files and by CREATE INDEX).
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
  2. The IVF search per file (probe the nearest clusters) scores only rows in that mask and
     outside the delete bitmap, which avoids an overfetch loop. Below a survivor threshold
     the file is scanned exactly instead.
  3. A global candidate top-k is formed. The index's metric is the SQL kernel itself, so its
     distances are exact for every candidate it returns.
  4. Pass 2 does a masked decode of the projected columns through the existing
     `submit_row_group(..., row_mask)`.
- **The index is not a "standalone library" in a new repo.** Its native parts go where
  standalone native code already lives:
  - distance kernels in **draken**
  - IVF-flat ANN search in **draken** (`draken/ops/ann/fp16_cosine_ivf.h`; vectors and
    centroids held in skene files; D-3, D-5)
  - both ship in both wheels and are Python-free.

  Planning glue goes in `opteryx/planner`, the execution `Source` in `src/cpp/engine`, and
  lifecycle in `opteryx-catalog`.
- **Indexes are accounted separately from data.** Each index file's size is recorded in the
  manifest, totalled in the snapshot summary apart from data bytes, and reported to billing as
  its own figure (§5.5). Compaction carries vectors instead of re-embedding (§5.6), and expiry
  reclaims index files with their recorded sizes (§5.4).
- **S3 Vectors compatibility and Hadro are re-scoped** (§11). Hadro is a stateless, read-only S3
  emulator with no concept of a dataset, so it cannot be the "dataset-aware" builder the draft
  describes.

**Phase 0 prerequisite (delivered in Stage A).** The runtime-loaded embedding provider (§9A)
and the removal of `VECTOR` from user land (§3).

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
| usearch 2.21.4 (HNSW) + SimSIMD + fp16 | `third_party/usearch/` | *(As found 2026-09-30; since D-5 only `fp16/` remains vendored.)* Vendored. `index_dense_gt` supports f16 scalars, `filtered_search`, `save`/`load`, and `view(memory_mapped_file_t)` (MAP_SHARED, **path-only** in this version), plus `exclude_vectors` serialization. |
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
| Index files + build + search | **skene** (vectors + centroids files) + **draken** `ops/ann/fp16_cosine_ivf.h` (IVF build, probe, top-k) | Both files are skene, written and read with `skene::write_morsel` / `FileReader`; skene is its own extension (`build_common.py:1034`) and depends on draken alone. IVF is draken's own code, with no Python, no opteryx dependency and no vendored ANN library (§5.2). |
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

**DECIDED (D-3 rev 3, D-5 ruled 2026-10-02: IVF-flat).** Two immutable skene objects per
(data file, index), both written before the commit that references them:

```text
<location>/index/<index_id>/<data-file-stem>-<nonce>.vectors.skene     -- (embedding VECTOR_FP16, ordinal UINT32)
<location>/index/<index_id>/<data-file-stem>-<nonce>.centroids.skene   -- (centroid VECTOR_FP16, rows UINT32, row_groups ARRAY<INT32>)
```

- **Keyed by the index id, not its name**, so `DROP INDEX x; CREATE INDEX x` never reuses a
  path. The nonce makes every build a new object (files are immutable).

- **Vectors file, grouped by cluster (rev 7):** every row group holds rows of ONE cluster,
  but a cluster may span several row groups, interleaved with other clusters'. The build
  streams: rows are held per cluster and a cluster's block is written as a row group every
  `flush_rows` rows (sized from the build's memory budget), the remainders at the end. A
  file's vectors are therefore never resident at once. `embedding` is the FIRST column, so
  the streaming skene writer streams it and stages only the ordinals. Written without
  read acceleration (no per-row-group statistics, zone maps or sketches: nothing prunes a
  vectors file on values). Only searchable, non-deleted rows are stored; `ordinal` is the
  row's PHYSICAL position in the data file, numbered before deletes. It is the only place the vector exists (§3), but it is
  still derived state, rebuildable from the source text with the same provider and model (§9A).
- **Centroids file:** row k is cluster k's unit-length centroid, its row count, and the
  list of its row groups in the vectors file (empty when the cluster is empty). Small (K × dim × 2 bytes; 0.44 MB at
  K=579, dim=384). It is read first; then only the `nprobe` probed row groups are fetched.
- **Binding:** a skene file has no free-form section. So the binding (index-definition id,
  embedding identity, data file path/size/row count) lives in the manifest entry that
  references both objects, and is checked against them when the plan is built.
- **The code lives in draken** (`draken/ops/ann/fp16_cosine_ivf.h`): `ivf_plan` (K and a
  seeded sample chosen from the candidate rows — valid, not deleted — before embedding),
  `ivf_train` (spherical k-means on the embedded sample), `ivf_assign` and `ClusterStream`
  (the streaming build); `ivf_build` composes them in memory and is bit-identical to its
  pre-split output (tested); `ivf_probe` (nearest non-empty centroids), and `TopK::offer` (scores a contiguous block keyed by ordinal, which
  is exactly a cluster row group as it arrives). `exact_topk` is the same accumulator over
  every row.
- **The metric is draken's own cosine** (the SIMD kernel from A1), as `1 - clip(cos)`. The
  distances returned are therefore the SQL kernel's values, and no re-rank pass is needed (§8).

### 5.3 Referencing from the manifest

**Built in C1 (paths) and C1b (sizes).** Six parallel array columns
on each manifest entry, one element per index that covers that data file:

| column | type | meaning |
|---|---|---|
| `vector_index_ids` | `ARRAY<VARCHAR>` | index definition id |
| `vector_index_vectors` | `ARRAY<VARCHAR>` | vectors file path |
| `vector_index_centroids` | `ARRAY<VARCHAR>` | centroids file path |
| `vector_index_vectors_bytes` | `ARRAY<INT64>` | vectors file on-disk size *(rev 5)* |
| `vector_index_centroids_bytes` | `ARRAY<INT64>` | centroids file on-disk size *(rev 5)* |
| `vector_index_logical_bytes` | `ARRAY<INT64>` | logical (decoded) size of both files together *(rev 6, billed; §5.5)* |

- Empty arrays mean "this file has no index". Old manifests without the columns read as empty.
- The arrays must have equal length. A mismatch is refused when the entry is read
  (`index_refs`), never repaired.
- Sizes are the sizes the builder wrote, handed to the commit with the paths. They are never
  read back from storage. A commit that names an index file without its size is refused.
- The engine's manifest decoder reads columns by name, so it ignores these until Stage D needs
  them.

Index **definitions** (name, id, column, method, metric, clusters, nprobe, build mode, embedding
identity and width, owner, created-at) live in the dataset subcollection `indexes`, following
`tags`. They must not live on the dataset document, because `save_dataset_metadata`'s `set()`
would erase them.

### 5.4 Lifecycle, mapped onto existing commits

| Event | What happens to sidecars |
|---|---|
| `CREATE INDEX` on an existing table | A maintenance operation builds sidecars for every live file above the threshold and commits a snapshot that changes only the manifest's index columns. With `build = 'sync'` CREATE INDEX runs this build before it returns; with `async` it fires `REFRESH INDEX` (§10). It is modelled on `refresh_manifest` (operation type e.g. `index-build`), with the same CAS. Queries before that commit see no sidecars. |
| `INSERT` / `append` / CTAS | Either the sink builds sidecars for its new files before commit (synchronous), or the files land unindexed and the commit fires `REFRESH INDEX`, per the index's build mode (D-7, D-16; §10). |
| `DELETE` / `UPDATE` / `MERGE` | Delete vectors mark ordinals. The search predicate excludes them (§8). New files from MERGE follow the append rule. |
| `OPTIMIZE TABLE` / compaction | Compaction never embeds (D-14). It only merges files with the same index coverage: indexed inputs give an output whose index is built by **carrying** their vectors, referenced in the same compaction commit; unindexed inputs give an unindexed output that the commit's `REFRESH INDEX` builds (§5.6). It holds the maintenance lease (§5.7). Retired files' index files leave the head manifest with them. |
| Rollback / `VERSION AS OF` / tags | Nothing extra. The older manifest references the older sidecars. |
| Expiry / deep clean | Sidecars must be recognised as **referenced** artifacts, exactly like `deletes-*.parquet`. Otherwise deep clean will quarantine them. Ownership rules (`ownership.py`) apply unchanged because they sit under the dataset's location. An index file is deleted once no retained snapshot or tag references it, and expiry counts its **recorded** size (§5.3) in the reclaimed-bytes tally (rev 5; today it counts 0). |
| `DROP INDEX` | Removes the definition and commits a manifest without that index's entries. Files are reclaimed by normal expiry, so older snapshots keep working. |
| Commit lost (CAS) after index files were written | The files are unreferenced objects under the dataset's location. Deep clean reclaims them like any orphaned data file. Nothing reads them, because no manifest names them. |
| Schema change to the indexed column | The sidecar header's definition id and dimension make a mismatch detectable. Plan-time use of a mismatched sidecar is **refused**, never skipped. |

### 5.5 Storage accounting and billing (rev 5; D-13 ruled rev 6)

Today the storage sweep (`xb500.opteryx` `app/operations/record_storage_billing.py`) bills each
dataset's **head** snapshot `total-data-size`: the logical (uncompressed) size of its data
files. Index files are not in that figure, and before rev 5 they could not be, because the
manifest held their paths only.

1. **Per file:** the manifest records each index file's on-disk size, and each index's logical
   size (§5.3).
   - **Logical size** of an index = the decoded bytes of its two skene files, which is what
     `uncompressed_size_in_bytes` means for a data file:
     - vectors: indexed rows × (4 + 2 × dim) bytes (`ordinal` UINT32 + fp16 embedding);
     - centroids: clusters × (2 × dim + 4 + 4) bytes (centroid + `row_group` + `rows`).
   - The builder computes it from what it wrote and hands it to the commit with the paths.
2. **Per snapshot:** the summary gains three counters, maintained by every commit exactly as
   `total-files-size` and `total-data-size` are:
   - `total-index-files`: index files (vectors + centroids) referenced by the manifest.
   - `total-index-size`: their on-disk bytes (for storage operations and expiry reporting).
   - `total-index-data-size`: their logical bytes (**billed**).

   `total-data-size` and `total-files-size` never include index bytes, so creating an index
   does not move the data figure. `index-build` and `index-drop` commits change only the index
   counters.
3. **Per index:** not stored. `SHOW INDEXES` (§7A) derives each index's bytes and indexed-file
   count from the head manifest, which the planner already reads and caches. A per-index copy
   on the summary would be a second truth every commit has to keep right.
4. **Billing (D-13 RULED 2026-10-02: logical bytes).** The sweep reads the head snapshot's
   `total-index-data-size` and reports it as its **own figure**, alongside and never inside
   `bytes_stored`: "data = 1 GB, indexes = 100 MB" per collection. Both figures are logical
   bytes, metered the same way.
   - Like data, only the head snapshot is metered. Index files kept alive by older snapshots
     are not, matching how data files are treated today.
   - The rate is a pricing setting, not an engine decision. How the embedding compute of a
     build is metered is not covered by this ruling (it is the compute of whatever job runs
     the build; §10).
5. **Tags:** `create_tag` records `pinned-bytes` and `pinned-bytes-on-disk`. It also records
   `pinned-index-bytes` (logical) and `pinned-index-bytes-on-disk` from the snapshot's summary,
   so pinning an indexed snapshot does not understate what it holds.

### 5.6 Compaction carries vectors and never embeds (D-7, D-14 ruled)

Compaction (OPTIMIZE, and any whole-file rewrite through `compaction_commit`) moves rows into
new data files. **Compaction never embeds** (D-14). Text that already has a vector keeps it
(D-7); text that has none is embedded only by `REFRESH INDEX`, never by compaction.

**Grouping rule.** Compaction only merges input files with the **same index coverage**: the
same set of index ids. So, per index, an output file's inputs are either all indexed or all
unindexed:
- **All indexed:** the output's index is built by carrying vectors (below) and lands in the
  same compaction commit.
- **All unindexed** (an async index that has not caught up, or files below the D-8 threshold):
  the output is unindexed. The compaction commit adds data files, so it fires
  `REFRESH INDEX` (§10), which embeds rows that were **never** embedded. Nothing is embedded
  twice.

Carrying, per index, per output file:
1. The writer records the mapping it already follows: output ordinal ← (input file, input
   ordinal). Rows removed by delete vectors are not written and have no mapping (compaction
   materialises deletes).
2. For each input, read its vectors file in full (a sequential read of bytes already written;
   no model involved) and look rows up by its `ordinal` column.
3. Run `ivf_build` over the output's vectors (seconds, not minutes; §13 B3) and write the two
   skene files.
4. The output entries carry their index references and sizes in the **same**
   `compaction_commit` snapshot. A compacted file is never unindexed in a window where its
   inputs were indexed.

Rules:
- A file is either fully indexed for an index or not indexed at all. A partial index file is
  never written.
- **Invariant:** for each indexed output, carried vectors = searchable rows written. A
  mismatch fails the compaction, like its row-count invariant.
- Every input index file's embedding identity must equal the definition's. A mismatch refuses
  the compaction for that table rather than mixing vectors from two models.
- Each index on the table is carried independently.
- Retired inputs' index files leave the head manifest in that commit and are reclaimed by
  expiry (§5.4).

### 5.7 Maintenance lease: compaction and index builds never overlap (D-14, rev 6)

D-14 also rules that compaction and indexing do not run at the same time on a table. The
commit CAS already stops either one from overwriting the other, but only at the end: an index
build that loses the race has spent minutes of embedding for nothing, and a compaction that
retires a file mid-build forces the build to start over. A lease prevents the overlap up front.

- **The lease:** one document per dataset, `maintenance/lease` (a subcollection, like
  `indexes`), holding `holder`, `operation` (`compaction` / `index-build`), `claimed-at-ms`
  and `expires-at-ms`.
- **Claimed** in one Firestore transaction (read, check free or expired, set), the same claim
  pattern as `claim_trigger_fire`. The holder renews it while it works and releases it after
  its commit. An expired lease (a crashed holder) can be claimed; the crashed holder's
  uncommitted files are orphans that deep clean reclaims (§5.4).
- **Who takes it:** every compaction (including on tables with no index, because a CREATE
  INDEX can arrive while it runs), `REFRESH INDEX`, and the build that a `sync` CREATE INDEX
  runs.
- **Who does not:** INSERT, CTAS, MERGE, DELETE and UPDATE, including the index build a `sync`
  index does inside them. They only index their **own new files**, which no compaction can
  have selected yet, and their commits go through CAS as today. Making every write wait for a
  compaction would be a regression for tables that never compact.
- **A refused claim is loud, never queued silently:**
  - `REFRESH INDEX` while a compaction holds the lease fails with a message naming the holder.
    Nothing is lost: the compaction's own commit adds files and fires `REFRESH INDEX` again.
  - A compaction while an index build holds the lease fails the same way, and its next
    scheduled run picks the table up again.
  - A `sync` CREATE INDEX that cannot get the lease fails; the definition is not created.
- **The CAS stays.** The lease avoids wasted work; correctness still rests on the commit's
  compare-and-set.

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
- Recall/cost knob: `nprobe` (clusters probed per file), as a session variable and/or a statement
  option (D-9). Default for ~0.95 recall (32 at K≈√N on NVD).
- DDL:
  `CREATE INDEX name ON table USING IVF (text_col) WITH (metric='cosine', clusters=…, …)` /
  `DROP INDEX name ON table`. This is sqlparser's `CreateIndex` shape, which DuckDB's vss extension
  also uses. It must be parse-checked. Governance matches other dataset DDL: owner/WRITE grant,
  and egress rules as for writes.

## 7A. Discovering indexes (rev 5)

- **`SHOW INDEXES FROM t`** (D-15 RULED: follow sqlparser) returns one row per index: `name`, `column`,
  `method`, `metric`, `build`, `clusters`, `embedding` (provider identity),
  `files_indexed`, `files_total`, `index_bytes`, `created_by`, `created_at`. (`nprobe` left
  the definition 2026-10-03; `index_bytes` is the LOGICAL size, the billed figure, §5.5.)
  **DELIVERED 2026-10-03 (C4):** planned as a `ShowStep` (`object_type = "INDEXES"`) so it
  binds exactly as SHOW CREATE TABLE (READ; refused on a connector without vector
  indexes); `ShowIndexesNode` reads `OpteryxConnector.vector_index_status` (definitions +
  head manifest). `SHOW INDEX FROM`, `SHOW INDEXES ON`, `SHOW KEYS FROM` and a bare
  `SHOW INDEXES` are refused naming the one spelling. `SHOW CREATE TABLE` appends one
  `CREATE INDEX ... USING IVF (col) WITH (build = '...'[, clusters = n])` per index, in
  name order. Tests: `tests/integration/test_vector_index_ddl_local.py` (discovery).
  - `files_indexed` / `files_total` and `index_bytes` come from the head manifest (§5.5). They
    show how far an async index lags and what it costs to store.
  - It reads the catalog only. It never opens an index file.
  - Permission: the same as `SHOW CREATE TABLE`.
  - sqlparser has no SHOW INDEX statement: `SHOW INDEXES FROM t`, `SHOW INDEX FROM t`,
    `SHOW INDEXES ON t` and `SHOW KEYS FROM t` all fold into its generic `ShowVariable` node
    (checked 2026-10-02). It is planned in `plan_show_variables`, exactly like
    `SHOW MANIFEST FOR t`. The one spelling is MySQL's `SHOW INDEXES FROM t`; the other word
    forms are refused with a message naming it (canonical spellings only).
- **`SHOW CREATE TABLE t`** appends the `CREATE INDEX` statements that recreate the table's
  indexes, reconstructed from the definitions like the table form.
- **EXPLAIN and telemetry** for the approximate form report indexed vs exact files, probed
  clusters and candidates (Stage D).
- No `information_schema` view in v1.

---

## 8. Execution

One native `Source`, planned by `_compile_scan` when the scan carries an index stamp:

```text
per surviving data file (after manifest + row-group pruning):
  pass 1  decode predicate columns (existing NativeScanColumnBuilder / Pass1Pred)
          → survivor mask M_f (bit per file ordinal), AND NOT delete-bitmap D_f
  search  if file has a sidecar and popcount(M_f) ≥ τ:
              ivf_search(q, k', nprobe, admitted = M_f)       -- filter while scoring
          else:
              exact SIMD distance over survivors of M_f       -- small / unindexed / very selective
          → candidates (ordinal, distance), k' per file  (distance = draken kernel value)
merge     global candidate set across files (k' × files, bounded)
reduce    draken SortKeyCmp / TopNSink to k   (same comparator as every other ORDER BY)
pass 2b   masked decode of remaining projected columns for the final k
```

Why this shape:
- **No overfetch loop.** Residual predicates are evaluated *before* the search. The mask is the IVF
  probe's admitted set (`TopK::offer(..., admitted)`), so only admissible rows are scored. This covers every
  predicate the engine can evaluate natively, not just an S3-style metadata-filter subset.
- **Selective filters.** With a selective filter, the admitted rows may lie outside the probed
  clusters, so IVF recall degrades — while brute force over a few survivors is cheap. So below
  a survivor threshold τ, the exact path is used for that file. The rule is deterministic, native and per-file, and both arms are correct under the
  "approximate" contract. τ is a measured constant, not a guess (§12).
- **The distance the user sees is always the SQL kernel's value.** The index's metric *is*
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

**Resolved:** D-5 was ruled IVF-flat on the measurements in §13 (B3). The table and the
recommendation below are kept as the reasoning.

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
  pooling/normalisation settings. The query text of the approximate form is embedded by the
  running provider, whose identity must equal the definition's, or the statement is **refused**. A new image with a different model therefore refuses
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

- **Where:** a native builder (C2b). Per data file: scan the indexed text column keeping file
  ordinals, embed through the registered provider (§9A), drop null, deleted, zero-magnitude and
  non-finite rows, run `ivf_build`, and write the vectors and centroids skene files through
  FileIO, returning their paths and sizes. The `index-build` commit is control-plane work after
  the native build.
- **Cost:** embedding dominates (~44 rows/s per thread with MiniLM; §13 B3). Clustering takes
  seconds.
- **When (D-7, ruled 2026-10-02):** per index, `build = 'sync' | 'async'`, default `async`,
  switched by `ALTER INDEX`.
  - `sync`: CREATE INDEX builds every existing file before it returns. Every commit that adds
    data files builds their index files first and references them in the same snapshot.
  - `async`: new files land unindexed and are searched exactly until `REFRESH INDEX` indexes
    them with an `index-build` commit.
  - Compaction never embeds; it carries vectors or leaves the output unindexed (§5.6).
- **Trigger (D-16 RULED 2026-10-02):** `REFRESH INDEX n ON t` is the one build primitive. It
  takes the maintenance lease (§5.7), embeds every live data file that index does not cover
  (above the D-8 threshold), and commits `index-build`.
  - Users can run it directly.
  - It is **fired automatically** for each async index on the table by every commit that adds
    data files (INSERT, CTAS, MERGE, UPDATE's rewritten files, OPTIMIZE / compaction) and by
    `CREATE INDEX`. The firing uses the existing commit-trigger path (`trigger_firing.py`),
    which already submits `REFRESH MATERIALIZED VIEW` to jobs.opteryx after a commit: the job
    runs as the commit's author, and a failure to fire is alerted and audited without failing
    the commit.
  - `REFRESH` is parsed by the aside parser, which today accepts only
    `REFRESH MATERIALIZED VIEW` (`src/aside/view.rs`). `REFRESH INDEX n ON t` is a new
    grammar there, like `ALTER INDEX`.
  - No separate scheduled task: every way an index can fall behind is a commit that fires it.
    A fire that fails (alerted) is caught up by the next one, or by running it by hand.
- **Small files (D-8, open):** files below a row threshold get no index and are always
  searched exactly.
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
"top-k nearest rows in this one Parquet object using its index files". It would use the same
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
  dimension. *(Superseded by the IVF layout in §5.2: cluster-ordered `(ordinal, embedding)`,
  searchable rows only.)* Original convention: the vectors file has ONE
  column `embedding VECTOR_FP16(dim)`, and row i is data-file ordinal i (null where the text is
  null). Deleted rows stay in the file; they are excluded at build and masked at search.
- **B2** *(superseded by IVF on D-5, 2026-10-02; deleted)*: `draken/ops/ann/fp16_cosine_hnsw.h`, built on usearch `index_gt<double, uint32_t, uint32_t>`.
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

**B3 + D-5 measurements on REAL embeddings (2026-10-02).**

Fixture (`dev/vector_fixture.py`): every NVD vulnerability description (`testdata.nvd`, 335,085
real English texts, ~320 characters each), embedded by the engine's own MiniLM capability.
The queries are 20 natural-language queries plus 180 held-in rows. Recall is tie-aware: a hit
counts if its distance is no greater than the exact 10th-nearest distance (NVD has many
duplicate descriptions). Laptop, 6 threads where stated.

**Embedding is the dominant build cost:** 710 rows/s on 16 threads (~44 rows/s per thread).
335k rows took 8 minutes; 1M rows would take ~23 minutes on 16 cores. That is 20-30x the
cost of building either index.

| path | build | per-query CPU (1 thread) | recall@10 | vector bytes read per query |
|---|---|---|---|---|
| exact scan | none | 23 ms (69 ns/row) | 1.00 | 100% (257 MB) |
| HNSW ef=64 | 16.5 s, 48.5 MB graph | 0.18 ms | 0.93 | 100% (must be resident) |
| HNSW ef=128 | ″ | 0.33 ms | 0.95 | 100% |
| IVF K=576, nprobe=16 | 23.5 s (8 k-means iterations), 0.44 MB centroids | 1.9 ms | 0.93 | 3.6% |
| IVF K=576, nprobe=32 | ″ | 3.7 ms | 0.95 | 6.8% |
| IVF K=576, nprobe=64 | ″ | 7.1 ms | 0.97 | 12.9% |

Reading: at equal recall (0.95), HNSW costs ~10x less CPU, but **IVF reads ~15x fewer bytes**.
In production (Cloud Run + GCS, 8 GiB workers, cold or partially cached), bytes read decide
latency and memory. Every path's CPU here is single-digit milliseconds, and IVF's probe scan
parallelises across row groups like any scan.

**IVF re-measured with the draken implementation, engine-shaped (2026-10-02).** Vectors are
copied into cluster order as the skene file stores them, each probed cluster is scored as a
contiguous block keyed by ordinal, and the probe is split across 6 threads:

| K | build (6 threads) | nprobe | 1 thread | 6 threads | recall@10 | bytes read |
|---|---|---|---|---|---|---|
| 579 (√N, default) | 4.4 s | 32 | 1.67 ms | 0.46 ms | 0.959 | 7.1% |
| 579 | ″ | 64 | 3.15 ms | 0.74 ms | 0.979 | 13.4% |
| 2048 | 35.7 s | 64 | 1.14 ms | 0.28 ms | 0.952 | 4.2% |

At equal recall IVF now matches HNSW's 0.33 ms on parallel wall time, and reads 4-7% of the
vectors instead of 100%. The prototype's "10x slower" was gather overhead plus a serial
probe. Training on a sample cut the K=579 build from 23.5 s to 4.4 s.

Consequences for open decisions:
- **D-5 RULED 2026-10-02: IVF-flat over fp16.** Implemented in
  `draken/ops/ann/fp16_cosine_ivf.h`: deterministic spherical k-means on a seeded sample, a
  cluster-major order, a non-empty-cluster probe, and `TopK::offer` over contiguous blocks.
  HNSW and the usearch graph and SimSIMD headers are deleted; only usearch's `fp16/` remains
  vendored, for draken. Tests: `tests/unit/core/test_vector_ann.py`, including "probing
  every cluster == the exact scan, bit for bit".
  - The vectors file is sorted by cluster, with one skene row group per cluster and a
    centroid table.
  - The default nprobe is set for ~0.95 recall, overridable by the recall setting (D-9).
  - HNSW would then be deleted. The exact path stays, for small files and selective filters.
- **D-7 RULED 2026-10-02:**
  1. **Compaction never re-embeds.** When OPTIMIZE (or any rewrite) moves rows into a new
     data file, the builder carries each row's existing vector across by the old-to-new
     row mapping and only re-clusters (seconds, not minutes). Rows with no vector to carry
     (from an unindexed source file) are the only rows ever embedded during a rewrite.
  2. **The build mode is per index, chosen at definition:**
     `CREATE INDEX … USING IVF (col) WITH (build = 'sync' | 'async', …)`.
     - `sync`: every commit that adds data files (INSERT, CTAS, MERGE, OPTIMIZE) embeds and
       builds their sidecars before it commits. The index is always complete; write latency
       pays ~1.4 ms per new row per embedding thread (MiniLM, 44 rows/s/thread).
     - `async`: commits land unindexed. A maintenance build commits the sidecars later, and
       until then those files are searched by the exact path (EXPLAIN and telemetry show
       indexed vs exact files).
     - **Default `async`** when `build` is omitted (ruled 2026-10-02).
     - **`ALTER INDEX … SET (build = 'sync' | 'async')`** switches modes (ruled 2026-10-02).
       The new mode applies from the next commit. Files written while the index was async
       stay unindexed until a maintenance build.


| Step | Work | Gate |
|---|---|---|
| **B1** | Writer and reader for the skene vectors file: `(ordinal UINT32, embedding VECTOR_FP16)` from a `DrakenVector` plus a delete mask. | C++ round-trip tests |
| **B2** | `draken/ops/ann/`: build an `index_gt` graph whose metric is the A1 kernel, search with a predicate, and the `.graph` header with its binding checks, serialised through `save_to_stream` / `load_from_stream`. | C++ tests: recall against an exact scan, predicate filtering, header-mismatch refusal |
| **B3** | Real-embedding fixture: a `dev/` script uses the A2 provider to embed a text dataset at 100k / 1M / 10M rows into vectors files. | Fixture exists, with checksums |
| **B4** | **Measurement gate** (§12 steps 2-6), which rules **D-5**, τ and **D-8**. If the vectors-file exact scan wins, B2 is deleted. | D-5 ruled |

### Stage C: catalog lifecycle

**C0, C1 and the C2 DDL delivered 2026-10-02.** The catalog suite (1401), `make q` and cargo
all pass.
- **C0:** the commit pointer check and the write are now ONE Firestore transaction
  (`_set_if_pointer_unmoved`). The old code checked in its own transaction and then wrote
  outside it. Test: `opteryx-catalog/tests/test_commit_pointer_atomic.py`.
- **C1:** `opteryx_catalog/catalog/vector_indexes.py`, covering the definition record and its
  validation, the sidecar paths, and the reference helpers.
  - Definitions live in an `indexes` subcollection, with `create/get/list/alter/drop_vector_index`
    and audit events.
  - The manifest gains three parallel `ARRAY<VARCHAR>` columns: `vector_index_ids`,
    `vector_index_vectors` and `vector_index_centroids`.
  - New commits: `commit_vector_index_files` (`index-build`) and `remove_vector_index_files`
    (`index-drop`).
  - References are carried by every other commit and by the statistics refresh, copied on
    fork, and protected by deep clean and expiry.
  - Tests: `opteryx-catalog/tests/test_vector_indexes.py` (24).
  - The engine needs no change to read these manifests: its decoder reads columns by name.
- **C2 DDL:**
  - `CREATE INDEX [IF NOT EXISTS] n ON t USING IVF (col) [WITH (build, clusters)]`
    uses sqlparser, with the dialect's WITH clause enabled.
  - `ALTER INDEX n ON t SET (build = …)` goes through the aside parser (`src/aside/index.rs`).
  - `DROP INDEX [IF EXISTS] n ON t` uses sqlparser. `ALTER INDEX … RENAME` is refused.
  - One `VectorIndexDdl` plan step and the `vector_index_ddl` relation-management action carry
    all three.
  - The binder requires ALTER permission and a connector with `supports_vector_indexes` (the
    Opteryx catalog only). It checks the column exists and is text, validates the options,
    and stamps the active embedding identity and width.
  - Tests: `tests/integration/test_vector_index_ddl_local.py` (15).
- **Not yet built:** the index BUILD (C2b, the native operator: scan text, embed, `ivf_build`,
  write the skene files, then the `index-build` commit) and the write-path policy (C3).
  Until C2b lands, CREATE INDEX only DEFINES the index. Even with `build = 'sync'` no file
  is indexed yet, and nothing reads indexes before Stage D, so nothing pretends otherwise.
- **C1b delivered 2026-10-02** (catalog suite 1422, `make q`, DDL 15):
  - `IndexFiles(vectors, centroids, vectors_bytes, centroids_bytes, logical_bytes)` is what
    `index_refs` returns and what `commit_vector_index_files` takes. A plain path pair, or any
    size that is not a positive integer, is refused; a manifest row whose six columns disagree
    is refused.
  - Every commit derives `total-index-files` / `total-index-size` / `total-index-data-size`
    from the manifest it writes (`index_totals`, beside `_totals_from_entries`), at all five
    summary sites. The data totals never include index bytes.
  - Expiry's size map carries the recorded index sizes. Tags record `pinned-index-bytes` and
    `pinned-index-bytes-on-disk`, and the `create_tag` audit carries them.
  - The lease (§5.7): `claim_maintenance_lease` / `renew_maintenance_lease` /
    `release_maintenance_lease` on `maintenance/lease`, the dataset's EXISTING `maintenance`
    subcollection (beside the orphan quarantine), so drop and rename already clean it up.
    Each claim gets a `claim-id`, so a late renew raises `MaintenanceLeaseLost` and a late
    release returns False without touching a newer claim. TTL 1..3600 s; renew to hold
    longer.
  - **C1 gaps fixed here:** `drop_dataset` left the `indexes` subcollection behind (a
    same-named new table would inherit the definitions), and `rename_dataset` moved neither
    the index files nor the definitions. Both now do, with tests.
- **C2b build, steps 1-4 delivered 2026-10-02** (order from the independent review; sizing
  premise corrected: the 4 GB file target is LOGICAL bytes, ~5.8M NVD-like rows per file):
  - *Streaming skene writer:* `FileWriter::begin(options, OutputStream*, prefix)` streams the
    lead column, stages the rest in memory, returns head + lead directory as a separate
    prefix; file = prefix ‖ body, byte-identical to the buffered writer (no format change;
    `skene/tests/test_streaming_writer.cpp`).
  - *IVF split:* `ivf_plan` / `ivf_train` / `ivf_assign` / `ClusterStream`; `ivf_build`
    bit-identical to its pre-split output (golden hashes in `test_vector_ann.py`).
  - *Native per-file builder* (`src/cpp/engine/vector_index_build.hpp`, entry points in
    `opteryx/operators/vector_index_build/`): footer via rugo, text column through rugo's
    `ParquetIOPipeline` + the scan's `NativeScanColumnBuilder`, sample pass with a row mask,
    full pass with deleted rows masked out, physical ordinals, `str_slice` batches into
    `draken_embed`, `ClusterStream` into the streaming writer; centroids file in memory.
    Deterministic for any thread count. Real MiniLM on NVD (335,085 rows): 410 s, 817 rows/s,
    peak RSS 1.05 GiB, 966 row groups / 579 clusters.
  - *Embedding batch = 1 (measured):* batching was both the memory blow-up (+10 GiB at
    64 rows x 12 threads — working set ~ batch x seq^2, not the ORT arena) and the slowness:
    batch 1 = 1207 rows/s on M5 (417 at 64), 356 on the x86 box (134 at 64).
  - *GCS body:* `HttpClient::put` + `GcsResumableBody` (native chunked PUT to a session URI,
    resume from the offset the session reports); `build_vector_index_to_session`. The
    catalog's `GcsFileIO` gains `open_upload_session`, `cancel_upload_session` and `compose`.
    Tested against a strict local stand-in of the resumable protocol with 503/429/500 and
    partial commits: prefix + streamed body is byte-identical to the local build.
- **C2b step 5 delivered 2026-10-03: `REFRESH INDEX n ON t`** (aside grammar in
  `src/aside/index.rs`; view.rs steps aside for it). Bound at the ALTER tier like the other
  index DDL; runs as `OpteryxConnector.refresh_vector_index`, control plane only:
  - refuses an index defined against another embedder (identity or width) before the lease;
  - claims the maintenance lease (`index-build`, 600 s), renewed every 120 s by a thread
    while the native build holds no GIL; a lost lease stops the next commit, and an
    overrun release is reported;
  - plans with the catalog's `Dataset.vector_index_build_plan(index_id)`: the live files
    the index does not cover, each with its size, its deleted ordinals at that snapshot and
    newly minted paths (`IndexBuildTask`);
  - per file, ONE native call, then ONE `index-build` commit (dataset reloaded, so a
    concurrent append does not fail an hours-long refresh): a failure keeps every file
    indexed before it. Local catalogs build to local files; GCS reads the data file through
    a 7-day signed URL (the builder now takes remote input via range GETs and the
    manifest's file size; a signed GET cannot answer a HEAD), streams the body into a
    session, uploads prefix + centroids, composes, deletes the parts; a failed or empty
    build cancels its session;
  - a file with no indexable row gets no index files and is re-planned by every REFRESH
    (cheap when the footer shows it: the footer null count; otherwise it is embedded again).
- **C3, sync CREATE INDEX delivered 2026-10-03:** the lease is claimed BEFORE the
  definition is created (refused ⇒ nothing created, §5.7); every existing file is built
  and committed before CREATE returns (receipt: "created index … : N file(s) indexed");
  a failed build drops the definition again with any files it committed, so a failed
  CREATE leaves no index. `async` CREATE takes no lease and builds nothing.
- **C3, commit-fired REFRESH INDEX delivered 2026-10-03** (three repos):
  - *Ruled 2026-10-03, supersedes D-16's "runs as the commit's author":* a fired refresh
    runs as the index's `created-by` and the creator pays; REFRESH INDEX stays owner-tier
    (a writer's INSERT would otherwise fire a refresh the binder denies).
  - opteryx-catalog `trigger_firing.fire_index_refreshes`: every commit that ADDED data
    files (compaction included, before the user-created gate in `_after_commit`) submits
    `REFRESH INDEX <name> ON <ws.coll.ds>` per async index to jobs, provenance under
    `client_info.index_refresh`; never raises; alert + audit per failure; same kill
    switch. Sync indexes are not fired (they build inside the write — not built yet).
  - jobs.opteryx `_resolve_index_refresh_submission`: platform-only provenance (403
    otherwise); identity, policies and billing from the definition's `created-by`
    (refused if absent, never defaulted); `origin: index-refresh` (kept off
    /jobs/recent); 60 s dedup window per (relation, index). A hand-run REFRESH INDEX
    takes the ordinary owner-tier permission check.
  - opteryx-core `analyze_query` now classifies every index statement against its
    RELATION at the owner tier (it reported `denied` with no table, and DROP INDEX named
    the index as the table — so index DDL through jobs could not have worked).
  - A fired refresh that meets a held lease fails loudly (job FAILED); the next commit's
    fire, or a hand run, catches up. worker.opteryx needs no change (no stamp).
- **C3 step 6, compaction carry delivered 2026-10-03** (ruled: mapping captured by the
  native writer path; no interim gate):
  - *Desugar:* a relation with any vector index compacts with ROW ORIGINS — OPTIMIZE lists
    its columns explicitly plus `$file AS $carry_file`, `$ordinal AS $carry_ordinal`, and
    stamps the scan for row identity (MERGE's mechanism; the scan runs on the trampoline
    Source, as MERGE's does). A relation with no index is planned exactly as before.
  - *Selection:* files are grouped by index coverage (catalog `vector_index_coverage` at
    the scan's snapshot); each group is selected alone and the pass takes the group whose
    plan rewrites the most bytes. `retired_files` is now in SCAN order (a `$file` is a
    position in it).
  - *Writer:* `DataFileStream(row_origins=True)` gives each output file a native
    `RowOriginRecorder`, which takes the two columns out of every row group before it is
    written; the ordinals never become Python objects.
  - *Carry:* `src/cpp/engine/vector_index_carry.hpp`, one native call per index: pass 0
    reads every input's `ordinal` column (candidates + the INVARIANT: an indexed live input
    row not written fails the compaction; a deleted one is let go; a row written twice
    fails), pass 1 the sampled vectors and trains per output, pass 2 streams each output's
    carried vectors through `IvfFilesWriter` — the write phase now shared with the build
    (refactor bit-identical). Inputs are read through `SkeneRangedFile`
    (`src/cpp/engine/skene_ranged_file.hpp`: skene's ranged reader over pread or signed-URL
    range GETs — the D2 reader can use it). GCS outputs stream into sessions and compose,
    as a build's do.
  - *Commit:* catalog `compaction_commit(index_files=...)` attaches the carried files in
    the SAME snapshot and refuses mixed input coverage, an output whose carried indexes
    differ from its inputs', unknown outputs, and non-`IndexFiles` values.
  - *Lease:* every compaction claims the maintenance lease (`compaction`, 3600 s) with its
    first rows, renews it around the carry, releases it after the commit or any failure in
    the sink; a statement that dies upstream of the sink leaves it to expire. Refused
    loudly while an index build holds it.
  - An output whose rows were all unindexed (no vector to carry) is refused loudly: it
    cannot be recorded as indexed, and compaction may not change coverage. Not reachable
    from a real indexed input (an index file is never written for a file with no vector).
- **C3 finished 2026-10-03:**
  - *Sync builds inside writes (D-7):* every engine write commits through the connector's
    `insert` / `merge_commit` / `replace_relation` (INSERT, CTAS replace, MERGE, UPDATE,
    DELETE's rewrites), which builds each new file's index files for every `sync` index
    BEFORE the commit and passes them to the catalog (`add_files` / `merge_commit` /
    `truncate_and_add_files` gain `index_files`), so the new files are indexed in their own
    snapshot. No lease (§5.7: a write indexes only its own new files). A new file with no
    indexable row gets none. The per-file build is shared with REFRESH.
  - *Async CREATE INDEX fires its REFRESH* from the catalog's `create_vector_index`
    (`fire_index_refreshes(..., only=name)`, never raises).
  - Verified end to end through SQL on local disk (INSERT/UPDATE sync builds, OPTIMIZE
    carry incl. mixed coverage and deletes, leases): 118 integration tests.
- **Stage D started 2026-10-03** (D-4 ruled `APPROX_COSINE_DISTANCE(text_col, 'query')`,
  D-9 ruled `nprobe`, the definition's value overridable per query):
  - *Per-file reader* (`src/cpp/engine/vector_index_search.hpp`, entry
    `search_vector_index_file`): embeds the query with the registered kernel, reads the
    whole centroids file, probes the `nprobe` nearest non-empty clusters, and fetches ONLY
    their vectors-file row groups through `SkeneRangedFile` (pread, or range GETs on a
    signed URL); scores with `TopK::offer` (the SQL kernel's distance; deleted ordinals
    excluded; an `admitted` mask is taken but not yet fed). Probing every cluster equals
    exact search; an index naming ordinals beyond its data file fails loud.
  - *Proven against a signature-checking S3 server* (hadro, `tests/integration/
    test_vector_index_s3_hadro.py`, 2026-10-03): every remote read the index makes goes
    through a presigned URL by range GETs — the build reading its data file, the search
    reading centroids + probed row groups, carry reading input vectors files — and each is
    byte-identical (or answer-identical) to the local read; a URL signed with the wrong
    secret is refused and the read fails loud. Real GCS (signed V4 URLs, resumable sessions,
    compose) is still unexercised.
- **RENAMED 2026-10-03: the index answers plain `COSINE_DISTANCE`.** Once the search
  became exact by default (D-9 re-ruling), "approximate" no longer described it:
  `APPROX_COSINE_DISTANCE` is deleted, and `ORDER BY COSINE_DISTANCE(col, 'q') LIMIT k`
  on an indexed column runs through the index. `VectorSearchStrategy` is no longer a
  gate: any other shape (no LIMIT, DESC, more keys, a WHERE left above the scan, a join,
  a non-literal query, no index, another embedder's index) runs as written without the
  index — EXPLAIN shows a "vector search" decision only when the index is used (or, for an
  embedder mismatch, why it was not). **Rows with no distance (NULL text, NaN) are
  dropped whenever COSINE_DISTANCE leads an ORDER BY — with or without an index, with or
  without a LIMIT** (ruled 2026-10-03: "drop NULLs everywhere"), so an index never changes
  an answer: the strategy marks the sort `drops_unsearchable` and both the Sort and the
  Top-N sinks drop them (`rows_with_distance`, native_sort.hpp). The block below
  describes the first delivery, under the old name.
- **D1 + D2 delivered 2026-10-03** — `ORDER BY APPROX_COSINE_DISTANCE(col, 'q') LIMIT k`
  runs through the index, end to end through SQL:
  - *Function:* `APPROX_COSINE_DISTANCE(text, text)`, the EXACT text-distance kernel under
    a registry alias ("approximate" chooses the rows, never the value).
  - *Gate + planner* (`VectorSearchStrategy`, after operator/project fusion, never
    disabled): the sole ascending ORDER BY key (directly, or through the projected alias)
    of a HeapSort over projections over ONE scan; the same call may repeat in the SELECT
    list. Refused otherwise: no LIMIT, more keys, DESC, WHERE (pushed or not), joins,
    any other use, a non-literal query, a non-catalog table, a column with no index, an
    index of another embedder. Stamps `scan.vector_search`; EXPLAIN shows
    "k=…, N of M file(s) indexed, X searched exactly".
  - *Execution:* the existing native parquet scan gained a `RowAdmission` hook (decided
    in `make_global`, masks into `submit_block`, empty units pruned through `kept`).
    `VectorIndexAdmission` embeds the query once, searches every indexed file (top-k,
    deletes excluded) and admits all non-deleted rows of uncovered files (ruled: exact,
    and reported). Deletes no longer decline the native scan for this shape. The
    projection computes each candidate's exact distance; the Top-N sink orders them.
  - *Rows with no embedding are never returned* (ruled 2026-10-03): the search's
    TopNSink drops rows whose distance is NULL or NaN (`drop_unsearchable_leading`), from
    indexed and uncovered files alike — fewer than k rows when fewer have one.
  - *nprobe* (D-9, re-ruled 2026-10-03): **the search is EXACT by default** — every
    stored vector of every indexed file is scored (the centroids file is not read); only
    `SET nprobe = n` (n >= 1) makes it approximate. The index definition no longer holds
    an `nprobe` (the option and the catalog's default 32 are removed). Read at compile.
  - *Telemetry:* the scan's facts gain files indexed / exact, clusters probed, index row
    groups read, candidates, rows searched exactly, nprobe.
  - Remote index files are read as gs:// with this process's bearer token (minted once,
    not refreshed; GCS V4 signing was an IAM signBlob call per file on Cloud Run, refused
    without `iam.serviceAccounts.signBlob` — changed 2026-10-03), or through S3 SigV4
    presigned URLs. The build's data-file read and compaction's carry do the same.
- **Recall measured 2026-10-03** (NVD 335,085 rows, real MiniLM fp32, K=579, 40
  security-phrase queries, k=10, M5 local warm; `dev/vector_index_recall.py`; truth =
  every cluster probed = exact):

  | nprobe | recall@10 mean | worst query | ms/query | vectors RGs read |
  |---|---|---|---|---|
  | 1 | 0.510 | 0.00 | 1.2 | 2 |
  | 4 | 0.812 | 0.10 | 1.5 | 8 |
  | 8 | 0.883 | 0.20 | 1.9 | 16 |
  | 16 | 0.948 | 0.50 | 2.6 | 31 |
  | 32 | 0.973 | 0.60 | 4.0 | 60 |
  | 64 | 0.983 | 0.70 | 6.7 | 118 |
  | 128 | 0.990 | 0.80 | 11.9 | 227 |
  | **exact (0, the default)** | **1.000** | — | 46.1 | all 966 |

  The worst query still loses 4 of 10 at 32: recall is a mean, not a floor, and a
  probe count holds no distance — it picks clusters by their CENTRES, and a row in an
  unprobed cluster can be nearer than every row scored.
- **A distance bound cannot make the exact search cheaper on this data (measured
  2026-10-03**, same index and queries, scratch script): visiting clusters by a lower
  bound on the distance any of their rows can have (centroid angle minus cluster radius;
  tightened with each row's Voronoi cell — every row IS in its nearest centroid's cell)
  and stopping once no cluster can beat the k-th best gives the exact top-10 for every
  query, but reads a median 567 of 579 clusters (97.8% of rows, worst 99.5%). The 10th
  neighbour sits at cosine distance 0.19-0.51 (~50°), clusters span ~67° from their
  centre and centres are ~35° apart, so almost no cluster can be ruled out. A fixed
  similarity floor does not help either: at similarity >= 0.7 the search still reads
  97.5% of rows (62.5% even at 0.9, where nothing matches), and only 7 of 40 queries
  have 10 rows that similar. Hence the ruling above: exact by default, `nprobe` as an
  explicit, unbounded approximation.
- **WHERE delivered 2026-10-03** (§8's pass-1 mask): a WHERE pushed into the scan is
  applied BEFORE the search. The compiler builds a pass-1 NativeScanPlan over the
  predicate columns and lowers the predicate to the latmat pass-1 C ABI
  (`Pass1PredResolver`); `VectorIndexAdmission` decodes and evaluates it per row group at
  execution start (row groups pruned by statistics hold no survivor), then:
  - an indexed file's search scores EVERY survivor from its stored vector (TopK's
    admitted mask over all row groups). `nprobe` is not applied under a WHERE (ruled
    2026-10-03): the probe picks clusters by proximity and the filter picks rows by
    predicate, and applying both keeps only rows that pass both — five survivors could
    come back as two. τ (and the interim "probe fell short" rule) are deleted;
  - an uncovered file admits all its survivors (exact).
  A WHERE that could not be pushed into the scan (a Filter left above it), or one that
  does not lower to c-native bytecode, is refused. The main scan does not arm its worker
  prefilter on this path (row masks and the prefilter's survivor gather do not compose);
  the relocated filter still runs natively after it.
- **Not yet:** real GCS.

| Step | Work | Gate |
|---|---|---|
| **C0** | *(needs your approval)* Fix the commit check-and-set race (§15 item 1) first, because every index commit relies on it. | Catalog race test |
| **C1** | opteryx-catalog: `indexes` subcollection (D-11), the index manifest columns (§5.3), deep-clean and expiry treating index files as referenced, and the `index-build` / `index-drop` commits. | Time-travel, rollback, expiry and deep-clean tests |
| **C1b** | Accounting (§5.5): the three size columns, `total-index-files` / `total-index-size` / `total-index-data-size` on every commit's summary, expiry's reclaimed bytes from recorded sizes, `pinned-index-bytes(-on-disk)` on tags; plus the maintenance lease (§5.7). | Sizes round-trip; counters across build, drop, compaction, fork; expiry tally; lease claim, renew, expiry, refusal |
| **C2** | `CREATE INDEX … USING IVF (text_col) WITH (…)` / `ALTER INDEX` / `DROP INDEX`: parse-check, binder, governance, plus (C2b) the native build operator (scan text → embed → `ivf_build` → write skene files → `index-build` commit; §10). | DDL tests on a local catalog |
| **C3** | Write-path policy (D-7, D-14, D-16, ruled): sync builds inside INSERT/CTAS/MERGE commits; `REFRESH INDEX` (aside grammar) fired by every file-adding commit and CREATE INDEX for async indexes; compaction grouping by coverage, vector carry, lease (§5.6, §5.7). | Lifecycle tests per mode; carry invariant; compaction never embeds; lease refusals |
| **C4** | Discovery (§7A, D-15): `SHOW INDEXES FROM t` in `plan_show_variables`, and index lines in `SHOW CREATE TABLE`. | SHOW tests |
| **C5** | Billing (§5.5, D-13), outside this repo: the xb500 storage sweep reports `total-index-data-size` (logical) as its own figure. | Sweep test |

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
| D-3 | Home of the ANN code and the index format | — | **DECIDED rev 3, amended by D-5:** draken `ops/ann/` (IVF-flat), vectors and centroids in skene files (§5.2). |
| D-4 | SQL spelling of "approximate" | `VECTOR_SEARCH` TVF / `APPROX_COSINE_DISTANCE` / `WITH INDEX` clause | **RULED 2026-10-03: `APPROX_COSINE_DISTANCE(text_col, 'query')`**, then **RE-RULED 2026-10-03: plain `COSINE_DISTANCE`** (the search is exact unless `SET nprobe`); a leading COSINE_DISTANCE key drops NULL-distance rows everywhere. |
| D-5 | ANN algorithm | — | **RULED 2026-10-02: IVF-flat over fp16** (§13 B3). HNSW and the usearch graph are deleted. |
| D-6 | SIMD distance: SimSIMD (vendored) vs draken-owned kernels | — | Draken-owned NEON/AVX2 via `SIMD_STATIC_SELECT`, consistent with the rest of draken. SimSIMD stays usearch-internal. |
| D-7 | When sidecars are built | — | **RULED 2026-10-02:** per index `build = 'sync' \| 'async'`, default async, `ALTER INDEX` switches; compaction never re-embeds (§5.6, §10). |
| D-8 | Minimum rows per file for a sidecar | fixed constant / measured / per-index option | Measured constant from §12, overridable per index. |
| D-9 | Recall knob exposure | session variable / statement option / index-definition default | **RULED 2026-10-03: `nprobe`**, then **RE-RULED 2026-10-03: exact by default; `SET nprobe = n` only** — no index-definition value, ignored under a WHERE. |
| D-10 | S3 Vectors façade | none / SQL-translation façade in the service tier | Defer. Revisit only with a consumer. |
| D-12 | Embedding provider | — | **APPROVED rev 3:** runtime-loaded ONNX Runtime C API (onnxruntime installed `--no-deps`), weights baked into the image at container build. MIT licence verified (§9A). |
| D-13 | Index storage billing (§5.5) | — | **RULED 2026-10-02: charged at logical bytes** (`total-index-data-size`), reported as its own figure beside data. |
| D-14 | Compaction and indexing (§5.6, §5.7) | — | **RULED 2026-10-02: they never happen at the same time.** Compaction never embeds and only merges files with equal index coverage (carrying vectors where indexed); a per-dataset maintenance lease keeps compaction and index builds apart. |
| D-15 | Discovery spelling (§7A) | — | **RULED 2026-10-02: follow sqlparser.** It has no SHOW INDEX statement and folds every form into `ShowVariable`, so the spelling is MySQL's `SHOW INDEXES FROM t`; plus the `SHOW CREATE TABLE` lines. |
| D-16 | Async build trigger (§10) | — | **RULED 2026-10-02:** `REFRESH INDEX n ON t`, fired by every commit that adds data files and by CREATE INDEX (via the existing commit-trigger path). No scheduled task. |
| D-11 | Index definition as a catalog object | dataset subcollection `indexes` / new `ResourceType` | Subcollection. An index has no independent ownership or grants; it belongs to its table. |

---

## 15. Unrelated issues found during research (reported, not fixed)

1. **Catalog commit CAS window.** `_refuse_if_pointer_moved`
   (`opteryx_catalog.py:8563-8588`) checks the head pointer in a read-only transaction. The
   `doc_ref.set()` at `:8662` runs outside it, while the docstring claims one transaction. **Fixed in C0.**
2. **Dead vector plan flag.** `vector_topk_candidate` (`operator_fusion.py`, `plan_steps.pyx`) is
   set but never read at runtime. The docstring of `tests/unit/planner/test_vector_topk_plan.py`
   claims it keeps `TopNScanPushdownStrategy` off, but that strategy never reads it. **Deleted in A4.**
3. **Likely dead code.** `src/cpp/vector_search_native.cpp` (`exact_search_cosine`) is reached
   only from a fallback at `registrar/utility.pyx:121`. `opteryx/types/vectors/vector_ranking.py`
   has no engine caller. **Deleted in A4.**
4. **Broken dev tooling.** `dev/run_usearch_smoke.sh` compiles `dev/usearch_smoke.cpp`, which
   does not exist. The docstring of `dev/vendor_usearch.py` gives its own path as
   `tools/vendor_usearch.py`. **Script deleted in A4.**
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
   then they are dead weight, and I have not touched them. **Replaced in A2.**
9. **Delete-vector files are unbilled and sized 0.** The storage sweep bills `total-data-size`
   only, so `deletes-*.parquet` files are not billed. Expiry records their size as 0, so its
   reclaimed-bytes tally undercounts. §5.5 fixes this for index files only; delete files are
   reported here and not changed.
10. **`rename_dataset` delete vectors and shared files** (found in C1b, not fixed).
    (a) It copies only `file_path`; `delete_file_path` is left pointing under the old
    location. (b) Its copy loop `continue`s for a file already copied, *before* rewriting
    the row, so every manifest after the first that names a shared file keeps the OLD
    path, and that path is then deleted by `_reclaim_paths`. The index files added in C1b
    are remapped on every row and do not have (b). **Fixed 2026-10-02:** one `_move` per path,
    applied on every row to the data file, delete vector and index files (tests in
    `test_rename_dataset.py`).
