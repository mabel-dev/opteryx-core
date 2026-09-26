# Fixed Per-Query Cost — ClickBench Shapes (2026-09-26)

## Scope and provenance

This note measures the cost every query pays regardless of how much data it touches, attributes it, and ranks the fixes.

- Build: `make compile`, opteryx 0.9.142 build 3566, working tree at the state of 2026-09-26 (uncommitted changes from other work present).
- Machine: Apple M5 Pro, 18 cores, 64 GB. Python 3.14.5. No allocator preload.
- Data: `scratch/hits_rugo_262k` (99 parquet files, 64k row groups in column-major blocks of 4, ~1.6k row groups total) and `scratch/hits_skene` (4 v3 `.skene` files, 1.8–4.3 GB each).
- The process is warm: the page cache is warm, the footer caches are hot, and the engine pool is reused. `time_engine_footer_fetch` reads 0 on every query.
- Only measurements from this checkout are used. The historical result files were not consulted.

Probes (all in `scratch/`, none of them production code):

| Script | What it does |
|---|---|
| `fixed_cost_probe.py` | Monkeypatched phase timers plus per-strategy optimizer timing, median of 15 |
| `fixed_cost_suite.py` | The full 43-query battery, 3 rounds, recording fixed-cost components per query |
| `fixed_cost_scanplan_split.py` | Splits the footer gate from `open_native_scan_plan` |
| `fixed_cost_scanplan_loop.py`, `fixed_cost_loop_query.py` | Loops sampled with macOS `sample` to attribute native time |

## Headline

| | parquet (`hits_rugo_262k`) | skene (`hits_skene`) |
|---|---|---|
| Suite wall (Σ min-of-3, 43 q) | 13,576 ms | 9,875 ms |
| Fixed cost, Σ over suite | **~1,045 ms (7.7%)** | **~295 ms (3.0%)** |
| Fixed share of Q37–Q43 (the short filtered block) | **~78%** (298 of 383 ms) | **~39%** (53 of 137 ms) |

The fixed cost is **not** mainly Python planning. On parquet it is the plan-time native scan setup, `open_native_scan_plan`, at 8–52 ms per scanning query. On skene it is the claim-set build, which is a serial prefix while every other worker waits, plus Python planning.

## Per-query breakdown (median of 15, ms)

| Phase | pq Q01 | pq Q07 | pq Q42 | sk Q01 | sk Q07 | sk Q42 | `SELECT 1` |
|---|---|---|---|---|---|---|---|
| SQL parse (sqloxide) | 0.04 | 0.06 | 0.11 | 0.05 | 0.06 | 0.11 | 0.04 |
| SQL + AST rewrite | 0.04 | 0.05 | 0.12 | 0.04 | 0.05 | 0.12 | 0.03 |
| Binder | 1.29 | 1.41 | 1.84 | 0.53 | 0.55 | 1.04 | 0.11 |
| ↳ `get_dataset_metadata` | 1.08 | 1.18 | 1.15 | 0.34 | 0.33 | 0.33 | – |
| ↳ ↳ `get_dataset_schema` (recomputed every query) | 0.50 | 0.53 | 0.50 | 0.21 | 0.21 | 0.21 | – |
| ↳ ↳ directory listing, ×2 | 0.53 | 0.57 | 0.56 | 0.11 | 0.10 | 0.10 | – |
| Optimizer (46 passes) | 0.29 | 0.38 | 1.11 | 0.31 | 0.34 | 0.86 | 0.18 |
| ↳ `refresh_statistics` | 0.02 | 0.03 | 0.76 | 0.03 | 0.03 | 0.21 | 0.03 |
| `measure_data_processed` | 0.01 | 0.01 | 0.02 | 0.01 | 0.01 | 0.02 | 0.01 |
| Physical plan | 0.03 | 0.03 | 0.06 | 0.03 | 0.03 | 0.06 | 0.03 |
| **Python planning total** | **1.88** | **2.22** | **4.43** | **1.18** | **1.27** | **2.81** | **0.56** |
| `compile_to_native` | 0.08 | 0.10 | **28.4** | 0.09 | 0.10 | 0.24 | 0.09 |
| ↳ footer gate (`native_scan_supported`) | – | – | 1.48 | – | – | – | – |
| ↳ `open_native_scan_plan` | – | – | **27.3** | – | – | – | – |
| Skene claim-set build (inside first-morsel) | – | – | – | – | – | **≈3.8** | – |
| Teardown, close scans | – | – | 3.0 | – | – | – | – |
| Wall | 2.25 | 2.62 | 40.7 | 1.55 | 1.67 | 9.58 | 0.93 |

The statistics-only rewrite fires for `COUNT(*)` and `MIN/MAX(EventDate)` on both formats. `optimization_statistics_only_response=1` is recorded, `scan_sources` is empty, and no scan is compiled. EventDate is UINT16 in this dataset, and the allowlist admits it. Q01 and Q07 cost 1.5–2.6 ms, nearly all of it binder plus optimizer.

## Attribution

### 1. Parquet: each query deep-copies every cached parsed footer

`open_native_scan_plan` costs 8.4 ms with no predicate and 25–52 ms with an equality predicate. A native sample of the no-predicate call (`fixed_cost_scanplan_loop.py`, main thread) shows:

- `ParquetParsedFooterCache::try_get` takes 91% of the call. Per query it does `out[0] = it->second`, a full `FileStats` copy for each of 99 files: every row group, every column's `ColumnStats`, the name strings and the min/max buffers. That is roughly 166k `ColumnStats` copies per query, and the profile is dominated by `ColumnStats::ColumnStats(const&)`, malloc and free.
- The copies are destroyed at teardown. `~FileStats` accounts for the ~3 ms `time_engine_teardown_close_scans` on every parquet query, together with a smaller pipeline-pool join.
- The cache already has a zero-copy `try_get_ptr`. Its docstring says a borrow that must outlive planning "must copy … or be pinned — see the Tier-2 pinning plan". That plan does not exist in `docs/`.

Suite cost: about 9 ms of copy plus 3 ms of free per parquet scan across 41 queries, roughly **0.45 s (3.3%)**.

### 2. Parquet: bloom probes open the file once per row group per equality predicate

Pruning in `_rg_passes_predicates_native` calls `TestBloomFilter(path, offset, len, value)` for every (row group, `Eq`/`InList` predicate) pair, and each call opens the file and reads the bitset. Varying only the operator on the same column, with the other terms unchanged:

| Predicate | `open_native_scan_plan` |
|---|---|
| `CounterID > 62` | 9.2 ms |
| `CounterID >= 62 AND CounterID <= 62` | 11.0 ms |
| `CounterID = 62` | 25.2 ms |
| `IsRefresh = 0` | 30.0 ms |

The probes do prune. Q20 (`UserID = …`) gets its first morsel in 0.65 ms, but its plan open costs 51.7 ms. Suite cost: about **0.21 s** across Q20 and Q37–Q43. The probes also run for columns where a bloom filter cannot help, such as `IsRefresh = 0` on a low-cardinality column where 0 is present in every row group.

### 3. Skene: the claim set is rebuilt per scan, serially, before any worker runs

In a sample of skene Q42 in a loop, one worker spends 24% of wall time inside `SkeneClaimSet::build` (≈3.8 ms/query) while the other 17 block in `std::call_once` (`native_skene_scan_source.hpp:992`). `open_file` is 91% of the build:

| Part of `open_file` | Share | What it is |
|---|---|---|
| `skene::open_reader_ranged` / `v3::parse_footer` | 65% | Footer parse |
| Suffix `pread` | 26% | `max(64 KiB, size/1024)` = 4.3 MB per 4.3 GB file, ~12 MB per query |

The remaining ~9% of the build is claim planning, which depends on the predicate.

Suite cost: about 3.5 ms × 41 ≈ **0.15 s (1.5%)**. This is extrapolated from Q42, since the build cost scales with file count and footer size, not with the query.

### 4. Python planning

- Planning is 2–5 ms per query, **184 ms (1.4%)** on parquet and **125 ms (1.3%)** on skene.
- The outliers are Q24 (15 ms) and Q30 (19 ms, 90 aggregate expressions). Their cost scales with expression count, not a fixed charge.
- Within planning, `get_dataset_metadata` is the largest fixed item: 1.1 ms on parquet, 0.33 ms on skene. The directory is listed twice, once directly and again inside `get_dataset_schema` → `read_dataset(just_schema=True)`, and the schema is rebuilt every query.
- Parse is 0.04–0.12 ms.
- The optimizer's 46 passes cost 0.2 ms on the trivial shape and ~1 ms on filtered shapes. The costly passes are the ones that do real work (ManifestPruning, PredicatePushdown, FunctionRewrite); passes that change nothing total about 0.5 ms.

## Recommendations, in priority order

| # | Fix | Saves (parquet / skene suite) | Short-query effect |
|---|---|---|---|
| 1 | **Stop copying `FileStats` out of the parsed-footer cache.** The cache holds `shared_ptr<const FileStats>`, and the plan's `footer_map` and the native Sources (parquet, latmat, `engine.hpp`) hold that pointer, which pins the entry for the query's lifetime. | ~0.45 s (3.3%) / – | −12 ms per parquet scan, ~30% of Q37–Q43 |
| 2 | **Bloom probe without a file open per row group.** `TestBloomFilter` (`rugo/src/parquet/bloom_filter.cpp:290`) opens an `ifstream` per call. Either (a) read each file's bloom regions once per scan with one fd and probe them with the existing in-memory `TestBloomFilterBytes`, or (b) cache the bitsets alongside the parsed footer. | ~0.21 s / – | Q20 −40 ms, Q37–Q43 −16…−36 ms each |
| 3 | **Cross-query skene metadata cache** keyed by `(path, size, mtime)` and holding the parsed `FileMetadata`. The suffix bytes are not cached; they are only needed for the block-0 merge. This removes the serial `call_once` prefix. | – / ~0.14 s (1.4%) | −3.4 ms per skene scan, ~25% of Q37–Q43 |
| 4 | **Filesystem connector:** list once, reuse the listing's size and mtime instead of calling `get_file_info` again, and cache the schema under the same file-set signature as the manifest. The schema is currently rebuilt because downstream mutates it (see the comment at `filesystem_connector.py` ~653), so caching needs a copy-on-hand-out, as `Manifest` already does. | ~35 ms / ~13 ms | −0.8 ms pq / −0.3 ms sk |
| 5 | Skip optimizer passes that cannot apply to the plan shape | ~20 ms / ~20 ms | −0.5 ms |
| – | Parse cache | < 5 ms | **Not worth it** |

### Not prototyped: design decisions for the architect

Fix 1 is material, but it changes ownership of cached footers. Today the cache hands out copies and evicts freely. Pinning entries through `shared_ptr` from the cache into three native Sources changes eviction semantics, the `footer_map` type (an ABI change across `pool_reader.pxd`, `_operators.pyx`, `native_parquet_scan_source.hpp`, `native_latmat_scan_source.hpp` and `engine.hpp`), and memory accounting, because pinned entries outlive the 512-entry bound while queries run. The "Tier-2 pinning plan" the cache refers to is the ruling this needs.

Fix 3 adds a new process-global cache inside the C++ engine, so it is a comparable decision.

Questions for the architect:

1. **Fix 1:** is `shared_ptr<const FileStats>` in the cache and in `footer_map` the pinning model you want? The alternative is a per-query borrow of `try_get_ptr`, with eviction blocked while any plan is open.
2. **Fix 3:** where should the skene metadata cache live? In `skene` (the format library, standalone-safe) or in the engine Source?
3. **Fix 2:** (a) or (b)? Option (b) raises the cache's memory footprint by the size of the bloom bitsets.
