# Proposal: Metal GPU offload on Apple-silicon Macs (lessons from ArrowMetal)

**Status:** PROPOSAL, 2026-11-22, **with PoC results the same day (§9), which overturn part of
it.** Read §9 first. §1-§8 are kept as written, on paper, so the delta stays visible. In those
sections every GPU figure is ArrowMetal's own, self-reported, measured on one Apple M4 Max, and
every Opteryx figure comes from an earlier measurement in project memory. §9 is the first
measurement of both sides on one machine (M5 Pro).

**Scope:** Mac builds only (the `macos arm64` opteryx_core wheel). The SaaS fleet (x86 Cloud
Run and the ARM fleet) has no GPUs and is unaffected. CPU-side ideas taken from ArrowMetal
are covered by a separate session and are not repeated here.

**Source read:** <https://github.com/singhpratech/ArrowMetal> at v0.6.1: `README.md`,
`docs/DESIGN.md`, `docs/BENCHMARKS_MATRIX.md`, `docs/CROSSOVER.md`, `docs/PARQUET.md`,
`docs/RESIDENT.md`, `Package.swift` and `include/arrowmetal.h`.

---

## 1. Summary

- **What the GPU wins is bandwidth and parallel ranking, at pipeline breakers.** ArrowMetal is
  far ahead on full sorts, order statistics (median and quantiles by radix select),
  high-cardinality hash GROUP BY and DISTINCT, and random gathers. It is behind or level on
  everything that streams one pass over a column: filters, projections, single aggregates and
  element-wise maths. It is far behind on float64 transcendentals.
- **The dispatch floor decides where an offload can go.** A Metal command-buffer round trip
  costs ~56-90 µs, ~57 µs of it the driver's completion notification, and no API removes it.
  One command buffer per morsel would cost more than the CPU work it replaced. So **an offload
  can only replace a sink's finalize** (sort, quantile, group-by merge), where the whole input
  is already collected and the row count is known. This is the same choice ArrowMetal's
  DataFusion and DuckDB integrations make: they swap only blocking nodes.
- **ArrowMetal's headline ratios are against much slower baselines than ours.** Its 22.7x on
  `ORDER BY` is against DataFusion's 1,581 ms sort of 50M rows. Opteryx already sorts 10M ints
  end to end in 62 ms on an M5 Pro (34 ms building the permutation, 24 ms gathering). The GPU
  would replace the 34 ms. That is a real gain, but it is around 2x end to end, not 20x.
- **Ranked by expected gain on our engine and by risk:**
  1. MEDIAN / PERCENTILE_CONT finalize by radix select (largest margin, simplest interface,
     exact answers).
  2. Full-sort permutation build (exact answers, an interface we already have).
  3. High-cardinality numeric GROUP BY finalize (large gain, the largest semantic and
     memory-accounting surface).
  4. Not recommended now: GPU Parquet decode, top-k, filters, element-wise work.
- **Recommended route:** write our own small Metal backend in draken for one or two kernels,
  using ArrowMetal's published algorithms as the reference, rather than depending on or
  vendoring ArrowMetal. Start with one PoC (the quantile finalize), and delete it if it does
  not beat the current CPU path on the M5. The architect decides the route (§7, D2).

---

## 2. What ArrowMetal is

| | |
|---|---|
| Language | Swift (~6 MB of source), with MSL kernels generated as strings and compiled at runtime |
| Boundary | `libArrowMetalC.dylib`, a C ABI (`include/arrowmetal.h`, ~1,900 lines) over the Arrow C Data / C Device Data Interfaces |
| Platform | macOS 14+ / iOS 17+, Apple silicon only; `Package.swift` links `Metal.framework` |
| License | Apache-2.0 |
| Maturity | Repository created 2026-09-06 (11 weeks old). 828 commits, 827 of them by one author. v0.6.1 (0.6.1 fixed pipeline creation on M1-generation GPUs). 18 stars. |
| Evidence | 339 benchmark rows against Polars, pyarrow/Acero, pandas and numpy; 41,688 differential cases against pyarrow. All on one M4 Max. |
| Integrations | DataFusion physical-optimizer rule (`MetalExec` replaces full `SortExec` and some hash aggregates); DuckDB optimizer extension (`ARROWMETAL_AGGREGATE` replaces `HASH_GROUP_BY` / `UNGROUPED_AGGREGATE`); Polars `MetalEngine` (replaces translatable subtrees) |

### 2.1 Where its speed comes from, and where it runs out

These are ArrowMetal's own measurements. They are the physics any Metal backend we write will
also live under.

- **Bandwidth.** M4 Max peak is ~546 GB/s. Single-pass kernels reach 290-375 GB/s
  (`DESIGN.md`, "Where the time goes"). But 11-15 CPU cores reach the same unified memory, so
  a single pass that reads one column and writes one is memory-bound on both sides, and the
  GPU's margin there is about 1.0-1.5x at best.
- **The floor.** An empty kernel costs about 1.7 µs on the GPU, 2.6 µs to encode and
  56-90 µs to round-trip (`RESIDENT.md` §4). Ten dispatches in one command buffer cost 69 µs
  in total (6.9 µs each). Batching into one command buffer is the only lever.
- **No resident worker.** A persistent kernel polling a ring buffer in shared memory does not
  work, because MSL has no system-scope coherence. CPU stores reached a running kernel 0-5
  times out of 599, about a second late (`RESIDENT.md` §2). Per-morsel streaming to a GPU
  that stays busy is therefore not available on this platform.
- **No hardware float64.** Metal has no `double`.
  - Compare, min, max, sort and select run on an order-preserving 64-bit integer key, so they
    are exact and fast.
  - Add, subtract, multiply and divide use a software binary64 that is bit-exact and runs
    near bandwidth.
  - `ln`, `exp`, `sin` and similar run at ~10 GB/s, 0.10x the CPU (`BENCHMARKS_MATRIX.md`).
- **No 64-bit atomic add.** Metal has 64-bit atomic min and max only.
  - Grouped int64 sums use pairs of 32-bit atomics with an explicit carry.
  - Grouped float64 sums use a 128-bit fixed-point accumulator in 32-bit words, which gives a
    *correctly rounded* sum (`DESIGN.md`, "Correctly rounded Float64 sums").
- **Zero-copy needs page alignment.** `makeBuffer(bytesNoCopy:)` wraps memory only when it
  starts on a page, which is 16 KiB on Apple silicon. Anything else is copied once. Wrapping a
  read-only `MAP_SHARED` mapping costs 7-10 ms/GB at first GPU touch, and a private writable
  one costs 67-78 ms/GB (`PARQUET.md`). That agrees with our own finding that macOS
  `MAP_PRIVATE` is expensive.

---

## 3. Candidate operators, with ArrowMetal's crossover evidence

"Crossover" is ArrowMetal's `CROSSOVER.md`: the row count from which the GPU stays ahead of
the fastest CPU library it measured (Polars, pyarrow/Acero, pandas or numpy) at every larger
size. Times are wall-clock ms on the M4 Max.

### 3.1 MEDIAN / PERCENTILE_CONT / quantile: radix select (rank 1)

| ArrowMetal | 1M | 10M | 50M | crossover |
|---|---:|---:|---:|---:|
| `quantile(float64, 0.5)` vs fastest CPU | 2.39x | 13.22x (3.63 ms vs Polars 35 ms) | 38.71x (4.61 ms vs Polars 178 ms) | 1M |
| `top_k(k=100, int64)` | 0.51x | 2.76x | 8.98x | 10M |

**The algorithm** (`DESIGN.md`, "Top-k and order statistics: GPU radix select"):

1. Build a 256-bin histogram of the top 8 bits of an order-preserving key. Each simdgroup keeps
   its own histogram in threadgroup memory, so only 32 lanes contend for any atomic.
2. On the host, pick the bin that holds the wanted rank.
3. Compact that bin's rows. This needs no barriers, because each sub-block owns its slice of
   the output.
4. Repeat on the next digit until fewer than 4,096 keys are left, then finish them on the host.

The column is read twice, whatever the rank. The answer is exact for integers. For floats, the
key merges -0.0 with +0.0 and collapses NaN payloads.

**Opteryx today:** ClickBench-extended Q55 (PERCENTILE_CONT p99 over 25.4M values) takes 131 ms
on the Mac after the 2026-10-23 combine fix (memory `q55-exact-quantile-profile-2026-10-23`).
Of that, the finalize is `nth_element` on one thread, ~115 ms before the fix and varying
38-172 ms from run to run. MEDIAN over the same column paid the same cost.

**Why it is first:**

- The interface is one contiguous key buffer in and one or two values out. No result column
  has to be materialised back into DrakenVectors.
- The answer is exact, so the GPU and CPU paths can be compared bit for bit (§5.4).
- The input is already collected into one buffer by the sink.

**Honest caveat:** a parallel CPU radix select would close much of this gap on every platform,
the fleet included. That belongs to the CPU session. The GPU margin should be weighed against
the CPU fix, not against today's serial `nth_element`.

### 3.2 Full ORDER BY: permutation build (rank 2)

| ArrowMetal | 1M | 10M | 50M | crossover |
|---|---:|---:|---:|---:|
| `argsort int64` | 3.87x | 7.10x (7.81 ms) | 7.71x (39.15 ms) | 1M |
| `argsort float64` | 4.66x | 9.97x (8.11 ms) | 10.52x (39.91 ms) | 1M |
| `lexsort (2 int32 keys)` | 1.83x | 16.17x (7.29 ms) | 24.02x (37.59 ms) | 1M |
| `argsort utf8` | | 16.58x (13.73 ms) | | |
| `take` (n/2 random indices) | 1.44x | 11.29x | 24.15x (5.90 ms) | 1M |
| DataFusion rule, full sorts | 6.9x to 28.8x, 250k to 50M rows | | | |
| DataFusion rule, top-k (`LIMIT 100`) | 0.14x to 0.41x: **left on the CPU** | | | |

**Opteryx today** (memory `order-by-sort-perf-audit-2026-11-05`, M5 Pro, DOP 16):

- A 10M int ORDER BY takes 62 ms end to end: scan 2 ms, key build and permutation 34 ms,
  gather 24 ms.
- With `SELECT *` over 9 columns it takes 355 ms, of which the gather is ~280 ms (79%).
- The resolved gather landed on 2026-11-05, so the gather share has shrunk since.
- A 10M h2o `id3` string permutation takes 76 ms.

**Expected gain.** On ArrowMetal's figures, the permutation step (34 ms) would drop to roughly
8 ms. The gather stays on the CPU unless it is moved too. ArrowMetal's random `take` is 24x
the fastest CPU idiom, but a GPU gather has to write DrakenVectors, including our string
layout (§5.3), which is a much larger change. So the realistic first gain is the permutation:
around 60 ms down to around 35 ms at 10M rows. Strings gain more (76 ms against ~14 ms for
ArrowMetal's utf8 argsort), but only after we solve string layout on the GPU.

Our top-N is already fast for small k (10M `LIMIT 10` takes 5.7 ms with boundary pushdown),
and the large-k cliff was fixed on the CPU on 2026-11-05. ArrowMetal itself leaves top-k on the
CPU in DataFusion and Polars. **Top-k is not a candidate.**

### 3.3 Hash GROUP BY: high-cardinality numeric keys (rank 3)

| ArrowMetal, `sum by int32 key` | 100k rows | 1M | 10M | 50M | crossover |
|---|---:|---:|---:|---:|---:|
| 1,000 groups | | 0.63x | 2.35x | 3.77x (4.89 ms) | 10M |
| 100,000 groups | 1.40x | 2.67x | 8.38x | 6.26x (7.41 ms) | 100k |
| 10,000,000 groups | | | 7.35x | 6.98x (46.12 ms) | |
| `sum by utf8 key`, 1,000 distinct | | 0.36x | 1.71x | 2.20x | 10M |
| `count_distinct by key`, 100k groups | 2.39x | 1.73x | 4.52x | 3.37x | 100k |

The DuckDB comparison matters more than the table, because DuckDB's group-by is the closest
to ours:

- Against DuckDB 1.5.5, `sum by int32 key` at 1,000 groups and 50M rows is **1.8x**, not 3.8x
  (README, "DuckDB on the same rows").
- ArrowMetal's DuckDB extension takes only `sum`, `avg`, `min`, `max` and `count` over
  integer, `DATE` and `TIMESTAMP` columns, grouped by one column or none.
- It leaves a single `sum` (0.32x), few groups with at most two aggregates (0.59x) and
  `VARCHAR` keys (0.22x) to DuckDB.
- On a first run after 500 ms idle, 4 of the 13 shapes it rewrites are *behind* DuckDB, down to
  0.38x.

**Opteryx today:** the GROUP BY topology (local pre-aggregation, then 64 partitions, then a
parallel merge) was reviewed in `docs/GROUPBY_REDESIGN_2026_10_12.md` and judged right. The
measured costs were storage and key copies, not the hash table.

**Why it is third:**

- It is the largest surface:
  - the full `AggFn` set (`native_group_sinks.hpp:337-394`: COUNT through CIDR_AGG, including
    HLL, t-digest and quantile states);
  - `__int128` exact integer sums;
  - DECIMAL, strings and NULL-group semantics;
  - the direct-address and ticket-table modes;
  - spill.
- The GPU wants the whole input before it starts. Our group-by streams (it pre-aggregates per
  worker and spills under the slab budget), so an offload would have to buffer every input row
  first, which raises peak memory.
- Float SUM changes answers (§5.4).

Only one shape is worth considering: one or two integer or temporal keys, an estimated NDV
≥ 100k, numeric `COUNT`/`SUM`/`MIN`/`MAX`/`AVG`, and in-memory input.

### 3.4 Not recommended

| Candidate | ArrowMetal's own evidence | Why not for us |
|---|---|---|
| Parquet decode | Uncompressed: 50-59 GB/s, 1.7x Polars on a whole table. Snappy/LZ4: 1.4-1.5x Polars, with token-dense pages decoded on the *host*. ZSTD and GZIP: host only (libzstd). | (1) The gain is in the post-decompression steps (levels, RLE/bit-unpacking, dictionaries). Decompression itself stays on the CPU, and zstd decompression was the single largest CPU cost in our 2026-10-05 profile. (2) The decoded column lands in GPU-visible memory, but our operators run on the CPU, so the "already resident" bonus that drives ArrowMetal's 2 ms-vs-6 ms sum does not apply. (3) A second Parquet decoder in rugo, for one platform only, duplicates the most correctness-sensitive code we own. rugo must also stay Python-free, build for wasm and keep its standalone wheel's macOS 11.0 floor. |
| Top-k (`ORDER BY … LIMIT k`) | Left on the CPU by its DataFusion (0.14x-0.41x) and Polars integrations | Already fast on the CPU (§3.2) |
| Filters and projections | 0.31x-0.79x (DataFusion), 0.14x-0.42x (Polars); left on the CPU | Memory-bound single passes, plus the per-morsel dispatch floor |
| Ungrouped `SUM`/`MIN`/`MAX` | DuckDB single `sum` 0.32x; the 50M `sum, max, avg` was 1.08x warm and 0.38x after idle | We already run near bandwidth (`ungrouped-sum-one-kernel-2026-10-31`) |
| Float64 transcendentals | `ln` 0.10x, `sin` 0.16x | No hardware double |
| Joins | Inner join 50M × 1M: 2.9x pyarrow. Polars integration takes joins. | Not in this brief's candidate list. A later candidate with the same shape as group-by (build at the breaker, probe streams), so the per-morsel probe runs into the floor. |

---

## 4. Where an offload would plug into Opteryx

### 4.1 The morsel arithmetic rules out per-morsel offload

A parquet morsel is one row group, 64K-262K rows. The writer's default of 65,536 is called "the
engine's measured best morsel size" (`opteryx/config.py:130`). At 64K int64 rows a CPU pass
takes a few tens of µs. One command buffer per morsel costs 56-90 µs before the GPU does any
work (§2.1), and a resident worker is impossible. So no streaming operator (`Operator::execute`
in `src/cpp/engine/operator.hpp`) is a candidate. Only `Sink::finalize` is: it sees all the
input once, knows the exact row count, and pays the floor once per query rather than once per
morsel.

### 4.2 The three sink seams

| Candidate | Sink | The step that would be offloaded |
|---|---|---|
| MEDIAN / PERCENTILE_CONT | `AggFn::Quantile` in `UngroupedAggSink` (`src/cpp/engine/native_group_sinks.hpp:2835`) and the grouped path; state `QuantileState` (`opteryx/operators/aggregate/_agg_kernels.hpp:87`) | Finalize: `std::nth_element(buf, …, fp_total_lt)` over one contiguous `double` buffer, plus linear interpolation (`_agg_kernels.hpp:254`). Its memory budget is 256 MB, escalating to 2 GB, then it fails loud (`native_group_sinks.hpp:21-33`). |
| Full ORDER BY | `SortSink` (`src/cpp/engine/native_sort.hpp:166`), built by `Engine::set_sort_sink` (`engine.hpp:1598`) | `sort_perm` → `radix_sort_perm<NPARTS>` (`draken/morsels/sort.hpp:1081`) over keys already normalised to order-preserving `uint64` (`sort_num_key` :141, `sort_float_key` :132; multi-key `RowKeyN<1..8>`). The GPU would replace the radix passes (and `radix_fixup_ties` :1426) and return the same permutation. `gather_rows` stays on the CPU. |
| GROUP BY | `GroupBySink` (`native_group_sinks.hpp:4604`): 64 hash partitions, a ParviMap → Medius → CarcharIndex ladder, parallel `finalize` (:7835), and spill | Hardest to cut (§3.3). A GPU path would have to take the whole input, not just merge partitions, because the per-morsel pre-aggregation is the streaming part. |

### 4.3 How a node is swapped

`compiler.py` `_Compiler.compile_node` (:2428) maps `SortNode` → `nplan.set_sort_sink`,
`GroupedAggregateHashedNode` → `set_groupby_sink`, and so on, through
`opteryx/operators/_operators.pyx` (`cppclass Engine` :572) into `src/cpp/engine/engine.hpp`.

There are two ways to swap:

- **(i) A new Sink subclass**, with its own setter, Cython declaration, NativePlan method and
  compiler branch. This mirrors ArrowMetal's `MetalExec`, but it puts the choice in the
  planner.
- **(ii) Leave the plan alone** and let the existing sink's finalize call a draken Metal kernel
  when a native rule says so.

(ii) is smaller and keeps the decision native, where the row count is exact. That agrees with
the standing rule against Python eligibility gates. The planner and compiler would not change.
(ii) is recommended.

### 4.4 Precedent for a platform-dependent choice

Every per-arch decision today is made at compile time: `SIMD_STATIC_SELECT(avx2, neon, rvv,
scalar)` in `draken/simd/simd_dispatch.h`, and `_EXPR_TILE_ROWS_DEFAULT` (4096 on x86-64, 0
elsewhere) in `evaluation.pyx:2394`. A Metal backend would be the first decision taken at
*runtime*: whether a Metal device exists, and whether n is above the crossover. D5 asks for that
to be ruled on explicitly.

---

## 5. DrakenVector ABI and memory

### 5.1 No ABI change is needed

The 40-byte `DrakenVector` stays frozen. A Metal kernel can follow the same uniform access
(`data[selection[i]]`) by binding `data` and `selection` as two buffers. The §11 contract
carries over unchanged: a GPU kernel must give the right answer for dense, constant and
dictionary shapes through the uniform path. Any shape-specialised GPU variant needs the same
architect approval a CPU one does.

### 5.2 Zero-copy wrapping: what it takes

`newBufferWithBytesNoCopy:length:options:deallocator:` requires:

- a start address on a 16 KiB page;
- a length that is a multiple of the page size;
- memory that stays alive and unmoved until the command buffer completes.

**Most of this is already true of draken.** `draken/core/alloc.h` sends every request of
64 KiB or more (`DRAKEN_SLAB_MIN`) to the slab pool (`draken/core/slab_pool.cpp`). The pool is
one 256 GiB `PROT_NONE` reservation aligned to 2 MiB slots:

- Requests of 64 KiB-1 MiB get power-of-two class blocks, so they are 64 KiB-aligned.
- Larger requests get runs of whole 2 MiB slots.
- `draken_slab_usable_size` returns the class or run size, which is always a multiple of
  64 KiB.

So **every draken buffer of 64 KiB or more already satisfies `bytesNoCopy`**: it starts on a
16 KiB page and its usable length is a multiple of 16 KiB. That covers any column worth
offloading (64K int64 rows is 512 KiB). Buffers under 64 KiB come from `malloc` and are not
page-aligned, but they are below every crossover anyway.

There are two caveats:

- The pool's memory is anonymous `MAP_PRIVATE`. ArrowMetal's residency costs were measured on
  *file* mappings (7-10 ms/GB shared read-only, 67-78 ms/GB private writable). The
  first-GPU-touch cost of wrapping anonymous slab memory is **unmeasured**, and it decides
  whether wrapping beats copying.
- Sort and GROUP BY lanes allocate through `draken/core/tracked_allocator.h` (`std::allocator`),
  not the slab pool. The sort's normalised-key buffer would have to move to a slab or
  page-aligned allocation before it could be wrapped.

The deallocator passed to `bytesNoCopy` must be `nil`, because draken keeps ownership through
`VectorOwner` / `OwnedBuffer` (`draken/core/vector_owner.h`). The sink must hold the buffer
until the command buffer completes, which a synchronous finalize does naturally.

There are two ways to feed a kernel the input of a sink that holds many morsels:

- **(a) Gather into one staging buffer** that is page-aligned and owned by the Metal backend.
  This costs one CPU pass. ArrowMetal measured 6.4 ms for three 50M-row int64 columns in 16
  chunks on 16 threads.
- **(b) Wrap each morsel's buffers in place** and bind a table of GPU addresses, as ArrowMetal
  does for string-view data buffers. This needs every morsel buffer page-aligned and padded,
  which means an allocator change in draken (D7).

For the sort permutation and the quantile select, the input is already a key buffer the sink
builds itself (the sort's normalised keys and the quantile's collected values). That buffer
can simply be *allocated* page-aligned and padded. No per-morsel wrapping is needed, which is
one more reason those two go first.

### 5.3 Strings

Draken strings are German strings: 16-byte `DrakenStringSlot`s (`draken/core/string_slot.h`;
inline up to 12 bytes, else a prefix plus a `uint32` arena offset) in a `DrakenStringArena`
(`buffers.h:142`). This is close to Arrow's `utf8_view`, and ArrowMetal's view accessor (one
indexed load per out-of-line view) shows that a GPU kernel can read the layout directly. It is
not identical, though: Arrow views carry a buffer index, and ours carry one arena offset.

Under Route A the boundary goes through `draken_to_arrow.h`, which rebuilds strings into Arrow
offsets plus bytes (a full copy). Under Route B a kernel binds the slot array and the arena as
two buffers. No string candidate is in the first two PoCs. The sort already reduces strings to
an 8-byte big-endian prefix key (`string_prefix_key`, `sort.hpp:526`), and that key is
fixed-width, so the GPU radix path takes it unchanged. The tie fix-up stays on the CPU.

### 5.4 Answers must not change

§11 says a fast path whose result differs from the uniform path is a bug. Applied to a GPU
path:

- **Sort permutations and order statistics are exact.** The GPU and CPU must agree row for
  row, including ties (stability), NULL placement (our ruling: NULL sorts lowest) and the
  float total order (-0.0 / +0.0 / NaN; ArrowMetal merges -0.0 with +0.0 in its key). Every
  difference here is a bug, and testable as one.
  Concretely, `QuantileState` orders by `draken::ops::fp_total_lt`. ArrowMetal's float key
  folds -0.0 into +0.0 and collapses NaN payloads, so a ported key map has to be changed to
  match `fp_total_lt` (or `sort_float_key` for the sort). It cannot be taken as it is.
- **Group identity is the 64-bit key hash** in `GroupBySink` (there is no key compare). A GPU
  group-by must use the identical hash function to produce the same groups. ArrowMetal's
  tables compare key bytes instead, so its group-by is not a drop-in even for integer keys.
- **Float SUM/AVG cannot match bit for bit.** The CPU sums grouped floats as a plain `double`
  per row in arrival order (`agg2_update`), and ungrouped floats as 8 independent lanes per
  morsel (`FpSumAcc`, `draken/ops/float_ops.h:507`). Neither is compensated, and both already
  vary with morsel arrival and worker combine order. ArrowMetal's 128-bit fixed-point sum is
  correctly rounded, so it is *more* accurate and deterministic, but it will differ from ours
  in the last bits. Whether "the answer" includes those bits is D6.

---

## 6. Build impact

| Item | Own kernels (Route B) | Depend on or vendor ArrowMetal (Route A) |
|---|---|---|
| Host API | Metal is an Objective-C API. From draken C++ there are two choices: Apple's `metal-cpp` (header-only C++ wrapper, Apache-2.0; vendoring it needs approval), or one Objective-C++ `.mm` translation unit in draken (no vendoring; clang builds it with `-x objective-c++ -fobjc-arc`). | Swift. Needs `swift build -c release --product ArrowMetalC` in the macOS wheel job, plus a bundled `libArrowMetalC.dylib`, `delocate`d into the wheel. The Swift runtime ships with macOS 14+. |
| Kernels | MSL source embedded as C++ string literals and compiled at first use with `newLibraryWithSource:`, cached per process. This is ArrowMetal's approach, so no Xcode `metal` toolchain is needed. Alternative: a precompiled `.metallib` built in CI (D4). | Comes with the dylib (runtime-compiled MSL). |
| Link | `-framework Metal -framework Foundation`, darwin only, in `build_common.py`. `setup.py` already adds darwin frameworks for libcurl (CoreFoundation, SystemConfiguration), so there is precedent. | Same, plus dylib loading at runtime. |
| Deployment target | opteryx_core macOS wheels are already `MACOSX_DEPLOYMENT_TARGET=15.0` (`release.yaml`), above Metal's needs and ArrowMetal's 14.0. No bump. | Same. |
| Linux, x86 and RISC-V wheels | Untouched: the Metal translation unit compiles only on darwin. | Untouched. |
| rugo standalone wheel and wasm | Excluded. rugo targets macOS 11.0 and wasm, and the candidates in §3 are engine sinks, not rugo. | Excluded. |
| Python-free execution | Holds. The backend is C++/ObjC++ inside draken, and the Metal device and pipelines are created natively. | The dylib is native, but its own Python bindings, router files and environment variables come with it unless we cut them out. |
| CI testing | **Risk:** GitHub's hosted macOS arm64 runners are virtual machines. Public reports say Metal on them is unreliable: PyTorch reports MPS as available on them and then fails, and added a skip for virtualised devices (pytorch/pytorch#125197). CircleCI's macOS VMs have no Metal device at all. GitHub states nothing about `macos-15`. This is unverified on our runner, and the first PoC should check it. Expect GPU tests to run only on the local Mac, which means a local gate (D9). | Same risk. |
| First-use cost | Runtime MSL compile adds latency on the first query that touches a kernel. ArrowMetal mitigates it with lazy source generation and a pipeline cache. Unmeasured for us. | Same. |

---

## 7. Integration options

### Route A: depend on ArrowMetal (`libArrowMetalC`) through the Arrow C Data Interface

The sink finalize exports its key column through `draken/interop/draken_to_arrow.h`, calls
`am_import` → `am_argsort` / `am_top_k` / `am_group_by` / quantile, and imports the result.

**For:**
- 284 GPU kernels, already measured.
- Radix select, LSD radix argsort, lexsort, string hash tables and exact grouped float sums
  all exist.
- 41,688 differential cases against pyarrow.

**Against:**
- **Two copies at the boundary.** `draken_to_arrow.h` always copies (`own_copy`, and
  `gather_fixed` for permutations). ArrowMetal copies again unless the buffer is page-aligned.
- **Arrow semantics, not ours.** NULL placement, float total order, quantile interpolation,
  DECIMAL and 32-bit string offsets all differ.
- **Hidden behaviour.**
  - Its own CPU/GPU router, read at load from `ARROWMETAL_ROUTER`.
  - A per-machine table at `~/.arrowmetal/router/<chip>.json`.
  - Host fallbacks inside the library (regex, casts).
  - Its own buffer pool and up to 16 import threads, outside our slab budget and
    `mem_account`.

  Each of these conflicts with "no hidden behaviour, no fallbacks" and with our memory
  accounting.
- **Single author, 11 weeks old, v0.x.** The C ABI is still adding `_ex2` variants.
- **A Swift toolchain in our build.**
- Vendoring ~6 MB of Swift is not "vendored and optional" in any practical sense.

### Route B: our own Metal backend in draken (recommended)

A small native module, for example `draken/metal/` (`_metal_backend.mm` plus MSL strings),
with one entry per kernel that takes a DrakenVector or a sink's key buffer and writes into
draken-owned, page-aligned buffers.

**For:**
- Our semantics and our memory accounting.
- DrakenVector-native (selection-aware) with no copies.
- No Swift and no dylib.
- The scope is the two or three kernels that §3 justifies.

**Against:**
- We own correctness and maintenance.
- GPU tests may be local-only.

ArrowMetal's algorithms are documented in detail (radix select, the histogram layout, simdgroup
ranking, 32-bit-carry int64 sums, 128-bit fixed-point float sums), so implementing them is
cheaper than inventing them.

### Route C: port specific ArrowMetal MSL kernels into Route B

Apache-2.0 allows it with attribution (`NOTICE`, file headers). In effect this is vendoring
source, so it needs the same approval. It saves kernel-writing time on radix select and
argsort, but the kernels assume Arrow's bitmaps and layouts and would need adapting to
validity-plus-selection.

### Recommended sequence (if D1 is yes)

1. **PoC 1: quantile finalize by radix select** on the M5, behind a native size crossover.
   Gate: at the sizes that matter (Q55 and MEDIAN over ≥ 1M values) it must beat the CPU path
   *as it stands after the CPU session's work*, with identical answers. If it does not, it is
   deleted.
2. **PoC 2: full-sort permutation** (fixed-width keys only, the AoS-eligible key set the CPU
   radix path already uses), with the same gate and row-for-row identical permutations.
3. **Group-by:** decide only after 1 and 2 have measured the real cost of the Metal floor,
   first-use compilation and memory inside our engine.

---

## 8. Open decisions for the architect

- **D1. In scope at all?** Is a second, Apple-only execution backend in draken acceptable? It
  runs only on Mac builds and may be testable only locally.
- **D2. Route.**
  - A: depend on or vendor ArrowMetal.
  - B: our own kernels (recommended).
  - C: B with ported ArrowMetal kernels.
- **D3. Host binding** (Route B):
  - vendor `metal-cpp`; or
  - one Objective-C++ `.mm` translation unit in draken, with no new vendored code.
- **D4. Shader delivery:**
  - MSL embedded as strings and compiled at runtime (no Xcode toolchain; costs first-query
    latency); or
  - a precompiled `.metallib` built in the macOS wheel job.
- **D5. Is a CPU path when the GPU declines a forbidden fallback?**
  - The CPU kernel is the primary implementation and the GPU an optional accelerator, chosen
    by a deterministic native rule: a pure function of (operation, type, row count), with
    constants compiled in.
  - This needs an explicit ruling that this is not a "fallback" in the §1 sense, and that "no
    Metal device" (VMs, old Macs) means the CPU path runs, visibly reported in telemetry.
  - Recommended: no per-machine calibration files and no ArrowMetal-style
    `~/.arrowmetal/router` (hidden behaviour). An env kill switch follows the existing
    `FEATURE_DISABLE_*` pattern.
- **D6. Float answers.** May a GPU float SUM/AVG differ in the last bits from the CPU path
  (correctly rounded on the GPU; order-dependent 8-lane partial sums on the CPU)? If not,
  GPU group-by is limited to integer, temporal and MIN/MAX/COUNT aggregates, or must
  reproduce our summation order exactly.
- **D7. Memory.**
  - Is Metal scratch (histograms, keys, staging buffers) charged to the slab budget and
    `mem_account`?
  - Should draken allocations above a size be 16 KiB-aligned and page-padded, so that sink
    buffers can be wrapped with no copy?
- **D8. Where the decision lives.** Proposed: natively, inside the sink's finalize, once the
  row count is exact. Not in the Python planner (memory `feedback_no_python_eligibility_gates`).
  The planner and compiler stay unchanged (§4.3 (ii)), and the decision is reported in the
  sink's telemetry.
- **D11. Anonymous-memory residency.** Approve one micro-measurement before any PoC: the
  first-GPU-touch cost of wrapping a slab-pool buffer with `bytesNoCopy`, against a copy into
  a Metal-owned buffer (§5.2). It decides the input path for every candidate.
- **D9. Testing gate.** If CI runners cannot run Metal compute, are GPU differential tests
  acceptable in `make q` on the Mac only? Does release sign-off then require a local Mac run?
- **D10. Parquet decode on the GPU.** Confirm it is out of scope (§3.4).

---

## 9. PoC results, 2026-11-22 (M5 Pro, measured)

### 9.1 What was run

The harness is [`scratch/metal_poc/`](../scratch/metal_poc/): `metal_poc.mm`, the MSL kernels
in `kernels_msl.h`, and `build.sh <out>`. It is standalone and not part of `make`. No engine
code changed. Our kernels were written from ArrowMetal's published algorithms; no ArrowMetal
code was used.

**Machine:** Apple M5 Pro, 6P + 12E CPU cores, 20-core GPU, 64 GB, on AC power. Load average was
1.8 at the start, and the architect confirmed the machine was free.

**CPU arms are the engine's own code, unmodified:**
- `opteryx::ungrouped::QuantileState::finalize_quantile`: `std::nth_element` with
  `fp_total_lt`, then interpolation.
- `radix_sort_perm<1>` at 16 threads (the engine DOP on this Mac), fed the engine's
  normalised keys (`x ^ SIGN` and `sort_float_key`).

**A third arm**, a 16-thread CPU radix select running the GPU's algorithm, tests §3.1's
caveat that a CPU-side fix closes much of the gap.

**Protocol:**
- Each configuration gets 1 warm-up round and 7 timed rounds.
- Arm order rotates every round.
- Times are wall-clock ms (median, with best where useful).
- The GPU arm wraps its input per query, as a finalize would, so first-touch residency is
  included.

Raw output is in `scratch/metal_poc/results_2026_11_22/` (`poc_residency.txt`, `poc_quantile.txt`,
`poc_sort.txt`, `poc_sortprof.txt`). The tables below are copied from it.

### 9.2 Correctness

- **Quantile:** 180 cases, across n = 1 to 2M, p ∈ {0, 0.25, 0.5, 0.99, 1}, with NaN, ±0, ±inf,
  heavy ties and a single repeated value. GPU and CPU radix both equal the engine under
  `fp_total_eq` in **180 of 180**. 178 are bit-identical. The 2 others are **-0.0 (engine)
  against +0.0 (radix)**, the case §5.4 predicted. The engine's answer there is itself whichever
  zero `nth_element` lands on, so the radix answer is the more deterministic of the two.
- **Sort:** 42 cases (block-edge sizes, ties, int and float keys). The GPU permutation is
  **identical to `radix_sort_perm` in 42 of 42**, and identical again in every timed
  configuration.

### 9.3 Platform figures

| Measure | Result | Paper expectation |
|---|---|---|
| Command-buffer round trip, empty kernel (`waitUntilCompleted`) | best 78 µs, median 136 µs | 56-90 µs (ArrowMetal uses a shared-event spin; not tried here) |
| 10 empty kernels in one command buffer | best 89 µs, median 127 µs, the same as one kernel | Confirmed: batch everything into one buffer |
| MSL compile + 10 pipelines | 140 ms on first ever run, then **1.1-1.6 ms** on every later process (Metal's on-disk shader cache) | **D4 settled in favour of runtime MSL**: the first-use cost is one-off |
| **D11.** Wrap a 400 MiB draken slab block (`bytesNoCopy`) | wrap 0.05-0.07 ms; first GPU read 6.0-7.4 ms against 1.7-3.4 ms warm, so **~3-4 ms of residency per 400 MiB** | — |
| D11. Wrap the same memory again (a new `MTLBuffer`) | 0.04 ms; first read 4.5-5.1 ms. Residency is paid again on every wrap. | — |
| D11. Copy route: new Metal buffer + `memcpy` | alloc 0.03 ms, first `memcpy` 20.2-20.7 ms (page faults), first GPU read still 6.4-6.9 ms | **Wrapping in place wins by ~20 ms per 400 MiB.** Slab blocks pass the alignment rule unchanged (asserted in the harness). |

### 9.4 Quantile: GPU radix select

Each cell is the median in ms. Q55's size (25.4M) is in bold.

| distribution | n | p | engine `nth_element` | CPU radix ×16 | GPU | engine / GPU | CPU radix / GPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| int [0,2000) | 1M | 0.5 | 6.11 | 1.66 | 1.49 | 4.1x | 1.11x |
| int [0,2000) | 10M | 0.5 | 52.77 | 7.94 | 4.92 | 10.7x | 1.61x |
| int [0,2000) | **25.4M** | 0.99 | **134.26** | **19.21** | **9.70** | 13.8x | 1.98x |
| int [0,2000) | 50M | 0.5 | 296.64 | 36.03 | 14.56 | 20.4x | 2.47x |
| uniform [0,1) | 1M | 0.5 | 6.44 | 1.74 | 1.87 | 3.4x | **0.93x** |
| uniform [0,1) | **25.4M** | 0.99 | **84.76** | **23.12** | **9.75** | 8.7x | 2.37x |
| uniform [0,1) | 50M | 0.5 | 216.65 | 39.38 | 13.59 | 15.9x | 2.90x |
| wide int64 | 1M | 0.5 | 5.45 | 0.81 | 1.30 | 4.2x | **0.62x** |
| wide int64 | 10M | 0.5 | 54.35 | 3.77 | 4.07 | 13.4x | **0.93x** |
| wide int64 | **25.4M** | 0.5 | **136.19** | **8.91** | **7.13** | 19.1x | 1.25x |
| wide int64 | 50M | 0.99 | 148.89 | 38.19 | 13.07 | 11.4x | 2.92x |

(The full 24-row table is in `poc_quantile.txt`.)

**Verdict on §3.1: the direction was right, but the attribution was wrong.**

- **Most of the gain is the algorithm, not the GPU.** Moving from `nth_element` to a 16-thread
  CPU radix select is worth 3.5-15x at ≥ 10M values, with identical answers. That works on
  every platform, the fleet included.
- **The GPU's own margin over that CPU fix is 0.6-2.9x.**
  - At 1M values it is a wash: 0.62-1.45x, all under 2 ms.
  - At 10M it is 0.93-1.77x.
  - At Q55's size it is 1.25-2.6x, which saves ~2-13 ms per quantile.
  - At 50M it is 1.5-2.9x.
- The GPU arm here is 13-15 ms at 50M, against ArrowMetal's self-reported 4.6 ms on an M4 Max.
  Part of the gap is hardware (a 20-core GPU against 40 cores). Part is that each pass here is
  its own round trip, and the first touch of the freshly wrapped input is included.

### 9.5 Sort permutation: GPU stable LSD radix argsort

| keys | n | engine `radix_sort_perm` ×16 | GPU | CPU / GPU |
|---|---:|---:|---:|---:|
| int64 full range (8 byte passes) | 1M | 2.54 | 3.21 | **0.79x** |
| | 10M | 18.23 | 26.94 | **0.68x** |
| | 50M | 88.94 | 136.49 | **0.65x** |
| int64 < 1e6 (3 passes) | 1M | 1.49 | 1.42 | 1.05x |
| | 10M | 11.85 | 13.10 | **0.90x** |
| | 50M | 64.28 | 67.54 | **0.95x** |
| float64 (8 passes) | 1M | 2.59 | 2.16 | 1.20x |
| | 10M | 18.49 | 24.36 | **0.76x** |
| | 50M | 88.05 | 123.93 | **0.71x** |

**Per-kernel profile of one GPU pass at 50M** (GPU timestamps, `poc_sortprof.txt`):

| Kernel | Time |
|---|---|
| Histogram + scans | 1.6 ms |
| **Scatter** | **10.8 ms** (1.2 GB moved, ~111 GB/s) |
| Plain 400 MB read, for reference | 1.4 ms (~286 GB/s) |

**Verdict on §3.2: the paper estimate was wrong, and the GPU sort loses on this machine.**

- **The baseline was misread.** §3.2 took the 34 ms "key build + permutation" from the
  2026-11-05 audit as the permutation alone. The engine's radix permutation is 18 ms at 10M.
- **The GPU kernel loses to it.** Mine is 0.65-0.79x on 8-pass keys and level on narrow keys.
- **A tuned kernel cannot win by much.**
  - Hist and scans take 1.6 ms per pass. If the scatter also ran at full read bandwidth
    (~4.2 ms), the GPU sort would be ~47 ms at 50M against 89 ms, about 1.9x. That is the
    ceiling.
  - ArrowMetal's published 39 ms argsort on an M4 Max moves ~246 GB/s, about 45% of that
    chip's peak. Applying the same 45% to this GPU's measured 286 GB/s read rate gives
    ~129 GB/s, or ~75 ms at 50M: **~1.2x**, on the permutation step only. That is an
    extrapolation, not a measurement.
  - The gather (the larger share of an ORDER BY) stays on the CPU.
- **Not worth an Apple-only backend.** Max-class Macs have twice the GPU (and more CPU cores)
  and are unmeasured.

### 9.6 What this changes in the proposal

| Section | On paper | After the PoC |
|---|---|---|
| §3.1 quantile, rank 1 | GPU radix select, large margin | **The win is a CPU radix select** (3.5-15x, all platforms). That is the CPU session's work; the hand-over note is in the shared folder. After it, the GPU adds ≤ 2.9x at ≥ 10M values on Macs only. |
| §3.2 sort, rank 2 | ~34 → ~8 ms | **Drop.** It loses on the M5 Pro, with a ~1.2x realistic ceiling (1.9x theoretical). |
| §3.3 group-by, rank 3 | Large gain, large surface | Not measured. The sort result is evidence against it: the CPU side is a tuned, memory-bound engine, and a GPU hash scatter faces the same scatter-bandwidth wall. |
| D4 shader delivery | Open | Runtime MSL is fine: 1-2 ms after the first-ever compile. |
| D11 residency | Open | Answered: wrap slab memory in place; ~3-4 ms per 400 MiB per wrap; copying costs ~20 ms more. |
| Route recommendation | B (own kernels) | **D1: recommend not building a Metal backend now.** The measurable Mac-only gain left is ~2-13 ms per large quantile, after a CPU fix that should come first. Revisit if (a) a Max-class Mac measurement changes the sort ceiling, or (b) a GPU-resident pipeline (decode → filter → aggregate with no CPU hop) is ever in scope, since that is where ArrowMetal's real advantage is. |

**Not covered by this PoC:**
- ArrowMetal's own kernels on this Mac (installing it is a download nobody approved).
- A Max-class Mac.
- GPU group-by.
- CI runners.
- The shared-event wait.
- A batched single-command-buffer radix select (the quantile GPU arm used one round trip per
  pass).
