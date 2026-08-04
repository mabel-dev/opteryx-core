# Native Extension Consolidation

Status: **design** — Step 0 (the linker tourniquet) is applied; Steps 1–4 are not started.

## 1. The problem

A C++ exception thrown inside `pool_reader.so` aborts the whole process instead of
becoming a catchable Python exception. Observed in production as a Cloud Run 503
with a `faulthandler` dump whose C stack reads:

```
pool_reader.so   +0xaa5d3        <- throw site (FetchParquetFooter)
draken_native.so __cxa_throw+0x37 <- the throw runs in ANOTHER .so's C++ runtime
...
libc gsignal+0x12                 <- raise(), i.e. abort()
```

`gsignal` is `raise()`, called by `abort()`. This is **SIGABRT, not SIGSEGV** — the
signature of `std::terminate()` firing because a thrown exception found no handler.

### Mechanism

Three build facts combine:

1. **Every C++ extension gets a private, statically-linked C++ runtime on Linux.**
   `LD_EXTRA` carries `-static-libstdc++ -static-libgcc`, and `build_common.py`'s
   `build_extension` hook appends it to *every* `language="c++"` extension at build
   time — not just the ones whose `Extension()` lists it:

   ```python
   if is_linux() and getattr(ext, "language", "") == "c++":
       ext.extra_link_args = list(getattr(ext, "extra_link_args", [])) + LD_EXTRA
   ```

   So `pool_reader`, whose `Extension()` does *not* list `LD_EXTRA`, still gets one
   on Linux. There are ~30 private copies of libstdc++/libgcc in the process.

2. **Those private runtimes are exported.** `-fvisibility=default` is set on both
   `CPP_FLAGS` and `C_FLAGS`, so each `.so` publishes its own `__cxa_throw`,
   `__gxx_personality_v0`, and `_Unwind_*` in its dynamic symbol table.

3. **One of them is published process-globally.** `draken/__init__.py` does
   `sys.setdlopenflags(ctypes.RTLD_GLOBAL | os.RTLD_NOW)` before importing
   `draken_native`, putting draken's private runtime into the global symbol table
   where it interposes for everything loaded afterwards.

Result: a throw raised in `pool_reader.so` is dispatched through *draken's* runtime
while the handler search runs against a different one. No handler matches;
`std::terminate()` aborts. Every `except +` in the affected modules is decorative.

macOS is immune — two-level namespaces bind each image to its own runtime, and the
`is_linux()` gate means mac extensions never receive `-static-libgcc` at all. **This
class of bug cannot be reproduced or validated on the dev platform.**

*Verified:* the build flags, the `RTLD_GLOBAL` load, and the symbol attribution in
the production stack. *Inferred:* the precise handler-search failure step. The
configuration is wrong regardless; the inference is about which of several ways it
fails.

## 2. Step 0 — tourniquet (applied)

`-Wl,--exclude-libs,ALL` added to the Linux branch of `LD_EXTRA`. It marks symbols
originating in static **archives** (`libstdc++.a`, `libgcc.a`, `libgcc_eh.a`) hidden
in the output `.so`, so no extension can export or interpose another's C++ runtime.
Each `.so` then uses its own copy consistently.

Symbols from an extension's **own objects** — `PyInit_*`, and the bridge symbols
`draken_vector_unwrap` / `draken_vector_own_raw` that the Cython shims resolve via
`RTLD_GLOBAL` — come from `.o` files, not archives, and are unaffected.

Because of the `build_extension` hook above, this reaches every C++ extension on
Linux from the single edit.

**Known limitation:** with each `.so` holding its own runtime, an exception thrown
in one and caught in another still cannot work. That is acceptable only because the
draken↔opteryx boundary is a **C ABI** (kernels return result structs, they do not
throw). Rugo's throwing code (`FetchParquetFooter`, `ReadParquetMetadataFromBuffer`)
is compiled *into* `pool_reader`, so its throws are intra-`.so`. If a genuine
cross-`.so` throw path exists, this does not fix it — consolidation does.

**Validation:** cannot be done on macOS. Needs a manylinux build plus a deployment
that exercises a throwing path (e.g. a footer fetch against a 403-ing object).

## 3. Why consolidation is the real fix

**Measurement warning.** An AST scan of literal `sources=[...]` lists **undercounts**:
`make_draken_extension` builds its source list programmatically, so four draken
extensions are invisible to it. Counts below use **live `Extension` objects** for
draken/rugo plus AST literals for `setup.py` (which does use literal lists). The
scratch inventory that gets this right is `map_exts2.py`; the naive one reported
`cpu_features` as 11 copies when it is 14, and `vector_alloc` as 3 when it is 7.

- **62 built `.so`**, from 56 `Extension()` definitions.
- Baseline: **~50 sources / ~118 duplicate compilations** (copies beyond the first).
- After Steps 1a + 1b: **39 sources / 75 duplicate compilations.**

Worst offenders — complete duplicate copies of real functionality:

| Source | Copies | Into |
|---|---|---|
| `draken/ops/kernels/*.cpp` (~25 files incl. `kernel_registry.cpp`) | ~~2 each~~ **1** | resolved in Step 1a |
| `rugo/src/parquet/*.cpp` (all 7) | 2 each | `pool_reader`, `rugo_native` |
| `src/cpp/cpu_features.cpp` | 14 → 10 | scattered; −4 in Step 1b |
| `nanobind/nb_combined.cpp` | 8 | every nanobind module |
| `src/cpp/simd_env.cpp` | 8 → 4 | −4 in Step 1b |
| `src/cpp/simd_search.cpp` | 8 | **blocked** — absent from `draken_native` |
| `third_party/yyjson/src/yyjson.c` | 6 | draken, expression, third_party, rugo |
| `draken/core/vector_alloc.cpp` | **7** | deliberate — see below |
| `src/cpp/memory_pool.cpp` | 4 | incl. the "must compile in, cannot borrow" site |
| `src/cpp/http_client.cpp` | 3 | http_client, pool_reader, `_operators` |

### Global-state audit of the duplicated sources

Duplicating a pure function is waste. Duplicating *state* is a correctness risk.
Audited; the finding is **overwhelmingly waste, not wrong answers**:

- **`draken/core/vector_alloc.cpp` (7 copies)** — holds the shared identity
  permutation, zero-selection and zero-validity buffers that §11's Dense and
  Constant shapes point `selection` at.

  **The invariant to defend here is behavioural, not structural.** This layout
  exists because an earlier arrangement had **multiple allocators**, and reserving
  in one while freeing in another crashed. What must hold is that an allocation and
  its matching free bottom out in the **same allocator** — which is why draken uses
  the system allocator (`draken/core/alloc.h`) and does not link mimalloc, so all
  copies share one heap and a cross-extension free is safe.

  So the copy count is **not** the thing to optimise, and "collapse it to one" is
  not automatically an improvement — a consolidation that changed which allocator
  any path used would reintroduce the original crash even at one copy. Any change
  here must be argued in terms of allocate/free pairing, not tidiness.

  Supporting detail: contents are identical by construction, vectors do not own the
  buffers (`owns_selection = false`), and grow leaks the old buffer deliberately
  ("other threads may hold pointers"), so nothing dangles. No pointer-identity shape
  check was found that would break — not exhaustively proven.
- **`kernel_registry.cpp` (2 copies)** — `static std::map<std::string, kernel_fn_t>`.
  It **is** mutated at runtime: `kernel_registry_register` is reachable from Python
  as `register_kernel` (`_kernel_registry.pyx:386`) and is called by
  `opteryx/types/vectors/embedding_capability.py:130` (the EMBED kernel) and
  `opteryx/compiled/vector_ops/vector_dfa_extract.pyx:1191` (the DFA runner).

  Still *waste, not wrongness* — but for a narrower reason than "it is immutable":
  the only writer (`register_kernel`) and the only reader (`lookup_kernel`,
  `_kernel_registry.pyx:137`) are **both in `_kernel_registry.pyx`**, so a
  register/lookup pair always resolves to the same copy. On macOS both bind locally
  to `_kernel_registry.so`'s map; on Linux both are interposed to `draken_native`'s
  map by RTLD_GLOBAL. Different copy per platform, internally consistent on each.
  `draken_native` itself never calls `kernel_registry_lookup`.

  This is load-bearing and fragile: the invariant is "writer and reader live in one
  module", not anything the code states or enforces. A future C-side lookup from
  `draken_native` on macOS would read a map the Python registrations never reached.
  Consolidation removes the second map and the invariant with it.
- **`src/cpp/http_client.cpp` (3 copies)** — the one real defect:
  `std::atomic<uint64_t> g_http_retries{0}` is a split counter, so **HTTP retry
  telemetry undercounts**. Also `thread_local` rng and 12 func-local config caches.
- **SIMD dispatch caches** (`simd_search`, `simd_hash`, `simd_string_ops`,
  `decode_encodings`, base64 `detected`) and `decode_column.cpp`'s
  `thread_local decomp_buf` — *waste*.

So the case for consolidation rests on the **crash class** and on **code/memory
size**, not on silent corruption.

## 4. Target shape — four units

Cross-`.so` traffic *inside one process* is what generates these bugs. The wheel
split forbids a single `.so` (rugo must ship without opteryx), so:

1. **draken** — all of `draken/`: native, kernels, vectors, morsels, sort. Deletes
   the shim layer and the `RTLD_GLOBAL` load.
2. **rugo** — Parquet/CSV/JSONL engine. Depends on draken.
3. **opteryx engine** — `compiled/`, `operators/`, `expression/`,
   `connectors/parquet_io/`, `types/vectors/`. Collapses the duplicate Parquet
   decoder and the four memory pools.
4. **third-party leaf codecs** — or folded into 1 and 3; leaf C with no
   cross-coupling. Judgement call, not a constraint.

Rugo wheel ships 1+2; opteryx_core ships 1+2+3. Invariants:

- each source compiled **exactly once per wheel**;
- **one** C++ runtime per `.so`, consistently linked, never exported;
- cross-unit calls over the deliberate narrow **C ABI**, not ambient symbols.

## 5. The `cimport` work

182 cross-module `cimport` sites, split by what they actually cost:

| Category | Sites | Work |
|---|---|---|
| Pure C/C++ header decls — `draken.core.buffers` (56), `cxx_morsel` (12), `rugo.parquet_reader` (10), `frame_arena` (3), +3 | **84** | none |
| The three draken shims — `vectors.vector` (30), `morsels.morsel` (19), `bool_vector` (9) | **58** | deleted, not migrated — they exist only to bridge the split |
| Real opteryx module linkage | **39** across 20 targets | the actual job |

The 39 are concentrated in about six files:

- `opteryx/operators/_operators.pyx` — 10 sites (the hub)
- `opteryx/connectors/parquet_io/pool_reader.pyx` + `.pxd` — 8
- `footer_cache`, `column_deserializer`, `distinct`, `parvi_set` — most of the rest
- 12 targets with a single site each

Most-cimported targets are `memory_pool` (7) and `compiled_expression` (5) — both
land *inside* the same proposed unit as nearly all their consumers, so those
`cimport`s become intra-module and need no leaf-file or `cpdef` treatment.

Technique is established: the expression engine is already a consolidated package
(umbrella `__init__.pyx` + `include` of leaves). Known trap — a module-level
`cimport` in the umbrella breaks package init with a misleading
`PyInit___init__` error; put it in the included leaf, or promote the callee to
`cpdef`.

## 6. Sequencing

Metric: **multiply-compiled source count**. Baseline 64.

- **Step 0** — linker tourniquet. *Applied.* Needs Linux validation.
- **Step 1a** — `_kernel_registry` stops recompiling draken_native's sources.
  **Done: 64 → 33.** Its `.pyx` needs only the `extern "C"` surface in
  `kernel_registry.h`, already in `draken_native.so`, which `draken/__init__.py`
  loads under RTLD_GLOBAL before this module can be imported. Sources went 46 → 1
  with `_shim_bridge_link_args` for runtime resolution. Removed the whole duplicated
  kernel set, its vendored digest/codec/ryu/yyjson backing, and the second registry
  map. Verified: `.so` 1,283,656 → 130,936 bytes; locally-defined `draken_*` symbols
  247 → 1 (Cython's module marker); `kernel_alloc_*` / `kernel_registry_*` now
  undefined and resolved from `draken_native`; lookup and register/lookup roundtrip
  exercised; `make q` 217/217.
- **Step 1b (partial)** — the four `make_draken_extension` modules (`vectors.vector`,
  `vectors.bool_vector`, `morsels.morsel`, `morsels.sort`) stop compiling
  `simd_hash.cpp`, `simd_env.cpp` and `cpu_features.cpp`; they resolve them from
  `draken_native` at runtime, which already exports `simd_mix_hash`,
  `opteryx_check_simd_env_or_abort` et al. **Done: 87 → 75 duplicate compilations**
  (12 removed, 3 sources × 4 extensions). `make q` 217/217.

  Verified with the check this platform makes mandatory: these modules link with
  `-undefined dynamic_lookup` (macOS) / `--allow-shlib-undefined` (Linux), under
  which a missing symbol is **not a link error but a crash at first call**. So every
  project-prefixed undefined symbol (`_simd_*`, `_draken_*`, `_opteryx_*`,
  `_kernel_*`, `_rugo_*`, `_avx_*`, `_neon_*`) in each rebuilt `.so` was checked
  against `draken_native`'s exported set — all resolve. Re-run that check after any
  further source removal here; a green build proves nothing.

  **`simd_search.cpp` — removed as dead code, not deduplicated. 75 → 71.** Attempting
  to centralise it revealed nothing in draken references `simd_search_substring` at
  all: all four modules carried it as dead weight, and adding it to `draken_native`
  merely moved the waste (the linker dead-stripped it — `draken_native` exported no
  simd_search symbol). Removed from all five. Other units (opteryx strings /
  vector_ops, rugo) do use it and compile it themselves.

  **`simd_bitops.cpp` — BLOCKED on a real defect, not a build detail.** Adding it to
  `draken_native` fails with `ld: 1 duplicate symbols`, because
  `size_t simd_popcount(const uint8_t*, size_t)` is defined **twice with external
  linkage, in two different implementations**:

  | File | Implementation |
  |---|---|
  | `draken/core/bitmap_ops.cpp:69` | byte-at-a-time, `__builtin_popcount` per byte |
  | `src/cpp/simd_bitops.cpp:330` | 8 bytes at a time, `__builtin_popcountll` |

  Same answer, materially different speed. They have never collided only because
  they were never linked into one binary. Which one a module *executes* today
  depends on which `.cpp` its source list happens to contain — and on Linux, with
  RTLD_GLOBAL and `-fvisibility=default`, on which `.so` loaded first. A module that
  deliberately compiled the fast version can be silently running the slow one.

  **Deciding which implementation is canonical is a prerequisite** to moving this
  file, and unblocks 4 more copies. It is a performance decision about draken's
  bitmap path, not a build one. Recorded inline in both build sites.

  **Not done — the `.so` count is unchanged.** These four are still separate modules;
  only their duplicated content went. Collapsing them into `draken_native` outright is
  option 1: it needs the 58 `cimport` sites to keep resolving to modules exposing
  `__pyx_vtable__`, which a plain Python re-export does not provide.

  **The RTLD_GLOBAL load cannot be removed here.** Step 1a made
  `_kernel_registry` resolve its C ABI through it, so it is now load-bearing for
  two consumers, not one. That is a deliberate trade: Step 1a swapped *duplicated
  state* (a second registry map behind an unstated invariant) for *deeper reliance
  on the global-symbol mechanism*. The trade is sound only because Step 0 separates
  the two things RTLD_GLOBAL was doing — publishing draken's own deliberate C ABI
  (wanted) and publishing its private C++ runtime (the crash). `--exclude-libs,ALL`
  kills the second and leaves the first.

  RTLD_GLOBAL goes away only when there is nothing left outside `draken_native`
  to resolve — i.e. when the shims *and* `_kernel_registry` are all inside it.
  Until then it stays, and `draken/__init__.py`'s load-order guarantee stays
  load-bearing.
- **Step 2** — merge `pool_reader` into the opteryx unit; drops the duplicated
  `rugo/src/parquet/*` (7 sources × 2) and collapses `memory_pool` (4 copies) and
  `http_client` (3 copies, the split retry counter).
- **Step 3** — fold the remaining `opteryx/compiled/structures/*` leaves in; the 39
  `cimport` sites resolve here.
- **Step 4** — deduplicate the leaf third-party codecs (`cpu_features` ×11,
  `nb_combined` ×8, `yyjson` ×6, re2 ×2).

### Build staleness — read before doing any step

Every step here is a **source-list edit**, which changes no source file. setuptools
compares each listed source's mtime against the `.so`, so it skips the extension
**silently** — the build log will not even name it — and leaves the previous `.so`
in place. `make c` reports success. Removing only the in-place `.so` is not enough:
it is restored from `build/lib...`. Three caches must go:

```
rm -f  <inplace>.so
rm -f  build/lib.*/<pkg path>/<mod>.*.so
rm -rf build/temp.*/<per_extension_dir>/
```

**Verify by symbol content, never by exit code.** A symbol that should now resolve
from another `.so` must appear in `nm -u`, not as `(__TEXT,__text) external`:

```
nm -g --defined-only <mod>.so | grep -c draken_
```

This cost two false "done" reports on Step 1a before it was caught.

## 7. Open questions

- Is there any genuine **cross-`.so` throw/catch** path today? If yes, Step 0 does
  not cover it and Step 1 becomes urgent rather than merely valuable.
- Does the `prod_sigsegv_github_events_hashed_agg` P0 dump show `__cxa_throw` +
  `gsignal` rather than a faulting instruction? If so it is this mechanism wearing
  a different label, not a separate bug.
- Should `-fvisibility=hidden` become the default, with explicit
  `__attribute__((visibility("default")))` on the bridge symbols? Stricter than
  `--exclude-libs,ALL` and would also stop non-runtime symbol collisions — but it is
  a larger change and needs the bridge surface enumerated first.
