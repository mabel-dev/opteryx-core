# RUGO-1 — Rugo to WebAssembly: spike results

**Verdict: practical. Route: Emscripten build of rugo's C++ core, thin JS API.**
The PoC reads a Parquet footer in the browser via HTTP `Range` (and `Blob.slice()` in
Node), using rugo's own footer parser (`rugo/src/parquet/metadata.cpp`) **compiled
unchanged**. On the 1.37 GB test file it does 2 reads (2.35 MB, 0.17 % of the file)
and returns schema plus row-group metadata in 28–31 ms (Node) or ~110 ms (browser,
including HTTP). The bundle is 73 KB gzipped.

## 1. Rugo's layers (confirmed)

Rugo is native code behind a Python wrapper, as assumed.

| Layer | Where | Wasm relevance |
|---|---|---|
| Native core (C++) | `rugo/src/parquet/*.cpp/.hpp` (footer, page decode, encodings, codecs, bloom, page index, writer), `rugo/src/{csv,jsonl,avro,orc}` | **What we compile.** The footer parser is self-contained: `metadata.cpp` + `thrift.hpp` + header-only `ankerl::unordered_dense`. |
| Draken (C++) | `draken/core`, `draken/ops` | The decode path's output type. `alloc.h` is **not wasm-portable** today (see §5). |
| Cython bindings | `rugo/rugo_native.pyx` → `src/parquet/parquet_reader.pxi` etc. | Replaced by a ~130-line C ABI shim (`rugo_wasm.cpp`). Holds one piece of logic the browser still needs: `decode_value` (stat min/max bytes → values). |
| Python API + CLI | `rugo/parquet.py`, `rugo/cli/commands.py`, `_render.py` | Mostly orchestration and rendering. Moves to JS. |

**Native dependencies (all vendored in `third_party/`):**
- zstd (decompression subset)
- snappy
- lz4 (block)
- libdeflate (gzip)
- ankerl (hash map)
- bshoshany / own `thread_pool.hpp` (threads)

No system libraries.

## 2. Route comparison

| | Emscripten core + JS API (**chosen**) | Pyodide wheel |
|---|---|---|
| Download | **73 KB** gzipped footer-only; **114 KB** with all page codecs | ~10 MB Pyodide runtime, plus rugo/draken built *for* Pyodide's Emscripten |
| Native work | The same C++ we already ship; a C shim per verb | All of `rugo_native` + `draken_native` must build under Pyodide's toolchain, including the parts that don't port (threads, the chunk cache). Pyodide doesn't make those go away. |
| Reuse | Verbs re-exposed in JS (the CLI verbs are thin: see §6) | Reuses `cli/commands.py`, but those functions open paths with `open()`/`f.read()` and would need rewriting for `File`/Range anyway |
| Byte I/O | JS owns it (`Blob.slice`, `fetch` + `Range`); the module only sees the bytes it needs | Same rewrite needed: Python's `open()` doesn't map to a lazy `File` |
| Fit with §1 | C++ engine that runs without Python, which is rugo's stated design | Puts a Python interpreter in front of a C++ engine that doesn't need one |

The Pyodide route's main selling point, reusing the Python layer, mostly disappears in
practice: the hard parts (lazy byte I/O, threads) have to be redone either way, and the
Python layer is mostly rendering.

## 3. Proof of concept (`rugo/wasm/`)

`rugo/wasm/` has no `__init__`, so neither wheel ships it. It is its own
distribution, hosted on rugo.dev (see Publishing below).

| File | Purpose |
|---|---|
| `rugo_wasm.cpp` | C ABI: `rugo_footer_length(trailer8)`, `rugo_parse_footer(buf, n)` → JSON (schema + row groups + per-chunk codec/encodings/sizes/null_count/stats presence) |
| `rugo_footer.mjs` | JS API: `blobSource(File)`, `httpSource(url)`, `readFooter(src)`. Reads the last 64 KB speculatively (an HTTP suffix range, which also gives the file size), then fetches only the missing footer prefix if the footer is bigger. |
| `build.sh` | Emscripten build (`dist/` = PoC, `build/` = codec size probe) |
| `bench_node.mjs` | Time-to-schema / bytes read / memory, in `Blob.slice` or local-Range-server mode |
| `serve.mjs`, `harness.html` | Browser check: Range-only server (**refuses non-Range requests with 416**) plus a bare test page. No UI (that's RUGO-2). |
| `crosscheck.py` | Compares wasm JSON against native rugo on the same file |
| `rugo_codecs_probe.cpp`, `test_codecs.mjs` | Codec size probe + gzip/zstd round-trip test |
| `test_footer.mjs` | CI gate: committed file through `Blob.slice` and HTTP Range, checked against native rugo's figures; refuses non-parquet input |

Build flags: `-O3 -fwasm-exceptions` (rugo throws on malformed input). Single-threaded,
no pthreads, so no COOP/COEP. `ALLOW_MEMORY_GROWTH`, `MAXIMUM_MEMORY=4GB`, `FILESYSTEM=0`.

```
rugo/wasm/build.sh
node rugo/wasm/test_footer.mjs
node rugo/wasm/test_codecs.mjs
node rugo/wasm/bench_node.mjs <file.parquet> [--http]
```

### Publishing

`.github/workflows/release-rugo-wasm.yaml` is triggered by the same tags as the rugo
wheels (`rugo-*`, `all-*`), but it is a separate workflow, so neither release can block
the other. It runs `build.sh` and both tests on emsdk 6.0.12, then commits the bundle
to `mabel-dev/rugo.dev` under `docs/wasm/` using the existing `RUGO_DEV_RELEASE_TOKEN`.
`workflow_dispatch` runs build and test only, without deploying.

```
https://rugo.dev/wasm/rugo_footer.mjs   JS API
https://rugo.dev/wasm/dist/rugo.{mjs,wasm}
https://rugo.dev/wasm/VERSION
```

`docs/wasm/` is replaced on each release, so the URLs are not versioned.

## 4. Measurements (Apple Silicon, emcc 6.0.12, Node 26.13, desktop-app Chromium)

**Bundle size**

| Build | raw | gzip -9 | brotli 11 |
|---|---|---|---|
| `rugo.wasm` (footer reader) | 195,748 | 73,237 | 59,517 |
| `rugo.mjs` (JS glue) | 11,467 | 4,014 | — |
| `rugo_codecs.wasm` (+ snappy, zstd, lz4, gzip) | 285,317 | 114,156 | 93,279 |

**Time to schema.** There was no 1.0 GB file locally; the nearest is 1.37 GB (ClickBench
part: 14,155,776 rows, 105 columns, 216 row groups, 2.29 MB footer).

| File | Path | Reads | Bytes read | Time to schema | Wasm init |
|---|---|---|---|---|---|
| 1.37 GB | Node `Blob.slice` | 2 | 2,352,081 | 28.1 / 28.2 / 30.9 ms | 1–3 ms |
| 1.37 GB | Node HTTP Range | 2 | 2,352,081 | 48.6–50.6 ms | 1.4 ms |
| 1.37 GB | **Browser HTTP Range** | 2 | 2,352,081 | **111.7 ms** | 7.3 ms |
| 14.8 GB (100 M rows) | **Browser HTTP Range** | 2 | 2,439,324 | **101.3 ms** | — |
| 14.8 GB | Node HTTP Range | 2 | 2,439,324 (served, server-side count) | 48.6 ms | — |
| 0.1 MB (5 k rows) | Node `Blob.slice` | 1 | 65,536 | 2.3 ms | — |

The 14.8 GB rows show that offsets past 4 GB work. Wasm32's 4 GB limit applies to the
heap, not to file size, because offsets live in JS.

**Peak memory.** Wasm linear memory only grows, so its final size is the high-water
mark: **36.5 MB** for the 1.37 GB file and 36.8 MB for the 14.8 GB file. That is about
16× the 2.3 MB footer: `FileStats` holds `std::string`s and a hash map for every one of
22,680 column chunks, plus the JSON copy. I did not break it down further. Whole-process
RSS (Node runtime included) was 96 MB in `Blob.slice` mode. I didn't measure per-tab
browser memory.

**Correctness.** `crosscheck.py` compared the wasm output with native rugo on the
1.37 GB file. All 22,680 column chunks match on name, physical type, logical type and
null_count, and num_rows and row-group count match too. A non-parquet input gets an
explicit error.

## 5. Codec coverage

| Codec | Native rugo | Wasm | Verified in wasm |
|---|---|---|---|
| UNCOMPRESSED | ✅ | ✅ | n/a |
| SNAPPY | ✅ | ✅ compiles + links | ❌ not exercised (no Node compressor) |
| GZIP (libdeflate) | ✅ | ✅ | ✅ round-trip + wrong-size refusal |
| ZSTD | ✅ | ✅ | ✅ round-trip + wrong-size refusal |
| LZ4_RAW | ✅ | ✅ compiles + links | ❌ not exercised |
| LZ4 (legacy Hadoop) | ❌ refused | ❌ refused | — |
| BROTLI | ❌ **refused, no decoder vendored** | ❌ refused (checked) | — |

Brotli support would mean vendoring a brotli decoder. That needs architect agreement
under §4, and it is a gap in rugo itself, not something wasm introduces.

**Fixed in Draken:** `draken/core/alloc.h` used to include `<execinfo.h>`
unconditionally, for the backtrace in the opt-in allocation trace. Emscripten doesn't
have it, which blocked every wasm build that touches Draken headers (the codecs via
`append_buffer.h`, and all of the decode path). The include is now guarded with
`__EMSCRIPTEN__`. On wasm the trace prints `(no backtrace on wasm)` instead of a stack.
Native builds are unchanged, and the codec probe now builds against the real header.

## 6. MVP verbs: port status

| Verb | Status | Why |
|---|---|---|
| `info` | **Clean** | Rows/columns from the footer; size is `File.size` / `Content-Range`. Parquet only: CSV/JSONL `info` needs a whole-file or sampled scan and is a separate question. |
| `schema` (+ `columns`, `count`) | **Done in PoC** | The CLI's type string is `logical_type or physical_type`, already in the JSON. |
| `describe` | **Needs work (small)** | Everything comes from the footer. Missing piece: min/max are raw bytes, and decoding them (`decode_value`) lives in Cython (`parquet_reader.pxi`), so it needs a C++ (or JS) home. Note: the native CLI's `describe`/`inspect` **read the whole file into memory** (`f.read()`) just to parse the footer. That's a separate issue, not fixed here. |
| `preview` | **Needs work (real)** | Page decode (`decode_column.cpp`) pulls in `thread_pool.hpp` (std::thread), the C7 chunk cache (`draken_native`) and the NEON/AVX2-guarded bit-unpack (a scalar path exists, but there's no wasm SIMD128 path). Output is Draken vectors, so rendering needs `_text_render.hpp`, which pulls in `_writer_pool.hpp` / `<future>`. Work: a single-threaded build config for the decode path (the Draken `execinfo.h` blocker is fixed). I/O is straightforward: footer offsets give exact column-chunk ranges, one Range read per chunk. |
| `validate` | **Needs definition** | No `validate` verb exists in `rugo/cli` today, so there's nothing to port. Needs a spec first. |

## Constraints check
- No threads, no COOP/COEP: ✅ (built without pthreads)
- 4 GB wasm32 ceiling: ✅ for the footer path (36 MB). For `preview`, decode one row group × the selected columns at a time, never the file.
- Only footer bytes read: ✅ `Blob.slice` / Range only. The browser test server refuses non-Range requests (416).

## Not done / open
- 1 GB timing is on a 1.37 GB file; there was no exact 1 GB file locally.
- `Blob.slice` past 4 GB isn't measured. Node's `fs.openAsBlob` wraps the size at 2³² (it reports 1,895,074,558 for a 14,779,976,446-byte file), so the Node harness can't open it. That's a Node bug; the HTTP path covers >4 GB offsets. In-browser `File.slice()` needs a manual file pick (harness `<input>`), which I didn't automate.
- Snappy and LZ4_RAW are linked but not exercised in wasm.
- Per-tab browser memory not measured.
