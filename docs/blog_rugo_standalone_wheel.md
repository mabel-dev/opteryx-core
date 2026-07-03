# One Tree, Two Wheels: Splitting Rugo Out as a Standalone Package

Opteryx's file engine — Parquet, CSV, and JSONL, read and write, no PyArrow, no NumPy — has always lived inside the `opteryx_core` wheel as `rugo` and `draken`. If you wanted a fast, dependency-free Parquet reader and didn't care about SQL, you still had to install the whole query engine to get it.

We split it out. `rugo` is now its own PyPI package: `pip install rugo` gets you draken + rugo and nothing else — no SQL planner, no optimizer, no Rust compute extension. `opteryx_core` is untouched; it still bundles the same rugo intrinsically. Same source tree, two wheels.

---

## The Constraint: One Build System

The easy way to ship a standalone package is to fork the source or maintain a second tree. We ruled that out immediately — two trees means two places for draken and rugo to drift apart, and the project's build story (`make compile`, one `setup.py`, no parallel tooling) is a hard rule, not a preference.

So the design constraint was: **one repo, one set of extension definitions, two `setup.py` invocations.**

```
opteryx-core/
├── build_common.py       # shared build machinery + single-source draken/rugo ext defs
├── setup.py               # opteryx_core wheel — imports build_common, adds 52 opteryx exts
├── rugo/
│   └── setup.py           # rugo wheel — imports build_common, draken+rugo exts ONLY
```

`build_common.py` holds everything that both builds need to agree on: the `build_ext` subclass, architecture detection, compiler/link flags, vendored zstd/lz4/parquet sources, and — critically — `draken_rugo_extensions()`, a function that returns the exact same 6 draken + 6 rugo `Extension` objects to both wheels. Neither `setup.py` defines its own copy. If a source file moves or a flag changes, both wheels see it or neither does.

Getting here meant deleting ~728 lines of now-duplicated inline extension logic from the root `setup.py` and proving, via a `setup()`/`cythonize`-intercept harness, that `opteryx_core` declares the *identical* 64-extension set before and after the refactor. "Untouched" had to be provable, not asserted.

---

## Re-Rooting a Second `setup.py`

`rugo/setup.py` lives in `rugo/` for discoverability, but it can't build from there — `build_common`'s source paths are relative to the repo root, and draken is a sibling package one level up. So the first thing it does is re-root:

```python
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
```

That single line surfaced two bugs that wouldn't show up any other way.

**Bug 1: `rugo/` shadows the standard library.** Running `python rugo/setup.py` puts `rugo/`'s own directory on `sys.path[0]`. The wheel toolchain's `import csv` then resolves to `rugo/csv` instead of the stdlib module, which tries to load `rugo.csv._csv_reader` before `draken_native` is even built — undefined symbol, hard crash. Fix: strip the script's own directory from `sys.path` before importing anything.

**Bug 2: the root `pyproject.toml` hijacks the build.** Building from the repo root means setuptools also sees the repo-root `pyproject.toml`, whose `[project]` table describes `opteryx_core` — wrong name, wrong version, and auto-discovery that pulls in opteryx's packages instead of rugo's. Fixed by neutralizing `setuptools.config.pyprojecttoml.apply_configuration` for this build only:

```python
_pyprojecttoml.apply_configuration = (
    lambda dist, filepath, ignore_option_errors=False: dist
)
```

This only affects the process running `rugo/setup.py`. `opteryx_core`'s own build is a separate invocation and never sees this monkeypatch.

**Bug 3: shared build directories leak.** `bdist_wheel` archives whatever's under the build base, and the repo's default `build/lib` is already full of opteryx `.so` files from `make c`. Without isolation, the rugo wheel would silently pick up opteryx binaries. Fixed with a dedicated build base: `options={"build": {"build_base": "build/_rugo_wheel"}}`.

None of these are exotic — they're the standard failure modes of building two distributions from one tree. But each one would have shipped a broken or bloated wheel silently if it hadn't been caught.

---

## A Real Design Decision, Not Just Plumbing: No Remote I/O

The standalone wheel build failed at compile time, not link time, and the reason mattered: `rugo/src/parquet/filesystem.hpp` unconditionally included `http_client.hpp`, which pulls in `<curl/curl.h>`. Rugo's standalone build never installs `curl-devel` and never compiles `http_client.cpp` — that wiring is opteryx-only.

We could have vendored curl into the rugo wheel to make it compile. We didn't. The architect's call: **standalone rugo is local-filesystem only.** No `gs://`, no `http(s)://`. If you need remote reads, you need `opteryx_core`, where GCS scanning is a first-class feature backed by real retry and batching logic — not something a lightweight file-reader wheel should carry as dead weight for users who'll never touch it.

The gate is a compile-time macro, not a runtime check:

```cpp
#ifdef RUGO_ENABLE_HTTP
    // HttpClient, gcs/http branches
#endif

#ifndef RUGO_ENABLE_HTTP
    // reject_remote_path() — throws, does not silently no-op
#endif
```

`opteryx_core` defines `RUGO_ENABLE_HTTP=1` on both `rugo.parquet_reader` and its own `parquet_io.pool_reader` extension, so its behavior is unchanged. `rugo/setup.py` never defines it, so a remote path fails loud at the call site — not a silent empty read, not a fallback to nothing. That's the fail-fast principle applied to a packaging decision, not just a code path.

---

## Why This Didn't Bloat the Wheel

Rugo's native surface used to be several separate `.so` files — `parquet_reader`, `parquet_writer`, `csv._csv_reader`, `csv._csv_writer`, `jsonl._jsonl_reader`, `jsonl._jsonl_writer` — each linking its own copy of the nanobind runtime and shared vendored sources. That's the same wheel-bloat pattern we'd already fixed for draken's 21 `vector_*` extensions: one `NB_MODULE` per file means duplicated runtime in every `.so`.

Same fix, applied to rugo before the standalone wheel shipped: each module's `NB_MODULE(x, m)` became a `register_x(nb::module_&)` function, and one dispatcher owns the actual `NB_MODULE` entry point. Rugo now builds as a single `rugo_native.so`. This wasn't optional cleanup — publishing a second wheel with the old per-extension duplication would have shipped the bloat twice.

---

## What's Still True

`opteryx` never depends on the published `rugo` wheel. It has its own copy of the same source, compiled as part of the same `opteryx_core` build. The two wheels share source, not a runtime dependency — installing both in one environment is a nonsensical combination (opteryx_core already contains rugo) and isn't something we guard against, but it isn't something the design relies on either.

The strict-libc++ include sweep that came out of the macOS CI matrix — 23 draken files missing an explicit `#include <new>` that libc++ used to pull in transitively — is a good illustration of what a second, differently-configured build buys you even when you're not trying to catch bugs: it's free coverage of assumptions the primary build never tested.

`make compile` and `make q` are unaffected. `pip install rugo` gets you a Parquet/CSV/JSONL engine with zero heavy dependencies, built from the exact same code opteryx_core ships — because it's not a fork, a rewrite, or a subset maintained by hand. It's the same tree, built twice, on purpose.
