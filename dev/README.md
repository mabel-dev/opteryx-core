# Development Tools

This directory contains scripts and tools used for Opteryx Core development, build support, vendoring, data generation, performance analysis, and release preparation. These are developer tools, not runtime modules imported by the engine, and nothing here is packaged in a wheel.

Run commands from the repository root unless a script says otherwise. Most day-to-day development does not need these scripts directly: prefer the top-level `Makefile` (`make compile`, `make q`, `make slt`, `make check`).

## Run for you

These are wired into the commit and release process. You do not normally run them by hand.

| Script | Run by | Purpose |
|--------|--------|---------|
| `build_counter.py` | pre-commit hook | Bumps the build number in `opteryx/__version__.py` and `pyproject.toml` on every commit |
| `generate_reference.py` | pre-commit hook, `make reference` | Regenerates the `reference/` catalogs. The output is committed; edit the generator, never the output |
| `build-wheels.sh`, `build-wheels-macos.sh` | release workflow | Builds the `opteryx_core` wheel |
| `build-rugo-wheels.sh`, `build-rugo-wheels-macos.sh` | `release-rugo` workflow | Builds the standalone `rugo` wheel |
| `verify-wheel-imports.sh` | release workflows | Checks a built wheel imports cleanly |
| `check_undefined_symbols.py` | `Makefile` | Pre-wheel-build check that the compiled extensions have no undefined symbols |

Wheels are built in CI, never locally. macOS releases are arm64-only; Intel/x86_64 macOS wheels are not built or released.

## Build and source hygiene

| Script | Purpose |
|--------|---------|
| `clean_cython_generated.py` | Delete Cython-generated C/C++ sources so the next build re-runs Cython |
| `fix_cython_whitespace.py` | Fix whitespace issues in Cython files |
| `audit_pxx_imports.py` | Audit Python imports inside `cdef`/`cpdef` bodies in `.pyx`/`.pxi` files |
| `check_string_arena_leaks.py` | Leak gate for the string-arena ownership channel |
| `check_vendored_dependencies.py` | Check the vendored headers used by the native bindings are present |
| `count_loc_basic.py` | Basic LOC counter |

## Vendoring

The vendoring scripts are intentionally explicit. Review their output and resulting diffs before committing. New third-party code is vendored only after agreement.

```bash
python dev/vendor_mimalloc.py
python dev/vendor_nanobind.py
python dev/vendor_usearch.py
python dev/vendor_boost_math.py
```

## Test data and fixtures

| Script | Purpose |
|--------|---------|
| `generate_*.py` | Generate test, fuzz, and benchmark datasets and fixtures: arrays, Avro, band-join, IPv4, row-group layouts, signals, unnest |
| `ssb_generate.py`, `tpcds/`, `tpch/` | Star Schema Benchmark, TPC-DS and TPC-H data and query generators; `tpch/generate_slt_golden.py` writes the TPC-H sqllogictest goldens |
| `rewrite_parquet_layout.py` | Rewrite a directory of Parquet files with rugo's writer at a chosen layout |
| `parquet_to_skene.py` | Convert a Parquet dataset tree to its `.skene` mirror |
| `vector_fixture.py` | Build the real-embedding fixture used for vector-index measurements |
| `bench_data/` | Generated (git-ignored) inputs for the remote-IO benchmarks |

## Benchmarks and profiling

| Script | Purpose |
|--------|---------|
| `ab_bench.py`, `arch_compare.py` | Interleaved A/B benchmark harness, and a side-by-side view of its results from several machines |
| `arch_tree.sh`, `build_so_variant.sh` | Copy the sources into an isolated benchmark tree (locally or on a remote box), and build one extension of it with extra `-D` defines as an A/B arm. Never benchmark in the shared working tree |
| `groupby_baseline.py`, `grouped_layout_*_bench.py`, `grouped_layout_get_count.py` | GROUP BY baseline and Parquet-layout comparison benchmarks |
| `suite_time_split.py`, `tpch_profile.py` | Per-query time split from the native trace, and a per-operator TPC-H profiler |
| `bench_*.py` | Targeted micro-benchmarks: Avro readers and writers, `IS NULL`, remote IO, UNNEST |
| `*_bench*.cpp`, `codec_*.cpp` | Standalone C++ benchmarks: hash mixing and compression codecs |
| `throttle_server.py` | Throttling HTTP file server for remote-storage IO benchmarks |
| `estimate_function_costs.py`, `sweep_function_costs.py`, `compare_function_costs.py`, `analyze_function_costs.py`, `import_function_costs.py`, `function_costs.json` | Measure, compare and import per-row function kernel costs |
| `io_waterfall/`, `instrument_engine.py` | I/O waterfall and native-trace tooling |
| `c1_skew_probe.py`, `native_residual_census.py` | Probes referenced from the design documents in `docs/` |

## Secrets

Local secrets live in the Vaultwarden at `https://vault.internal:8443`, not in the repo-root `.env`. `secret_store.py` reads them through the Bitwarden CLI. Vaultwarden is LAN-only, so CI cannot reach it; CI keeps passing its own secrets as environment variables.

```bash
python dev/secret_store.py list                               # item names, never values
python dev/secret_store.py run GITPAT -- <command>            # <command> runs with GITPAT in its environment
```

From Python, put `dev/` on `sys.path` and call `get_secret("NAME")`; it raises `KeyError` if no item has that name. The AIStor and OData perf runners (`tests/performance/_common.py`, `clickbench/duckdb/runner.py`, `clickbench/odata/runner.py`) already do.

Each secret is a Vaultwarden item named exactly like its environment variable, with the value in the login's password field (or the body of a secure note).

| Where | What |
|-------|------|
| Vaultwarden | `AISTOR_ACCESS_KEY_ID`, `AISTOR_SECRET_ACCESS_KEY`, `CLIENT_ID`, `CLIENT_SECRET`, `CLIENT_ID_2`, `CLIENT_SECRET_2`, `CLIENT_ID_3`, `CLIENT_SECRET_3`, `OPTERYX_CONNECTION_STRING`, `FIREHOSE_TAP_TOKEN`, `IMPROVMX_API_KEY`, `GITPAT`, `STATUS_IO`, `COCKROACH`, `YUGABYTE` |
| `.env` | `SECRET_MANAGER_USERNAME` / `SECRET_MANAGER_PASSWORD` (the Vaultwarden login), `DATA_CATALOG_CONNECTION` and `VALKEY_CONNECTION` (read as environment variables by the engine and by tests that also run in CI), `GOOGLE_APPLICATION_CREDENTIALS` (a path to a key file), and non-secret config |

Setting up a new machine:

1. `brew install bitwarden-cli`
2. Pin the server's self-signed certificate:
   `mkdir -p ~/.config/opteryx-secrets && openssl s_client -connect vault.internal:8443 </dev/null 2>/dev/null | openssl x509 > ~/.config/opteryx-secrets/vault.internal.pem`
3. Put the Vaultwarden account in `.env`: `SECRET_MANAGER_USERNAME` must be the account's **email address**, plus `SECRET_MANAGER_PASSWORD`.
4. `python dev/secret_store.py list` should print the item names.

The helper keeps its bw login in `~/.config/opteryx-secrets/bw`, apart from any personal `bw` login, and unlocks once per process.

## Other tools

| Path | Purpose |
|------|---------|
| `worker_executor.py` | Local stand-in for the deployed worker: run one statement the way it does |
| `temporal_arith_duckdb_harness.py` | DuckDB-comparison harness for native temporal arithmetic |
| `vector_index_recall.py` | Recall of the IVF vector index against exact search |
| `kafka_avro/` | Docker-compose Kafka and Avro fixtures |
| `odata_benchmark/`, `parity/` | OData snapshot fetcher and native parity checks |
| `clickbench/` | ClickBench submission entry, golden-result and data-conversion helpers |
| `rugo_example.py` | rugo reading and writing examples |
