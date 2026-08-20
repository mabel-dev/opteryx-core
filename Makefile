# Modernized Makefile for Opteryx
# Use bash shell for consistency across environments
SHELL := /bin/bash

# Variables
# By default we don't force Python to run with the GIL disabled. Some Python
# builds don't support disabling the GIL and will abort at startup when the
# environment requests it (fatal error: config_read_gil). To enable running
# without the GIL you can override this like:
#
#   make PYTHON_GIL=0 <target>
#
PYTHON_GIL ?=
# Prefer python3.14 by default for consistent ABI and compiled artifacts.
# Users may override by passing PYTHON='python3.x' on the make commandline.
PYTHON ?= $(if $(PYTHON_GIL),PYTHON_GIL=$(PYTHON_GIL)) python3.14
UV := $(PYTHON) -m uv
PIP := $(UV) pip
PYTEST := $(PYTHON) -m pytest
COVERAGE := $(PYTHON) -m coverage

# Allocator preload for the benchmark targets (clickbench / tpch / b). The engine
# makes large per-query allocations that fragment the system allocator, so RSS
# climbs across queries (and OOMs on Linux prod). Preload a fragmentation-aware
# allocator so local/CI benchmarks match production behaviour. Platform-split:
#   Linux → vendored mimalloc via LD_PRELOAD (draken.preload_library_path()).
#   macOS → jemalloc via DYLD_INSERT_LIBRARIES — mimalloc SIGTRAPs on macOS 3.14t
#           because it clashes with the interpreter's own bundled mimalloc; jemalloc
#           does not (see mimalloc_preload_mac_crash memory). `brew install jemalloc`.
# Empty (allocator not found) => no preload; the target still runs.
ifeq ($(shell uname),Darwin)
  _BENCH_JE := $(firstword $(wildcard /opt/homebrew/lib/libjemalloc.dylib /usr/local/lib/libjemalloc.dylib))
  BENCH_PRELOAD := $(if $(_BENCH_JE),DYLD_INSERT_LIBRARIES=$(_BENCH_JE),)
else
  BENCH_PRELOAD = LD_PRELOAD=$(shell $(PYTHON) -c 'import draken; print(draken.preload_library_path() or "")' 2>/dev/null) MIMALLOC_PURGE_DELAY=100
endif
MYPY := $(PYTHON) -m mypy

# Parallel job count for compilation
JOBS := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Directories
SRC_DIR := opteryx
TEST_DIR := tests
BUILD_DIR := build
DIST_DIR := dist

# Colors for output (using echo -e for proper ANSI handling)
define print_green
	@echo -e "\033[0;32m$(1)\033[0m"
endef

define print_blue
	@echo -e "\033[0;34m$(1)\033[0m"
endef

define print_yellow
	@echo -e "\033[1;33m$(1)\033[0m"
endef

define print_red
	@echo -e "\033[0;31m$(1)\033[0m"
endef

.PHONY: help lint format check test test-battery coverage mypy compile compile-quick draken clean distclean update dev-install all check-python dt rt st et q rugo-floor reference function-costs

# Default target
.DEFAULT_GOAL := help

# === REFERENCE CATALOGS ===

reference: check-python ## Regenerate all reference catalogs (JSON + catalog Python files)
	$(call print_blue,"Regenerating reference catalogs...")
	@$(PYTHON) dev/generate_reference.py

# Calibration target (like clickbench-duckdb / tpch-bench-duckdb): re-runs a
# benchmark to regenerate stored numbers. Deliberately OUTSIDE `reference` and the
# pre-commit hook — it is slow and machine-dependent. It measures each function
# kernel's marginal per-row cost through the real bytecode evaluator, writes the
# results into the registrars, recompiles so the catalog carries them, then
# regenerates `reference` (function_signatures.json embeds cost_us_per_million).
# Override the sweep with COST_ARGS, e.g. COST_ARGS="--budget 0.3 --reps 5".
function-costs: check-python ## Re-measure function execution costs and bake them into the catalog + reference (slow)
	$(call print_blue,Sweeping function execution costs (subprocess-isolated; a few minutes)...)
	@$(PYTHON) dev/sweep_function_costs.py --output dev/function_costs.json $(COST_ARGS)
	$(call print_blue,Writing measured costs into the registrars...)
	@$(PYTHON) dev/import_function_costs.py dev/function_costs.json --apply
	$(call print_blue,Recompiling so the catalog carries the refreshed costs...)

# === LINTING AND FORMATTING ===

# Enforce Python 3.14 for CI and developer tools. This will abort early if the configured
# python interpreter is not 3.14; set PYTHON to override or install 3.13 via your environment.
check-python:
	@ver=`$(PYTHON) -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')" 2>/dev/null`; \
	if [ "$$ver" != "3.14" ]; then \
		echo "\nERROR: Python 3.14 is required for builds in this repository; found $$ver\n" >&2; \
		echo "Set your local Python to 3.14 (pyenv local 3.14.5) or override with: make PYTHON=python3.14 <target>" >&2; \
		exit 1; \
	fi

help: ## Show this help message
	$(call print_green,"Opteryx Development Makefile")
	$(call print_blue,"Available targets:")
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[1;33m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# === LINTING AND FORMATTING ===

lint: check-python ## Run all linting tools
	$(call print_blue,"Installing linting tools...")
	@$(PIP) install --quiet --upgrade pycln isort ruff yamllint cython-lint
	$(call print_blue,"Removing whitespace in pyx files...")
	@$(PYTHON) dev/fix_cython_whitespace.py
	$(call print_blue,"Running Cython lint...")
	@cython-lint $(SRC_DIR)/compiled/**/*.pyx || true
	$(call print_blue,"Running Ruff checks...")
	@$(PYTHON) -m ruff check --fix --exit-zero
	$(call print_blue,"Cleaning unused imports...")
	@$(PYTHON) -m pycln . --extend-exclude 'third_party/usearch/fp16'
	$(call print_blue,"Sorting imports...")
	@$(PYTHON) -m isort .
	$(call print_blue,"Formatting code...")
	@$(PYTHON) -m ruff format $(SRC_DIR)
	$(call print_green,"Linting complete!")

format: ## Format code only
	$(call print_blue,"Formatting code...")
	@$(PYTHON) -m ruff format $(SRC_DIR)
	@$(PYTHON) -m isort .

check: ## Check code without fixing
	$(call print_blue,"Checking code style...")
	@$(PYTHON) -m ruff check
	@$(PYTHON) -m isort --check-only .

# === DEPENDENCIES ===

dev-install: ## Install development dependencies
	$(call print_blue,"Installing development dependencies...")
	@$(PIP) install --upgrade pip uv
	@$(PIP) install --upgrade -r tests/requirements.txt

update: ## Update all dependencies
	$(call print_blue,"Updating dependencies...")
	@$(PYTHON) -m pip install --upgrade pip uv
	@$(UV) pip install --upgrade -r tests/requirements.txt
	@$(UV) pip install --upgrade -r pyproject.toml

# === TESTING ===

test: ## Run full test suite with compiled extensions
	@$(PIP) install --upgrade pytest pytest-xdist
	@clear || true
	@MANUAL_TEST=1 VALIDATE_OPTIMIZER_PLANS=1 $(PYTEST) -n auto --color=yes



q:
	@clear || true
	@VALIDATE_OPTIMIZER_PLANS=1 $(PYTHON) tests/integration/sql_battery/test_shapes_basic.py

rugo-floor: ## Run the rugo release floor (oracle + notebook actions + cli) — gates the rugo wheel
	$(call print_blue,"Running rugo release floor: parquet oracle conformance...")
	@$(PYTEST) tests/rugo/test_oracle_conformance.py -q
	$(call print_blue,"Running rugo release floor: csv oracle conformance...")
	@$(PYTEST) tests/rugo/test_csv_oracle_conformance.py -q
	$(call print_blue,"Running rugo release floor: jsonl oracle conformance...")
	@$(PYTEST) tests/rugo/test_jsonl_oracle_conformance.py -q
	$(call print_blue,"Running rugo release floor: notebook actions...")
	@$(PYTEST) tests/rugo/test_notebook_actions.py -q
	$(call print_blue,"Running rugo release floor: cli...")
	@$(PYTEST) tests/rugo/test_cli.py -q

dt: ## Run draken unit tests
	$(call print_blue,"Running draken unit tests...")
	@clear || true
	@$(PYTEST) draken/tests/ -v --tb=short

rt: ## Run rugo unit tests
	$(call print_blue,"Running rugo unit tests...")
	@clear || true
	@$(PYTEST) tests/rugo/ -v --tb=short

st: ## Run skene regression suite (native C++ suite + Python binding/scan/dispatch tests)
	$(call print_blue,"Running skene native test suite...")
	@clear || true
	@$(MAKE) -C skene test
	$(call print_blue,"Running skene Python binding tests...")
	@$(PYTEST) tests/unit/skene/ tests/unit/operators/test_skene_latmat_scan.py \
	  tests/sql/test_dataset_formats.py tests/unit/planner/test_dataset_format.py \
	  -v --tb=short

et: compile ## Run expression engine tests (value-checked gates)
	$(call print_blue,"Running expression engine tests...")
	@clear || true
	@$(PYTEST) tests/test_expression_engine.py -v --tb=short

medius-test: ## Build and run the Medius bounded middle-tier tests
	$(call print_blue,"Building and running Medius tests...")
	@mkdir -p /tmp/opteryx-tests
	@cd /tmp/opteryx-tests && \
	  clang++ -std=c++20 -O2 -DNDEBUG \
	    -I$(CURDIR) \
	    -I$(CURDIR)/draken \
	    -I$(CURDIR)/draken/simd \
	    -I$(CURDIR)/third_party/mabel/carchar \
	    -I$(CURDIR)/third_party/mabel/medius \
	    -o medius_test $(CURDIR)/third_party/mabel/medius/medius_test.cpp
	@/tmp/opteryx-tests/medius_test

rle-dict-test: ## Build and run the RLE skip-dense -> Dict direct-builder tests
	$(call print_blue,"Building and running RLE direct-dict tests...")
	@mkdir -p /tmp/opteryx-tests
	@cd /tmp/opteryx-tests && \
	  clang++ -std=c++20 -O1 -DNDEBUG \
	    -DHAVE_SNAPPY=1 -DHAVE_ZSTD=1 -DZSTD_STATIC_LINKING_ONLY=1 -DHAVE_CONFIG_H=1 \
	    -I$(CURDIR)/rugo/src/parquet \
	    -I$(CURDIR) \
	    -I$(CURDIR)/src/cpp \
	    -I$(CURDIR)/draken \
	    -I$(CURDIR)/draken/core \
	    -I$(CURDIR)/draken/simd \
	    -I$(CURDIR)/src/c \
	    -I$(CURDIR)/third_party/mabel \
	    -I$(CURDIR)/third_party/mabel/base16 \
	    -I$(CURDIR)/third_party/mabel/base64 \
	    -I$(CURDIR)/third_party/mabel/base85 \
	    -I$(CURDIR)/third_party/mabel/carchar \
	    -I$(CURDIR)/third_party/mabel/parvi \
	    -I$(CURDIR)/third_party/mabel/perfect_hash \
	    -I$(CURDIR)/third_party/fastfloat \
	    -I$(CURDIR)/third_party/fastfloat/fast_float \
	    -I$(CURDIR)/third_party/yyjson/src \
	    -I$(CURDIR)/third_party/re2 \
	    -I$(CURDIR)/third_party/cyan4973 \
	    -I$(CURDIR)/third_party/tdigest-c/src \
	    -I$(CURDIR)/third_party/ulfjack/ryu \
	    -I$(CURDIR)/third_party/nanobind \
	    -I$(CURDIR)/third_party/crypto \
	    -I$(CURDIR)/third_party/bshoshany \
	    -I$(CURDIR)/third_party/moodycamel \
	    -I$(CURDIR)/third_party/boost_math \
	    -I$(CURDIR)/third_party/utf8h \
	    -I$(CURDIR)/third_party/pcg \
	    -I$(CURDIR)/third_party/snappy \
	    -I$(CURDIR)/third_party/zstd \
	    -I$(CURDIR)/third_party/zstd/common \
	    -I$(CURDIR)/third_party/zstd/decompress \
	    -I$(CURDIR)/third_party/lz4 \
	    -I$(CURDIR)/third_party/miniz \
	    $(CURDIR)/rugo/src/parquet/rle_direct_dict_test.cpp \
	    -o rle_direct_dict_test
	@/tmp/opteryx-tests/rle_direct_dict_test
	$(call print_green,"✓ RLE direct-dict tests passed")

kernel-parity: compile ## Build and run Phase 9a C ABI parity test
	$(call print_blue,"Building and running C ABI parity test...")
	@mkdir -p /tmp/opteryx-tests
	@cd /tmp/opteryx-tests && \
	  clang++ -std=c++20 -O3 \
	    -I$(CURDIR) \
	    -I$(CURDIR)/src/cpp \
	    -I$(CURDIR)/draken \
	    -I$(CURDIR)/draken/core \
	    -I$(CURDIR)/third_party/boost_math \
	    -I$(CURDIR)/third_party/cyan4973 \
	    -I$(CURDIR)/third_party/utf8h \
	    -I$(CURDIR)/third_party/mabel/carchar \
	    -I$(CURDIR)/third_party/mabel/parvi \
	    -I$(CURDIR)/third_party/fastfloat \
	    -I$(CURDIR)/third_party/fastfloat/fast_float \
	    -I$(CURDIR)/third_party/ulfjack/ryu \
	    -I$(CURDIR)/third_party/yyjson/src \
	    $(CURDIR)/draken/ops/kernels/c_abi_test.cpp \
	    $(CURDIR)/draken/core/vector_alloc.cpp \
	    $(CURDIR)/draken/ops/kernels/error_handling.cpp \
	    $(CURDIR)/draken/ops/kernels/result_helpers.cpp \
	    $(CURDIR)/draken/ops/kernels/kernel_registry.cpp \
	    $(CURDIR)/draken/ops/kernels/cast_numeric.cpp \
	    $(CURDIR)/draken/ops/kernels/cast_string.cpp \
	    $(CURDIR)/draken/ops/kernels/cast_temporal.cpp \
	    $(CURDIR)/draken/ops/kernels/cast_dispatch.cpp \
	    $(CURDIR)/draken/ops/kernels/extraction.cpp \
	    $(CURDIR)/draken/ops/kernels/function_kernels.cpp \
	    $(CURDIR)/draken/ops/kernels/string_trim.cpp \
	    $(CURDIR)/draken/ops/kernels/string_reverse_initcap.cpp \
	    $(CURDIR)/draken/ops/kernels/string_pad.cpp \
	    $(CURDIR)/draken/ops/kernels/string_replace_soundex.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_arithmetic.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_other.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_temporal.cpp \
	    $(CURDIR)/draken/ops/kernels/binop_dispatch.cpp \
	    $(CURDIR)/third_party/ulfjack/ryu/d2fixed.c \
	    $(CURDIR)/third_party/ulfjack/ryu/d2s.c \
	    $(CURDIR)/third_party/yyjson/src/yyjson.c \
	    -o c_abi_test
	@/tmp/opteryx-tests/c_abi_test
	$(call print_green,"✓ C ABI parity test passed")

# The draken C-ABI kernel TUs. kernel_registry.cpp's registry table names every
# kernel, so the whole set has to be linked even when a standalone binary only
# calls one of them. Mirrors the list in build_common.py — keep them in step.
DRAKEN_KERNEL_SRCS := \
	$(CURDIR)/draken/core/vector_alloc.cpp \
	$(CURDIR)/draken/ops/compare_dv.cpp \
	$(CURDIR)/draken/ops/arithmetic_dv.cpp \
	$(CURDIR)/draken/ops/kernels/error_handling.cpp \
	$(CURDIR)/draken/ops/kernels/result_helpers.cpp \
	$(CURDIR)/draken/ops/kernels/kernel_registry.cpp \
	$(CURDIR)/draken/ops/kernels/cast_numeric.cpp \
	$(CURDIR)/draken/ops/kernels/cast_string.cpp \
	$(CURDIR)/draken/ops/kernels/cast_temporal.cpp \
	$(CURDIR)/draken/ops/kernels/cast_dispatch.cpp \
	$(CURDIR)/draken/ops/kernels/extraction.cpp \
	$(CURDIR)/draken/ops/kernels/binary_op_arithmetic.cpp \
	$(CURDIR)/draken/ops/kernels/binary_op_other.cpp \
	$(CURDIR)/draken/ops/kernels/binary_op_temporal.cpp \
	$(CURDIR)/draken/ops/kernels/binop_dispatch.cpp \
	$(CURDIR)/draken/ops/kernels/function_kernels.cpp \
	$(CURDIR)/draken/ops/kernels/string_trim.cpp \
	$(CURDIR)/draken/ops/kernels/string_reverse_initcap.cpp \
	$(CURDIR)/draken/ops/kernels/string_pad.cpp \
	$(CURDIR)/draken/ops/kernels/string_replace_soundex.cpp \
	$(CURDIR)/draken/ops/kernels/string_humanize.cpp \
	$(CURDIR)/draken/ops/kernels/function_hash_encoding.cpp \
	$(CURDIR)/draken/ops/kernels/function_codec.cpp \
	$(CURDIR)/draken/ops/kernels/function_array_json.cpp \
	$(CURDIR)/draken/ops/kernels/function_temporal.cpp \
	$(CURDIR)/draken/ops/kernels/function_numeric.cpp \
	$(CURDIR)/draken/ops/kernels/function_string_extra.cpp \
	$(CURDIR)/draken/ops/kernels/function_null_conditional.cpp \
	$(CURDIR)/draken/ops/kernels/function_vector_distance.cpp \
	$(CURDIR)/draken/ops/kernels/function_rlike.cpp \
	$(CURDIR)/draken/ops/kernels/function_like_any.cpp \
	$(CURDIR)/draken/core/frame_arena.cpp \
	$(CURDIR)/third_party/crypto/md5.cpp \
	$(CURDIR)/third_party/crypto/sha1.cpp \
	$(CURDIR)/third_party/crypto/sha2.cpp \
	$(CURDIR)/third_party/crypto/sha512.cpp \
	$(CURDIR)/src/cpp/simd_hash.cpp \
	$(CURDIR)/src/cpp/simd_env.cpp \
	$(CURDIR)/src/cpp/cpu_features.cpp

# C (not C++) TUs. The vendored mabel codecs are C99 and do NOT compile as C++
# (designated initializers, char-array init rules), so they get their own pass
# with a C compiler — the same split setuptools does for them in the real build.
DRAKEN_KERNEL_C_SRCS := \
	$(CURDIR)/third_party/mabel/base16/_base16.c \
	$(CURDIR)/third_party/mabel/base64/_base64.c \
	$(CURDIR)/third_party/mabel/base64/_base64_dispatch.c \
	$(CURDIR)/third_party/mabel/base64/_base64_neon.c \
	$(CURDIR)/third_party/mabel/base64/_base64_avx2.c \
	$(CURDIR)/third_party/mabel/base64/_base64_rvv.c \
	$(CURDIR)/third_party/mabel/base85/_base85.c \
	$(CURDIR)/third_party/ulfjack/ryu/d2fixed.c \
	$(CURDIR)/third_party/ulfjack/ryu/d2s.c \
	$(CURDIR)/third_party/yyjson/src/yyjson.c

DRAKEN_KERNEL_INCLUDES := \
	-I$(CURDIR) \
	-I$(CURDIR)/src/cpp \
	-I$(CURDIR)/draken \
	-I$(CURDIR)/draken/core \
	-I$(CURDIR)/third_party/boost_math \
	-I$(CURDIR)/third_party/cyan4973 \
	-I$(CURDIR)/third_party/utf8h \
	-I$(CURDIR)/third_party/mabel \
	-I$(CURDIR)/third_party/mabel/base16 \
	-I$(CURDIR)/third_party/mabel/base64 \
	-I$(CURDIR)/third_party/mabel/base85 \
	-I$(CURDIR)/third_party/mabel/carchar \
	-I$(CURDIR)/third_party/mabel/parvi \
	-I$(CURDIR)/third_party/mabel/perfect_hash \
	-I$(CURDIR)/third_party/crypto \
	-I$(CURDIR)/third_party/pcg \
	-I$(CURDIR)/third_party/fastfloat \
	-I$(CURDIR)/third_party/fastfloat/fast_float \
	-I$(CURDIR)/third_party/ulfjack/ryu \
	-I$(CURDIR)/third_party/yyjson/src \
	-I$(CURDIR)/third_party/usearch/fp16/include \
	-I$(CURDIR)/third_party/tdigest-c/src

# JSON_BENCH_ARGS is passed straight through, e.g.
#   make json-extract-bench JSON_BENCH_ARGS="--rows 500000 --csv /tmp/je.csv --label after"
JSON_BENCH_ARGS ?=
JSON_BENCH_DIR := /tmp/opteryx-tests/json-extract-bench

json-extract-bench: ## Build + run the draken `->`/`->>` kernel microbenchmark (JSON_BENCH_ARGS="...")
	$(call print_blue,"Building JSON extraction microbenchmark...")
	@mkdir -p $(JSON_BENCH_DIR)
	@cd $(JSON_BENCH_DIR) && \
	  clang -std=c11 -O3 -w $(DRAKEN_KERNEL_INCLUDES) -c $(DRAKEN_KERNEL_C_SRCS)
	@cd $(JSON_BENCH_DIR) && \
	  clang++ -std=c++20 -O3 -w $(DRAKEN_KERNEL_INCLUDES) \
	    $(CURDIR)/draken/ops/kernels/json_extract_bench.cpp \
	    $(DRAKEN_KERNEL_SRCS) \
	    $(JSON_BENCH_DIR)/*.o \
	    -o json_extract_bench
	@cd $(CURDIR) && $(JSON_BENCH_DIR)/json_extract_bench $(JSON_BENCH_ARGS)

tpch: ## Run TPC-H benchmark vs DuckDB on the skene mirror (generates testdata/tpch_10_skene from testdata/tpch_10 on first run)
	$(call print_blue,"Running TPC-H benchmark vs DuckDB...")
	@# skene-only: TPC-H is tested against the skene mirror exclusively — the
	@# plain-parquet variant (this target's old name/body) was removed
	@# 2026-08-16, and tpch-skene renamed to tpch in its place.
	@#
	@# Stamped on the LAYOUT, not just on the directory: the mirror generated
	@# before row groups were packed into files is a different set of objects
	@# with different names, and the converter refuses to write over it. A
	@# missing stamp with a populated directory therefore means "regenerate",
	@# which is what the rm does.
	@#
	@# The `zstd 7` arguments are NOT optional, for the same reason they are not
	@# optional on clickbench-skene: they ARE the reference storage posture
	@# (WriteOptions::for_storage, architect 2026-08-11). This target passed NO
	@# codec until then, so the mirror was written uncompressed — 8.2 GB from
	@# 2.8 GB of parquet — and the TPC-H skene number was quoted against a
	@# dataset in the spill posture rather than the stored one.
	@test -f testdata/tpch_10_skene.skene-v2 || { rm -rf testdata/tpch_10_skene && $(PYTHON) dev/parquet_to_skene.py testdata/tpch_10 testdata/tpch_10_skene lz4 && touch testdata/tpch_10_skene.skene-v2; }
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/tpch/runner.py --variant skene

tpch-sf100: ## Run TPC-H benchmark on the skene mirror of SF100 (generates testdata/tpch_100 via tpchgen-cli, then testdata/tpch_100_skene, on first run)
	$(call print_blue,Running TPC-H benchmark on skene SF100...)
	@# testdata/tpch_100 is not vendored (SF100 parquet runs ~30GB) and has no
	@# in-tree generator — same as every other scale here (tpch_1/_10/_001),
	@# which were produced out-of-band by tpchgen-cli
	@# (github.com/clflushopt/tpchgen-rs, `cargo install tpchgen-cli`). That's
	@# why SF10 is 16 numbered files per table with nation/region only on file
	@# .1 — that's tpchgen-cli's `--parts 16` convention, not ours, so this
	@# target reproduces it with the same tool rather than a from-scratch
	@# writer. The parquet here is an intermediate file only; the skene mirror
	@# below is the actual benchmark artifact.
	@command -v tpchgen-cli >/dev/null 2>&1 || { echo "tpchgen-cli not found on PATH — install with: cargo install tpchgen-cli"; exit 1; }
	@test -d testdata/tpch_100 || { \
		echo "Generating testdata/tpch_100 (SF100) via tpchgen-cli..."; \
		tpchgen-cli -s 100 --format parquet --parts 16 --output-dir testdata/tpch_100; \
	}
	@test -f testdata/tpch_100_skene.skene-v2 || { rm -rf testdata/tpch_100_skene && $(PYTHON) dev/parquet_to_skene.py testdata/tpch_100 testdata/tpch_100_skene lz4 && touch testdata/tpch_100_skene.skene-v2; }
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/tpch/runner.py --scale 100 --variant skene

b: check-python
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) scratch/brace.py

g:
	@$(PYTHON) tests/groupby_combo_generator_resilient.py --config tests/groupby_combo_tests_config.json

gv:
	@$(PYTHON) tests/groupby_combo_generator_resilient.py --config tests/groupby_combo_tests_config.json --verbose

go:
	@$(PYTHON) tests/groupby_combo_generator_resilient.py --config tests/groupby_combo_tests_config.json --output /tmp/groupby_results.json

clickbench:
	@clear || true
	@$(PYTHON) -c "import sys; print(f'Running ClickBench on Python {sys.version.split()[0]}  (GIL enabled: {sys._is_gil_enabled()})')"
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/clickbench/opteryx/runner.py

clickbench-skene: ## Run ClickBench on the skene mirror of the dataset (generates scratch/hits_skene, lz4 (performance posture), from scratch/hits_rugo_262k on first run)
	$(call print_blue,"Running ClickBench benchmark on skene...")
	@# Gate on a completion stamp, not on the directory: an interrupted conversion
	@# leaves a partial tree that `test -d` would accept, silently benchmarking a
	@# fraction of the dataset. The stamp lives outside the dataset dir so it can
	@# never trip the single-format manifest check. Re-running the converter over
	@# an existing tree is idempotent (deterministic output filenames).
	@#
	@# The `lz4` argument is NOT optional, and it is NOT what a deployed dataset
	@# uses. It is WriteOptions::for_fast_reads — the LOCAL BENCHMARK posture.
	@# Dropping it rebuilds an UNCOMPRESSED mirror into the same path and nothing
	@# downstream would say so; the suite would report a different number for a
	@# dataset nobody knew had changed.
	@#
	@# ⛔ THE PARQUET/SKENE CODEC GAP IS INTENTIONAL. The parquet corpora are
	@# written in the STORAGE posture (zstd, per-type level, 95% keep floor) and
	@# these skene mirrors in the PERFORMANCE posture (lz4). That asymmetry is a
	@# deliberate decision (architect, 2026-08-11), not drift:
	@#
	@#   - Deployed data is read REMOTELY, where bytes dominate. At ~64 MB/s
	@#     achieved on the 1 Gbps Cloud Run link, zstd-7 beats lz4 by 1.39x on
	@#     total time, and lz4 only overtakes it above ~1.25 GB/s (10 Gbps) —
	@#     an order of magnitude away. So deployed data takes for_storage().
	@#   - These mirrors are read LOCALLY off NVMe, where the pipe is not the
	@#     bottleneck and decompression is pure cost. Measured on TPC-H SF10,
	@#     interleaved, min of 3: lz4 6041ms vs zstd-7 7153ms — 1.1s, 16%.
	@#
	@# ⛔ Because of that split, a skene number here and a parquet number from
	@# `make clickbench` are NOT a like-for-like format comparison: they are
	@# different codecs by design. Comparing them measures the codec choice as
	@# much as the format. Say which posture any quoted figure came from.
	@# (`make tpch` is skene/lz4 now too — see its own target — so it IS
	@# comparable to this one on codec, unlike `make clickbench`.)
	@#
	@# The stamp is named for the FORMAT VERSION the mirror was written at, not
	@# just for "converted". A mirror written before row groups were packed into
	@# files is a different set of objects under different names, so the old
	@# stamp must not satisfy this gate and the old tree must go — otherwise the
	@# converter refuses and the benchmark never runs.
	@test -f scratch/hits_skene.skene-v2 || { rm -rf scratch/hits_skene scratch/hits_skene.converted && $(PYTHON) dev/parquet_to_skene.py scratch/hits_rugo_262k scratch/hits_skene lz4 && touch scratch/hits_skene.skene-v2; }
	@clear || true
	@$(PYTHON) -c "import sys; print(f'Running ClickBench (skene) on Python {sys.version.split()[0]}  (GIL enabled: {sys._is_gil_enabled()})')"
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/clickbench/opteryx/runner.py --variant skene

clickbench-profile: ## ClickBench + per-operator self-time profile (where the time goes)
	@clear || true
	@$(PYTHON) tests/performance/clickbench/opteryx/runner.py --profile

clickbench-duckdb: ## Re-run DuckDB ClickBench calibration (regenerates duckdb/results.local.json)
	@$(PYTHON) tests/performance/clickbench/duckdb/runner.py

jsonbench: ## Run JSONBench (Bluesky NDJSON) vs DuckDB via Opteryx SQL / READ_JSONL (JSONBENCH_SIZE=1|10|100, default 10)
	@clear || true
	@$(PYTHON) tests/performance/jsonbench/runner.py --size $(if $(JSONBENCH_SIZE),$(JSONBENCH_SIZE),10)

tpcds: ## Run the TPC-DS SF1 smoke suite (coverage, not performance — see runner.py docstring)
	@# Data is generated via DuckDB's dsdgen (dev/tpcds/generate_data.py), not
	@# vendored: gated on the dataset directory existing, regenerate by removing
	@# testdata/tpcds_1 and re-running.
	@test -d testdata/tpcds_1 || $(PYTHON) dev/tpcds/generate_data.py --scale 1
	@clear || true
	@$(PYTHON) tests/performance/tpcds/runner.py --scale 1

tpcds-001: ## Run the TPC-DS suite at SF0.01 (fast iteration — same label convention as testdata/tpch_001)
	@test -d testdata/tpcds_001 || $(PYTHON) dev/tpcds/generate_data.py --scale 001
	@clear || true
	@$(PYTHON) tests/performance/tpcds/runner.py --scale 001

jsonbench-data: ## Fetch + decompress the JSONBench Bluesky dataset (JSONBENCH_SIZE=1|10|100, default 10)
	@$(PYTHON) tests/performance/jsonbench/fetch_data.py --size $(if $(JSONBENCH_SIZE),$(JSONBENCH_SIZE),10)

jsonbench-duckdb: ## Re-run DuckDB JSONBench calibration (regenerates duckdb/results.local.<N>m.json)
	@$(PYTHON) tests/performance/jsonbench/duckdb/runner.py --size $(if $(JSONBENCH_SIZE),$(JSONBENCH_SIZE),10)

dash: ## Run odata_dashboard benchmark vs DuckDB (real-world OData query-log shapes)
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/odata_dashboard/runner.py

dash-duckdb: ## Re-run DuckDB odata_dashboard calibration (regenerates duckdb/results.local.json)
	@$(PYTHON) tests/performance/odata_dashboard/duckdb/runner.py

m4-sweep: ## M4 DOP-sweep gate: serial-parity at DOP=1 + scaling above (M4_DATASET=… M4_ITERS=…)
	@$(PYTHON) dev/m4_dop_sweep.py

tpch-bench: ## Run TPC-H performance benchmark (Opteryx)
	@clear || true
	@$(PYTHON) tests/performance/tpch/opteryx/runner.py

tpch-bench-duckdb: ## Re-run DuckDB TPC-H calibration (regenerates duckdb/results.sf*.json)
	@$(PYTHON) tests/performance/tpch/duckdb/runner.py

job: ## Run Join Order Benchmark (JOB) on the skene mirror (generates testdata/job_skene from testdata/job on first run)
	$(call print_blue,"Running JOB on skene...")
	@# skene, not parquet. JOB does not stipulate a storage format — upstream
	@# ships the IMDB CSVs, not files — so the choice was always ours, and it is
	@# made the same way here as in the weekly suite (mabel-dev/wrenchy-bench)
	@# so a laptop number and a CI number are the same measurement.
	@#
	@# The `lz4` argument is NOT optional: it is WriteOptions::for_fast_reads,
	@# the local-benchmark posture every other skene mirror in this tree uses.
	@# Dropping it rebuilds an UNCOMPRESSED mirror into the same path and
	@# nothing downstream would say so.
	@#
	@# Gated on a completion stamp rather than on the directory: an interrupted
	@# conversion leaves a partial tree that `test -d` would accept, silently
	@# benchmarking a fraction of the dataset.
	@test -f testdata/job_skene.skene-v2 || { rm -rf testdata/job_skene && $(PYTHON) dev/parquet_to_skene.py testdata/job testdata/job_skene lz4 && touch testdata/job_skene.skene-v2; }
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/job/runner.py --variant skene

job-duckdb: ## Re-run DuckDB JOB calibration (regenerates duckdb/results.json)
	@$(PYTHON) tests/performance/job/duckdb/runner.py

h2o: ## Run H2O db-benchmark on the skene mirror (groupby + join, medium; generates testdata/h2o_skene on first run)
	$(call print_blue,"Running H2O on skene...")
	@# medium (1e8 rows), not small. At 1e7 rows small is 630MB, which sits
	@# entirely in page cache on any development machine — it measured compute
	@# with the storage layer removed, and is no longer benchmarked.
	@#
	@# skene for the same reason as `job` above: H2O ships a data GENERATOR,
	@# not files, so the storage format was always this repo's choice. See that
	@# target for why `lz4` is not optional and why the stamp gates the build.
	@#
	@# The mirror is flat — testdata/h2o_skene/<table> — because it is built at
	@# one size, so there is no size level to carry. The parquet tree keeps its
	@# testdata/h2o/<size>/<table> layout.
	@test -d testdata/h2o/medium || { echo "testdata/h2o/medium not found — generate it with: PYTHONPATH=. $(PYTHON) tests/performance/h2o/generate_data.py --size medium"; exit 1; }
	@test -f testdata/h2o_skene.skene-v2 || { rm -rf testdata/h2o_skene && $(PYTHON) dev/parquet_to_skene.py testdata/h2o/medium testdata/h2o_skene lz4 && touch testdata/h2o_skene.skene-v2; }
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/h2o/runner.py --variant skene --size medium --workload both

h2o-duckdb: ## Re-run DuckDB H2O calibration (regenerates duckdb/results.<size>.json)
	@$(PYTHON) tests/performance/h2o/duckdb/runner.py --size medium --workload both

signals: ## Run signals benchmark suite (synthetic security-findings dataset, no DuckDB baseline)
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/signals/runner.py

medicare1-fetch: ## Download and convert Medicare1 benchmark data
	@$(PYTHON) tests/performance/medicare1/fetch_data.py

medicare1: ## Run Medicare1 benchmark vs DuckDB
	@clear || true
	@$(PYTHON) tests/performance/medicare1/run.py

medicare1-duckdb: ## Re-run DuckDB Medicare1 calibration (regenerates duckdb/results.json)
	@$(PYTHON) tests/performance/medicare1/duckdb/runner.py



coverage: ## Generate test coverage report
	$(call print_blue,"Running coverage analysis...")
	@$(PIP) install --upgrade coverage pytest
	@clear
	@MANUAL_TEST=1 $(COVERAGE) run -m pytest --color=yes
	@$(COVERAGE) report --include=$(SRC_DIR)/** --fail-under=80 -m
	@$(COVERAGE) html --include=$(SRC_DIR)/**
	$(call print_green,"Coverage report generated in htmlcov/")

# Runs exactly what CI runs, so a local pass means the same thing as a CI pass.
# The fuzzers seed themselves randomly and print the seed of every case; set
# TEST_SEED to replay a run, and TEST_ITERATIONS to go deeper than the default.
fuzz: check-python dev-install ## Run the fuzzing suite (TEST_ITERATIONS=N for a longer run)
	$(call print_blue,"Running fuzzing suite...")
	@clear || true
	@TEST_ITERATIONS=$${TEST_ITERATIONS:-1000} $(PYTHON) -m pytest tests/fuzzing/ --color=yes
	$(call print_green,"Fuzzing complete!")

# The file parsers are fuzzed separately from the SQL surface: different inputs
# (bytes we did not write), different oracle (ASan/UBSan rather than a result
# comparison), and a different toolchain (clang, no Python). See
# tests/fuzzing/native/README.md.
fuzz-native: ## Replay the native parser corpus under ASan/UBSan
	$(call print_blue,"Replaying native parser corpus...")
	@$(MAKE) -C tests/fuzzing/native replay
	$(call print_green,"Native corpus clean!")

fuzz-native-run: ## Search for new parser crashes (FUZZ_SECONDS=N per target)
	$(call print_blue,"Fuzzing native parsers...")
	@$(MAKE) -C tests/fuzzing/native run

# === TYPE CHECKING ===

mypy: ## Run type checking
	$(call print_blue,"Running type checking...")
	@$(PIP) install --upgrade mypy
	@clear
	@$(MYPY) --ignore-missing-imports --python-version 3.13 --no-strict-optional --check-untyped-defs $(SRC_DIR)

# === COMPILATION ===

# Build system: setuptools (Cython) + setuptools-rust (Rust compute crate).
# `make compile` is the only supported local path — never use `pip install`.
# Wheels for PyPI are built in CI via the same setup.py path.

compile: check-python clean ## Compile all extensions in-place
	$(call print_blue,Building Opteryx extensions...)
	@$(PYTHON) -m pip install --quiet --upgrade setuptools wheel setuptools-rust cython
	@$(PYTHON) setup.py build_ext --inplace -j $(JOBS)
	$(call print_green,Compilation complete.)

compile-quick: check-python ## Incremental compilation (alias: c)
	$(call print_blue,Incremental build...)
	@$(PYTHON) setup.py build_ext --inplace -j $(JOBS)
	$(call print_green,Incremental build complete.)

draken: check-python ## Build draken extensions only — isolated from opteryx Cython breakage
	$(call print_blue,Building Draken extensions \(DRAKEN_BUILD=1\)...)
	@$(PYTHON) -m pip install --quiet --upgrade setuptools wheel cython
	@DRAKEN_BUILD=1 $(PYTHON) setup.py build_ext --inplace -j $(JOBS)
	$(call print_green,Draken build complete.)

# Alias for backward compatibility
c: compile-quick

check-symbols: ## Verify no undefined symbols in compiled extensions (run after compile)
	$(call print_blue,"Checking for undefined symbols...")
	@$(PYTHON) dev/check_undefined_symbols.py || (echo "❌ Undefined symbols found — setup.py missing vendored library sources" && exit 1)
	$(call print_green,"✓ All symbols resolved")

# === CLEANUP ===

clean: ## Clean build artifacts
	$(call print_blue,"Cleaning build artifacts...")
	@find . -name '*.so' -delete
	@$(PYTHON) dev/clean_cython_generated.py
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.egg-info' -type d -exec rm -rf {} + 2>/dev/null || true
	@rm -rf $(BUILD_DIR) $(DIST_DIR) target/ .coverage htmlcov/ .pytest_cache/ .mypy_cache/
	$(call print_green,"Cleanup complete!")

distclean: clean ## Deep clean including generated source files
	$(call print_blue,"Deep cleaning (removing generated files)...")
	@find . -name '*.c' -path '*/opteryx/compiled/*' -delete
	@find . -name '*.cpp' -path '*/opteryx/compiled/*' -delete
	$(call print_green,"Deep clean complete!")

# === CONVENIENCE TARGETS ===

all: compile test ## Full build and test workflow

check-all: lint mypy test coverage ## Run all checks without compilation

waterfall: ## Run IO waterfall profiler (usage: make waterfall ARGS="trace scratch/io_trace.jsonl")
	@PYTHONPATH=dev $(PYTHON) -m io_waterfall $(ARGS)

loc: ## Count LOC for production code only (excludes tests)
	$(call print_blue,'Counting LOC for production files (excluding tests)')
	@$(PYTHON) dev/count_loc_basic.py --exclude build,temp,third_party,dev,scratch,tests --ext py,pyx,c,cpp,cc,cxx,h,hpp --per-file

edge-executor: check-python compile ## Test edge-based executor with real physical plan
	$(call print_blue,"Testing EdgeBasedExecutor with real Opteryx plan...")
	@$(PYTHON) scratch/test_edge_executor_real.py

# === SQL LOGIC TESTS ===

SLT_DRIVER   := tests/tools/sqllogictest/opteryx_driver.py
SLT_ROOT     := tests/tools/sqllogictest/tests
# Use the binary on PATH if available, otherwise fall back to the sibling checkout.
SQLLOGICTEST ?= $(shell command -v sqllogictest 2>/dev/null || echo ../sqllogictest/target/release/sqllogictest)

.PHONY: slt slt-shapes slt-results slt-run-only slt-install

slt-install: ## Install sqllogictest binary from mabel-dev fork
	cargo install sqllogictest-bin \
	  --git https://github.com/mabel-dev/sqllogictest \
	  --locked

slt-shapes: ## Run shape-checking slt tests
	$(SQLLOGICTEST) \
	  --engine external \
	  --external-engine-command-template "$(PYTHON) $(SLT_DRIVER)" \
	  '$(SLT_ROOT)/shapes/*.slt'

slt-results: ## Run result-checking slt tests
	$(SQLLOGICTEST) \
	  --engine external \
	  --external-engine-command-template "$(PYTHON) $(SLT_DRIVER)" \
	  '$(SLT_ROOT)/results/*.slt'

slt-run-only: ## Run execute-only slt tests (no result checks)
	$(SQLLOGICTEST) \
	  --engine external \
	  --external-engine-command-template "$(PYTHON) $(SLT_DRIVER)" \
	  '$(SLT_ROOT)/run_only/*.slt'

slt: slt-shapes slt-results slt-run-only ## Run the full sqllogictest suite
