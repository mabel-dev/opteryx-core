# Modernized Makefile for Opteryx
# Use bash shell for consistency across environments
SHELL := /bin/bash

# Variables
# By default we don't force Python to run with the GIL disabled. Some Python
# builds don't support disabling the GIL and will abort at startup when the
# environment requests it (fatal error: config_read_gil). To enable running
# without the GIL you can override this like:
#
#   make PYTHON_GIL='PYTHON_GIL=0' <target>
#
PYTHON_GIL ?=0
# Prefer python3.14 by default for consistent ABI and compiled artifacts.
# Users may override by passing PYTHON='python3.x' on the make commandline.
PYTHON ?= PYTHON_GIL=$(PYTHON_GIL) PYENV_VERSION=3.14.5t pyenv exec python
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

.PHONY: help lint format check test test-battery coverage mypy compile compile-quick draken clean distclean update dev-install all check-python dt et q reference function-costs

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
		echo "Set your local Python to 3.14 (pyenv local 3.14.5t) or override with: make PYTHON=python3.14 <target>" >&2; \
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

dt: ## Run draken unit tests
	$(call print_blue,"Running draken unit tests...")
	@clear || true
	@$(PYTEST) draken/tests/ -v --tb=short

et: compile ## Run expression engine tests (value-checked gates)
	$(call print_blue,"Running expression engine tests...")
	@clear || true
	@$(PYTEST) tests/test_expression_engine.py -v --tb=short

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
	    -I$(CURDIR)/third_party/mabel/carchar \
	    -I$(CURDIR)/third_party/mabel/parvi \
	    -I$(CURDIR)/third_party/fastfloat \
	    -I$(CURDIR)/third_party/fastfloat/fast_float \
	    -I$(CURDIR)/third_party/ulfjack/ryu \
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
	    $(CURDIR)/draken/ops/kernels/binary_op_arithmetic.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_other.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_temporal.cpp \
	    $(CURDIR)/draken/ops/kernels/binop_dispatch.cpp \
	    $(CURDIR)/third_party/ulfjack/ryu/d2fixed.c \
	    $(CURDIR)/third_party/ulfjack/ryu/d2s.c \
	    -o c_abi_test
	@/tmp/opteryx-tests/c_abi_test
	$(call print_green,"✓ C ABI parity test passed")

tpch: ## Run TPC-H benchmark vs DuckDB (defaults to SF=1)
	$(call print_blue,"Running TPC-H benchmark vs DuckDB...")
	@clear || true
	@env $(BENCH_PRELOAD) $(PYTHON) tests/performance/tpch/runner.py

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

clickbench-profile: ## ClickBench + per-operator self-time profile (where the time goes)
	@clear || true
	@$(PYTHON) tests/performance/clickbench/opteryx/runner.py --profile

clickbench-duckdb: ## Re-run DuckDB ClickBench calibration (regenerates duckdb/results.local.json)
	@$(PYTHON) tests/performance/clickbench/duckdb/runner.py

m4-sweep: ## M4 DOP-sweep gate: serial-parity at DOP=1 + scaling above (M4_DATASET=… M4_ITERS=…)
	@$(PYTHON) dev/m4_dop_sweep.py

tpch-bench: ## Run TPC-H performance benchmark (Opteryx)
	@clear || true
	@$(PYTHON) tests/performance/tpch/opteryx/runner.py

tpch-bench-duckdb: ## Re-run DuckDB TPC-H calibration (regenerates duckdb/results.sf*.json)
	@$(PYTHON) tests/performance/tpch/duckdb/runner.py

job: ## Run Join Order Benchmark (JOB) vs DuckDB
	@clear || true
	@$(PYTHON) tests/performance/job/runner.py

job-duckdb: ## Re-run DuckDB JOB calibration (regenerates duckdb/results.json)
	@$(PYTHON) tests/performance/job/duckdb/runner.py

h2o: ## Run H2O db-benchmark vs DuckDB (groupby + join, size=small)
	@clear || true
	@$(PYTHON) tests/performance/h2o/runner.py --size small --workload both

h2o-duckdb: ## Re-run DuckDB H2O calibration (regenerates duckdb/results.<size>.json)
	@$(PYTHON) tests/performance/h2o/duckdb/runner.py --size small --workload both

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

fuzz: check-python dev-install ## Run fuzzing tests (existing + metamorphic)
	$(call print_blue,"Running fuzzing suite...")
	@clear || true
	$(call print_blue,"Phase 1: Fuzzing literals...")
	@$(PYTHON) tests/fuzzing/fuzz_literals.py --iterations 100
	$(call print_blue,"Phase 2: Fuzzing joins...")
	@$(PYTHON) tests/fuzzing/fuzz_joins.py --iterations 50
	$(call print_blue,"Phase 3: Fuzzing single table select...")
	@$(PYTHON) tests/fuzzing/fuzz_single_table_select.py --iterations 50
	$(call print_blue,"Phase 4: Metamorphic fuzzing...")
	@$(PYTHON) tests/fuzzing/fuzz_metamorphic.py --iterations 500 --verbose
	$(call print_blue,"Phase 5: Constant Folding fuzzing...")
	@$(PYTHON) tests/fuzzing/fuzz_constant_folding.py --iterations 500 --verbose
	$(call print_green,"Fuzzing complete!")

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
