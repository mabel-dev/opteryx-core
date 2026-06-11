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
PYTHON_GIL ?=
# Prefer python3.13 by default for consistent ABI and compiled artifacts.
# Users may override by passing PYTHON='python3.x' on the make commandline.
PYTHON ?= $(PYTHON_GIL) python
UV := $(PYTHON) -m uv
PIP := $(UV) pip
PYTEST := $(PYTHON) -m pytest
COVERAGE := $(PYTHON) -m coverage
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

.PHONY: help lint format check test test-battery coverage mypy compile compile-quick draken clean distclean update dev-install all check-python dt et q reference

# Default target
.DEFAULT_GOAL := help

# === REFERENCE CATALOGS ===

reference: check-python ## Regenerate all reference catalogs (JSON + catalog Python files)
	$(call print_blue,"Regenerating reference catalogs...")
	@$(PYTHON) dev/generate_reference.py

# === LINTING AND FORMATTING ===

# Enforce Python 3.13 for CI and developer tools. This will abort early if the configured
# python interpreter is not 3.13; set PYTHON to override or install 3.13 via your environment.
check-python:
	@ver=`$(PYTHON) -c "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')" 2>/dev/null`; \
	if [ "$$ver" != "3.13" ]; then \
		echo "\nERROR: Python 3.13 is required for builds in this repository; found $$ver\n" >&2; \
		echo "Set your local Python to 3.13 (pyenv local 3.13.5) or override with: make PYTHON=python3.13 <target>" >&2; \
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
	@MANUAL_TEST=1 $(PYTEST) -n auto --color=yes



q:
	@clear || true
	@$(PYTHON) tests/integration/sql_battery/test_shapes_basic.py

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
	  clang++ -std=c++17 -O3 \
	    -I$(CURDIR) \
	    -I$(CURDIR)/src/cpp \
	    -I$(CURDIR)/draken \
	    -I$(CURDIR)/draken/core \
	    -I$(CURDIR)/third_party/boost_math \
	    -I$(CURDIR)/third_party/cyan4973 \
	    -I$(CURDIR)/third_party/mabel/carchar \
	    -I$(CURDIR)/third_party/mabel/parvi \
	    -I$(CURDIR)/third_party/mimalloc/include \
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
	    $(CURDIR)/draken/ops/kernels/binary_op_arithmetic.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_other.cpp \
	    $(CURDIR)/draken/ops/kernels/binary_op_temporal.cpp \
	    $(CURDIR)/build/temp.mimalloc.o \
	    -o c_abi_test
	@/tmp/opteryx-tests/c_abi_test
	$(call print_green,"✓ C ABI parity test passed")

tpch: ## Run TPC-H benchmark vs DuckDB (defaults to SF=1)
	$(call print_blue,"Running TPC-H benchmark vs DuckDB...")
	@clear || true
	@$(PYTHON) tests/performance/tpch/runner.py

b: check-python
	@clear || true
	@$(PYTHON) scratch/brace.py

g:
	@$(PYTHON) tests/groupby_combo_generator_resilient.py --config tests/groupby_combo_tests_config.json

gv:
	@$(PYTHON) tests/groupby_combo_generator_resilient.py --config tests/groupby_combo_tests_config.json --verbose

go:
	@$(PYTHON) tests/groupby_combo_generator_resilient.py --config tests/groupby_combo_tests_config.json --output /tmp/groupby_results.json

clickbench:
	@clear || true
	@$(PYTHON) tests/performance/clickbench/opteryx/runner.py

clickbench-duckdb: ## Re-run DuckDB ClickBench calibration (regenerates duckdb/results.local.json)
	@$(PYTHON) tests/performance/clickbench/duckdb/runner.py

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

# === CLEANUP ===

clean: ## Clean build artifacts
	$(call print_blue,"Cleaning build artifacts...")
	@find . -name '*.so' -delete
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
