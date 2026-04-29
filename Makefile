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

.PHONY: help lint format check test test-quick test-battery coverage mypy compile compile-quick clean distclean update dev-install all check-python dt

# Default target
.DEFAULT_GOAL := help

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

test: check-python compile dev-install ## Run full test suite with compiled extensions
	$(call print_blue,"Running full test suite...")
	@$(PIP) install --upgrade pytest pytest-xdist
	@clear
	@MANUAL_TEST=1 $(PYTEST) -n auto --color=yes

test-quick: check-python compile ## Run quick test (alias: t)
	@clear
	@$(PYTHON) tests/integration/sql_battery/run_shapes_battery.py

q:
	@clear
	@$(PYTHON) tests/integration/sql_battery/test_shapes_basic.py

dt: ## Run draken unit tests
	$(call print_blue,"Running draken unit tests...")
	@clear || true
	@$(PYTEST) draken/tests/ -v --tb=short

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
	@clear
	@$(PYTHON) tests/performance/clickbench/clickbench.py

# Aliases for backward compatibility
t: test-quick

coverage: ## Generate test coverage report
	$(call print_blue,"Running coverage analysis...")
	@$(PIP) install --upgrade coverage pytest
	@clear
	@MANUAL_TEST=1 $(COVERAGE) run -m pytest --color=yes
	@$(COVERAGE) report --include=$(SRC_DIR)/** --fail-under=80 -m
	@$(COVERAGE) html --include=$(SRC_DIR)/**
	$(call print_green,"Coverage report generated in htmlcov/")

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
