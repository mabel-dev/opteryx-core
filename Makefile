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
PYTHON ?= $(PYTHON_GIL) python3.13
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

.PHONY: help lint format check test test-quick test-battery coverage mypy compile clean install update dev-install all check-python

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
	@$(PYTHON) -m pycln .
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

install: ## Install package in development mode
	$(call print_blue,"Installing package...")
	@$(PIP) install -e .

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

test: check-python dev-install ## Run full test suite
	$(call print_blue,"Running full test suite...")
	@$(PIP) install --upgrade pytest pytest-xdist
	@clear
	@MANUAL_TEST=1 $(PYTEST) -n auto --color=yes

test-quick: check-python ## Run quick test (alias: t)
	@clear
	@$(PYTHON) tests/integration/sql_battery/run_shapes_battery.py

 b: check-python
	@clear
	@$(PYTHON) scratch/brace.py

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

compile: check-python clean ## Compile Cython extensions
	$(call print_blue,"Compiling Cython extensions...")
	@$(PIP) install --upgrade pip uv numpy cython setuptools setuptools_rust
	@$(PYTHON) setup.py clean
	@$(PYTHON) setup.py build_ext --inplace -j $(JOBS)
	$(call print_green,"Compilation complete!")

compile-quick: check-python ## Quick compilation (alias: c)
	@$(PYTHON) setup.py build_ext --inplace

# Alias for backward compatibility
c: compile-quick

# === CLEANUP ===

clean: ## Clean build artifacts
	$(call print_blue,"Cleaning build artifacts...")
	@find . -name '*.so' -delete
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
	@find . -name '*.egg-info' -type d -exec rm -rf {} + 2>/dev/null || true
	@rm -rf $(BUILD_DIR) $(DIST_DIR) .coverage htmlcov/ .pytest_cache/
	$(call print_green,"Cleanup complete!")

distclean: clean ## Deep clean including compiled extensions
	$(call print_blue,"Deep cleaning...")
	@find . -name '*.so' -delete
	@find . -name '*.c' -path '*/opteryx/compiled/*' -delete

# === CONVENIENCE TARGETS ===

all: clean dev-install lint mypy test compile ## Run complete development workflow

check-all: lint mypy test coverage ## Run all checks without compilation

loc: ## Count LOC for production code only (excludes tests)
	$(call print_blue,'Counting LOC for production files (excluding tests)')
	@$(PYTHON) dev/count_loc_basic.py --exclude build,temp,third_party,dev,scratch,tests --ext py,pyx,c,cpp,cc,cxx,h,hpp --per-file
