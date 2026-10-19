# Contributing to Opteryx Core

We're excited you're considering contributing to Opteryx Core and look forward to reviewing your contribution.

Opteryx Core is open source software, we value your feedback and want to make contributing to this project as easy and transparent as possible.

We use GitHub to host code, and to track issues and feature requests.

**Quick Links**   
[Building and Testing](#building-and-testing)     
[Submitting Code Changes](#submitting-code-changes)     
[Reporting a Bug](#reporting-a-bug)    
[Reporting a Security Issue](SECURITY.md)

## Building and Testing

The engine is native code (Cython, C++ and Rust) behind a Python API, so it has to be compiled before it can be tested. The supported local build path is the repository `Makefile`; see the [README](README.md#requirements) for the toolchain you need.

```bash
make dev-install   # test and development dependencies
make compile       # clean, in-place build of all extensions
make q             # fast regression check - run this while working
make check         # Ruff and import-order checks, without modifying files
make slt           # full sqllogictest suite - run before you ask for review
```

Do not use `pip install .` or `pip install -e .` for development: `make compile` builds in place and matches the layout the repository expects. Wheels are built in CI, never locally.

A change is not complete until `make q` passes, and not ready for review until `make slt` also passes. `make test` runs the full pytest suite.

## Submitting Code Changes

We actively welcome your improvements to the Opteryx Core project. If you are addressing an existing issue or a minor change, you can just raise a Pull Request with the details of the changes you are making and why, new Pull Requests have a template to help prompt you for the information we'd like to see.

If you're unfamiliar with the Pull Request process, see the [GitHub Help Pages](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) for more information on using pull requests.

If you are considering a significant change to the code, or adding a new feature not discussed previously, we ask that you raise an [issue](https://github.com/mabel-dev/opteryx-core/issues) before starting.

### Design Constraints

Some constraints are structural rather than stylistic. Please raise an issue before a change that touches them:

- Query **planning** (parse, bind, optimize, build the physical plan) is Python; query **execution** is native. Execution does not move into Python.
- The engine has zero install dependencies, and PyArrow and NumPy are not used anywhere in the engine (`opteryx/`, `rugo/`, `draken/`). New third-party code is vendored, and only after discussion.
- Failures are raised, not hidden. A change must not silently degrade behaviour or add a fallback path.

### All Changes must meet Quality Bars

We have included a number of tests which run automatically when code is submitted using GitHub Actions. These tests must pass before a change is eligible to be merged.

These tests include a regression test suite, security tests and other quality checks.

New contributors may need approval before these run.

#### All contributions will be under Apache 2.0 Licence

The project is licensed under [Apache 2.0](LICENSE), your submissions will be under this same licence.

Code which is licensed under a compatible or more open license (e.g. Public Domain) will be considered.

#### Format and Style Guidance

For consistent style, code should look like:

- Imports on separate lines (`import`s then `from`s)
- Variables should be in `snake_case`
- Classes should be in `PascalCase`
- Constants should be in `UPPER_SNAKE_CASE`
- Methods should have docstrings
- Formatted with [Ruff](https://docs.astral.sh/ruff/) and sorted with [isort](https://pycqa.github.io/isort/) (`make format`; `make check` verifies)
- Self-explanatory method, class and variable names
- Type hints, especially in function definitions

Code should have:  

- Corresponding unit/regression tests
- Attributed external sources - even if there is no explicit license requirement

A note about comments:  

- Computers will interpret anything, humans need help interpreting code
- Spend time writing readable code rather than verbose comments
- Humans struggle with threading, recursion, parallelization, variables called `x` and more than 10... of anything
- Comments should usually be more than just the code in other words
- Well-written code doesn't always need comments

## Reporting a Bug

We would love to hear how we can make Opteryx Core better. We use GitHub [issues](https://github.com/mabel-dev/opteryx-core/issues) to track bugs and improvement requests.

Great Bug and Feature requests usually have:

- A summary of the problem
- Sample code, if possible
- What you expected to happen
- What actually happened
- Any other context (did you try other things to get it to work, did it stop working after something happened)

For a suspected security vulnerability, do not open a public issue - follow [SECURITY.md](SECURITY.md).
