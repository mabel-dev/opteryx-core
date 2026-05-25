# Development Tools

This directory contains scripts and tools used for Opteryx Core development, build support, vendoring, data generation, performance analysis, and release preparation. These are developer tools, not runtime modules imported by the engine.

## Contents

| File or group | Purpose |
|---------------|---------|
| `build_counter.py` | Manages build version numbering for releases |
| `build-wheels.sh` | Builds Python wheels for distribution |
| `requirements_embedded.txt` | Optional requirements for embedded/minimal installs |
| `vendor_*.py` | Refresh selected vendored dependencies under `third_party/` |
| `generate_*.py` | Generate test/security datasets and function metadata |
| `estimate_*_costs.py`, `compare_function_costs.py` | Function/operator cost analysis tools |
| `io_waterfall/` | I/O waterfall tracing helpers |
| `bench/` and `*_bench*.py` | Local benchmark and profiling helpers |

## Usage

Run commands from the repository root unless a script says otherwise.

### Build Wheels

```bash
./dev/build-wheels.sh
```

macOS releases are arm64-only; Intel/x86_64 macOS wheels are not built by CI or released.

### Update Build Counter

```bash
python dev/build_counter.py
```

### Refresh Vendored Dependencies

The vendoring scripts are intentionally explicit. Review their output and resulting diffs before committing.

```bash
python dev/vendor_mimalloc.py
python dev/vendor_nanobind.py
python dev/vendor_usearch.py
```

## Notes

Most day-to-day development does not require running these scripts directly. Prefer the top-level `Makefile` for common tasks such as `make compile`, `make q`, `make test`, and `make check`.
