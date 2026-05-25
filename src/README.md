# Rust Extension Source

This directory contains the Rust code compiled into the `opteryx.compute` Python extension through `setuptools-rust`.

The main Opteryx Python package lives in `opteryx/`. Native C++ support code lives mainly in `src/cpp/`, `draken/`, `rugo/`, and `opteryx/compiled/`.

## Files

| Path | Purpose |
|------|---------|
| `lib.rs` | Rust crate entry point and Python module export |
| `opteryx_dialect.rs` | SQL parser dialect customizations for Opteryx |
| `cpp/` | C++ support code used by Cython/native extensions |

## Build

Use the repository Makefile:

```bash
make compile
```

`setup.py` wires this crate via `RustExtension("opteryx.compute", "Cargo.toml", debug=False)`, so Rust code is built as part of the normal extension build.
