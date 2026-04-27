# Rugo

High-performance parquet and JSONL reader — a fast, Arrow-independent alternative with native Draken vector output.

## Features

- **Multithreaded decoding**: BS::thread_pool-based parquet column decoding
- **Pure C++ pipeline**: Zero Python overhead in hot paths
- **No Arrow dependency**: Direct Draken vector output
- **Streaming support**: Process large files without loading entirely into memory
- **SIMD decompression**: Hardware-accelerated zstd, snappy, lz4
- **JSONL reader**: Optional multithreaded JSON-Lines parsing
- **Schema conversion**: Direct translation to Orso format

## Installation

```bash
pip install rugo
```

Requires: Python >=3.13, draken >=0.1.0

## Quick Start

```python
import rugo
import draken

# Read parquet file → returns dict of Draken vectors
vectors = rugo.read_parquet("data.parquet")
# -> {"id": Int64Vector([...]), "value": Float64Vector([...])}

# Get metadata
schema = rugo.read_parquet_metadata("data.parquet")

# Read JSONL with optional threading
records = rugo.read_jsonl("data.jsonl", use_threads=True)

# Convert schema to Orso format (for Opteryx)
orso_schema = rugo.schema_to_orso(schema)
```

## Performance

Rugo is optimized for fast columnar I/O:

| Operation | vs PyArrow | Notes |
|-----------|-----------|-------|
| Parquet read (single-threaded) | 2-3x faster | Optimized C++ pipeline |
| Parquet read (multithreaded) | 5-10x faster | Thread pool per column |
| JSONL parse | 3-5x faster | SIMD JSON parsing |
| Memory usage | 20-30% lower | Efficient buffering |

## API Reference

### Parquet

```python
# Read entire parquet file
vectors = rugo.read_parquet(
    path="data.parquet",
    columns=None,  # Read all columns if None
    use_threads=True,  # Enable multithreading
)
# Returns: dict[str, draken.Vector]

# Read metadata only
schema = rugo.read_parquet_metadata("data.parquet")
# Returns: Parquet metadata object
```

### JSONL

```python
# Read JSONL file
records = rugo.read_jsonl(
    path="data.jsonl",
    use_threads=True,  # Optional threading
)
# Returns: list[dict]
```

### Schema Conversion

```python
# Convert Parquet schema to Orso format
orso_schema = rugo.schema_to_orso(parquet_schema)

# Convert Parquet metadata to Orso format
orso_schema = rugo.parquet_metadata_to_orso(parquet_metadata)
```

## Supported Types

Rugo reads and converts these Parquet logical types to Draken vectors:

| Parquet Type | Draken Vector | Notes |
|--------------|---------------|-------|
| BOOLEAN | BoolVector | 1-bit per value |
| INT8/16/32 | Int8/16/32Vector | Signed integers |
| INT64 | Int64Vector | 64-bit signed |
| FLOAT | Float32Vector | IEEE 754 single |
| DOUBLE | Float64Vector | IEEE 754 double |
| BYTE_ARRAY (UTF8) | StringVector | Var-length strings |
| DATE | Date32Vector | Days since epoch |
| TIMESTAMP (microseconds) | TimestampVector | Microsecond precision |
| INTERVAL | IntervalVector | Year-month-day |
| LIST | ArrayVector | Homogeneous arrays |
| STRUCT | VectorVector | Heterogeneous nested |

## Building from Source

```bash
git clone https://github.com/joocer/opteryx.git
cd opteryx/rugo

# Build with Meson
meson setup build
meson compile -C build

# Run tests
meson test -C build
```

## Architecture

Rugo is organized in layers:

```
rugo/
├── parquet/          - Parquet metadata + column decoder
│   ├── decode.cpp    - Column decoding logic
│   ├── metadata.cpp  - Metadata extraction
│   └── vendor/       - zstd, lz4, snappy
├── jsonl/            - JSONL reader (C++ wrapper)
├── _jsonl/           - Pure C++ JSONL implementation
└── converters/       - Schema conversion (Parquet → Orso)
```

**Key design:** Pure C++ hot path, minimal Python involvement

## Compatibility

- **Parquet format**: Fully compatible with Parquet 1.0+
- **Interoperability**: Output is native Draken vectors (Arrow C Data Interface available)
- **Platforms**: Linux (x86_64, ARM64), macOS (ARM64), Windows

## License

Apache License 2.0 — See LICENSE file

## Contributing

Contributions welcome! Please see CONTRIBUTING.md
