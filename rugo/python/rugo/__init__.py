"""
Rugo: High-performance parquet and JSONL reader

Rugo provides fast, Arrow-independent parquet and JSONL decoding with native Draken
vector output. Key features:

- Multithreaded parquet decoding (BS::thread_pool)
- Pure C++ IO pipeline with zero Python overhead
- Optional JSONL reader with threading support
- Native Draken vector output (no Arrow conversion)
- SIMD-accelerated decompression (zstd, snappy, lz4)
- Schema conversion to Orso format

Basic usage:
    import rugo
    import draken

    # Read parquet file
    vectors = rugo.read_parquet("data.parquet")  # Returns dict[str, draken.Vector]

    # Get schema information
    schema = rugo.read_parquet_metadata("data.parquet")

    # Read JSONL file
    records = rugo.read_jsonl("data.jsonl")  # Returns list[dict]
"""

__version__ = "0.1.0"

# TODO: Import compiled extensions
# from rugo.parquet import read_parquet, read_parquet_metadata
# from rugo.jsonl import read_jsonl
# from rugo.converters import schema_to_orso, parquet_metadata_to_orso

__all__ = [
    # Parquet functions (to be imported from compiled extensions)
    # "read_parquet",
    # "read_parquet_metadata",
    # JSONL functions
    # "read_jsonl",
    # Schema conversion
    # "schema_to_orso",
    # "parquet_metadata_to_orso",
]
