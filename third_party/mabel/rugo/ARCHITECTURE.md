Rugo is a data reading library, supporting reading of parquet files and jsonl files only.

It's designed use is as the parquet reading library for opteryx, however it can be used as a standalone reader.

It exists because of a few factors:
- pyarrow does not appear to expose column reading primitives, which didn't support opteryx's goal of making reads as fine grained as possible
- fast-parquet wasn't fast enough
- the parquet reader in duckdb was too intertwined into the engine

This gives rise to three of the key design goals:
- support reading as little data as possible to form a logical construct (usually a column)
- speed is not optional
- the reader must be separate from the engine
