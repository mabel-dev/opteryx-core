"""
Convert a parquet hits split into ORC files for `make clickbench-orc`.

    python dev/clickbench/parquet_to_orc.py <parquet_dir> <out_dir> [<codec>]

Reads with rugo and writes with pyarrow.orc (the Apache ORC C++ library - an independent writer, so
the benchmark reads what a Hive / Spark pipeline's files look like, not our own idea of ORC). pyarrow is
a development tool here: this script is under dev/, never imported by the engine. Each source file is
converted in its own process and cut into 1,000,000-row shards, <out_dir>/hits_<file>_<shard>.orc - the
same shard size as the json and avro variants.

Writer settings (they are the stamp `make clickbench-orc` gates on; change any and bump it):
  codec zstd (the parquet mirror's codec), 256 KiB compression blocks, 64 MiB stripes, 10,000-row index
  stride, string dictionaries on at the usual 0.8 key-ratio threshold, file version 0.12 (RLE v2).

Types are the source's, as close as ORC has them: INT16 -> SHORT, INT32 / UINT16 -> INT (ORC has no
unsigned integers), INT64 -> LONG, FLOAT64 -> DOUBLE, DATE32 -> DATE, TIMESTAMP64 -> TIMESTAMP_INSTANT
(microseconds, UTC), VARBINARY -> BINARY, VARCHAR -> STRING. Any other type is refused, naming the
column. hits has no NULLs.

Written into <out_dir>.partial and renamed only when every source row is accounted for, so an
interrupted run never leaves a short tree that looks complete.
"""

import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor

# pyarrow's ORC writer records the machine's zone, and a plain TIMESTAMP is wall-clock in it; the
# instants are written as UTC TIMESTAMP_INSTANT, but pin the zone anyway.
os.environ["TZ"] = "UTC"

import pyarrow as pa
import pyarrow.orc as po

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

ROWS_PER_SHARD = 1_000_000

ARROW_TYPES = {
    "INT16": pa.int16(),
    "UINT16": pa.int32(),
    "INT32": pa.int32(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE32": pa.date32(),
    "TIMESTAMP64": pa.timestamp("us", tz="UTC"),
    "VARBINARY": pa.binary(),
    "VARCHAR": pa.string(),
}


def arrow_schema(names, types):
    fields = []
    for name, draken_type in zip(names, types):
        if draken_type.name not in ARROW_TYPES:
            raise ValueError(f"column {name!r}: {draken_type.name} has no ORC mapping here")
        fields.append(pa.field(name, ARROW_TYPES[draken_type.name], nullable=True))
    return pa.schema(fields)


def convert(job):
    """One source file -> its shards. Returns (source, rows read, rows written)."""
    from rugo import parquet

    file_index, source, out_dir, codec = job
    rows_read = 0
    rows_written = 0
    shard = 0
    in_shard = 0
    writer = None
    schema = None
    names = None

    def open_shard():
        nonlocal shard, writer, in_shard
        path = os.path.join(out_dir, f"hits_{file_index:03d}_{shard:02d}.orc")
        shard += 1
        in_shard = 0
        writer = po.ORCWriter(
            path,
            file_version="0.12",
            stripe_size=64 * 1024 * 1024,
            compression=codec,
            compression_block_size=256 * 1024,
            row_index_stride=10000,
            dictionary_key_size_threshold=0.8,
        )

    def close_shard():
        nonlocal writer
        writer.close()
        writer = None

    with parquet.read_parquet(source) as reader:
        for morsel in reader:
            if names is None:
                names = [n.decode() if type(n) is bytes else n for n in morsel.column_names]
                schema = arrow_schema(names, morsel.column_types)
            table = pa.table(
                {name: pa.array(morsel.column(name).to_pylist(), type=schema.field(name).type) for name in names},
                schema=schema,
            )
            rows_read += table.num_rows
            offset = 0
            while offset < table.num_rows:
                if writer is None:
                    open_shard()
                take = min(table.num_rows - offset, ROWS_PER_SHARD - in_shard)
                writer.write(table.slice(offset, take))
                rows_written += take
                in_shard += take
                offset += take
                if in_shard == ROWS_PER_SHARD:
                    close_shard()
    if writer is not None:
        close_shard()
    return source, rows_read, rows_written


def main():
    if len(sys.argv) not in (3, 4):
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    source_dir, out_dir = sys.argv[1:3]
    codec = sys.argv[3] if len(sys.argv) == 4 else "zstd"
    if os.path.exists(out_dir):
        sys.exit(f"{out_dir} already exists - remove it first")
    sources = sorted(
        os.path.join(source_dir, name) for name in os.listdir(source_dir) if name.endswith(".parquet")
    )
    if not sources:
        sys.exit(f"no parquet files in {source_dir}")

    partial = out_dir + ".partial"
    shutil.rmtree(partial, ignore_errors=True)
    os.makedirs(partial)

    jobs = [(index, source, partial, codec) for index, source in enumerate(sources)]
    total_read = total_written = 0
    with ProcessPoolExecutor(max_workers=min(len(jobs), os.cpu_count() or 1)) as pool:
        for source, rows_read, rows_written in pool.map(convert, jobs):
            print(f"{os.path.basename(source)}: {rows_written:,} rows", flush=True)
            if rows_read != rows_written or rows_read == 0:
                sys.exit(f"{source}: read {rows_read} rows, wrote {rows_written}")
            total_read += rows_read
            total_written += rows_written

    os.rename(partial, out_dir)
    shards = len(os.listdir(out_dir))
    print(f"{total_written:,} rows -> {shards} shards in {out_dir} ({codec}, pyarrow.orc)")


if __name__ == "__main__":
    main()
