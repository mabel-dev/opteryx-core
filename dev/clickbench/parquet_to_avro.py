"""
Convert a parquet hits split into Avro object container files for
`make clickbench-avro`.

    python dev/clickbench/parquet_to_avro.py <parquet_dir> <out_dir> <codec>

Reads with rugo, writes with fastavro (dev-only; the engine never imports it).
Each source file is converted on its own process and cut into 1,000,000-row
shards, <out_dir>/hits_<file>_<shard>.avro - the same shard size as the json
variant (dev/clickbench/split_hits_json.sh).

Types are the source's, as close as Avro has them: INT16 / UINT16 / INT32 ->
`int` (Avro has no 16-bit int), INT64 -> `long`, VARBINARY -> `bytes`,
VARCHAR -> `string`, FLOAT64 -> `double`. Fields are required - hits has no
NULLs, and a NULL makes fastavro raise, never a silent default. Any other
type is refused.

Blocks are cut every 64,000 bytes - the JVM writer's default (Avro Java
DataFileConstants.DEFAULT_SYNC_INTERVAL), which is what Kafka Connect, Spark
and Iceberg write with; fastavro's own default is 16,000.

Written into <out_dir>.partial and renamed only when every source row is
accounted for, so an interrupted run never leaves a short tree that looks
complete.
"""

import os
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor

import fastavro

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

ROWS_PER_SHARD = 1_000_000
SYNC_INTERVAL = 64_000

AVRO_TYPES = {
    "INT16": "int",
    "UINT16": "int",
    "INT32": "int",
    "INT64": "long",
    "VARBINARY": "bytes",
    "VARCHAR": "string",
    "FLOAT64": "double",
}


def avro_schema(names, types):
    fields = []
    for name, draken_type in zip(names, types):
        type_name = draken_type.name
        if type_name not in AVRO_TYPES:
            raise ValueError(f"column {name!r}: {type_name} has no Avro mapping here")
        fields.append({"name": name, "type": AVRO_TYPES[type_name]})
    return {"type": "record", "name": "hits", "fields": fields}


def convert(job):
    """One source file -> its shards. Returns (rows read, rows written)."""
    from rugo import parquet

    file_index, source, out_dir, codec = job
    rows_read = 0
    rows_written = 0
    shard = 0
    handle = None
    writer = None
    names = None

    def open_shard():
        nonlocal shard, handle, writer
        path = os.path.join(out_dir, f"hits_{file_index:03d}_{shard:02d}.avro")
        shard += 1
        handle = open(path, "wb")
        writer = fastavro.write.Writer(handle, schema, codec=codec, sync_interval=SYNC_INTERVAL)

    def close_shard():
        nonlocal handle, writer
        writer.flush()
        handle.close()
        handle = None
        writer = None

    in_shard = 0
    with parquet.read_parquet(source) as reader:
        for morsel in reader:
            if names is None:
                names = [n.decode() if type(n) is bytes else n for n in morsel.column_names]
                schema = fastavro.parse_schema(avro_schema(names, morsel.column_types))
            columns = [morsel.column(name).to_pylist() for name in names]
            rows_read += morsel.num_rows
            for row in zip(*columns):
                if writer is None:
                    open_shard()
                writer.write(dict(zip(names, row)))
                rows_written += 1
                in_shard += 1
                if in_shard == ROWS_PER_SHARD:
                    close_shard()
                    in_shard = 0
    if writer is not None:
        close_shard()
    return source, rows_read, rows_written


def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    source_dir, out_dir, codec = sys.argv[1:]
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
    with ProcessPoolExecutor(max_workers=min(len(jobs), os.cpu_count())) as pool:
        for source, rows_read, rows_written in pool.map(convert, jobs):
            print(f"{os.path.basename(source)}: {rows_written:,} rows", flush=True)
            if rows_read != rows_written or rows_read == 0:
                sys.exit(f"{source}: read {rows_read} rows, wrote {rows_written}")
            total_read += rows_read
            total_written += rows_written

    os.rename(partial, out_dir)
    shards = len(os.listdir(out_dir))
    print(f"{total_written:,} rows -> {shards} shards in {out_dir} ({codec})")


if __name__ == "__main__":
    main()
