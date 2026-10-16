"""
Convert a parquet hits split into Avro object container files for
`make clickbench-avro`.

    python dev/clickbench/parquet_to_avro.py <parquet_dir> <out_dir> <codec> <writer>

Reads with rugo; <writer> is `rugo` (rugo.avro.write_avro) or `fastavro`
(dev-only; the engine never imports either here). Each source file is converted
on its own process and cut into 1,000,000-row shards,
<out_dir>/hits_<file>_<shard>.avro - the same shard size as the json variant
(dev/clickbench/split_hits_json.sh).

`rugo` writes each shard in one call at the writer's defaults
(docs/AVRO_WRITER_DESIGN.md): 1 MiB blocks, every hits field required (no
NULLs), libdeflate. hits' text is VARBINARY, so it is `bytes` either way and no
column is an enum (and none could be: every low-cardinality text column holds a
value that is not an Avro name, doc §14). `fastavro` writes what a JVM writer
would, below.

Types are the source's, as close as Avro has them: INT16 / UINT16 / INT32 ->
`int` (Avro has no 16-bit int), INT64 -> `long`, VARBINARY -> `bytes`,
VARCHAR -> `string`, FLOAT64 -> `double`. Fields are required - hits has no
NULLs, and a NULL makes fastavro raise, never a silent default. Any other
type is refused.

With `fastavro`, blocks are cut every 64,000 bytes - the JVM writer's default
(Avro Java DataFileConstants.DEFAULT_SYNC_INTERVAL), which is what Kafka
Connect, Spark and Iceberg write with; fastavro's own default is 16,000.

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


def convert_rugo(job):
    """One source file -> its shards, each written by rugo.avro.write_avro in one call
    (a one-shot write sees the whole shard: required fields, enums where they apply).
    Returns (source, rows read, rows written)."""
    from draken.morsels.morsel import Morsel
    from rugo import avro, parquet

    file_index, source, out_dir, codec = job
    rows_read = 0
    rows_written = 0
    shard = 0
    pending = []
    in_shard = 0

    def flush():
        nonlocal shard, pending, in_shard, rows_written
        morsel = Morsel.combine(pending)
        path = os.path.join(out_dir, f"hits_{file_index:03d}_{shard:02d}.avro")
        with open(path, "wb") as f:
            f.write(avro.write_avro(morsel, codec=codec, record_name="hits"))
        rows_written += morsel.num_rows
        shard += 1
        pending = []
        in_shard = 0

    with parquet.read_parquet(source) as reader:
        for morsel in reader:
            rows_read += morsel.num_rows
            offset = 0
            while offset < morsel.num_rows:
                take = min(morsel.num_rows - offset, ROWS_PER_SHARD - in_shard)
                pending.append(morsel if take == morsel.num_rows else morsel.slice(offset, take))
                in_shard += take
                offset += take
                if in_shard == ROWS_PER_SHARD:
                    flush()
    if pending:
        flush()
    return source, rows_read, rows_written


WRITERS = {"fastavro": (convert, os.cpu_count()), "rugo": (convert_rugo, 4)}


def main():
    if len(sys.argv) != 5:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    source_dir, out_dir, codec, writer = sys.argv[1:]
    if writer not in WRITERS:
        sys.exit(f"writer must be one of {sorted(WRITERS)}, got {writer!r}")
    convert_fn, max_workers = WRITERS[writer]
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
    # rugo: four processes, each write already spans every core and holds a 1M-row shard.
    with ProcessPoolExecutor(max_workers=min(len(jobs), max_workers)) as pool:
        for source, rows_read, rows_written in pool.map(convert_fn, jobs):
            print(f"{os.path.basename(source)}: {rows_written:,} rows", flush=True)
            if rows_read != rows_written or rows_read == 0:
                sys.exit(f"{source}: read {rows_read} rows, wrote {rows_written}")
            total_read += rows_read
            total_written += rows_written

    os.rename(partial, out_dir)
    shards = len(os.listdir(out_dir))
    print(f"{total_written:,} rows -> {shards} shards in {out_dir} ({codec}, {writer})")


if __name__ == "__main__":
    main()
