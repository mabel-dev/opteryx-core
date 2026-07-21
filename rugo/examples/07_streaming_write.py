"""
07_streaming_write.py — Stream several morsels to a Parquet file at constant
memory (one row group per morsel), then round-trip it.

Unlike write_parquet (whole morsel in, whole file out), open_parquet_writer
writes one row group per write_row_group() call and pushes each chunk of bytes
to a sink as it goes — peak memory stays ~one row group regardless of how many
batches are written.

To run, execute:
    python 07_streaming_write.py
"""
import os, tempfile

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import read_metadata, read_parquet, open_parquet_writer


def make_batch(names, radii, rocky, moons):
    return Morsel.from_vectors(
        [b"name", b"radius_km", b"rocky", b"moons"],
        [vector_from_sequence(names, DrakenType.VARCHAR),
         vector_from_sequence(radii, DrakenType.FLOAT64),
         vector_from_sequence(rocky, DrakenType.BOOL),
         vector_from_sequence(moons, DrakenType.INT64)],
    )


batches = [
    make_batch(["Mercury", "Venus"], [2439.7, 6051.8], [True, True], [0, 0]),
    make_batch(["Earth", "Mars"], [6371.0, 3389.5], [True, True], [1, 2]),
    make_batch(["Jupiter"], [69911.0], [False], [95]),
]

# ── Stream each batch to a file as its own row group ─────────────────────────
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
    path = f.name

try:
    with open(path, "wb") as out:
        with open_parquet_writer(out.write) as writer:
            for batch in batches:
                writer.write_row_group(batch)
    print(f"wrote: {os.path.getsize(path)} bytes")

    meta = read_metadata(path)
    print(f"rows: {meta.num_rows}")
    for col in meta.schema_columns:
        print(f"  {col.name:15s}  {col.physical_type}")

    # ── Round-trip read: one Morsel per row group written ─────────────────────
    print()
    with read_parquet(path) as reader:
        for i, morsel in enumerate(reader):
            names = morsel.column(b"name").to_pylist()
            print(f"row group {i}: {names}")

finally:
    os.unlink(path)
