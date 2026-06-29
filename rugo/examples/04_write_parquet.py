"""
04_write_parquet.py — Write a Parquet file and round-trip it.

Run from the repo root:
    python rugo/examples/04_write_parquet.py
"""
import os, sys, tempfile
sys.path.insert(0, os.path.join(sys.path[0], "..", ".."))

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet_writer import write_parquet
from rugo.parquet_reader import read_parquet, read_metadata

# ── Build a Morsel from Python lists ─────────────────────────────────────────
names  = vector_from_sequence(["Mercury", "Venus", "Earth", "Mars", "Jupiter"], DrakenType.VARCHAR)
radii  = vector_from_sequence([2439.7, 6051.8, 6371.0, 3389.5, 69911.0],       DrakenType.FLOAT64)
rocky  = vector_from_sequence([True, True, True, True, False],                   DrakenType.BOOL)
moons  = vector_from_sequence([0, 0, 1, 2, 95],                                 DrakenType.INT64)

morsel = Morsel.from_vectors(
    [b"name", b"radius_km", b"rocky", b"moons"],
    [names, radii, rocky, moons],
)

# ── Serialize to Parquet bytes ────────────────────────────────────────────────
parquet_bytes = write_parquet(morsel)
print(f"serialized: {len(parquet_bytes)} bytes")

# ── Write to file and inspect footer ─────────────────────────────────────────
with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
    f.write(parquet_bytes)
    path = f.name

try:
    meta = read_metadata(path)
    print(f"rows: {meta.num_rows}")
    for col in meta.schema_columns:
        print(f"  {col.name:15s}  {col.physical_type}")

    # ── Round-trip read ───────────────────────────────────────────────────────
    print()
    morsels = read_parquet(parquet_bytes)
    m = morsels[0]
    for col_name in m.column_names:
        vec = m.column(col_name)
        label = col_name.decode() if isinstance(col_name, bytes) else col_name
        print(f"{label}: {vec.to_pylist()}")

finally:
    os.unlink(path)
