import os, sys
import opteryx
import draken.draken_native as dn
from draken.vectors.vector import Vector
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

_VALUES = list(range(0, 200_000, 2))

def write_dataset(folder, bloom):
    os.makedirs(folder, exist_ok=True)
    morsel = Morsel.from_vectors(["i"], [Vector(dn.vector_from_sequence(_VALUES))])
    path = os.path.join(folder, "part.parquet")
    with open(path, "wb") as fh:
        fh.write(write_parquet(morsel, bloom_filters=bloom, dictionary=False))
    return path

base = "/Users/justin/Nextcloud/opteryx-core/scratch_bloomtest"
on = os.path.join(base, "on")
off = os.path.join(base, "off")
p_on = write_dataset(on, True)
p_off = write_dataset(off, False)
print("on size", os.path.getsize(p_on))
print("off size", os.path.getsize(p_off))

from rugo import rugo_native as pr

for label, path in [("ON", p_on), ("OFF", p_off)]:
    with open(path, "rb") as fh:
        data = fh.read()
    rgs = pr.read_rowgroup_stats(data)
    print(f"--- {label} --- num_row_groups={len(rgs)}")
    for rg in rgs:
        print("  num_rows:", rg["num_rows"])
        for c in rg["columns"]:
            print("   col:", c["name"], "phys:", c["physical_type"],
                  "min:", c["min"], "max:", c["max"],
                  "bloom_offset:", c["bloom_offset"], "bloom_length:", c["bloom_length"])

print("=== direct bloom probe test (off dataset, should have no bloom) ===")
from rugo import rugo_native as rn
import struct
off_rgs = rn.read_rowgroup_stats(open(p_off, "rb").read())
col = off_rgs[0]["columns"][0]
print("off bloom_offset:", col["bloom_offset"], "bloom_length:", col["bloom_length"])

on_rgs = rn.read_rowgroup_stats(open(p_on, "rb").read())
col_on = on_rgs[0]["columns"][0]
print("on bloom_offset:", col_on["bloom_offset"], "bloom_length:", col_on["bloom_length"])

# probe bloom filter directly for value 5 on the ON dataset
v5_bytes = struct.pack("<q", 5)
print("ON: bloom_filter_maybe_contains(5) =", rn.bloom_filter_maybe_contains(p_on, col_on["bloom_offset"], col_on["bloom_length"], v5_bytes))
v4_bytes = struct.pack("<q", 4)
print("ON: bloom_filter_maybe_contains(4) =", rn.bloom_filter_maybe_contains(p_on, col_on["bloom_offset"], col_on["bloom_length"], v4_bytes))
