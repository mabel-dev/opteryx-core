"""Generate ORC fixtures and benchmark native fixed-width stripe decoding.

Run via `make bench-orc-fixed`; see dev/bench_orc_fixed_baseline_20261027.md.
PyArrow is used only as an independent development-time ORC writer. The timed
loop is C++, calling the existing compiled rugo reader directly.
"""

import argparse
from array import array
import ctypes
import hashlib
import io
import json
from pathlib import Path
import platform
import statistics
import struct
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pyarrow as pa  # noqa: E402
import pyarrow.orc as orc  # noqa: E402
import rugo.rugo_native as native  # noqa: E402

MASK = (1 << 64) - 1
# Name, Arrow writer type, ORC kind, source Draken type, destination type,
# value distribution. Widening cases use precisely the same inputs as the base.
TYPES = (
    ("BYTE", pa.int8(), 1, 1, 1, "int8"),
    ("BYTE->SHORT", pa.int8(), 1, 1, 2, "int8"),
    ("BYTE->INT", pa.int8(), 1, 1, 3, "int8"),
    ("BYTE->LONG", pa.int8(), 1, 1, 4, "int8"),
    ("SHORT", pa.int16(), 2, 2, 2, "int16"),
    ("SHORT->INT", pa.int16(), 2, 2, 3, "int16"),
    ("SHORT->LONG", pa.int16(), 2, 2, 4, "int16"),
    ("INT", pa.int32(), 3, 3, 3, "int32"),
    ("INT->LONG", pa.int32(), 3, 3, 4, "int32"),
    ("LONG", pa.int64(), 4, 4, 4, "int64"),
    ("FLOAT", pa.float32(), 5, 20, 20, "float32"),
    ("FLOAT->DOUBLE", pa.float32(), 5, 20, 21, "float32"),
    ("DOUBLE", pa.float64(), 6, 21, 21, "float64"),
    ("DATE", pa.date32(), 15, 30, 30, "date"),
    ("TIMESTAMP", pa.timestamp("us"), 9, 40, 40, "timestamp"),
    ("TIMESTAMP_INSTANT", pa.timestamp("us", tz="UTC"), 18, 40, 40, "timestamp"),
)
NULL_PERMILLE = (0, 10, 500, 900)


def mix(value):
    value = (value + 0x9E3779B97F4A7C15) & MASK
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK
    return value ^ (value >> 31)


def fixture(rows, dtype, distribution, null_permille):
    expected = array("q")
    valid = bytearray()
    values = []
    for i in range(rows):
        x = mix(i + 0x5EED)
        present = mix(i + 0xC0FFEE) % 1000 >= null_permille
        if distribution.startswith("int"):
            bits = int(distribution[3:])
            value = x & ((1 << bits) - 1)
            if value & (1 << (bits - 1)):
                value -= 1 << bits
        elif distribution == "date":
            value = x % 73001 - 36500  # days around the Unix epoch
        elif distribution == "timestamp":
            value = 1577836800000000 + x % 31536000000000  # 2020, microseconds UTC
        elif distribution == "float32":
            value = ((x & 0xFFFFFFFF) - (1 << 31)) / 1024.0
            value = struct.unpack("=f", struct.pack("=f", value))[0]
        elif distribution == "float64":
            value = ((x & ((1 << 53) - 1)) - (1 << 52)) / 1024.0
        else:
            raise ValueError(f"unknown fixture distribution: {distribution}")
        # Floating-point results are checked by their binary64 representation;
        # a FLOAT destination is exactly promoted to binary64 in the native bridge.
        expected.append(struct.unpack("=q", struct.pack("=d", value))[0]
                        if distribution.startswith("float") else value)
        valid.append(present)
        values.append(value if present else None)
    table = pa.table({"v": pa.array(values, type=dtype)})
    out = io.BytesIO()
    orc.write_table(table, out, file_version="0.12", compression="uncompressed",
                    stripe_size=64 * 1024 * 1024, row_index_stride=10000)
    return out.getvalue(), expected, valid


def positive(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("bridge", help="path to the native benchmark bridge built by make")
    ap.add_argument("--rows", type=positive, default=65536)
    ap.add_argument("--rounds", type=positive, default=7)
    ap.add_argument("--iterations", type=positive, default=32)
    ap.add_argument("--output", help="optional JSON result path")
    args = ap.parse_args()
    if args.rows > 0xFFFFFFFF or args.iterations > 0xFFFFFFFF:
        ap.error("rows and iterations must fit in uint32")
    binary = Path(native.__file__).resolve()
    for source in ("orc_reader.cpp", "orc_reader.hpp", "orc_proto.hpp", "orc_rle.hpp"):
        path = ROOT / "rugo" / "src" / "orc" / source
        if path.stat().st_mtime_ns > binary.stat().st_mtime_ns:
            sys.exit(f"{path} is newer than {binary}; run make c before benchmarking")

    # Promote the already-imported reader's symbols for the bridge. The bridge
    # does not rebuild or replace the production reader being measured.
    reader = ctypes.CDLL(str(binary), mode=ctypes.RTLD_GLOBAL)
    bridge = ctypes.CDLL(args.bridge)
    bridge.orc_fixed_create.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32,
                                        ctypes.c_uint, ctypes.c_uint, ctypes.c_uint,
                                        ctypes.c_void_p, ctypes.c_void_p]
    bridge.orc_fixed_create.restype = ctypes.c_void_p
    bridge.orc_fixed_sample.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_uint64)]
    bridge.orc_fixed_sample.restype = ctypes.c_double
    bridge.orc_fixed_destroy.argtypes = [ctypes.c_void_p]
    bridge.orc_fixed_destroy.restype = None

    cases = []
    for name, dtype, kind, source_type, output_type, distribution in TYPES:
        for density in NULL_PERMILLE:
            data, expected, valid = fixture(args.rows, dtype, distribution, density)
            backing = ctypes.create_string_buffer(data, len(data))
            expected_view = (ctypes.c_int64 * args.rows).from_buffer(expected)
            valid_view = (ctypes.c_uint8 * args.rows).from_buffer(valid)
            handle = bridge.orc_fixed_create(backing, len(data), args.rows, kind, source_type,
                                             output_type, expected_view, valid_view)
            cases.append({"name": name, "null_permille": density, "nulls": args.rows - sum(valid),
                          "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                          "backing": backing, "handle": handle, "samples": []})

    print(f"ORC fixed-width: {args.rows} rows, {args.rounds} rounds, {args.iterations} decodes/round; "
          f"PyArrow {pa.__version__}, {platform.platform()}", flush=True)
    print(f"rugo_native sha256={hashlib.sha256(binary.read_bytes()).hexdigest()}", flush=True)
    print("All rows and validity verified before timing; one warm-up, alternating case order.", flush=True)
    checksum = ctypes.c_uint64()
    for round_no in range(args.rounds + 1):
        for c in cases if round_no % 2 == 0 else reversed(cases):
            ns = bridge.orc_fixed_sample(c["handle"], args.iterations, ctypes.byref(checksum))
            if round_no:
                c["samples"].append(ns)

    print(f"{'type':18} {'null%':>6} {'nulls':>7} {'bytes':>9} {'fixture sha256':64} "
          f"{'median ns/row':>13} {'min':>9} {'max':>9}")
    results = []
    for c in cases:
        med = statistics.median(c["samples"])
        print(f"{c['name']:18} {c['null_permille'] / 10:6.1f} {c['nulls']:7} {c['bytes']:9} "
              f"{c['sha256']:64} {med:13.3f} {min(c['samples']):9.3f} {max(c['samples']):9.3f}")
        results.append({k: v for k, v in c.items() if k not in ("backing", "handle")})
        bridge.orc_fixed_destroy(c["handle"])
    print(f"checksum={checksum.value}")
    if args.output:
        report = {"rows": args.rows, "rounds": args.rounds, "iterations": args.iterations,
                  "pyarrow": pa.__version__, "python": sys.version, "platform": platform.platform(),
                  "rugo_native_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                  "checksum": checksum.value, "cases": results}
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    # Keep both dynamic libraries alive through handle destruction.
    del reader


if __name__ == "__main__":
    main()
