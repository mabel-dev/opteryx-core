"""Measure the existing native ORC reader on deterministic PRESENT layouts.

Run through make bench-orc-present; see dev/bench_orc_present_baseline_20261028.md.
Fixtures and the original native binaries are archived once; subsequent runs use
those exact ORC bytes and verify every decoded validity bit and valid value.
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
import zipfile

from bench_orc_fixed import ROOT, mix, positive
import draken.draken_native as draken_native
import pyarrow as pa
import pyarrow.orc as orc
import rugo.rugo_native as native

TYPES = {
    "byte": (pa.int8(), 1, 1, 8),
    "int": (pa.int32(), 3, 3, 32),
    "long": (pa.int64(), 4, 4, 64),
    "double": (pa.float64(), 6, 21, 0),
}
LAYOUTS = ("no_nulls", "random_1", "random_50", "random_90", "alternating_0",
           "alternating_1", "clustered_1", "clustered_50", "clustered_90", "all_null")
BOUNDARIES = (1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 127, 128, 129, 1023, 1024, 1025)


def suite(rows):
    cases = []
    for dtype in ("long", "double"):
        for layout in LAYOUTS:
            cases.append({"name": f"{dtype}_{layout}_{rows}", "rows": rows,
                          "columns": [[dtype, layout]]})
    for n in (*BOUNDARIES, rows - 1, rows + 1):
        for layout in ("alternating_0", "alternating_1"):
            cases.append({"name": f"int_{layout}_{n}", "rows": n,
                          "columns": [["int", layout]]})
    cases.append({"name": f"mixed_{rows}", "rows": rows,
                  "columns": [["byte", "random_1"], ["int", "alternating_0"],
                              ["long", "clustered_90"], ["double", "clustered_50"]]})
    return cases


def is_valid(i, layout):
    if layout == "no_nulls":
        return True
    if layout == "all_null":
        return False
    if layout.startswith("random_"):
        return mix(i + 0xC0FFEE) % 100 >= int(layout[7:])
    if layout.startswith("alternating_"):
        return i % 2 == int(layout[-1])
    if layout.startswith("clustered_"):
        # 1000-row periods, with one contiguous null run at the beginning.
        return i % 1000 >= int(layout[10:]) * 10
    raise ValueError(f"unknown layout {layout}")


def column_data(rows, dtype, layout, column):
    arrow_type, _, _, bits = TYPES[dtype]
    expected = array("q")
    valid = bytearray()
    values = []
    for i in range(rows):
        x = mix(i + 0x5EED + column * 0x100000)
        if bits:
            value = x & ((1 << bits) - 1)
            if value & (1 << (bits - 1)):
                value -= 1 << bits
            expected.append(value)
        else:
            value = ((x & ((1 << 53) - 1)) - (1 << 52)) / 1024.0
            expected.append(struct.unpack("=q", struct.pack("=d", value))[0])
        present = is_valid(i, layout)
        valid.append(present)
        values.append(value if present else None)
    return pa.array(values, type=arrow_type), expected, valid


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("bridge")
    ap.add_argument("--fixtures", required=True, help="fixture/binary archive (never overwritten)")
    ap.add_argument("--create-fixtures", action="store_true", help="create archive rather than reuse it")
    ap.add_argument("--rows", type=positive, default=65536)
    ap.add_argument("--rounds", type=positive, default=9)
    ap.add_argument("--iterations", type=positive, default=64)
    ap.add_argument("--output", required=True, help="new JSON sample file (never overwritten)")
    ap.add_argument("--allow-stale-reader", action="store_true",
                    help="explicitly measure existing binary even if ORC sources have newer timestamps")
    args = ap.parse_args()
    if args.rows <= 1025 or args.rows >= 0xFFFFFFFF or args.iterations > 0xFFFFFFFF:
        ap.error("rows must be 1026..4294967294 and iterations must fit uint32")
    binary = Path(native.__file__).resolve()
    draken_binary = Path(draken_native.__file__).resolve()
    reader_hash = digest(binary.read_bytes())
    draken_hash = digest(draken_binary.read_bytes())
    source_paths = [ROOT / "rugo" / "src" / "orc" / s for s in
                    ("orc_reader.cpp", "orc_reader.hpp", "orc_proto.hpp", "orc_rle.hpp")]
    sources = {str(p.relative_to(ROOT)): {"sha256": digest(p.read_bytes()), "mtime_ns": p.stat().st_mtime_ns}
               for p in source_paths}
    newer = [name for name, info in sources.items() if info["mtime_ns"] > binary.stat().st_mtime_ns]
    if newer and not args.allow_stale_reader:
        sys.exit(f"ORC sources newer than reader: {newer}. Rebuild or explicitly use --allow-stale-reader.")
    if newer:
        print(f"WARNING: measuring existing binary despite newer ORC sources: {newer}", flush=True)
    if Path(args.output).exists():
        sys.exit(f"output already exists, refusing to overwrite: {args.output}")
    specs = suite(args.rows)
    archive_path = Path(args.fixtures)
    if args.create_fixtures and archive_path.exists():
        sys.exit(f"fixture archive already exists, refusing to overwrite: {archive_path}")
    if not args.create_fixtures and not archive_path.is_file():
        sys.exit(f"fixture archive does not exist: {archive_path}")
    print(f"Preparing {len(specs)} cases; archive={archive_path}", flush=True)

    reader = ctypes.CDLL(str(binary), mode=ctypes.RTLD_GLOBAL)
    bridge = ctypes.CDLL(args.bridge)
    bridge.orc_present_create.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_uint32, ctypes.c_uint,
                                          ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    bridge.orc_present_create.restype = ctypes.c_void_p
    bridge.orc_present_sample.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.POINTER(ctypes.c_uint64)]
    bridge.orc_present_sample.restype = ctypes.c_double
    bridge.orc_present_destroy.argtypes = [ctypes.c_void_p]
    bridge.orc_present_destroy.restype = None

    # Exclusive creation protects the baseline and the archived binary snapshots.
    if args.create_fixtures:
        archive_file = archive_path.open("xb")
        archive = zipfile.ZipFile(archive_file, "w", compression=zipfile.ZIP_DEFLATED)
        archive.writestr("suite.json", json.dumps(specs, indent=2) + "\n")
        for label, path in (("rugo", binary), ("draken", draken_binary)):
            archive.writestr(f"binaries/{label}/{path.name}", path.read_bytes())
    else:
        archive_file = archive_path.open("rb")
        archive = zipfile.ZipFile(archive_file, "r")
        if json.loads(archive.read("suite.json")) != specs:
            sys.exit("fixture archive suite differs from requested case definitions")

    cases = []
    for spec in specs:
        expected = array("q")
        valid = bytearray()
        arrays = {}
        nulls = []
        kinds = []
        types = []
        for j, (dtype, layout) in enumerate(spec["columns"]):
            column, exp, mask = column_data(spec["rows"], dtype, layout, j)
            arrays[f"v{j}"] = column
            expected.extend(exp)
            valid.extend(mask)
            nulls.append(spec["rows"] - sum(mask))
            kinds.append(TYPES[dtype][1])
            types.append(TYPES[dtype][2])
        member = f"fixtures/{spec['name']}.orc"
        if args.create_fixtures:
            out = io.BytesIO()
            orc.write_table(pa.table(arrays), out, file_version="0.12", compression="uncompressed",
                            stripe_size=64 * 1024 * 1024, row_index_stride=10000)
            data = out.getvalue()
            archive.writestr(member, data)
        else:
            data = archive.read(member)
        n = spec["rows"] * len(types)
        backing = ctypes.create_string_buffer(data, len(data))
        exp_view = (ctypes.c_int64 * n).from_buffer(expected)
        valid_view = (ctypes.c_uint8 * n).from_buffer(valid)
        kind_view = (ctypes.c_uint * len(kinds))(*kinds)
        type_view = (ctypes.c_uint * len(types))(*types)
        print(f"  validating {spec['name']} nulls={nulls}", flush=True)
        handle = bridge.orc_present_create(backing, len(data), spec["rows"], len(types), kind_view,
                                           type_view, exp_view, valid_view)
        cases.append({**spec, "nulls": nulls, "bytes": len(data), "sha256": digest(data),
                      "backing": backing, "handle": handle, "samples": []})
    archive.close()
    archive_file.close()

    print(f"Timing {args.rounds} rounds, {args.iterations} decodes/round; one warm-up, alternating order.",
          flush=True)
    print(f"rugo_native sha256={reader_hash}", flush=True)
    checksum = ctypes.c_uint64()
    for round_no in range(args.rounds + 1):
        for case in cases if round_no % 2 == 0 else reversed(cases):
            ns = bridge.orc_present_sample(case["handle"], args.iterations, ctypes.byref(checksum))
            if round_no:
                case["samples"].append(ns)
    for case in cases:
        print(f"{case['name']:36} {statistics.median(case['samples']):9.3f} ns/column-row "
              f"[{min(case['samples']):.3f}, {max(case['samples']):.3f}]")
        bridge.orc_present_destroy(case["handle"])
    if digest(binary.read_bytes()) != reader_hash or digest(draken_binary.read_bytes()) != draken_hash:
        sys.exit("native binary changed during benchmark")
    report = {"rows": args.rows, "rounds": args.rounds, "iterations": args.iterations,
              "fixture_archive": str(archive_path), "fixture_archive_sha256": digest(archive_path.read_bytes()),
              "pyarrow": pa.__version__, "python": sys.version, "platform": platform.platform(),
              "rugo_native_sha256": reader_hash, "rugo_native_mtime_ns": binary.stat().st_mtime_ns,
              "draken_native_sha256": draken_hash, "orc_sources": sources,
              "newer_sources": newer, "allow_stale_reader": args.allow_stale_reader,
              "bridge_sha256": digest(Path(args.bridge).read_bytes()),
              "benchmark_source_sha256": digest(Path(__file__).read_bytes()),
              "bridge_source_sha256": digest((ROOT / "dev/bench_orc_fixed.cpp").read_bytes()),
              "checksum": checksum.value,
              "cases": [{k: v for k, v in c.items() if k not in ("backing", "handle")} for c in cases]}
    with Path(args.output).open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    print(f"checksum={checksum.value}; samples written to {args.output}")
    del reader


if __name__ == "__main__":
    main()
