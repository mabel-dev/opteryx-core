#!/usr/bin/env python3
"""
A tour of the skene Python boundary: write a file, read it back, and use the
parts of the format that only exist because reads matter more than writes.

One `.skene` file is one row group of draken vectors. The whole Python surface
is five functions:

    write_morsel(morsel, **options) -> bytes
    read_morsel(buf, columns=None)  -> Morsel
    read_metadata(buf)              -> dict     (footer only; never decodes data)
    probe_version(head)             -> int      (first 8 bytes)
    footer_extent(tail, file_bytes) -> (offset, nbytes)

Run it:

    python skene/examples/01_write_and_read.py
    python skene/examples/01_write_and_read.py testdata/tpch_1/nation/nation.parquet

Section 6 converts a Parquet file with rugo — skene has no reader for foreign
formats, so Parquet comes in through rugo and leaves as draken Morsels, which
is the only thing skene writes.

Dev tooling / documentation only — never imported by production code (§5).
"""

from __future__ import annotations

import os
import sys
import tempfile
import time

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, _REPO_ROOT)

import skene  # noqa: E402
from draken.draken_native import DrakenType  # noqa: E402
from draken.interop.vector_sequence import vector_from_sequence  # noqa: E402
from draken.morsels.morsel import Morsel  # noqa: E402
from rugo.parquet import read_parquet  # noqa: E402

DEFAULT_PARQUET = os.path.join(_REPO_ROOT, "testdata", "tpch_1", "customer", "customer.parquet")

# Footer enums — skene/include/skene/format.h is the authority.
TYPE_NAMES = {t.value: t.name for t in DrakenType}
SELECTION_KINDS = {0: "constant", 1: "identity", 2: "stored"}
VALUE_ORDERS = {0: "as-written", 1: "ascending"}
STAT_FLAGS = [
    (1 << 0, "min"),
    (1 << 1, "max"),
    (1 << 2, "null_count"),
    (1 << 3, "sum"),
    (1 << 4, "row_sorted"),
    (1 << 5, "row_sorted_desc"),
]


def banner(title: str) -> None:
    print(f"\n{'─' * 78}\n{title}\n{'─' * 78}")


def stat_flag_names(flags: int) -> str:
    return ", ".join(name for bit, name in STAT_FLAGS if flags & bit) or "none"


def human_bytes(n: int) -> str:
    for unit, scale in (("MB", 1e6), ("KB", 1e3)):
        if n >= scale:
            return f"{n / scale:,.1f}{unit}"
    return f"{n}B"


def column_names(morsel: Morsel) -> list[str]:
    """Morsel identities are bytes; skene's API speaks str."""
    return [name.decode("utf-8") for name in morsel.column_names]


# ─── 1. Write a morsel, read it back ────────────────────────────────────────


def sample_morsel() -> Morsel:
    """Five rows, four types, nulls in three of them."""
    return Morsel.from_vectors(
        ["id", "colour", "score", "active"],
        [
            vector_from_sequence([5, 3, 3, None, 9], DrakenType.INT64),
            vector_from_sequence(["red", "blue", "red", "green", None], DrakenType.VARCHAR),
            vector_from_sequence([1.5, -0.0, 0.0, 2.25, None], DrakenType.FLOAT64),
            vector_from_sequence([True, False, True, True, False], DrakenType.BOOL),
        ],
    )


def section_roundtrip() -> bytes:
    banner("1. write_morsel → read_morsel: the round trip is lossless")

    morsel = sample_morsel()
    buf = skene.write_morsel(
        morsel,
        read_acceleration=True,  # value ordering + statistics + zone maps
        zstd_level=0,  # per-section compression off — see section 3
        writer_tag="examples/01",  # free-text provenance, lands in the footer
        field_ids=[101, 102, 103, 104],  # stable ids, for schema evolution
        created_at_unix_us=1_754_500_000_000_000,
    )
    print(f"wrote {len(buf)} bytes for {morsel.num_rows} rows × {morsel.num_columns} columns")

    back = skene.read_morsel(buf)
    back.materialize()  # Cxx-backed until asked; materialize() is in-place

    for name in column_names(morsel):
        original = morsel.column(name).to_pylist()
        restored = back.column(name).to_pylist()
        assert original == restored, f"{name}: {original} != {restored}"
        print(f"  {name:<8} {restored}")

    print("\n  Lossless means the DrakenType, the logical descriptor, the null mask")
    print("  and the dictionary selection all come back as they went in — skene is")
    print("  a serialization of draken vectors, not a conversion to another model.")
    return buf


# ─── 2. Footer-only metadata ────────────────────────────────────────────────


def section_metadata(buf: bytes) -> None:
    banner("2. read_metadata: the whole footer, without decoding a single value")

    # It parses the footer and validates the index sections next to it; the data
    # region is never touched. Section 6 shows what that means for a remote read.

    print(f"probe_version(first 8 bytes) = {skene.probe_version(buf[:8])}")

    meta = skene.read_metadata(buf)
    print(f"version={meta['version']}  rows={meta['row_count']}  "
          f"writer_tag={meta['writer_tag']!r}")
    print(f"file_uuid={meta['file_uuid'].hex()}  created_at_unix_us={meta['created_at_unix_us']}")

    for col in meta["columns"]:
        print(f"\n  {col['name']}  ({TYPE_NAMES.get(col['type'], col['type'])}, "
              f"field_id={col['field_id']})")
        print(f"    rows={col['length']}  distinct={col['data_length']}  "
              f"selection={SELECTION_KINDS.get(col['selection_kind'])}  "
              f"order={VALUE_ORDERS.get(col['value_order'])}")
        print(f"    bytes=[{col['byte_offset']}, +{col['byte_bytes']})  "
              f"bloom={col['has_bloom']}")
        stats = col["statistics"]
        if stats is not None:
            # Only the fields whose flag is set are meaningful — the rest are
            # zero and MUST NOT be read. "Absent" is never "zero".
            flags = stats["flags"]
            tracked = []
            if flags & (1 << 0):
                tracked.append(f"min_ordinal={stats['min_ordinal']}")
            if flags & (1 << 1):
                tracked.append(f"max_ordinal={stats['max_ordinal']}")
            if flags & (1 << 2):
                tracked.append(f"nulls={stats['null_count']}")
            if flags & (1 << 3):
                tracked.append(f"sum={stats['sum']}")
            print(f"    stats({stat_flag_names(flags)}): {'  '.join(tracked)}")
        if col["zone_map"] is not None:
            zm = col["zone_map"]
            print(f"    zone_map: {len(zm['chunks'])} chunk(s) of {zm['chunk_rows']} rows")

    # `data_length` under value ordering is the EXACT distinct count, not an
    # estimate — the writer deduplicated on the sorted values.
    ids = next(c for c in meta["columns"] if c["name"] == "id")
    print(f"\n  id has {ids['data_length']} distinct values across {ids['length']} rows "
          f"(exact, not a sketch)")
    print("  min/max are ORDINALS — draken's order-preserving projection of the value,")
    print("  not the value. For INT64 that projection is the identity, so 3 and 9 read")
    print("  literally; for VARCHAR it packs the first 8 bytes, which is MONOTONIC but")
    print("  NOT INJECTIVE — comparable for pruning, never decodable back to a string.")


# ─── 3. Projection ──────────────────────────────────────────────────────────


def section_projection(buf: bytes) -> None:
    banner("3. read_morsel(columns=...): projection is pushed, and it is strict")

    narrow = skene.read_morsel(buf, columns=["colour", "id"])
    narrow.materialize()
    print(f"asked for ['colour', 'id'] → got {column_names(narrow)}")
    print(f"  colour = {narrow.column('colour').to_pylist()}")

    # A column that is not there is an ERROR. Returning fewer columns than the
    # caller asked for would hide their bug.
    try:
        skene.read_morsel(buf, columns=["nope"])
    except skene.SkeneError as exc:
        print(f"asked for ['nope'] → SkeneError({exc.code}): {exc}")
    else:
        raise AssertionError("a missing column must not read successfully")


# ─── 6. The remote read path ────────────────────────────────────────────────

FILE_HEAD_BYTES = 16  # kFileHeadBytes
FILE_TAIL_BYTES = 24  # kFileTailBytes


def data_region_end(columns: list) -> int:
    """Last byte of the data region — where the INDEX region begins."""
    end = 0
    for col in columns:
        if col["byte_bytes"]:
            end = max(end, col["byte_offset"] + col["byte_bytes"])
        if col["children"]:
            end = max(end, data_region_end(col["children"]))
    return end


def section_remote(buf: bytes) -> None:
    banner("6. footer_extent: ranged GETs instead of pulling the whole object")

    file_bytes = len(buf)

    # Request 1: the last kFileTailBytes of the object. footer_extent validates
    # the tail before trusting it, then says where the footer lives.
    tail = buf[-FILE_TAIL_BYTES:]
    offset, nbytes = skene.footer_extent(tail, file_bytes)
    print(f"object is {file_bytes:,} bytes; the tail says the footer is "
          f"[{offset:,}, +{nbytes:,}) — {nbytes / file_bytes:.3%} of it")

    # Request 2 fetches from the start of the INDEX region to the end of the
    # object: blooms, zone maps and permutations sit contiguous with the footer
    # precisely so a pruning reader gets all of it in ONE request. (A remote
    # client that has no manifest over-fetches backwards by a slack; here the
    # boundary is measured from the file's own metadata.)
    index_start = data_region_end(skene.read_metadata(buf)["columns"])
    print(f"index region is [{index_start:,}, {offset:,}) = "
          f"{human_bytes(offset - index_start)} of blooms and zone maps")

    # Stitch what was fetched into an object-sized buffer. Nothing in the data
    # region is ever read, so those bytes stay zero.
    sparse = bytearray(file_bytes)
    sparse[:FILE_HEAD_BYTES] = buf[:FILE_HEAD_BYTES]
    sparse[index_start:] = buf[index_start:]
    fetched = FILE_HEAD_BYTES + (file_bytes - index_start)

    meta = skene.read_metadata(memoryview(bytes(sparse)))
    blooms = sum(1 for c in meta["columns"] if c["has_bloom"])
    print(f"parsed {len(meta['columns'])} columns, {meta['row_count']:,} rows and "
          f"{blooms} bloom filters from {fetched:,} fetched bytes "
          f"({fetched / file_bytes:.1%} of the object)")

    # Fetch too little and it fails loudly. A footer-only request on a file that
    # HAS index sections cannot silently return metadata with the blooms missing.
    short = bytearray(file_bytes)
    short[:FILE_HEAD_BYTES] = buf[:FILE_HEAD_BYTES]
    short[offset:] = buf[offset:]
    try:
        skene.read_metadata(memoryview(bytes(short)))
    except skene.SkeneError as exc:
        print(f"footer alone, index region missing → SkeneError({exc.code}) — "
              f"never a quiet metadata read with the blooms dropped")
    else:
        print("footer alone was sufficient — this file carries no index sections")


# ─── 4. Parquet in, skene out ───────────────────────────────────────────────


def section_parquet(parquet_path: str, out_dir: str) -> tuple[bytes, Morsel]:
    """Returns the first file's bytes and the morsel that produced it."""
    banner("4. rugo reads Parquet, skene writes it back out")

    print(f"source: {parquet_path}")
    src_bytes = os.path.getsize(parquet_path)
    stem = os.path.splitext(os.path.basename(parquet_path))[0]

    written = []
    total_rows = 0
    started = time.perf_counter()
    with read_parquet(parquet_path) as reader:
        # One parquet ROW GROUP is one skene FILE — that is the format's unit.
        for rg_index, morsel in enumerate(reader):
            payload = skene.write_morsel(morsel, read_acceleration=True, zstd_level=0,
                                         writer_tag="examples/01")
            out_path = os.path.join(out_dir, f"{stem}-rg{rg_index:04d}.skene")
            with open(out_path, "wb") as handle:
                handle.write(payload)
            written.append((out_path, morsel))
            total_rows += morsel.num_rows
    convert_s = time.perf_counter() - started

    dst_bytes = sum(os.path.getsize(path) for path, _ in written)
    print(f"{total_rows:,} rows → {len(written)} skene file(s) in {convert_s:.2f}s")
    print(f"{human_bytes(src_bytes)} parquet (zstd) → {human_bytes(dst_bytes)} skene "
          f"(uncompressed) = {dst_bytes / src_bytes:.2f}x on disk")

    # Read every file back and check it against the morsel that produced it.
    started = time.perf_counter()
    for path, source_morsel in written:
        with open(path, "rb") as handle:
            payload = handle.read()
        back = skene.read_morsel(payload)
        back.materialize()
        assert back.num_rows == source_morsel.num_rows
        assert list(back.column_names) == list(source_morsel.column_names)
        assert list(back.column_types) == list(source_morsel.column_types)
    read_s = time.perf_counter() - started
    print(f"read all {len(written)} file(s) back in {read_s * 1000:.0f}ms — row counts, "
          f"identities and types match")

    # Value parity on one column, spot-checked end to end — and the cost of the
    # projection next to the cost of the whole row group (single runs, cold).
    first_path, first_morsel = written[0]
    with open(first_path, "rb") as handle:
        payload = handle.read()
    name = column_names(first_morsel)[0]

    started = time.perf_counter()
    whole = skene.read_morsel(payload)
    whole.materialize()
    whole_ms = (time.perf_counter() - started) * 1000

    started = time.perf_counter()
    probe = skene.read_morsel(payload, columns=[name])
    probe.materialize()
    probe_ms = (time.perf_counter() - started) * 1000

    assert probe.column(name).to_pylist() == first_morsel.column(name).to_pylist()
    print(f"column {name!r} matches value-for-value ({first_morsel.num_rows:,} rows)")
    print(f"all {first_morsel.num_columns} columns: {whole_ms:.2f}ms   "
          f"just {name!r}: {probe_ms:.2f}ms — the projection is not decoded and thrown away")

    # The footer alone answers "how many rows, which columns, what types" — that
    # is what makes a manifest cheap to build over a directory of these.
    meta = skene.read_metadata(payload)
    print(f"\nfooter of {os.path.basename(first_path)}: rows={meta['row_count']:,}")
    for col in meta["columns"][:6]:
        stats = col["statistics"]
        bounds = (f"[{stats['min_ordinal']}, {stats['max_ordinal']}]"
                  if stats is not None else "not tracked")
        print(f"  {col['name']:<16} {TYPE_NAMES.get(col['type'], col['type']):<12} "
              f"distinct={col['data_length']:<8} ordinal bounds {bounds}")
    if len(meta["columns"]) > 6:
        print(f"  … {len(meta['columns']) - 6} more")

    flat = [c for c in meta["columns"]
            if c["statistics"] is not None
            and c["statistics"]["min_ordinal"] == c["statistics"]["max_ordinal"]
            and c["data_length"] > 1]
    for col in flat:
        print(f"\n  {col['name']} has {col['data_length']:,} distinct values but a single")
        print("  ordinal bound: string ordinals pack the first 8 bytes, and a shared")
        print("  prefix collapses them. Monotonic, not injective — safe for range")
        print("  pruning, never for an equality or not-equals decision.")

    return payload, first_morsel


# ─── 5. Write postures ──────────────────────────────────────────────────────


def section_postures(morsel: Morsel) -> None:
    banner("5. write postures: spill vs read-first vs compressed")

    postures = [
        ("spill        ", dict(read_acceleration=False, zstd_level=0)),
        ("read-first   ", dict(read_acceleration=True, zstd_level=0)),
        ("compressed-1 ", dict(read_acceleration=True, zstd_level=1)),
        ("compressed-9 ", dict(read_acceleration=True, zstd_level=9)),
    ]

    print(f"one row group: {morsel.num_rows:,} rows × {morsel.num_columns} columns\n")
    print(f"{'posture':<14} {'bytes':>12} {'write ms':>9} {'read ms':>9}   footer statistics")
    for label, options in postures:
        started = time.perf_counter()
        buf = skene.write_morsel(morsel, **options)
        write_ms = (time.perf_counter() - started) * 1000

        started = time.perf_counter()
        back = skene.read_morsel(buf)
        back.materialize()
        read_ms = (time.perf_counter() - started) * 1000
        assert back.num_rows == morsel.num_rows

        meta = skene.read_metadata(buf)
        tracked = any(c["statistics"] is not None for c in meta["columns"])
        print(f"{label} {len(buf):>12,} {write_ms:>9.1f} {read_ms:>9.1f}   "
              f"{'present' if tracked else 'none — absent means NOT TRACKED'}")

    print("\n  read_acceleration=True buys value ordering, statistics and zone maps;")
    print("  a spill file wants none of that and carries no statistics at all — where")
    print("  absent means 'not tracked', never 'zero'.")
    print("  zstd is the dominant read tax on local storage; on network-bound storage")
    print("  the trade inverts. Pick the posture for where the file will be read from.")


# ─── 7. Failures are loud ───────────────────────────────────────────────────


def section_failures(buf: bytes) -> None:
    banner("7. the error model: every failure is a SkeneError with a code")

    cases = [
        ("not a skene file", lambda: skene.probe_version(b"PAR1\x00\x00\x00\x00")),
        ("truncated file", lambda: skene.read_morsel(buf[:64])),
        ("empty buffer", lambda: skene.read_metadata(b"")),
        ("corrupted footer", lambda: skene.read_metadata(buf[:-40] + b"\x00" * 40)),
    ]
    for label, call in cases:
        try:
            call()
        except skene.SkeneError as exc:
            print(f"  {label:<20} → SkeneError({exc.code})")
        else:
            raise AssertionError(f"{label} must not succeed")

    print("\n  No silent degradation: a damaged file never reads back as fewer rows.")


# ─── main ───────────────────────────────────────────────────────────────────


def main() -> int:
    parquet_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PARQUET
    if not os.path.isfile(parquet_path):
        print(f"ERROR: parquet source not found: {parquet_path}")
        return 1

    buf = section_roundtrip()
    section_metadata(buf)
    section_projection(buf)

    with tempfile.TemporaryDirectory(prefix="skene-example-") as out_dir:
        real_file, real_morsel = section_parquet(parquet_path, out_dir)

    section_postures(real_morsel)
    section_remote(real_file)
    section_failures(buf)

    print("\n✅ every section completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
