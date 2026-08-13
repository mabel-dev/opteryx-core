"""Demo: ALTER TABLE ... ADD / DROP / RENAME COLUMN on a real relation.

Copies $planets into a local-store workspace, then alters its columns and shows
both that the DATA survives and - the point of the whole design - that the
surviving columns' encoded parquet pages are copied BYTE-FOR-BYTE rather than
decoded and re-encoded.

    python dev/demo_column_ddl.py

Run from the repo root, so `import rugo` picks up the compiled tree rather than
an installed wheel.
"""

import os
import shutil
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], ".."))

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector

RELATION = "ws.planets"


def run(session, sql):
    return list(session.execute_to_morsels(sql))


def rows(session, sql):
    out = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None:
            continue
        d = morsel.to_arrow().to_pydict()
        if not d:
            continue
        for i in range(len(next(iter(d.values())))):
            out.append({k: v[i] for k, v in d.items()})
    return out


def columns(session, relation=RELATION):
    for morsel in session.execute_to_morsels(f"SELECT * FROM {relation}"):
        if morsel is not None:
            return list(morsel.to_arrow().to_pydict().keys())
    return []


def data_files(root, relation="planets"):
    directory = os.path.join(root, "ws", relation)
    return sorted(
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.startswith("data-") and f.endswith(".parquet")
    )


def page_region(path):
    """The encoded pages of a parquet file, and nothing else.

    Parquet's trailer is self-describing - [PAR1][pages][footer][u32 len][PAR1] -
    so this finds the boundary without decoding a single value.
    """
    with open(path, "rb") as f:
        raw = f.read()
    assert raw[:4] == b"PAR1" and raw[-4:] == b"PAR1"
    footer_len = int.from_bytes(raw[-8:-4], "little")
    return raw[4 : len(raw) - 8 - footer_len]


def new_file_since(root, before):
    added = set(data_files(root)) - set(before)
    assert len(added) == 1, f"expected one new data file, got {len(added)}"
    return added.pop()


def banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def main():
    root = tempfile.mkdtemp(prefix="column-ddl-demo-")
    try:
        register_workspace("ws", LocalStoreConnector, store_root=root)
        session = opteryx.session()

        banner("1. Copy $planets into a writable relation")
        run(session, f"CREATE TABLE {RELATION} AS SELECT * FROM $planets")
        original_columns = columns(session)
        planets = rows(session, f"SELECT name, diameter, number_of_moons FROM {RELATION}")
        print(f"{RELATION} has {len(original_columns)} columns, {len(planets)} rows")
        print("columns:", ", ".join(original_columns))
        print()
        for row in planets[:4]:
            print(f"   {row['name']:<10} diameter={row['diameter']:<8} moons={row['number_of_moons']}")
        print("   ...")

        # ---------------------------------------------------------------
        banner("2. DROP COLUMN - the dropped column's pages are not carried over")
        before_files = data_files(root)
        before_region = page_region(before_files[0])
        before_size = os.path.getsize(before_files[0])

        run(session, f"ALTER TABLE {RELATION} DROP COLUMN surface_pressure")
        dropped_once = new_file_since(root, before_files)

        mid_files = data_files(root)
        run(session, f"ALTER TABLE {RELATION} DROP COLUMN orbital_eccentricity")
        live_file = new_file_since(root, mid_files)

        after_columns = columns(session)
        print(f"columns: {len(original_columns)} -> {len(after_columns)}")
        print("gone   :", sorted(set(original_columns) - set(after_columns)))

        dropped_file = dropped_once
        after_region = page_region(dropped_file)
        print(f"file   : {before_size} -> {os.path.getsize(dropped_file)} bytes"
              f"   (after dropping surface_pressure)")
        print(f"pages  : {len(before_region)} -> {len(after_region)} bytes")

        try:
            rows(session, f"SELECT surface_pressure FROM {RELATION}")
            print("!! dropped column was still selectable")
        except Exception as exc:
            print(f"SELECT surface_pressure -> {type(exc).__name__}: {str(exc).split('.')[0]}.")

        kept = rows(session, f"SELECT name, diameter, number_of_moons FROM {RELATION}")
        print("data survived the drop:", kept == planets)

        # ---------------------------------------------------------------
        banner("3. RENAME COLUMN - zero data bytes change")
        before_files = data_files(root)
        before_region = page_region(live_file)   # the file the rename will patch

        run(session, f"ALTER TABLE {RELATION} RENAME COLUMN number_of_moons TO moons")

        renamed_file = new_file_since(root, before_files)
        after_region = page_region(renamed_file)

        print("columns now include 'moons':", "moons" in columns(session))
        print("old name 'number_of_moons' gone:", "number_of_moons" not in columns(session))
        print()
        print(f"pages before : {len(before_region)} bytes")
        print(f"pages after  : {len(after_region)} bytes")
        print(f"BYTE-IDENTICAL: {after_region == before_region}")
        print()
        print("   ^ this is the property that matters. A rename that decoded and")
        print("     re-encoded would return the same VALUES and different BYTES;")
        print("     only a footer edit leaves the pages bit-for-bit unchanged.")

        moons = rows(session, f"SELECT name, moons FROM {RELATION}")
        print()
        print("values read back under the new name:")
        for row in moons[:4]:
            print(f"   {row['name']:<10} moons={row['moons']}")

        same = [r["moons"] for r in moons] == [r["number_of_moons"] for r in planets]
        print("identical to the pre-rename values:", same)

        # ---------------------------------------------------------------
        banner("4. ADD COLUMN - one repeated value, near-zero cost")
        before_files = data_files(root)
        before_region = page_region(renamed_file)
        before_size = os.path.getsize(renamed_file)

        run(session, f"ALTER TABLE {RELATION} ADD COLUMN discovered_by VARCHAR")
        added_null = new_file_since(root, before_files)

        mid_files = data_files(root)
        run(session, f"ALTER TABLE {RELATION} ADD COLUMN catalogued BOOL DEFAULT TRUE")
        live_file = new_file_since(root, mid_files)

        after_region = page_region(added_null)
        print(f"file   : {before_size} -> {os.path.getsize(added_null)} bytes"
              f"   (after adding discovered_by)")
        print(f"pages  : {len(before_region)} -> {len(after_region)} bytes")
        print(f"existing pages unchanged: {after_region[:len(before_region)] == before_region}")
        print()
        print("   ^ the new column's chunk is appended AFTER everything copied,")
        print("     so the bytes that were already there are a byte-for-byte")
        print("     prefix of the new file. Nothing was decoded to add a column.")
        print()

        added = rows(session, f"SELECT name, moons, discovered_by, catalogued FROM {RELATION}")
        for row in added[:4]:
            print(f"   {row['name']:<10} moons={str(row['moons']):<5} "
                  f"discovered_by={row['discovered_by']!r:<6} catalogued={row['catalogued']}")
        print("   ...")
        print()
        print("no DEFAULT      -> every existing row reads NULL:",
              all(r["discovered_by"] is None for r in added))
        print("DEFAULT TRUE    -> every existing row reads TRUE:",
              all(r["catalogued"] is True for r in added))

        # ---------------------------------------------------------------
        banner("5. Superseded files stay put, so time travel still works")
        print(f"data files on disk: {len(data_files(root))}")
        print("Each ALTER wrote a NEW file and pointed the new snapshot at it.")
        print("The originals are still referenced by earlier snapshots, so they")
        print("are left alone rather than rewritten in place.")

        # ---------------------------------------------------------------
        banner("6. ALTER COLUMN TYPE - most widenings cost nothing at all")
        before_files = data_files(root)
        before_region = page_region(live_file)

        run(session, f"ALTER TABLE {RELATION} ALTER COLUMN moons TYPE INT64")
        widened_file = new_file_since(root, before_files)

        print("moons was INT32 on disk (parquet has no int8/int16 - all three ride")
        print("physical int32), so widening it to INT64 does change the stored width:")
        print(f"pages  : {len(before_region)} -> {len(page_region(widened_file))} bytes")
        print()
        after_widen = rows(session, f"SELECT name, moons FROM {RELATION}")
        same = [r["moons"] for r in after_widen] == [r["moons"] for r in moons]
        print("values read back unchanged:", same)
        print()
        print("   ^ only THAT column was decoded and re-encoded. Had the widening")
        print("     been INT8 -> INT32, parquet's physical type would not change at")
        print("     all and even that column's pages would have been copied.")

        banner("7. Rejected before anything is written")
        for sql, why in (
            (f"ALTER TABLE {RELATION} DROP COLUMN nope", "no such column"),
            (f"ALTER TABLE {RELATION} RENAME COLUMN name TO moons", "target name taken"),
            (f"ALTER TABLE {RELATION} ALTER COLUMN diameter TYPE INT32", "not a widening"),
        ):
            try:
                run(session, sql)
                print(f"   {why:<20} -> unexpectedly succeeded")
            except Exception as exc:
                print(f"   {why:<20} -> {type(exc).__name__}")

        print()
        print("Done.")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
