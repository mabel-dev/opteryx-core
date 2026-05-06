"""
End-to-end demo of the LocalStoreConnector — CREATE / INSERT / CTAS /
SELECT / TRUNCATE / DROP.

Run from the repo root:

    python scratch/demo_local_store.py

Writes its data to ./scratch/demo_store/  (cwd-relative). Cleans up at the
end. Re-runs are idempotent because the cleanup is unconditional.
"""

import os
import shutil
import sys

# So this runs from the repo root without `pip install -e .`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector


STORE_ROOT = os.path.join(os.path.dirname(__file__), "demo_store")


def banner(title):
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def run(session, sql):
    print(f"\n>>> {sql}")
    morsels = list(session.execute_to_morsels(sql))
    rows = 0
    for m in morsels:
        if m is None:
            continue
        # Pretty-print the morsel (Morsel.__str__ formats as a small ASCII table)
        text = str(m)
        if text.strip():
            print(text)
        rows += len(m)
    if not morsels or all(m is None for m in morsels):
        print("(no tabular result)")
    return rows


def main():
    # Clean slate
    if os.path.isdir(STORE_ROOT):
        shutil.rmtree(STORE_ROOT)

    # Register the writable connector at prefix "demo"
    register_workspace("demo", LocalStoreConnector, store_root=STORE_ROOT)

    session = opteryx.session()

    banner("1. CREATE TABLE")
    run(session, "CREATE TABLE demo.planets (id BIGINT, name VARCHAR, mass DOUBLE)")
    print(f"\nFolder created: {os.path.join(STORE_ROOT, 'demo', 'planets')}")
    print("Files:", sorted(os.listdir(os.path.join(STORE_ROOT, "demo", "planets"))))

    banner("2. INSERT VALUES")
    run(session, "INSERT INTO demo.planets VALUES (1, 'Mercury', 0.330)")
    run(session, "INSERT INTO demo.planets VALUES (2, 'Venus', 4.87), (3, 'Earth', 5.97)")
    print("\nFiles after two INSERTs:")
    for f in sorted(os.listdir(os.path.join(STORE_ROOT, "demo", "planets"))):
        print(f"  {f}")

    banner("3. SELECT FROM the relation")
    # NOTE: `SELECT * ... ORDER BY` against a multi-file LocalStore relation
    # currently misroutes numeric columns (separate engine bug, unrelated to
    # the LocalStore work). Use explicit projections or unordered SELECT *.
    run(session, "SELECT * FROM demo.planets")
    run(session, "SELECT COUNT(*) AS row_count FROM demo.planets")
    run(session, "SELECT name, mass FROM demo.planets WHERE mass > 1.0 ORDER BY mass")

    banner("4. INSERT ... SELECT (with explicit column reorder)")
    run(session, "CREATE TABLE demo.heavy_planets (label VARCHAR, weight DOUBLE)")
    run(
        session,
        "INSERT INTO demo.heavy_planets (weight, label) "
        "SELECT mass, name FROM demo.planets WHERE mass > 1.0",
    )
    run(session, "SELECT * FROM demo.heavy_planets ORDER BY weight")

    banner("5. CREATE TABLE AS SELECT (CTAS)")
    run(
        session,
        "CREATE TABLE demo.lights AS "
        "SELECT id, name FROM demo.planets WHERE mass < 1.0",
    )
    run(session, "SELECT * FROM demo.lights ORDER BY id")

    banner("6. TRUNCATE")
    run(session, "TRUNCATE TABLE demo.heavy_planets")
    run(session, "SELECT COUNT(*) AS rows_after_truncate FROM demo.heavy_planets")

    banner("7. DROP TABLE")
    run(session, "DROP TABLE demo.heavy_planets")
    run(session, "DROP TABLE IF EXISTS demo.heavy_planets")  # idempotent
    print("\nRemaining relations under demo/:")
    for entry in sorted(os.listdir(os.path.join(STORE_ROOT, "demo"))):
        print(f"  {entry}")

    banner("8. Nested schema names (arbitrary depth)")
    run(session, "CREATE TABLE demo.sol.inner.rocky (name VARCHAR)")
    run(session, "INSERT INTO demo.sol.inner.rocky VALUES ('Mercury'), ('Venus'), ('Earth'), ('Mars')")
    run(session, "SELECT name FROM demo.sol.inner.rocky ORDER BY name")
    print(f"\nLayout on disk:")
    for root, dirs, files in os.walk(os.path.join(STORE_ROOT, "demo", "sol")):
        rel = os.path.relpath(root, STORE_ROOT)
        for f in sorted(files):
            print(f"  {os.path.join(rel, f)}")

    banner("9. Read-only connector rejects writes")
    try:
        run(session, "CREATE TABLE testdata.nope (x BIGINT)")
    except Exception as e:
        print(f"\n[expected] {type(e).__name__}: {e}")

    banner("Done — cleaning up")
    shutil.rmtree(STORE_ROOT)
    print(f"Removed {STORE_ROOT}")


if __name__ == "__main__":
    main()
