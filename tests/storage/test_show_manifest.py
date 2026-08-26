# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""SHOW MANIFEST FOR - shipped with zero test coverage in 54af4f67, closing that gap."""

import pytest

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.local_store_connector import LocalStoreConnector

_OWNER_POLICY = [{"pattern": "*", "role": "owner"}]


def _setup_workspace(tmp_path):
    register_workspace("ws", LocalStoreConnector, store_root=str(tmp_path))


def _morsels_to_rows(morsels):
    rows = []
    for morsel in morsels:
        if morsel is None:
            continue
        pydict = morsel.to_arrow().to_pydict()
        n = len(next(iter(pydict.values()))) if pydict else 0
        for i in range(n):
            rows.append({k: vs[i] for k, vs in pydict.items()})
    return rows


def test_show_manifest_returns_file_metadata(tmp_path):
    _setup_workspace(tmp_path)
    owner = opteryx.session(user="olive", access_policies=_OWNER_POLICY)
    list(owner.execute_to_morsels("CREATE TABLE ws.dst AS SELECT 1 AS a, 'hello' AS b"))

    rows = _morsels_to_rows(owner.execute_to_morsels("SHOW MANIFEST FOR ws.dst"))

    assert len(rows) == 1
    assert rows[0]["file_path"]
    assert rows[0]["record_count"] == 1


def test_bounds_of_mixed_types_render_as_text():
    """One row's bounds list holds one bound per FIELD ID, so its elements are
    as heterogeneous as the table's columns are - an int for column `a` and a
    str for column `b` when the producer stores real decoded values (an
    external catalog such as opteryx-iceberg does; opteryx_catalog's own stats
    builder stores int64 ordinals throughout, which is why this never bit
    there). A draken ARRAY vector carries ONE child type for the whole column,
    so the mixture raised `vector_array_from_sequence: string child element
    must be str/bytes/None` and SHOW MANIFEST FOR could not answer at all.

    The statement renders bounds as text for every source. This exercises the
    builder directly because no connector in this repo produces mixed bounds.
    """
    from opteryx.models.file_entry import FileEntry
    from opteryx.models.manifest_io import file_entries_to_manifest_morsel
    from opteryx.models.manifest_io import manifest_output_schema
    from opteryx.types import logical_type as lt
    from opteryx.types.schema import RelationSchema
    from opteryx.types.schema import SchemaColumn
    from opteryx.types.schema import mint_column_identity

    schema = RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=name,
                column_type=column_type,
                identity=mint_column_identity("t", name),
            )
            for name, column_type in (("a", lt.INT64), ("b", lt.VARCHAR), ("c", lt.FLOAT64))
        ],
    )
    entry = FileEntry(
        file_path="a.parquet",
        file_format="parquet",
        record_count=2,
        file_size_in_bytes=100,
        min_values=[1961, "Apollo", 1.5],
        max_values=[1975, "Soyuz", None],
    )

    morsel = file_entries_to_manifest_morsel([entry], schema, bounds_as_text=True)

    assert morsel.column(b"min_values").to_pylist() == [["1961", "Apollo", "1.5"]]
    assert morsel.column(b"max_values").to_pylist() == [["1975", "Soyuz", None]]
    # The declared output type has to match what the vector actually holds.
    declared = {c.name: str(c.column_type) for c in manifest_output_schema().columns}
    assert declared["min_values"] == "ARRAY<VARCHAR>"
    assert declared["max_values"] == "ARRAY<VARCHAR>"


def test_the_persisted_manifest_keeps_typed_bounds():
    """The text rendering is SHOW MANIFEST's alone. write_manifest_parquet goes
    through the same builder, and the bounds it writes are read back into
    FileEntry.lower_bounds/upper_bounds and COMPARED AGAINST PREDICATE LITERALS
    by Manifest.prune_files - stringifying them there would turn every numeric
    comparison into a lexicographic one and prune the wrong files."""
    from opteryx.models.file_entry import FileEntry
    from opteryx.models.manifest_io import file_entries_to_manifest_morsel
    from opteryx.types import logical_type as lt
    from opteryx.types.schema import RelationSchema
    from opteryx.types.schema import SchemaColumn
    from opteryx.types.schema import mint_column_identity

    schema = RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=name,
                column_type=lt.INT64,
                identity=mint_column_identity("t", name),
            )
            for name in ("a", "b")
        ],
    )
    entry = FileEntry(
        file_path="a.parquet",
        file_format="parquet",
        record_count=2,
        file_size_in_bytes=100,
        min_values=[1, 2],
        max_values=[3, 4],
    )

    morsel = file_entries_to_manifest_morsel([entry], schema)

    assert morsel.column(b"min_values").to_pylist() == [[1, 2]]
    assert morsel.column(b"max_values").to_pylist() == [[3, 4]]
