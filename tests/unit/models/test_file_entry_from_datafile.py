# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
FileEntry.from_datafile — the catalog-origin FileEntry constructor.

Covers the min_length_bounds/max_length_bounds extraction added to back the
length-aware selectivity guard (STARTS_WITH/INSTR/ENDS_WITH): before this,
from_datafile read min_values/max_values/field_ids into lower_bounds/
upper_bounds but silently dropped min_lengths/max_lengths entirely, even
though the catalog's own manifest entry dict carries them (confirmed against
the installed opteryx_catalog package: ParquetManifestEntry.to_dict()
includes "min_lengths"/"max_lengths" as positional lists parallel to
field_ids, same shape as min_values/max_values).
"""

from __future__ import annotations

from types import SimpleNamespace

from opteryx.models.file_entry import FileEntry


def _datafile(entry: dict):
    return SimpleNamespace(entry=entry)


def test_from_datafile_extracts_length_bounds_by_real_field_id():
    # field_ids deliberately non-sequential/offset from position, mirroring
    # the live catalog schema that exposed the ordinal_bounds field_id bug.
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "field_ids": [1, 3, 4],
        "min_values": [1, 2, 3],
        "max_values": [9, 8, 7],
        "min_lengths": [2, 40, 7],
        "max_lengths": [5, 60, 9],
    }
    fe = FileEntry.from_datafile(_datafile(entry))

    assert fe.min_length_bounds == {1: 2, 3: 40, 4: 7}
    assert fe.max_length_bounds == {1: 5, 3: 60, 4: 9}


def test_from_datafile_length_bounds_positional_fallback_without_field_ids():
    # Older manifest rows with no field_ids at all -- fall back to positional
    # indexing, same convention lower_bounds/upper_bounds already use.
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "min_values": [1, 2],
        "max_values": [9, 8],
        "min_lengths": [3, 11],
        "max_lengths": [6, 20],
    }
    fe = FileEntry.from_datafile(_datafile(entry))

    assert fe.min_length_bounds == {0: 3, 1: 11}
    assert fe.max_length_bounds == {0: 6, 1: 20}


def test_from_datafile_length_bounds_none_when_absent():
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "min_values": [1],
        "max_values": [9],
    }
    fe = FileEntry.from_datafile(_datafile(entry))

    assert fe.min_length_bounds is None
    assert fe.max_length_bounds is None


def test_from_datafile_fallback_attribute_shape_has_no_length_bounds():
    # The non-dict-entry fallback path (direct attribute access) has no known
    # producer carrying length stats -- must not crash, and must leave
    # length_bounds unset rather than fabricate a guess.
    datafile = SimpleNamespace(
        file_path="f1",
        record_count=10,
        file_size_in_bytes=100,
        min_values=[1],
        max_values=[9],
    )
    fe = FileEntry.from_datafile(datafile)

    assert fe.min_length_bounds is None
    assert fe.max_length_bounds is None



def test_from_datafile_keys_by_the_rows_own_tuple_field_ids_in_file_order():
    # The live catalog bulk-scan hands `field_ids` over as a TUPLE, in the
    # FILE's column order. A list-only check discarded it and keyed by
    # `schema_field_ids` (schema order), so each column read another column's
    # stats - public.github.events had created_at bounded by a URL string.
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "field_ids": (1, 3, 2),
        "min_values": (10, "https://a", 100),
        "max_values": (19, "https://z", 199),
        "null_counts": (0, 4, 0),
    }
    fe = FileEntry.from_datafile(_datafile(entry), schema_field_ids=[1, 2, 3])

    assert fe.lower_bounds == {1: 10, 2: 100, 3: "https://a"}
    assert fe.upper_bounds == {1: 19, 2: 199, 3: "https://z"}
    assert fe.null_value_counts == {1: 0, 2: 0, 3: 4}


def test_from_datafile_tuple_field_ids_shorter_than_schema_keeps_its_stats():
    # A file written without some schema columns (github.events rows with no
    # org_* columns): its own ids line up with its own stats, so the stats are
    # kept. Keyed against the full schema list instead, the length mismatch
    # dropped every bound and null count on the file.
    entry = {
        "file_path": "f1",
        "record_count": 10,
        "file_size_in_bytes": 100,
        "field_ids": (2, 1),
        "min_values": (100, 10),
        "max_values": (199, 19),
        "null_counts": (0, 0),
    }
    fe = FileEntry.from_datafile(_datafile(entry), schema_field_ids=[1, 2, 3])

    assert fe.lower_bounds == {1: 10, 2: 100}
    assert fe.upper_bounds == {1: 19, 2: 199}
    assert fe.null_value_counts == {1: 0, 2: 0}


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
