"""Format-dispatched dataset Scans: JSONL and skene datasets end-to-end.

Datasets are single-format (format discovered from file suffixes, mixed
listings raise); the physical planner dispatches the Scan to the format's
reader via opteryx.models.dataset_format.SCAN_READERS.
"""

import os
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "..", ".."))

import pytest

import opteryx


@pytest.fixture(scope="module")
def session():
    return opteryx.session()


def _run(session, sql):
    rows = {}
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        if morsel.num_rows == 0:
            continue
        for name in morsel.column_names:
            key = name.decode() if isinstance(name, bytes) else name
            rows.setdefault(key, []).extend(morsel.column(name).to_pylist())
    return rows


@pytest.fixture(scope="module")
def jsonl_dataset():
    with tempfile.TemporaryDirectory() as folder:
        with open(os.path.join(folder, "part-1.jsonl"), "w") as f:
            f.write('{"name":"alpha","value":10}\n{"name":"beta","value":20}\n')
        with open(os.path.join(folder, "part-2.jsonl"), "w") as f:
            f.write('{"name":"gamma","value":30}\n{"name":"delta","value":40}\n')
        yield folder


@pytest.fixture(scope="module")
def skene_dataset():
    import skene
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    with tempfile.TemporaryDirectory() as folder:
        for i, rows in enumerate([[("alpha", 10), ("beta", 20)], [("gamma", 30), ("delta", 40)]]):
            names = vector_from_sequence([r[0] for r in rows], DrakenType.VARCHAR)
            values = vector_from_sequence([r[1] for r in rows], DrakenType.INT64)
            morsel = Morsel.from_vectors(["name", "value"], [names, values])
            with open(os.path.join(folder, f"part-{i}.skene"), "wb") as f:
                f.write(skene.write_morsel(morsel, read_acceleration=True, zstd_level=1))
        yield folder


@pytest.mark.parametrize("dataset_fixture", ["jsonl_dataset", "skene_dataset"])
def test_dataset_scan(session, dataset_fixture, request):
    folder = request.getfixturevalue(dataset_fixture)

    result = _run(session, f"SELECT * FROM '{folder}' ORDER BY value")
    assert result["name"] == ["alpha", "beta", "gamma", "delta"], result
    assert result["value"] == [10, 20, 30, 40], result

    result = _run(session, f"SELECT name FROM '{folder}' WHERE value > 15 ORDER BY name")
    assert result["name"] == ["beta", "delta", "gamma"], result

    result = _run(session, f"SELECT COUNT(*) FROM '{folder}'")
    assert list(result.values()) == [[4]], result

    result = _run(session, f"SELECT SUM(value) FROM '{folder}'")
    assert list(result.values()) == [[100]], result

    result = _run(session, f"SELECT name FROM '{folder}' LIMIT 3")
    assert sum(len(v) for v in result.values()) == 3, result


def test_skene_pruning_notEq_shared_prefix_strings(session, tmp_path):
    """String ordinals pack the first 8 bytes and collide on shared prefixes:
    a file holding 'abcdefgh1' AND 'abcdefgh2' has min == max ORDINAL with
    non-uniform values. NotEq pruning on ordinal bounds must not treat that
    as uniformity — the file must be read, and the other value returned."""
    import skene
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel

    strings = vector_from_sequence(["abcdefgh1", "abcdefgh2"], DrakenType.VARCHAR)
    values = vector_from_sequence([1, 2], DrakenType.INT64)
    morsel = Morsel.from_vectors(["s", "v"], [strings, values])
    (tmp_path / "f.skene").write_bytes(skene.write_morsel(morsel, read_acceleration=True))

    result = _run(session, f"SELECT s FROM '{tmp_path}' WHERE s != 'abcdefgh1'")
    assert result["s"] == ["abcdefgh2"], result


def test_skene_file_pruning_from_parent_filter(session, skene_dataset):
    """Skene declines predicate pushdown (the Filter stays in the plan), but
    manifest pruning still drops provably-excluded files from the parent
    Filter's predicates — a contradiction must return zero rows, and a
    selective filter the right ones, with the Filter doing row-level work."""
    result = _run(session, f"SELECT COUNT(*) FROM '{skene_dataset}' WHERE value < 0")
    assert list(result.values()) == [[0]], result

    result = _run(session, f"SELECT name FROM '{skene_dataset}' WHERE value = 30")
    assert result["name"] == ["gamma"], result


def test_mixed_format_dataset_raises(session, tmp_path):
    from opteryx.models.dataset_format import MixedFormatDatasetError

    (tmp_path / "a.jsonl").write_text('{"x":1}\n')
    (tmp_path / "b.skene").write_bytes(b"SKEN not a real file, suffix is enough for listing")
    with pytest.raises(MixedFormatDatasetError):
        list(session.execute_to_morsels(f"SELECT * FROM '{tmp_path}'"))


if __name__ == "__main__":
    from tests import run_tests

    run_tests()
