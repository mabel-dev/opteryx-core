"""`NativeManifest.find_file`: the row of the file at a path - the first row
when several share it - or None. It answers from a path index kept by every
path write (add_file, relocate_file, with_paths, subset), so a renamed row is
found at its new path and never at its old one."""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from opteryx.compiled.planner.native_manifest import NativeManifestBuilder


def _manifest(paths, relocate=None):
    builder = NativeManifestBuilder((), (), True, True)
    for path in paths:
        builder.add_file(path, "PARQUET", 1, 1)
    for row, path in (relocate or {}).items():
        builder.relocate_file(row, path, 1)
    return builder.build({})


def test_find_file_rows_and_not_found():
    m = _manifest(["a", "b", "c"])
    assert [m.find_file(p) for p in ("a", "b", "c")] == [0, 1, 2]
    assert m.find_file("d") is None


def test_find_file_duplicate_paths_answer_the_first_row():
    m = _manifest(["a", "b", "a", "b"])
    assert m.find_file("a") == 0
    assert m.find_file("b") == 1


def test_relocate_moves_the_row_to_its_new_path():
    m = _manifest(["a", "b"], relocate={0: "z"})
    assert m.find_file("a") is None
    assert m.find_file("z") == 0
    assert m.file_paths() == ["z", "b"]


def test_relocate_first_of_duplicates_hands_the_path_to_the_next():
    m = _manifest(["a", "b", "a", "a"], relocate={0: "z"})
    assert m.find_file("a") == 2
    assert m.find_file("z") == 0


def test_relocate_onto_an_earlier_row_of_a_shared_path():
    m = _manifest(["x", "a", "b"], relocate={2: "a", 1: "y"})
    assert m.find_file("a") == 2
    assert m.find_file("y") == 1
    assert m.find_file("b") is None


def test_relocate_to_an_earlier_path_takes_the_first_row():
    m = _manifest(["a", "b", "c"], relocate={0: "c"})
    assert m.find_file("c") == 0
    assert m.find_file("a") is None


def test_with_paths_reindexes_and_leaves_the_source_untouched():
    m = _manifest(["a", "b", "c"])
    swapped = m.with_paths(["c", "a", "b"])
    assert [swapped.find_file(p) for p in ("a", "b", "c")] == [1, 2, 0]
    assert [m.find_file(p) for p in ("a", "b", "c")] == [0, 1, 2]


def test_subset_indexes_its_own_rows():
    m = _manifest(["a", "b", "c", "b"])
    sub = m.subset([3, 0])
    assert sub.find_file("b") == 0
    assert sub.find_file("a") == 1
    assert sub.find_file("c") is None


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
