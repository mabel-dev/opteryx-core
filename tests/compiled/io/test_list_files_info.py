import pytest

ci = pytest.importorskip("opteryx.compiled.io")

if not hasattr(ci, 'list_files_info') or not hasattr(ci, 'list_directory'):
    pytest.skip("compiled io functions not available")


def test_list_directory_and_files_info(tmp_path):
    base = tmp_path / "root"
    base.mkdir()

    # create files and subdirs
    (base / "a.txt").write_text("foo")
    (base / "b.log").write_text("bar")
    sub = base / "subdir"
    sub.mkdir()
    (sub / "c.txt").write_text("baz")

    from opteryx.compiled.io import list_files_info, list_directory

    # Non-recursive directory listing
    entries = list_directory(str(base))
    names = set([e[0] for e in entries])
    assert "a.txt" in names
    assert "b.log" in names
    assert "subdir" in names

    # Recursive listing with info, no extension filter
    info = list_files_info(str(base), ())
    paths = set([p for p, is_dir, is_file, size, mtime in info])
    assert str(base / "a.txt") in paths
    assert str(base / "b.log") in paths
    assert str(base / "subdir" / "c.txt") in paths

    # Filter by extension
    txt_files = list_files_info(str(base), (".txt",))
    txt_paths = set([p for p, *_ in txt_files])
    assert str(base / "a.txt") in txt_paths
    assert str(base / "subdir" / "c.txt") in txt_paths
    assert str(base / "b.log") not in txt_paths
