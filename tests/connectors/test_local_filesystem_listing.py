from pyarrow.fs import FileSelector
from opteryx.connectors.io_systems.local_filesystem import OpteryxLocalFileSystem


def test_local_filesystem_list(tmp_path):
    base = tmp_path / "root"
    base.mkdir()
    (base / "a.txt").write_text("hello")
    sub = base / "sub"
    sub.mkdir()
    (sub / "b.txt").write_text("world")

    fs = OpteryxLocalFileSystem()
    selector = FileSelector(str(base), recursive=True)
    infos = fs.get_file_info(selector)
    paths = set(info.path for info in infos)

    # Listing yields files only - never the directory itself.
    assert paths == {str(base / "a.txt"), str(base / "sub" / "b.txt")}

    # non-recursive
    selector_nr = FileSelector(str(base), recursive=False)
    infos_nr = fs.get_file_info(selector_nr)
    paths_nr = set(info.path for info in infos_nr)

    assert paths_nr == {str(base / "a.txt")}
