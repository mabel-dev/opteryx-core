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
    paths = set([info.path for info in infos if info.type.name == 'File'])

    assert str(base / "a.txt") in paths
    assert str(base / "sub" / "b.txt") in paths

    # non-recursive
    selector_nr = FileSelector(str(base), recursive=False)
    infos_nr = fs.get_file_info(selector_nr)
    paths_nr = set([info.path for info in infos_nr if info.type.name == 'File'])

    assert str(base / "a.txt") in paths_nr
    assert not any(str(base / "sub" / "b.txt") == p for p in paths_nr)
