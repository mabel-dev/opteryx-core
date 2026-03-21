import os
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


ROOT = Path(__file__).resolve().parents[3]
CARCHAR_GROUP_STATE_ENGINE = ROOT / "opteryx/compiled/aggregations/group_by_engine.pyx"
MORSEL_IO = ROOT / "third_party/mabel/draken/storage/morsel_io.pyx"

MOTOR_PATH_FILES = (
    CARCHAR_GROUP_STATE_ENGINE,
    MORSEL_IO,
)


def test_constant_motor_paths_have_no_numpy_dependency():
    for path in MOTOR_PATH_FILES:
        source = path.read_text(encoding="utf-8")
        assert "import numpy" not in source, str(path)
        assert "from numpy" not in source, str(path)


def test_constant_motor_paths_have_no_arrow_compute_dependency():
    for path in MOTOR_PATH_FILES:
        source = path.read_text(encoding="utf-8")
        assert "pyarrow.compute" not in source, str(path)
        assert "from pyarrow import compute" not in source, str(path)


def test_constant_groupby_motor_path_has_no_to_pylist_calls():
    source = CARCHAR_GROUP_STATE_ENGINE.read_text(encoding="utf-8")
    assert ".to_pylist(" not in source
