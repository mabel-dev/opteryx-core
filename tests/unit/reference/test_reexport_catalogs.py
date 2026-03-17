
from opteryx.reference.reexport_catalogs import reexport_reference_catalogs


def test_reexport_reference_catalogs_writes_expected_targets(tmp_path):
    output_paths = reexport_reference_catalogs(tmp_path)

    assert output_paths == {
        "operators": tmp_path / "opteryx/reference/operators.json",
        "types": tmp_path / "opteryx/reference/types.json",
        "functions": tmp_path / "opteryx/functions/function_signatures.json",
    }

    for output_path in output_paths.values():
        assert output_path.exists()
        assert output_path.read_text(encoding="utf8").endswith("\n")
