"""The footer gate refuses a file that stores a column as another type than declared.

The compiler qualifies a plain integer kind with the DECLARED width ("int:int8") and a
decimal kind with the DECLARED precision/scale ("decimal64:decimal(3,1)"). Before
that, the gate only checked the family: a DECIMAL(21,1) file under a DECIMAL(3,1)
schema, or an INT64 file under INT8, was admitted and misdecoded - silently, and for
DECIMAL corrupting rows of the OTHER files in the scan too. A narrower integer file is
still admitted: the Source widens it (the ALTER COLUMN ... TYPE path).
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../.."))

import pytest

import opteryx
from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection
from rugo import parquet as rugo_parquet


def _write(tmp_path, name, sql):
    path = str(tmp_path / name)
    morsels = list(opteryx.session().execute_to_morsels(sql))
    with open(path, "wb") as handle:
        handle.write(rugo_parquet.write_parquet(morsels[0]))
    return path


@pytest.fixture(scope="module")
def files(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("declared")
    return {
        # g DECIMAL(3,1), m INT8 - exactly what $planets declares
        "declared": _write(
            tmp_path, "declared.parquet",
            "SELECT gravity AS g, number_of_moons AS m FROM $planets",
        ),
        # what INSERT ... SELECT gravity + 1 / moons + 1 used to write
        "wide": _write(
            tmp_path, "wide.parquet",
            "SELECT CAST(gravity AS DECIMAL(21,1)) AS g, "
            "CAST(number_of_moons AS INT64) AS m FROM $planets",
        ),
        # same int64 backing, another precision/scale; a narrower-than-INT32 int
        "other": _write(
            tmp_path, "other.parquet",
            "SELECT CAST(gravity AS DECIMAL(4,2)) AS g, "
            "CAST(number_of_moons AS INT16) AS m FROM $planets",
        ),
    }


def test_files_matching_the_declaration_are_admitted(files):
    assert native_scan_rejection(
        [files["declared"]], ["g", "m"], ["decimal64:decimal(3,1)", "int:int8"]
    ) is None


def test_a_wider_decimal_is_refused_naming_both_types(files):
    reason = native_scan_rejection(
        [files["declared"], files["wide"]], ["g"], ["decimal64:decimal(3,1)"]
    )
    assert reason == (
        f"column 'g' is declared DECIMAL(3,1) but {files['wide']} row group 0 stores it "
        "as physical type 'fixed_len_byte_array', logical type 'decimal(21,1)', which the "
        "scan cannot read as DECIMAL(3,1)"
    )


def test_another_decimal_scale_is_refused(files):
    reason = native_scan_rejection([files["other"]], ["g"], ["decimal64:decimal(3,1)"])
    assert reason is not None
    assert "declared DECIMAL(3,1)" in reason and "'decimal(4,2)'" in reason


def test_a_decimal128_declaration_must_match_too(files):
    assert native_scan_rejection([files["wide"]], ["g"], ["decimal128:decimal(21,1)"]) is None
    reason = native_scan_rejection([files["wide"]], ["g"], ["decimal128:decimal(22,1)"])
    assert reason is not None and "declared DECIMAL(22,1)" in reason


def test_a_decimal_kind_must_carry_its_declaration(files):
    reason = native_scan_rejection([files["declared"]], ["g"], ["decimal64"])
    assert reason == "column 'g' has kind 'decimal64' with no declared precision and scale"


def test_a_wider_integer_is_refused_naming_both_types(files):
    reason = native_scan_rejection([files["wide"]], ["m"], ["int:int8"])
    assert reason == (
        f"column 'm' is declared INT8 but {files['wide']} row group 0 stores it as physical "
        "type 'int64', logical type 'int64', which the scan cannot read as INT8"
    )
    reason = native_scan_rejection([files["other"]], ["m"], ["int:int8"])
    assert reason is not None and "declared INT8" in reason and "'int16'" in reason


@pytest.mark.parametrize("declared", ["int:int16", "int:int32", "int:int64"])
def test_a_narrower_integer_is_admitted_for_widening(files, declared):
    assert native_scan_rejection([files["declared"], files["other"]], ["m"], [declared]) is None


def test_an_integer_never_widens_across_to_unsigned(files):
    reason = native_scan_rejection([files["declared"]], ["m"], ["int:uint64"])
    assert reason is not None and "declared UINT64" in reason


def test_only_int_and_decimal_kinds_carry_a_declaration(files):
    reason = native_scan_rejection([files["declared"]], ["m"], ["float64:float64"])
    assert reason == "column 'm' has kind 'float64:float64', which cannot carry a declared type"


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
