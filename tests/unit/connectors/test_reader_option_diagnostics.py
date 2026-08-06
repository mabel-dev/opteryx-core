"""Option-diagnostic tests for the file-reading dataset functions.

READ_JSONL/READ_CSV/READ_PARQUET take one positional argument (the path) and
their options by name. Options written with '=' instead of '=>' do not reach
`named_args` at all -- sqlparser only produces a named argument for the '=>'
form, so 'ignore_errors = true' arrives as a positional Eq *expression*. That
used to be dropped on the floor: the query bound clean and read with the option
at its default. These tests pin the three things the error now has to tell the
user -- that the option was not recognized, that the operator was wrong, and
which option was probably meant.

The binder branch under test is _validate_reader_options in
opteryx.planner.binder.dataset.
"""

import os
import sys

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.exceptions import InvalidFunctionParameterError
from opteryx.exceptions import NotSupportedError


def _run(sql):
    session = opteryx.session()
    return list(session.execute_to_morsels(sql))


@pytest.fixture
def jsonl_file(tmp_path):
    path = tmp_path / "data.jsonl"
    path.write_text('{"a": 1}\n{"a": 2}\n')
    return path


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "data.csv"
    path.write_text("a\n1\n2\n")
    return path


@pytest.fixture
def parquet_file(tmp_path):
    # READ_PARQUET's option check runs in the binder before the file is opened,
    # so an unreadable placeholder is enough to reach it -- and a real Parquet
    # file would let a missed check fall through to a successful read instead.
    path = tmp_path / "data.parquet"
    path.write_bytes(b"PAR1")
    return path


def test_valid_option_name_with_wrong_operator_names_the_operator(jsonl_file):
    """The exact mistake that used to bind silently."""
    with pytest.raises(InvalidFunctionParameterError) as err:
        _run(f"SELECT * FROM READ_JSONL('{jsonl_file}', ignore_errors=true)")
    message = str(err.value)
    assert "ignore_errors" in message
    assert "'=>'" in message and "'='" in message


def test_wrong_option_name_and_wrong_operator_names_both(jsonl_file):
    """`fail_on_error=false` -- rugo's parameter name, not the SQL option's."""
    with pytest.raises(InvalidFunctionParameterError) as err:
        _run(f"SELECT * FROM READ_JSONL('{jsonl_file}', fail_on_error=false)")
    message = str(err.value)
    assert "unrecognized option 'fail_on_error'" in message
    assert "ignore_errors" in message  # the valid-options list
    assert "'=>'" in message


def test_near_miss_option_name_suggests_the_real_one(jsonl_file):
    with pytest.raises(InvalidFunctionParameterError) as err:
        _run(f"SELECT * FROM READ_JSONL('{jsonl_file}', ignore_error=>true)")
    assert "Did you mean 'ignore_errors'?" in str(err.value)


def test_csv_near_miss_option_name_suggests_the_real_one(csv_file):
    with pytest.raises(InvalidFunctionParameterError) as err:
        _run(f"SELECT * FROM READ_CSV('{csv_file}', seperator=>',')")
    assert "Did you mean 'separator'?" in str(err.value)


def test_unrecognized_name_without_a_near_miss_lists_valid_options(csv_file):
    with pytest.raises(InvalidFunctionParameterError) as err:
        _run(f"SELECT * FROM READ_CSV('{csv_file}', has_header=>true)")
    message = str(err.value)
    assert "unrecognized option 'has_header'" in message
    assert "has_header_row" in message
    assert "separator" in message


@pytest.mark.parametrize("extra", ["'surplus'", "1+1", "some_identifier"])
def test_surplus_positional_argument_rejected(jsonl_file, extra):
    with pytest.raises(InvalidFunctionParameterError) as err:
        _run(f"SELECT * FROM READ_JSONL('{jsonl_file}', {extra})")
    assert "single positional argument" in str(err.value)


def test_parquet_takes_no_options_in_either_spelling(parquet_file):
    for option in ("ignore_errors=>true", "ignore_errors=true"):
        with pytest.raises(InvalidFunctionParameterError) as err:
            _run(f"SELECT * FROM READ_PARQUET('{parquet_file}', {option})")
        message = str(err.value)
        assert "does not take options" in message
        # No "did you mean" or '=>' advice -- no spelling of this would work.
        assert "Did you mean" not in message


def test_explicit_schema_reports_the_gap_in_either_spelling(jsonl_file):
    for option in ("explicit_schema=>1", "explicit_schema=1"):
        with pytest.raises(NotSupportedError) as err:
            _run(f"SELECT * FROM READ_JSONL('{jsonl_file}', {option})")
        assert "explicit_schema" in str(err.value)


@pytest.mark.parametrize(
    "options",
    [
        "ignore_errors=>true",
        "infer_sample_size=>10",
        "ignore_errors=>true, infer_sample_size=>10",
        "infer_schema=>true",
    ],
)
def test_correctly_spelled_options_still_bind(jsonl_file, options):
    morsels = _run(f"SELECT * FROM READ_JSONL('{jsonl_file}', {options})")
    assert sum(m.num_rows for m in morsels) == 2


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
