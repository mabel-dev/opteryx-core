"""The footer gate says WHY it refused a scan.

`native_scan_rejection` returns None for an admitted scan, otherwise a reason that
names the column, the kind asked for, the file and the footer's types. There is no
fallback reader behind the gate (ruled 2026-10-03), so its reason IS the user's
error: a bare "(footer_gate)" sent production on a hunt for the column that a
MERGE's row-identity scan was asking every footer for.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../../../.."))

import pytest

import opteryx
from opteryx.connectors.parquet_io.pool_reader import native_scan_rejection
from opteryx.exceptions import NotSupportedError

_TWEETS = "testdata/flat/formats/parquet/tweets.parquet"


def test_an_admissible_scan_has_no_rejection():
    assert native_scan_rejection([_TWEETS], ["user_name", "followers"], ["varchar", "int"]) is None


def test_a_missing_column_is_named_with_its_file():
    reason = native_scan_rejection([_TWEETS], ["user_name", "absent"], ["varchar", "int"])
    assert reason == f"column 'absent' is not in {_TWEETS} (row group 0)"


def test_a_synthesized_row_identity_column_is_named():
    """The 0.9.155 production failure: `$file` handed to the gate as if it were stored."""
    reason = native_scan_rejection([_TWEETS], ["user_name", "$file"], ["varchar", "int"])
    assert reason is not None and "'$file'" in reason and _TWEETS in reason


def test_a_type_mismatch_names_the_kind_and_the_footer_types():
    reason = native_scan_rejection([_TWEETS], ["user_name"], ["int"])
    assert reason == (
        f"column 'user_name' (kind 'int') in {_TWEETS} row group 0 has physical type "
        "'byte_array' and logical type 'varchar', which do not decode as 'int'"
    )


def test_a_kind_with_no_decoder_is_named():
    reason = native_scan_rejection([_TWEETS], ["user_name"], ["struct"])
    assert reason == "column 'user_name' has kind 'struct', which the native scan has no decoder for"


def test_the_sql_refusal_carries_the_reason():
    """End to end: a schema-evolved dataset (a projected column absent from one file)
    is refused, and the message names the column and the file."""
    with pytest.raises(NotSupportedError) as err:
        list(opteryx.session().execute_to_morsels("SELECT followers FROM 'testdata/flat/different'"))
    message = str(err.value)
    assert "(footer_gate: column 'followers' is not in testdata/flat/different/" in message


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
