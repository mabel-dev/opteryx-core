"""What a reader is told when a value will not CAST.

A value that does not fit the cast it was asked for is a DATA error: the query is
well-formed, the engine is healthy, and the only thing that can fix it is knowing
WHICH value failed and what to do about it. So these failures travel the engine's
data-error channel (kErrCodeDataError -> opteryx DataError, message verbatim) and
NOT the internal-fault channel, which frames a message with the operator name and
the failing opcode -- neither of which a reader can act on.

Three things are pinned per message, and each of them was missing before:
  - the offending VALUE appears in it;
  - TRY_CAST is named, because that is the remedy;
  - no engine internals are in front of it.

The kernel that parses a string into a number serves more than one target
(FLOAT32 casts through the FLOAT64 kernel, the narrow ints through the INT64 one),
so these messages deliberately name what the value is not rather than the target
type -- a message that said FLOAT64 would misname half of its own failures.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx
from opteryx.exceptions import DataError
from opteryx.exceptions import SqlError

# (expression over $planets.name, the phrase that identifies the failure)
CAST_FAILURES = [
    ("CAST(name AS FLOAT64)", "is not numeric"),
    ("CAST(name AS FLOAT32)", "is not numeric"),
    ("CAST(name AS INT64)", "is not a whole number"),
    ("CAST(name AS INT8)", "is not a whole number"),
    ("CAST(name AS UINT64)", "is not a whole number"),
    ("CAST(name AS BOOL)", "true/false/1/0/yes/no/on/off"),
    ("CAST(name AS DATE)", "expected YYYY-MM-DD"),
    ("CAST(name AS IPV4)", "expected A.B.C.D"),
    ("CAST(name AS DECIMAL(10, 2))", "is not a number"),
]


def _failure(sql):
    session = opteryx.session()
    try:
        with pytest.raises(DataError) as raised:
            list(session.execute_to_morsels(sql))
    finally:
        session.close()
    return str(raised.value)


@pytest.mark.parametrize("expression, phrase", CAST_FAILURES)
def test_cast_failure_names_the_value_and_the_remedy(expression, phrase):
    message = _failure(f"SELECT {expression} FROM $planets")

    assert phrase in message, message
    # 'Mercury' is $planets' first row; the value that failed is what the reader
    # needs to find the row, and quoting it is what separates it from prose.
    assert "'Mercury'" in message, message
    assert "TRY_CAST" in message, message


@pytest.mark.parametrize("expression, phrase", CAST_FAILURES)
def test_cast_failure_carries_no_engine_framing(expression, phrase):
    message = _failure(f"SELECT {expression} FROM $planets")

    # The internal-fault channel's vocabulary. Any of these in front of the
    # message means the failure took the wrong channel, not that the wording
    # drifted -- the reader gets an opcode number where the value should be.
    for internal in ("err_op", "Operator", "expression evaluation failed", "[1]:"):
        assert internal not in message, message


def test_cast_failure_in_a_predicate_takes_the_same_channel():
    """The filter operator drives the same span, so it must classify the same."""
    message = _failure("SELECT name FROM $planets WHERE CAST(name AS FLOAT64) > 1")

    assert "'Mercury'" in message, message
    assert "err_op" not in message, message


# A CAST of a LITERAL never reaches these kernels — it folds at plan time and
# fails in the planner with its own (already value-naming) message. Every case
# below therefore builds its string from a COLUMN, which is the only way to put
# a chosen value in front of the kernel.
_ID = "CAST(id AS VARCHAR)"


def test_decimal_failures_name_the_declared_precision_and_scale():
    """DECIMAL knows its exact target, so unlike the shared parsers it names it."""
    scale = _failure(
        f"SELECT CAST(CONCAT({_ID}, '.239') AS DECIMAL(10, 2)) FROM $planets"
    )
    assert "DECIMAL(10, 2)" in scale, scale
    assert "'1.239'" in scale, scale
    assert "decimal places" in scale, scale

    overflow = _failure(
        f"SELECT CAST(CONCAT({_ID}, '23456789012.99') AS DECIMAL(6, 2)) FROM $planets"
    )
    assert "DECIMAL(6, 2)" in overflow, overflow
    assert "overflows" in overflow, overflow


def test_out_of_range_integers_name_the_range_they_left():
    message = _failure(
        f"SELECT CAST(CONCAT({_ID}, '9999999999999999999') AS INT64) FROM $planets"
    )
    assert "out of range for a 64-bit integer" in message, message
    assert "'19999999999999999999'" in message, message

    negative = _failure(
        f"SELECT CAST(CONCAT('-', {_ID}) AS UINT64) FROM $planets"
    )
    assert "is negative" in negative, negative
    assert "'-1'" in negative, negative


@pytest.mark.parametrize("expression, phrase", CAST_FAILURES)
def test_try_cast_still_nulls_instead_of_raising(expression, phrase):
    """The remedy the messages advertise has to actually work."""
    session = opteryx.session()
    try:
        morsels = list(
            session.execute_to_morsels(f"SELECT TRY_{expression} FROM $planets")
        )
        assert sum(m.num_rows for m in morsels) > 0
    finally:
        session.close()
def test_folded_literal_failure_names_a_type_the_dialect_accepts():
    """The planner's own message must not name an INTERNAL spelling.

    FLOAT/FLOAT64 normalize to one name before anything downstream sees them, and
    that name reaches the reader — as the cast target in this message, and as the
    auto-generated column name below. It used to be DOUBLE, which this dialect
    REJECTS as a cast target, so a query that said FLOAT was answered with a type
    it would refuse if the reader typed it back.
    """
    session = opteryx.session()
    try:
        with pytest.raises(SqlError) as raised:
            list(session.execute_to_morsels("SELECT CAST('4.3s' AS FLOAT) FROM $planets"))
    finally:
        session.close()

    message = str(raised.value)
    assert "FLOAT64" in message, message
    assert "DOUBLE" not in message, message


@pytest.mark.parametrize(
    "query, expected_name",
    [
        ("SELECT CAST(id AS FLOAT) FROM $planets", "id::FLOAT64"),
        ("SELECT CAST(id AS FLOAT64) FROM $planets", "id::FLOAT64"),
        ("SELECT TRY_CAST(id AS FLOAT) FROM $planets", "id::TRY_FLOAT64"),
    ],
)
def test_cast_column_name_uses_a_type_the_dialect_accepts(query, expected_name):
    session = opteryx.session()
    try:
        morsel = next(iter(session.execute_to_morsels(query)))
        names = [
            n.decode("utf-8") if isinstance(n, (bytes, bytearray)) else n
            for n in morsel.column_names
        ]
    finally:
        session.close()

    assert expected_name in names, names
