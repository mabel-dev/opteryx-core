"""
The string transforms keep their input's string type.

REVERSE, SUBSTRING/LEFT/RIGHT, REPLACE, TRIM/LTRIM/RTRIM, LPAD/RPAD and the case
functions run kernels that preserve the input's tag, but the catalog declared a
fixed VARCHAR return. The bound type disagreed with the vector, so:
  * `REVERSE(nv) || nv` was refused as VARCHAR || NVARCHAR,
  * `REVERSE(vb) || 'x'` passed bind as VARCHAR || VARCHAR and died in the kernel,
  * a constant-folded NVARCHAR result was rebuilt as a VARCHAR literal.

Also here: `CAST(NULL AS NVARCHAR)` had no path at all (the runtime CAST's NVARCHAR
arm is string-sourced); it now folds to a typed NVARCHAR null like VARCHAR does.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from draken.draken_native import DrakenType
from opteryx.exceptions import IncorrectTypeError

_SESSION = opteryx.session()


def _typed(sql):
    values = []
    physical = None
    for morsel in _SESSION.execute_to_morsels(sql):
        column = morsel.column("r")
        physical = column.type
        values.extend(column.to_pylist())
    return physical, values


_TRANSFORMS = [
    "REVERSE({x})",
    "TRIM({x})",
    "LTRIM({x})",
    "RTRIM({x})",
    "LPAD({x}, 12, '*')",
    "RPAD({x}, 12, '*')",
    "SUBSTRING({x}, 2)",
    "SUBSTRING({x}, 2, 2)",
    "LEFT({x}, 2)",
    "RIGHT({x}, 2)",
    "REPLACE({x}, 'a', 'b')",
]


@pytest.mark.parametrize("transform", _TRANSFORMS)
@pytest.mark.parametrize(
    "source,expected",
    [
        ("name", DrakenType.VARCHAR),
        ("CAST(name AS NVARCHAR)", DrakenType.NVARCHAR),
        ("CAST(name AS VARBINARY)", DrakenType.VARBINARY),
    ],
)
def test_transform_keeps_input_type_through_concat(transform, source, expected):
    # Concatenating with the source is only legal when the bound type matches it.
    expr = transform.format(x=source)
    physical, values = _typed(f"SELECT {expr} || {source} AS r FROM $planets")
    assert physical == expected
    assert len(values) == 9


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT REVERSE(CAST('héllo' AS NVARCHAR)) AS r", "olléh"),
        ("SELECT LPAD(CAST('hé' AS NVARCHAR), 4, 'x') AS r", "xxhé"),
        ("SELECT TRIM(CAST(' hé ' AS NVARCHAR)) AS r", "hé"),
        ("SELECT SUBSTRING(CAST('héllo' AS NVARCHAR), 2, 2) AS r", "él"),
    ],
)
def test_folded_nvarchar_transform_stays_nvarchar(sql, expected):
    physical, values = _typed(sql)
    assert physical == DrakenType.NVARCHAR
    assert values == [expected]


def test_varbinary_transform_mixed_with_varchar_is_refused_at_bind():
    with pytest.raises(IncorrectTypeError):
        _typed("SELECT REVERSE(CAST(name AS VARBINARY)) || 'x' AS r FROM $planets")


@pytest.mark.parametrize("cast", ["CAST", "TRY_CAST"])
def test_cast_null_as_nvarchar_is_a_typed_null(cast):
    physical, values = _typed(f"SELECT {cast}(NULL AS NVARCHAR) AS r")
    assert physical == DrakenType.NVARCHAR
    assert values == [None]


def test_cast_null_as_nvarchar_concats_with_nvarchar():
    physical, values = _typed(
        "SELECT CAST(NULL AS NVARCHAR) || CAST(name AS NVARCHAR) AS r FROM $planets"
    )
    assert physical == DrakenType.NVARCHAR
    assert values == [None] * 9


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
