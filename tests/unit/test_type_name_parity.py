"""SQL type names: draken owns the mapping, opteryx delegates, the format is pinned.

`str(ColumnType)` is not a display string — it is PERSISTED into stored schemas.
A TIMESTAMP written at `ms` and read back as the `us` default reads every value
1000x off, silently, so the exact text is a storage format.

Draken is the single source of that mapping (architect's ruling, 2026-08-08):
`draken/logical_type.h::type_display_name_parts`. `ColumnType.__str__` delegates
to it rather than keeping a second table, because two tables either side of a
module boundary is how one surface renders a column `UINT32` while another
renders the same column `IPV4` — the defect this replaced, in draken's own
Morsel renderer.

Three things are pinned here, and they protect different failures:

  1. LITERAL EXPECTED NAMES. Comparing `str(ColumnType)` against draken would be
     tautological now that it delegates. Only fixed strings can catch a change to
     the stored format, whichever side made it.
  2. `_NAME_OF` against draken. Draken owns name GENERATION; opteryx still owns
     name PARSING (`_NAME_TO_PHYSICAL` is `_NAME_OF` inverted). That seam is the
     one place the two directions can still drift — a rename on the draken side
     alone would write names opteryx can no longer read back.
  3. Descriptor-refined names. A UINT32 carrying IPV4 must not fall through to
     "UINT32": that loses the descriptor on round-trip, which is what made an
     IPv4 column render as an integer.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

from draken.draken_native import TimestampUnit
from draken.vectors.vector import type_display_name as draken_name

import opteryx.types.logical_type as lt

_UNIT_TEXT = {
    TimestampUnit.SECONDS: "s",
    TimestampUnit.MILLISECONDS: "ms",
    TimestampUnit.MICROSECONDS: "us",
    TimestampUnit.NANOSECONDS: "ns",
}

# The persisted format. Change a value here only when deliberately changing what
# is written to storage — and then the catalog needs a backfill, not just a test
# edit.
EXPECTED = [
    (lt.INT8, "INT8"),
    (lt.INT16, "INT16"),
    (lt.INT32, "INT32"),
    (lt.INT64, "INT64"),
    (lt.UINT8, "UINT8"),
    (lt.UINT16, "UINT16"),
    (lt.UINT32, "UINT32"),
    (lt.UINT64, "UINT64"),
    (lt.FLOAT32, "FLOAT32"),
    (lt.FLOAT64, "FLOAT64"),
    (lt.BOOLEAN, "BOOL"),
    (lt.DATE, "DATE"),
    (lt.INTERVAL, "INTERVAL"),
    (lt.VARCHAR, "VARCHAR"),
    (lt.NVARCHAR, "NVARCHAR"),
    (lt.VARBINARY, "VARBINARY"),
    (lt.VARIANT, "VARIANT"),
    (lt.NULL, "NULL"),
    # UINT32 refined by the IPV4 descriptor — the tag alone would say UINT32.
    (lt.IPV4, "IPV4"),
    # Note the space after the comma: it is part of the stored format.
    (lt.DECIMAL(10, 2), "DECIMAL(10, 2)"),
    (lt.DECIMAL(3, 1), "DECIMAL(3, 1)"),
    (lt.DECIMAL(38, 0), "DECIMAL(38, 0)"),
    (lt.VECTOR(384), "VECTOR(384)"),
    # The unit is emitted ALWAYS, not only when non-default, so a reader never
    # has to know what the default is.
    (lt.TIMESTAMP(TimestampUnit.SECONDS), "TIMESTAMP[s]"),
    (lt.TIMESTAMP(TimestampUnit.MILLISECONDS), "TIMESTAMP[ms]"),
    (lt.TIMESTAMP(TimestampUnit.MICROSECONDS), "TIMESTAMP[us]"),
    (lt.TIMESTAMP(TimestampUnit.NANOSECONDS), "TIMESTAMP[ns]"),
    (lt.TIME(TimestampUnit.SECONDS), "TIME[s]"),
    (lt.TIME(TimestampUnit.MICROSECONDS), "TIME[us]"),
    # ARRAY composes its element type, which draken has no concept of — draken
    # names the tag, opteryx composes the rest.
    (lt.ARRAY(lt.VARCHAR), "ARRAY<VARCHAR>"),
    (lt.ARRAY(lt.INT64), "ARRAY<INT64>"),
    (lt.ARRAY(lt.IPV4), "ARRAY<IPV4>"),
]


def _draken_name_of(column_type):
    """Draken's name for a ColumnType's physical tag plus descriptor."""
    logical = column_type.logical
    return draken_name(
        column_type.physical,
        kind=(logical.kind if logical is not None else None),
        unit=(_UNIT_TEXT.get(logical.unit) if logical is not None else None),
        precision=(logical.precision if logical is not None else 0),
        scale=(logical.scale if logical is not None else 0),
        dimension=(logical.dimension if logical is not None else 0),
    )


def test_stored_type_names_are_exact():
    """The persisted format, pinned to literals."""
    for column_type, expected in EXPECTED:
        assert str(column_type) == expected, (expected, str(column_type))


def test_draken_agrees_for_every_delegated_type():
    """Every non-ARRAY name comes from draken unchanged.

    ARRAY is excluded deliberately: draken returns the bare tag and opteryx adds
    the element, so they are expected to differ there — asserted below.
    """
    for column_type, expected in EXPECTED:
        if column_type.physical.name == "ARRAY":
            continue
        assert _draken_name_of(column_type) == expected, (
            expected, _draken_name_of(column_type)
        )


def test_array_tag_is_named_by_draken_element_by_opteryx():
    array_type = lt.ARRAY(lt.VARCHAR)
    assert _draken_name_of(array_type) == "ARRAY"
    assert str(array_type) == "ARRAY<VARCHAR>"


def test_name_of_table_matches_draken():
    """Guards the seam: draken generates names, opteryx still PARSES them.

    `_NAME_TO_PHYSICAL` is `_NAME_OF` inverted, so a rename on draken's side alone
    would emit names opteryx could no longer read back — a write path and a read
    path disagreeing, which is worse than either being wrong on its own.
    """
    for physical, name in lt._NAME_OF.items():
        assert draken_name(physical) == name, (physical, name, draken_name(physical))


def test_parse_round_trips_every_stored_name():
    """Every name written can be read back to the same type."""
    for column_type, expected in EXPECTED:
        parsed = lt.parse_column_type(expected)
        assert str(parsed) == expected, (expected, str(parsed))


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
