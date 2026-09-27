"""Regression: `IN` / `NOT IN` over DECIMAL and DECIMAL128 columns.

A multi-value IN list over a DECIMAL column was refused ("a comparison in a filter
predicate `d IN [3.3, 7.7]`, outside the c-native kernel set, is not supported"),
and so was `d = 3.30 OR d = 7.7`, which the optimizer rewrites into that IN list.
Through a connector that pushes DECIMAL predicates into the scan (an Iceberg
catalog), the same list failed later with `vector_in_list: unsupported vector type`.

`draken_in_list` already read int64-backed DECIMAL on its kind-0 (sorted int64)
arm; `_build_in_list_blob` (compiled_expression.pyx) simply had no DECIMAL branch
to rescale the literals onto the column's scale. DECIMAL128 gained a kind-4
(sorted int128) arm. A literal no stored value can equal (off the scale grid) is
dropped from the set - exact, never rounded to a neighbouring value.

Every expected count is arithmetic on VALUES below, not observed behaviour.
"""

import os
import sys
from decimal import Decimal

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import opteryx

VALUES = [Decimal(v) for v in ("1.10", "2.20", "3.30", "4.40", "5.50", "6.60", "7.70")] + [None]

# (storage layout id, precision, store_decimal_as_integer, use_dictionary)
LAYOUTS = [
    ("int32", 9, True, True),
    ("int64", 18, True, True),
    ("flba-9", 9, False, True),
    ("decimal128-plain", 38, False, False),
    ("decimal128-dict", 38, False, True),  # pyarrow's default: DK_DECIMAL128_DICT
]


@pytest.fixture(scope="module", params=LAYOUTS, ids=[layout[0] for layout in LAYOUTS])
def dataset(request, tmp_path_factory):
    name, precision, as_integer, use_dictionary = request.param
    directory = tmp_path_factory.mktemp(f"decimal_in_{name}")
    table = pa.table({"d": pa.array(VALUES, pa.decimal128(precision, 2))})
    pq.write_table(
        table,
        os.path.join(directory, "part.parquet"),
        store_decimal_as_integer=as_integer,
        use_dictionary=use_dictionary,
    )
    return str(directory)


def _count(dataset, predicate):
    sql = f"SELECT d FROM '{dataset}' WHERE {predicate}"
    return sum(morsel.num_rows for morsel in opteryx.session().execute_to_morsels(sql))


@pytest.mark.parametrize(
    "predicate, expected",
    [
        ("d IN (3.30, 7.70)", 2),
        ("d IN (1.10, 7.70)", 2),  # both ends of the range
        ("d IN (3.30, 7.70, 3.305)", 2),  # an off-grid member matches nothing
        ("d IN (9.99, 3.305)", 0),
        ("d IN (1.1, 1.10, 2.2)", 2),  # the same value spelled twice
        ("d IN (3, 7)", 0),  # integer literals: 3.00 and 7.00 are not stored
        ("d = 3.30 OR d = 7.7", 2),  # rewritten into an IN list by the optimizer
        # NOT IN: the NULL row is neither in nor not in the list.
        ("d NOT IN (3.30, 7.70)", 5),
        ("d NOT IN (3.305)", 7),
    ],
)
def test_decimal_membership(dataset, predicate, expected):
    assert _count(dataset, predicate) == expected
