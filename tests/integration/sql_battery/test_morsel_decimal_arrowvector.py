import sys
import os

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

import pyarrow

import opteryx


def test_decimal_column_empty_and_take_empty():
    """A DECIMAL128 column reduced to zero rows keeps its row count and column
    names through Morsel.take([]) — same as every other physical type.

    Construction goes through the real engine (SQL → session.execute_to_morsels)
    rather than Morsel.from_arrow(), which no longer exists on Morsel; pyarrow is
    only the read-side oracle here (to_arrow()), which CLAUDE.md §4 allows in
    tests. Morsel.empty() has also been removed — take([]) is the current way to
    reach a zero-row morsel.
    """
    session = opteryx.session()
    morsel = next(iter(session.execute_to_morsels("SELECT CAST(gravity AS DECIMAL(30,4)) AS d FROM $planets")))
    assert morsel.num_rows == 9

    empty = morsel.take([])
    assert empty.num_rows == 0
    out = empty.to_arrow()
    assert out.num_rows == 0
    assert out.schema.names == ["d"]

    # DECIMAL128 keeps its declared (precision, scale) through to_arrow() even
    # at zero rows: Vector.to_arrow()'s fallback (_vector_shim.pyx) resolves
    # the pyarrow type from the vector's own descriptor (build_arrow_type_for)
    # instead of inferring it from an empty to_pylist(), which pyarrow can't
    # do and used to silently collapse to pa.null().
    assert out.schema.field("d").type == pyarrow.decimal128(30, 4)


if __name__ == "__main__":
    test_decimal_column_empty_and_take_empty()
    print("✅ test_decimal_column_empty_and_take_empty")
