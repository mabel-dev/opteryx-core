"""VECTOR is not a SQL type (architect ruling 2026-10-01).

Vectors exist only inside vector indexes, which embed text themselves. Every surface that
could put a vector in user hands is refused; the text similarity surface, which embeds
internally and returns a FLOAT64, stays.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "../.."))

import pytest

import opteryx
from opteryx.exceptions import FunctionNotFoundError
from opteryx.exceptions import SqlError


def _run(sql):
    return list(opteryx.session().execute_to_morsels(sql))


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CAST([1.0, 2.0] AS VECTOR(2))",
        "SELECT name::VECTOR(4) FROM $planets",
        "SELECT TRY_CAST(name AS VECTOR(4)) FROM $planets",
    ],
)
def test_vector_type_spelling_is_refused(sql):
    with pytest.raises(SqlError) as err:
        _run(sql)
    assert "vector indexes" in str(err.value), str(err.value)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT EMBED('hello')",
        "SELECT COSINE_SIMILARITY(EMBED(name), EMBED('earth')) FROM $planets",
    ],
)
def test_embed_is_not_a_sql_function(sql):
    with pytest.raises(FunctionNotFoundError):
        _run(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT COSINE_SIMILARITY('quick brown fox', 'brown fox')",
        "SELECT name, COSINE_DISTANCE(name, 'mars') AS d FROM $planets ORDER BY d LIMIT 3",
        "SELECT name FROM $planets WHERE MATCH(name) AGAINST('earth')",
    ],
)
def test_text_similarity_surface_stays(sql):
    _run(sql)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
