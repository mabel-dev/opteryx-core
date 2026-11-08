"""A shared CTE whose body is a bare `SELECT * FROM <another CTE>` must plan.

`WITH a AS (...), b AS (SELECT * FROM a) SELECT ... FROM b JOIN b ...` failed at
submit with "Relation 'a' cannot be expanded here — it has no consumer."

`SELECT * FROM a` plans as a lone Scan with nothing above it, so the body of `b`
is a one-node plan. `b` is referenced twice, so that body stays a standalone
shared plan; `a` is referenced once (from inside it), so `_finalize_cte_sharing`
splices `a` in at that Scan — and `_splice` refused, because the Scan has no
consumer to hang the Subquery boundary under. Any one of the conditions alone was
fine: a CTE referenced twice, a chain referenced once, a shared body that
projects (`SELECT id, name FROM a`).

It is the shape Apache Superset emits for a virtual dataset when the engine spec
sets `allows_cte_in_subquery = False`: the user's CTE is hoisted and wrapped as
`__cte AS (SELECT * FROM <user cte>)`, and a chart with a series limit reads
`__cte` twice — once in the outer FROM, once in the top-N subquery.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

PLANETS = [
    "Earth",
    "Jupiter",
    "Mars",
    "Mercury",
    "Neptune",
    "Pluto",
    "Saturn",
    "Uranus",
    "Venus",
]


def rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        morsel.materialize()
        names = morsel.column_names
        columns = [morsel.column(n) for n in names]
        for i in range(morsel.num_rows):
            out.append(tuple(columns[j][i] for j in range(len(names))))
    return out


def names(sql):
    return [r[0] for r in rows(sql)]


def test_bare_chain_self_join():
    assert names(
        "WITH a AS (SELECT id, name FROM $planets), "
        "     b AS (SELECT * FROM a) "
        "SELECT x.name FROM b AS x JOIN b AS y ON x.id = y.id ORDER BY x.name"
    ) == PLANETS


def test_bare_chain_second_reference_in_derived_table():
    assert names(
        "WITH a AS (SELECT id, name FROM $planets), "
        "     b AS (SELECT * FROM a) "
        "SELECT b.name FROM b JOIN (SELECT id AS i FROM b) s ON b.id = s.i ORDER BY b.name"
    ) == PLANETS


def test_three_level_bare_chain_self_join():
    assert names(
        "WITH a AS (SELECT id, name FROM $planets), "
        "     b AS (SELECT * FROM a), "
        "     c AS (SELECT * FROM b) "
        "SELECT x.name FROM c AS x JOIN c AS y ON x.id = y.id ORDER BY x.name"
    ) == PLANETS


def test_three_level_bare_chain_shared_at_two_levels():
    # `b` is read from `c`'s body AND from the outer query, `c` once - so `b` is
    # shared and `c` splices a reference to it inline
    assert names(
        "WITH a AS (SELECT id, name FROM $planets), "
        "     b AS (SELECT * FROM a), "
        "     c AS (SELECT * FROM b) "
        "SELECT x.name FROM c AS x JOIN b AS y ON x.id = y.id ORDER BY x.name"
    ) == PLANETS


def test_superset_series_limit_shape():
    # the hoisted-CTE wrapper Superset generates, with a top-N series subquery
    result = rows(
        "WITH planets AS (SELECT id, name, gravity FROM $planets), "
        "     __cte AS (SELECT * FROM planets) "
        "SELECT __cte.name, MAX(__cte.gravity) AS g "
        "FROM __cte "
        "JOIN (SELECT name AS name__, MAX(gravity) AS mx FROM __cte "
        "      GROUP BY name ORDER BY mx DESC LIMIT 3) AS series_limit "
        "  ON __cte.name = series_limit.name__ "
        "GROUP BY __cte.name ORDER BY g DESC"
    )
    assert [r[0] for r in result] == ["Jupiter", "Neptune", "Earth"], result


def test_bare_chain_matches_projecting_chain():
    # the projecting spelling never hit the defect; both must agree
    bare = rows(
        "WITH a AS (SELECT id, name FROM $planets), b AS (SELECT * FROM a) "
        "SELECT x.id, y.name FROM b AS x JOIN b AS y ON x.id = y.id ORDER BY x.id"
    )
    projecting = rows(
        "WITH a AS (SELECT id, name FROM $planets), b AS (SELECT id, name FROM a) "
        "SELECT x.id, y.name FROM b AS x JOIN b AS y ON x.id = y.id ORDER BY x.id"
    )
    assert bare == projecting, (bare, projecting)
    assert len(bare) == 9, bare


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
