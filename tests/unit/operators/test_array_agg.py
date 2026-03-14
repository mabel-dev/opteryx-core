import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

from opteryx.compiled.aggregations.array_agg import ArrayAggState


def _run(sql):
    session = opteryx.session()
    session.execute(sql)
    return session.fetchall()


def test_array_agg_state_distinct_and_limit_on_entry():
    state = ArrayAggState({"distinct": True, "limit": 2})
    state.add_value(b"A")
    state.add_value(b"A")
    state.add_value(None)
    state.add_value(b"B")

    assert state.finalize() == [b"A", None]


def test_array_agg_state_ordered_descending():
    state = ArrayAggState({"ordered": True, "descending": True, "limit": 2})
    state.add_value(b"a")
    state.add_value(b"c")
    state.add_value(b"b")

    assert state.finalize() == [b"c", b"b"]


def test_array_agg_group_by_query():
    rows = _run(
        "SELECT ARRAY_AGG(name) AS names, planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        ([b"Moon"], 3),
        ([b"Phobos", b"Deimos"], 4),
        (
            [
                b"Io",
                b"Europa",
                b"Ganymede",
                b"Callisto",
                b"Amalthea",
                b"Himalia",
                b"Elara",
                b"Pasiphae",
                b"Sinope",
                b"Lysithea",
                b"Carme",
                b"Ananke",
                b"Leda",
                b"Thebe",
                b"Adrastea",
                b"Metis",
                b"Callirrhoe",
                b"Themisto",
                b"Megaclite",
                b"Taygete",
                b"Chaldene",
                b"Harpalyke",
                b"Kalyke",
                b"Iocaste",
                b"Erinome",
                b"Isonoe",
                b"Praxidike",
                b"Autonoe",
                b"Thyone",
                b"Hermippe",
                b"Aitne",
                b"Eurydome",
                b"Euanthe",
                b"Euporie",
                b"Orthosie",
                b"Sponde",
                b"Kale",
                b"Pasithee",
                b"Hegemone",
                b"Mneme",
                b"Aoede",
                b"Thelxinoe",
                b"Arche",
                b"Kallichore",
                b"Helike",
                b"Carpo",
                b"Eukelade",
                b"Cyllene",
                b"Kore",
                b"Herse",
                b"S/2000 J11",
                b"S/2003 J2",
                b"S/2003 J3",
                b"S/2003 J4",
                b"S/2003 J5",
                b"S/2003 J9",
                b"S/2003 J10",
                b"S/2003 J12",
                b"S/2003 J15",
                b"S/2003 J16",
                b"S/2003 J18",
                b"S/2003 J19",
                b"S/2003 J23",
                b"S/2010 J1",
                b"S/2010 J2",
                b"S/2011 J1",
                b"S/2011 J2",
            ],
            5,
        ),
    ]


def test_array_agg_distinct_limit_query():
    rows = _run(
        "SELECT ARRAY_AGG(DISTINCT LEFT(name, 1) LIMIT 2) AS initials, planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        ([b"M"], 3),
        ([b"P", b"D"], 4),
        ([b"I", b"E"], 5),
    ]


def test_array_agg_ordered_limit_query():
    rows = _run(
        "SELECT ARRAY_AGG(name ORDER BY name DESC LIMIT 2) AS names, planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        ([b"Moon"], 3),
        ([b"Phobos", b"Deimos"], 4),
        ([b"Thyone", b"Themisto"], 5),
    ]


def test_array_agg_multi_aggregate_query():
    rows = _run(
        "SELECT COUNT(*), ARRAY_AGG(name LIMIT 2), planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        (1, [b"Moon"], 3),
        (2, [b"Phobos", b"Deimos"], 4),
        (67, [b"Io", b"Europa"], 5),
    ]
