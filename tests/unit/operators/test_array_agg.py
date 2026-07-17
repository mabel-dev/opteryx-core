import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

from tests.helpers import execute_and_fetch_all


def _run(sql):
    rows = execute_and_fetch_all(sql)
    return [tuple(row.values()) for row in rows]


def test_array_agg_group_by_query():
    rows = _run(
        "SELECT ARRAY_AGG(name) AS names, planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        (["Moon"], 3),
        (["Phobos", "Deimos"], 4),
        (
            [
                "Io",
                "Europa",
                "Ganymede",
                "Callisto",
                "Amalthea",
                "Himalia",
                "Elara",
                "Pasiphae",
                "Sinope",
                "Lysithea",
                "Carme",
                "Ananke",
                "Leda",
                "Thebe",
                "Adrastea",
                "Metis",
                "Callirrhoe",
                "Themisto",
                "Megaclite",
                "Taygete",
                "Chaldene",
                "Harpalyke",
                "Kalyke",
                "Iocaste",
                "Erinome",
                "Isonoe",
                "Praxidike",
                "Autonoe",
                "Thyone",
                "Hermippe",
                "Aitne",
                "Eurydome",
                "Euanthe",
                "Euporie",
                "Orthosie",
                "Sponde",
                "Kale",
                "Pasithee",
                "Hegemone",
                "Mneme",
                "Aoede",
                "Thelxinoe",
                "Arche",
                "Kallichore",
                "Helike",
                "Carpo",
                "Eukelade",
                "Cyllene",
                "Kore",
                "Herse",
                "S/2000 J11",
                "S/2003 J2",
                "S/2003 J3",
                "S/2003 J4",
                "S/2003 J5",
                "S/2003 J9",
                "S/2003 J10",
                "S/2003 J12",
                "S/2003 J15",
                "S/2003 J16",
                "S/2003 J18",
                "S/2003 J19",
                "S/2003 J23",
                "S/2010 J1",
                "S/2010 J2",
                "S/2011 J1",
                "S/2011 J2",
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
        (["M"], 3),
        (["P", "D"], 4),
        (["I", "E"], 5),
    ]


def test_array_agg_ordered_limit_query():
    rows = _run(
        "SELECT ARRAY_AGG(name ORDER BY name DESC LIMIT 2) AS names, planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        (["Moon"], 3),
        (["Phobos", "Deimos"], 4),
        (["Thyone", "Themisto"], 5),
    ]


def test_array_agg_multi_aggregate_query():
    rows = _run(
        "SELECT COUNT(*), ARRAY_AGG(name LIMIT 2), planetId "
        "FROM testdata.satellites GROUP BY planetId ORDER BY planetId LIMIT 3"
    )

    assert rows == [
        (1, ["Moon"], 3),
        (2, ["Phobos", "Deimos"], 4),
        (67, ["Io", "Europa"], 5),
    ]
