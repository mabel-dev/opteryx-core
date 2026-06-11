# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
planets
---------

This is a sample dataset build into the engine, this simplifies a few things:

- We can write test scripts using this data, knowing that it will always be available.
- We can write examples using this data, knowing the results will always match.

This data was obtained from:
https://github.com/devstronomy/nasa-data-scraper/blob/master/data/json/planets.json

Licence @ 02-JAN-2022 when acquired - MIT Licences attested, but data appears to be
from NASA, which is Public Domain.

To access this dataset you can either run a query against dataset :planets: or you
can instantiate a PlanetData() class and use it like a Relation.
"""

import datetime
import decimal

from draken.draken_native import DrakenType
from draken.draken_native import DrakenType as DT
from draken.morsels.morsel import Morsel

from draken.interop.vector_sequence import vector_from_sequence
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema, SchemaColumn
from opteryx.utils import single_item_cache

__all__ = ("read", "schema")


def read(at_date=None, variables=None) -> Morsel:
    # fmt:off
    # Define the data
    column_names = [
        "id", "name", "mass", "diameter", "density", "gravity", "escape_velocity", "rotation_period",
        "length_of_day", "distance_from_sun", "perihelion", "aphelion", "orbital_period", "orbital_velocity",
        "orbital_inclination", "orbital_eccentricity", "obliquity_to_orbit", "mean_temperature", "surface_pressure",
        "number_of_moons",
    ]
    # Prepare the data as a list of Draken Vectors.
    vectors = [
        vector_from_sequence([1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=DT.INT8),
        vector_from_sequence(["Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune", "Pluto"], dtype=DrakenType.VARCHAR),
        vector_from_sequence([0.33, 4.87, 5.97, 0.642, 1898, 568, 86.8, 102, 0.0146], dtype=DrakenType.FLOAT64),
        vector_from_sequence([4879, 12104, 12756, 6792, 142984, 120536, 51118, 49528, 2370], dtype=DT.INT32),
        vector_from_sequence([5427, 5243, 5514, 3933, 1326, 687, 1271, 1638, 2095], dtype=DT.INT16),
        vector_from_sequence(list(map(decimal.Decimal, ("3.7", "8.9", "9.8", "3.7", "23.1", "9", "8.7", "11", "0.7"))), dtype=DrakenType.DECIMAL),
        vector_from_sequence([4.3, 10.4, 11.2, 5, 59.5, 35.5, 21.3, 23.5, 1.3], dtype=DT.FLOAT32),
        vector_from_sequence([1407.6, -5832.5, 23.9, 24.6, 9.9, 10.7, -17.2, 16.1, -153.3], dtype=DT.FLOAT32),
        vector_from_sequence([4222.6, 2802, 24, 24.7, 9.9, 10.7, 17.2, 16.1, 153.3], dtype=DT.FLOAT32),
        vector_from_sequence([57.9, 108.2, 149.6, 227.9, 778.6, 1433.5, 2872.5, 4495.1, 5906.4], dtype=DT.FLOAT32),
        vector_from_sequence([46, 107.5, 147.1, 206.6, 740.5, 1352.6, 2741.3, 4444.5, 4436.8], dtype=DT.FLOAT32),
        vector_from_sequence([69.8, 108.9, 152.1, 249.2, 816.6, 1514.5, 3003.6, 4545.7, 7375.9], dtype=DT.FLOAT32),
        vector_from_sequence([88, 224.7, 365.2, 687, 4331, 10747, 30589, 59800, 90560], dtype=DT.FLOAT32),
        vector_from_sequence([47.4, 35, 29.8, 24.1, 13.1, 9.7, 6.8, 5.4, 4.7], dtype=DT.FLOAT32),
        vector_from_sequence([7, 3.4, 0, 1.9, 1.3, 2.5, 0.8, 1.8, 17.2], dtype=DT.FLOAT32),
        vector_from_sequence([0.205, 0.007, 0.017, 0.094, 0.049, 0.057, 0.046, 0.011, 0.244], dtype=DT.FLOAT32),
        vector_from_sequence([0.03, 177.4, 23.4, 25.2, 3.1, 26.7, 97.8, 28.3, 122.5], dtype=DT.FLOAT32),
        vector_from_sequence([167, 464, 15, -63, -108, -139, -197, -201, -225], dtype=DT.INT16),
        vector_from_sequence([0, 92, 1, 0.001, None, None, None, None, 0.00001], dtype=DT.FLOAT32),
        vector_from_sequence([0, 0, 1, 2, 79, 82, 27, 14, 5], dtype=DT.INT8),
    ]
    full_morsel = Morsel.from_vectors(column_names, vectors)

    if at_date is None:
        return full_morsel

    # Make the planet data act like it supports temporality
    if at_date < datetime.datetime(1781, 4, 26):
        # April 26, 1781 - Uranus discovered by Sir William Herschel
        return full_morsel.copy(mask=[0, 1, 2, 3, 4, 5])
    if at_date < datetime.datetime(1846, 11, 13):
        # November 13, 1846 - Neptune discovered, so only planets through Uranus exist
        return full_morsel.copy(mask=[0, 1, 2, 3, 4, 5, 6])
    if at_date < datetime.datetime(1930, 3, 13):
        # March 13, 1930 - Pluto discovered by Clyde William Tombaugh
        return full_morsel.copy(mask=[0, 1, 2, 3, 4, 5, 6, 7])

    return full_morsel


def schema():
    # fmt:off
    from opteryx.types.schema import mint_column_identity
    def fc(name, **kw):
        return SchemaColumn(name=name, identity=mint_column_identity("$planets", name), **kw)
    return RelationSchema(
        name="$planets",
        columns=[
            fc(name="id",                  column_type=_lt.INT8),
            fc(name="name",                column_type=_lt.VARCHAR),
            fc(name="mass",                column_type=_lt.FLOAT64),
            fc(name="diameter",            column_type=_lt.INT32),
            fc(name="density",             column_type=_lt.INT16),
            fc(name="gravity",             column_type=_lt.DECIMAL(3, 1)),
            fc(name="escape_velocity",     column_type=_lt.FLOAT32, aliases=["escapeVelocity"]),
            fc(name="rotation_period",     column_type=_lt.FLOAT32, aliases=["rotationPeriod"]),
            fc(name="length_of_day",       column_type=_lt.FLOAT32, aliases=["lengthOfDay"]),
            fc(name="distance_from_sun",   column_type=_lt.FLOAT32, aliases=["distanceFromSun"]),
            fc(name="perihelion",          column_type=_lt.FLOAT32),
            fc(name="aphelion",            column_type=_lt.FLOAT32),
            fc(name="orbital_period",      column_type=_lt.FLOAT32, aliases=["orbitalPeriod"]),
            fc(name="orbital_velocity",    column_type=_lt.FLOAT32, aliases=["orbitalVelocity"]),
            fc(name="orbital_inclination", column_type=_lt.FLOAT32, aliases=["orbitalInclination"]),
            fc(name="orbital_eccentricity",column_type=_lt.FLOAT32, aliases=["orbitalEccentricity"]),
            fc(name="obliquity_to_orbit",  column_type=_lt.FLOAT32, aliases=["obliquityToOrbit"]),
            fc(name="mean_temperature",    column_type=_lt.INT16,   aliases=["meanTemperature"]),
            fc(name="surface_pressure",    column_type=_lt.FLOAT32, aliases=["surfacePressure"]),
            fc(name="number_of_moons",     column_type=_lt.INT8,    aliases=["numberOfMoons"]),
          ],
      )
      # fmt:on
