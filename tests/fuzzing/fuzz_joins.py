"""
Generate random SQL JOINs

These are pretty basic joins but this approach still finds bugs.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import argparse
import datetime
import random
import time

import pytest

from opteryx.models import QueryTelemetry
from opteryx.types import OrsoTypes
from opteryx.utils import random_int, random_string
from opteryx.utils.formatter import format_sql
from tests.helpers import (
    execute_and_fetch_all,
    execute_and_get_arrow,
    execute_and_get_rowcount,
    execute_and_get_shape,
)


def random_value(t):
    if t == OrsoTypes.VARCHAR:
        return f"'{random_string(4)}'"
    if t == OrsoTypes.BLOB:
        return f"b'{random_string(8)}'"
    if t in (OrsoTypes.DATE, OrsoTypes.TIMESTAMP):
        # Use a fixed reference date to ensure reproducibility
        reference_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
        if random.random() < 0.5:
            return f"'{reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))}'::TIMESTAMP"
        return f"'{(reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))).date()}'::TIMESTAMP"
    if random.random() < 0.5:
        return random.randint(-1000000, 1000000)
    return random.randint(-1000000, 1000000) / 1000


def generate_condition(table, columns):
    where_column = columns[random.choice(range(len(columns)))]
    while where_column.type in (OrsoTypes.ARRAY, OrsoTypes.STRUCT):
        where_column = columns[random.choice(range(len(columns)))]
    if random.random() < 0.1:
        where_operator = random.choice(["IS", "IS NOT"])
        if where_column.type == OrsoTypes.BOOLEAN:
            where_value = random.choice(["TRUE", "FALSE", "NULL"])
        else:
            where_value = "NULL"
    elif where_column.type in (OrsoTypes.VARCHAR, OrsoTypes.BLOB) and random.random() < 0.5:
        where_operator = random.choice(
            ["LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "RLIKE", "NOT RLIKE"]
        )
        where_value = (
            random_value(where_column.type).replace("1", "%").replace("A", "%").replace("6", "_")
        )
    elif random.random() < 0.8:
        where_operator = random.choice(["==", "<>", "=", "!=", "<", "<=", ">", ">="])
        where_value = f"{str(random_value(where_column.type))}"
    else:
        return f"{table}.{where_column.name} BETWEEN {str(random_value(where_column.type))} AND {str(random_value(where_column.type))}"
    return f"{table}.{where_column.name} {where_operator} {where_value}"


def generate_random_sql_join(columns1, table1, columns2, table2) -> str:
    join_type = random.choice(
        [
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            # "RIGHT JOIN",
            "FULL OUTER JOIN",
            "LEFT ANTI JOIN",
            "LEFT SEMI JOIN",
            "ANTI JOIN",
            "SEMI JOIN",
        ]
    )

    last_value = -1
    this_value = random.random()
    conditions = []
    # we add multiple conditions by cycling over ever increasing random values until we get a lower one
    while this_value > last_value:
        last_value = this_value
        this_value = random.random()

        # Try up to 5 times to find matching column types
        attempts = 0
        left_column = None
        right_column = None
        while attempts < 5:
            left_column = columns1[random.choice(range(len(columns1)))]
            right_column = columns2[random.choice(range(len(columns2)))]
            if left_column.type == right_column.type and left_column.type not in (
                OrsoTypes.ARRAY,
                OrsoTypes.STRUCT,
            ):
                break
            attempts += 1

        # If we couldn't find matching types after 5 attempts, skip this join condition
        if left_column.type != right_column.type or left_column.type in (
            OrsoTypes.ARRAY,
            OrsoTypes.STRUCT,
        ):
            return None  # Signal that this table pair doesn't work

        condition = f"{table1}.{left_column.name} = {table2}.{right_column.name}"
        conditions.append(condition)

    join_condition = " AND ".join(conditions) if conditions else None
    if not join_condition:
        return None

    if join_type in ("LEFT ANTI JOIN", "LEFT SEMI JOIN", "ANTI JOIN", "SEMI JOIN"):
        selected_columns = [f"{table1}.{col.name}" for col in columns1 if random.random() < 0.2]
    else:
        selected_columns = [f"{table1}.{col.name}" for col in columns1 if random.random() < 0.2] + [
            f"{table2}.{col.name}" for col in columns2 if random.random() < 0.2
        ]
    if len(selected_columns) == 0:
        selected_columns = ["*"]
    select_clause = "SELECT " + ", ".join(selected_columns)

    where_clause = "--"
    # Generate a WHERE clause with 70% chance
    if random.random() < 0.3:
        if where_clause == "--":
            where_clause = " WHERE "
        where_clause += generate_condition(table1, columns1)
        # add an abitrary number of additional conditions
        while random.random() < 0.1:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_clause += f" {linking_condition} {generate_condition(table1, columns1)}"

    if (
        join_type not in ("LEFT ANTI JOIN", "LEFT SEMI JOIN", "ANTI JOIN", "SEMI JOIN")
        and random.random() < 0.3
    ):
        if where_clause == "--":
            where_clause = " WHERE "
        else:
            where_clause += f" {random.choice(['AND', 'OR', 'AND NOT'])} "
        where_clause += generate_condition(table2, columns2)
        # add an abitrary number of additional conditions
        while random.random() < 0.1:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_clause += f" {linking_condition} {generate_condition(table2, columns2)}"

    query = f"{select_clause} FROM {table1} {join_type} {table2} ON {join_condition} {where_clause}"

    return query


import pyarrow.parquet as pq

# Hardcoded schema from Parquet files - extracted once at setup time
TESTDATA_SCHEMAS = {
    "testdata.planets": [
        ("id", OrsoTypes.INTEGER),
        ("name", OrsoTypes.VARCHAR),
        ("mass", OrsoTypes.DOUBLE),
        ("diameter", OrsoTypes.DOUBLE),
        ("density", OrsoTypes.DOUBLE),
        ("gravity", OrsoTypes.DOUBLE),
        ("escapeVelocity", OrsoTypes.DOUBLE),
        ("rotationPeriod", OrsoTypes.DOUBLE),
        ("lengthOfDay", OrsoTypes.DOUBLE),
        ("distanceFromSun", OrsoTypes.DOUBLE),
        ("perihelion", OrsoTypes.DOUBLE),
        ("aphelion", OrsoTypes.DOUBLE),
        ("orbitalPeriod", OrsoTypes.DOUBLE),
        ("orbitalVelocity", OrsoTypes.DOUBLE),
        ("orbitalInclination", OrsoTypes.DOUBLE),
        ("orbitalEccentricity", OrsoTypes.DOUBLE),
        ("obliquityToOrbit", OrsoTypes.DOUBLE),
        ("meanTemperature", OrsoTypes.DOUBLE),
        ("surfacePressure", OrsoTypes.DOUBLE),
        ("numberOfMoons", OrsoTypes.INTEGER),
    ],
    "testdata.satellites": [
        ("id", OrsoTypes.INTEGER),
        ("planetId", OrsoTypes.INTEGER),
        ("name", OrsoTypes.VARCHAR),
        ("gm", OrsoTypes.DOUBLE),
        ("radius", OrsoTypes.DOUBLE),
        ("density", OrsoTypes.DOUBLE),
        ("magnitude", OrsoTypes.DOUBLE),
        ("albedo", OrsoTypes.DOUBLE),
    ],
    "testdata.missions": [
        ("Company", OrsoTypes.VARCHAR),
        ("Location", OrsoTypes.VARCHAR),
        ("Price", OrsoTypes.DOUBLE),
        ("Lauched_at", OrsoTypes.TIMESTAMP),
        ("Rocket", OrsoTypes.VARCHAR),
        ("Rocket_Status", OrsoTypes.VARCHAR),
        ("Mission", OrsoTypes.VARCHAR),
        ("Mission_Status", OrsoTypes.VARCHAR),
    ],
    "testdata.astronauts": [
        ("name", OrsoTypes.VARCHAR),
        ("year", OrsoTypes.INTEGER),
        ("group", OrsoTypes.DOUBLE),
        ("status", OrsoTypes.VARCHAR),
        ("birth_date", OrsoTypes.DATE),
        ("birth_place", OrsoTypes.VARCHAR),
        ("gender", OrsoTypes.VARCHAR),
        ("undergraduate_major", OrsoTypes.VARCHAR),
        ("graduate_major", OrsoTypes.VARCHAR),
        ("military_rank", OrsoTypes.VARCHAR),
        ("military_branch", OrsoTypes.VARCHAR),
        ("space_flights", OrsoTypes.INTEGER),
        ("space_flight_hours", OrsoTypes.INTEGER),
        ("space_walks", OrsoTypes.INTEGER),
        ("space_walks_hours", OrsoTypes.DOUBLE),
        ("death_date", OrsoTypes.DATE),
        ("death_mission", OrsoTypes.VARCHAR),
    ],
}


def _get_testdata_schema(table_path):
    """Get hardcoded schema for testdata table"""
    if table_path in TESTDATA_SCHEMAS:
        columns = []

        class Column:
            def __init__(self, name, type_):
                self.name = name
                self.type = type_

        for col_name, col_type in TESTDATA_SCHEMAS[table_path]:
            columns.append(Column(col_name, col_type))
        return table_path, columns
    return None, []


_tables_cache = None


def get_tables():
    """Lazy initialization of tables to avoid expensive setup during test collection"""
    global _tables_cache
    if _tables_cache is not None:
        return _tables_cache

    _tables_cache = []

    # Load testdata tables
    for table_path in [
        "testdata.planets",
        "testdata.satellites",
        "testdata.missions",
        "testdata.astronauts",
    ]:
        try:
            name, fields = _get_testdata_schema(table_path)
            if name and fields:
                _tables_cache.append({"name": name, "fields": fields})
        except Exception:
            pass  # Skip tables that don't exist or fail to load

    # Fallback to virtual datasets if no testdata available
    if not _tables_cache:
        try:
            from opteryx.managers import virtual_datasets

            _tables_cache.append(
                {
                    "name": virtual_datasets.planets.schema().name,
                    "fields": virtual_datasets.planets.schema().columns,
                }
            )
        except Exception:
            pass

    return _tables_cache


# Keep old TABLES reference for compatibility but make it lazy
class LazyTables:
    def __getitem__(self, key):
        return get_tables()[key]

    def __iter__(self):
        return iter(get_tables())

    def __len__(self):
        return len(get_tables())


TABLES = LazyTables()

TEST_CYCLES: int = 10


@pytest.mark.parametrize("i", range(TEST_CYCLES))
def test_sql_fuzzing_join(i):
    # Use test iteration number as seed for reproducibility
    seed = random.randint(-10_000_000, 10_000_000)
    random.seed(seed)

    statement = None
    attempts = 0
    max_attempts = 20  # Retry up to 20 times to find valid table pair

    while statement is None and attempts < max_attempts:
        table1 = TABLES[random.choice(range(len(TABLES)))]
        table2 = TABLES[random.choice(range(len(TABLES)))]
        while table1 == table2:
            table2 = TABLES[random.choice(range(len(TABLES)))]
        statement = generate_random_sql_join(
            table1["fields"], table1["name"], table2["fields"], table2["name"]
        )
        attempts += 1

    if statement is None:
        print(
            f"⚠ Test Cycle {i + 1} Seed: {seed}: Could not generate valid join after {max_attempts} attempts"
        )
        return

    formatted_statement = format_sql(statement)

    print(formatted_statement)

    start_time = time.time()  # Start timing the query execution
    try:
        shape = execute_and_get_shape(statement)
        execution_time = time.time() - start_time  # Measure execution time
        print(f"Shape: {shape}, Execution Time: {execution_time:.2f} seconds, Seed: {seed} ({i})")
        # Additional success criteria checks can be added here
    except Exception as e:
        import traceback

        print(f"\033[0;31mError in Test Cycle {i + 1} Seed: {seed} \033[0m: {e}")
        print(traceback.print_exc())
        # Log failing statement and error for analysis
        raise e
    print()


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Fuzz test SQL JOINs")
    parser.add_argument(
        "--iterations", type=int, default=TEST_CYCLES, help="Number of test iterations"
    )
    args = parser.parse_args()

    for i in range(args.iterations):
        print(f"Test Cycle {i + 1} Seed: {i}")
        test_sql_fuzzing_join(i)

    print("✅ okay")
