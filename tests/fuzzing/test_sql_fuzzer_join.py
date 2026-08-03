"""
Generate random SQL JOINs

These are pretty basic joins but this approach still finds bugs.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import random
import time
from dataclasses import dataclass

import pytest

import opteryx
from opteryx.types import LogicalCategory
from opteryx.types.logical_type import _CATEGORY_OF
from opteryx.utils import random_string
from opteryx.utils.formatter import format_sql
from tests.helpers import execute_and_get_shape


def random_value(t):
    if t == LogicalCategory.VARCHAR:
        return f"'{random_string(4)}'"
    if t == LogicalCategory.VARBINARY:
        return f"b'{random_string(8)}'"
    if t in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP):
        # Use a fixed reference date to ensure reproducibility.
        # The ::TIMESTAMP cast is required, not decorative: Opteryx does not
        # implicitly coerce a string literal to a temporal column type, and
        # rejects `date_col = '1930-01-01'` with IncompatibleTypesError. Without
        # the cast every temporal predicate this generator emits dies in the
        # binder, so no temporal join or filter is ever actually executed.
        reference_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
        if random.random() < 0.5:
            return f"'{reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))}'::TIMESTAMP"
        return f"'{(reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))).date()}'::TIMESTAMP"
    if random.random() < 0.5:
        return random.randint(-1000000, 1000000)
    return random.randint(-1000000, 1000000) / 1000


def generate_condition(table, columns):
    where_column = columns[random.choice(range(len(columns)))]
    # STRUCT has no LogicalCategory member; ARRAY is the only complex type to exclude.
    while where_column.category in (LogicalCategory.ARRAY,):
        where_column = columns[random.choice(range(len(columns)))]
    if random.random() < 0.1:
        where_operator = random.choice(["IS", "IS NOT"])
        if where_column.category == LogicalCategory.BOOLEAN:
            where_value = random.choice(["TRUE", "FALSE", "NULL"])
        else:
            where_value = "NULL"
    elif where_column.category in (LogicalCategory.VARCHAR, LogicalCategory.VARBINARY) and random.random() < 0.5:
        where_operator = random.choice(
            ["LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "RLIKE", "NOT RLIKE"]
        )
        where_value = (
            random_value(where_column.category).replace("1", "%").replace("A", "%").replace("6", "_")
        )
    elif random.random() < 0.8:
        where_operator = random.choice(["==", "<>", "=", "!=", "<", "<=", ">", ">="])
        where_value = f"{str(random_value(where_column.category))}"
    else:
        return f"{table}.{where_column.name} BETWEEN {str(random_value(where_column.category))} AND {str(random_value(where_column.category))}"
    return f"{table}.{where_column.name} {where_operator} {where_value}"


def generate_random_sql_join(columns1, table1, columns2, table2) -> str:
    join_type = random.choice(
        [
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "LEFT OUTER JOIN",
            "RIGHT JOIN",
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

        left_column = columns1[random.choice(range(len(columns1)))]
        right_column = columns2[random.choice(range(len(columns2)))]
        while left_column.category != right_column.category or left_column.category in (
            LogicalCategory.ARRAY,
        ):
            left_column = columns1[random.choice(range(len(columns1)))]
            right_column = columns2[random.choice(range(len(columns2)))]

        condition = f"{table1}.{left_column.name} = {table2}.{right_column.name}"
        conditions.append(condition)

    join_condition = " AND ".join(conditions)

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


# The $satellites, $astronauts and $missions virtual datasets no longer exist —
# only $planets survives in opteryx.managers.virtual_datasets. The same four
# relations are still present as parquet under testdata/, so the join fuzzer
# reads them from there. A join fuzzer needs two DIFFERENT relations, so a
# single-table fallback would leave it generating nothing.
FUZZ_TABLES = (
    "testdata.planets",
    "testdata.satellites",
    "testdata.missions",
    "testdata.astronauts",
)


@dataclass(frozen=True)
class FuzzColumn:
    """A column as the generators need it: a name plus a dispatch category.

    Mirrors the `.name` / `.category` surface of `SchemaColumn`, which is all
    the generators below ever touch.
    """

    name: str
    category: LogicalCategory


# Tables to use for fuzzing
_tables_cache = None


def get_tables():
    """Lazy initialization of tables to avoid expensive setup during test collection"""
    global _tables_cache
    if _tables_cache is not None:
        return _tables_cache

    # Ask the engine what each relation actually contains rather than carrying a
    # hardcoded copy of the schemas — a hardcoded table drifts silently the
    # moment the test data changes, and a fuzzer built on a stale schema
    # generates queries that only ever exercise the binder's error path.
    _tables_cache = []
    for table in FUZZ_TABLES:
        session = opteryx.session()
        morsels = list(session.execute_to_morsels(f"SELECT * FROM {table}"))
        if not morsels:
            raise ValueError(f"fuzzing source table {table!r} returned no data")
        fields = [
            FuzzColumn(name=name, category=_CATEGORY_OF[physical])
            for name, physical in morsels[0].schema.items()
        ]
        _tables_cache.append({"name": table, "fields": fields})
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
    seed = i
    random.seed(seed)

    table1 = TABLES[random.choice(range(len(TABLES)))]
    table2 = TABLES[random.choice(range(len(TABLES)))]
    while table1 == table2:
        table2 = TABLES[random.choice(range(len(TABLES)))]
    statement = generate_random_sql_join(
        table1["fields"], table1["name"], table2["fields"], table2["name"]
    )
    formatted_statement = format_sql(statement)

    print(formatted_statement)

    start_time = time.time()  # Start timing the query execution
    try:
        shape = execute_and_get_shape(statement)
        execution_time = time.time() - start_time  # Measure execution time
        print(
            f"Shape: {shape}, Execution Time: {execution_time:.2f} seconds, Seed: {seed} ({i})"
        )
        # Additional success criteria checks can be added here
    except Exception as e:
        import traceback

        print(f"\033[0;31mError in Test Cycle {i + 1} Seed: {seed} \033[0m: {e}")
        print(traceback.print_exc())
        # Log failing statement and error for analysis
        raise e
    print()


if __name__ == "__main__":  # pragma: no cover
    for i in range(TEST_CYCLES):
        test_sql_fuzzing_join(i)

    print("✅ okay")
