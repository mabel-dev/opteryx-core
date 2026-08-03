"""
Generate random SQL statements

These are pretty basic statements but this has still found bugs.

We test virtual datasets and parquet file datasets.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import random
import time

import pytest

from opteryx.managers import virtual_datasets
from opteryx.types import LogicalCategory
from opteryx.utils import random_string
from opteryx.utils.formatter import format_sql
from tests.helpers import execute_and_get_shape


def random_value(t):
    if t == LogicalCategory.VARCHAR:
        return f"'{random_string(4)}'"
    if t == LogicalCategory.VARBINARY:
        return f"b'{random_string(8)}'"
    if t in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP):
        # Use a fixed reference date to ensure reproducibility
        reference_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
        if random.random() < 0.5:
            return f"'{reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))}'"
        return f"'{(reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))).date()}'"
    if random.random() < 0.5:
        return random.randint(-1000000, 1000000)
    return random.randint(-1000000, 1000000) / 1000


def generate_condition(columns):
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
        return f"{where_column.name} BETWEEN {str(random_value(where_column.category))} AND {str(random_value(where_column.category))}"
    return f"{where_column.name} {where_operator} {where_value}"


def generate_random_sql_select(columns, table):
    # Generate a list of column names to select
    column_list = list(
        set(random.choices(range(len(columns)), k=max(int(random.random() * len(columns)), 1)))
    )
    column_list = [columns[i] for i in column_list]
    agg_column = None
    is_count_star = False
    is_distinct = False
    # Add DISTINCT keyword with 20% chance
    if random.random() < 0.2:
        is_distinct = True
        select_clause = "SELECT DISTINCT " + ", ".join(c.name for c in column_list)
    elif random.random() < 0.3:
        distinct = "DISTINCT " if random.random() < 0.1 else ""
        agg_func = random.choice(["MIN", "MAX", "SUM", "AVG", "COUNT", "COUNT_DISTINCT"])
        agg_column = columns[random.choice(range(len(columns)))]
        while agg_func in ("SUM", "AVG") and agg_column.category in (
            LogicalCategory.ARRAY,
            LogicalCategory.VARCHAR,
            LogicalCategory.VARBINARY,
            LogicalCategory.TIMESTAMP,
            LogicalCategory.DATE,
        ):
            agg_column = columns[random.choice(range(len(columns)))]
        while agg_func in ("MIN", "MAX", "COUNT_DISTINCT", "COUNT") and agg_column.category in (
            LogicalCategory.ARRAY,
        ):
            agg_column = columns[random.choice(range(len(columns)))]
        select_clause = "SELECT " + distinct + agg_func + "(" + agg_column.name + ")"

        column_list = [c for c in column_list if c.category not in (LogicalCategory.ARRAY,)]
    elif random.random() < 0.8:
        select_clause = "SELECT " + ", ".join(c.name for c in column_list)
    elif random.random() < 0.5:
        select_clause = "SELECT COUNT(*) "
        is_count_star = True
    else:
        select_clause = "SELECT *"
    # Add table name
    if random.random() < 0.1:
        return f"SELECT * FROM ({generate_random_sql_select(columns, table)}) as table_{random_string(4)}"
    else:
        select_clause = select_clause + " FROM " + table
    # Generate a WHERE clause with 70% chance
    if random.random() < 0.7:
        where_clause = generate_condition(columns)
        # add an abitrary number of additional conditions
        while random.random() < 0.3:
            linking_condition = random.choice(["AND", "OR", "AND NOT"])
            where_clause += f" {linking_condition} {generate_condition(columns)} "
        select_clause = f"{select_clause} WHERE {where_clause}"
    # Add GROUP BY clause with 40% chance
    if agg_column and random.random() < 0.4:
        column_list = [c.name for c in column_list]
        select_clause = select_clause + " GROUP BY " + ", ".join(column_list + [agg_column.name])
    # Add ORDER BY clause with 60% chance
    if not agg_column and not is_count_star and random.random() < 0.6:
        # Under SELECT DISTINCT the sort key must appear in the select list —
        # that is the SQL standard, not an Opteryx limitation (Postgres rejects
        # it with the same message). Ordering by an arbitrary column here just
        # generates invalid SQL, which tests the parser's error path over and
        # over instead of the executor.
        if is_distinct:
            order_column = column_list[random.choice(range(len(column_list)))]
        else:
            order_column = columns[random.choice(range(len(columns)))]
        if order_column.category not in (LogicalCategory.ARRAY,):
            order_direction = random.choice(["ASC", "DESC", ""])
            select_clause = select_clause + " ORDER BY " + order_column.name + " " + order_direction
    if random.random() < 0.2:
        select_clause = select_clause + " LIMIT " + str(int(random.random() * 10))
    return select_clause


# Tables to use for fuzzing
_tables_cache = None


def get_tables():
    """Lazy initialization of tables to avoid expensive setup during test collection"""
    global _tables_cache
    if _tables_cache is not None:
        return _tables_cache

    _tables_cache = [
        {
            "name": virtual_datasets.planets.schema().name,
            "fields": virtual_datasets.planets.schema().columns,
        }
    ]
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

TEST_CYCLES: int = 1000


@pytest.mark.parametrize("i", range(TEST_CYCLES))
def test_sql_fuzzing_single_table(i):

    # Use test iteration number as seed for reproducibility
    seed = i
    random.seed(seed)

    table = TABLES[random.choice(range(len(TABLES)))]
    statement = generate_random_sql_select(table["fields"], table["name"])
    formatted_statement = format_sql(statement)

    print(formatted_statement)

    print(f"Seed: {seed}, Cycle: {i}, ", end="")

    start_time = time.time()  # Start timing the query execution
    try:
        # The assertion is "did not raise". `execute_and_get_shape` drains every
        # morsel, so the whole plan runs — a lazy generator left undrained would
        # make this fuzzer assert nothing at all.
        shape = execute_and_get_shape(statement)
        execution_time = time.time() - start_time  # Measure execution time
        print(f"Shape: {shape}, Execution Time: {execution_time:.2f} seconds")
        # Additional success criteria checks can be added here
    except Exception as e:
        import traceback

        print(f"\033[0;31mError in Test Cycle {i + 1}\033[0m: {e}")
        print(traceback.print_exc())
        # Log failing statement and error for analysis
        raise e
    print()


if __name__ == "__main__":  # pragma: no cover
    for i in range(TEST_CYCLES):
        test_sql_fuzzing_single_table(i)

    print("✅ okay\n")
