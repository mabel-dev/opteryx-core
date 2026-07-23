"""
The best way to test a SQL Engine is to throw queries at it.

This is part of a suite of tests which are based on running many SQL statements.

    Run Only
 >  Shape Checking
    Results Checking
    Compare to DuckDB

This file tests: Basic queries and dataset shape validation

This tests that the shape of the response is as expected: the right number of columns,
the right number of rows and, if appropriate, the right exception is thrown.
"""

import os
import sys

# import opteryx
from typing import Optional

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

# from opteryx.connectors import AwsS3Connector, DiskConnector
from opteryx.exceptions import (
    AmbiguousDatasetError,
    AmbiguousIdentifierError,
    ArrayWithMixedTypesError,
    ColumnNotFoundError,
    ColumnReferencedBeforeEvaluationError,
    DatasetNotFoundError,
    EmptyDatasetError,
    FunctionExecutionError,
    FunctionNotFoundError,
    IncompatibleTypesError,
    InconsistentSchemaError,
    IncorrectTypeError,
    InvalidFunctionParameterError,
    InvalidTemporalRangeFilterError,
    MissingSqlStatement,
    ParameterError,
    PermissionsError,
    SqlError,
    UnexpectedDatasetReferenceError,
    UnnamedColumnError,
    UnsupportedSyntaxError,
    VariableNotFoundError,
)
from opteryx.utils.formatter import format_sql

# fmt:off
# fmt:off
STATEMENTS = [
        # Are the datasets the shape we expect?
        ("SELECT * FROM testdata.satellites", 177, 8, None),
        ("SELECT * FROM $planets", 9, 20, None),
        ("SELECT * FROM testdata.astronauts", 357, 19, None),
        ("SELECT * FROM $no_table", 1, 1, None),
        ("SELECT * FROM $variables", 39, 5, None),
        ("SELECT * FROM testdata.missions", 4630, 8, None),
        (b"SELECT * FROM testdata.satellites", 177, 8, None),
        ("SELECT * FROM testdata.missions", 4630, 8, None),
        ("SELECT * FROM testdata.satellites", 177, 8, None),
        ("SELECT * FROM testdata.planets", 9, 20, None),

        ("SELECT COUNT(*) FROM testdata.missions", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.satellites", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.planets", 1, 1, None),

        # Time-travel syntax (supported)
        ("SELECT * FROM $planets TIMESTAMP AS OF INTERVAL '1' DAY", 9, 20, None),
        ("SELECT * FROM $planets TIMESTAMP AS OF '2024-12-15 00:00:00'", 9, 20, None),
        ("SELECT * FROM $planets TIMESTAMP AS OF CURRENT_DATE - INTERVAL '7' DAY", 9, 20, None),
        ("SELECT * FROM $planets TIMESTAMP AS OF TRUNC(CURRENT_DATE, 'month')", 9, 20, None),

        # Temporal binary arithmetic (regression: restored after the draken C++-first
        # rebuild removed the Arrow-backed interval materialization). DuckDB-verified
        # semantics live in dev/ harnesses; these guard the shape + that they execute.
        ("SELECT birth_date + INTERVAL '1' MONTH FROM testdata.astronauts", 357, 1, None),
        ("SELECT birth_date - INTERVAL '1' MONTH FROM testdata.astronauts", 357, 1, None),
        ("SELECT birth_date + INTERVAL '1' DAY FROM testdata.astronauts", 357, 1, None),
        ("SELECT birth_date + INTERVAL '13' MONTH FROM testdata.astronauts", 357, 1, None),
        ("SELECT INTERVAL '1' MONTH + INTERVAL '2' MONTH FROM testdata.astronauts", 357, 1, None),
        ("SELECT INTERVAL '5' MONTH - INTERVAL '2' MONTH FROM testdata.astronauts", 357, 1, None),
        ("SELECT birth_date - birth_date FROM testdata.astronauts", 357, 1, None),
        ("SELECT birth_date + (INTERVAL '1' MONTH + INTERVAL '10' DAY) FROM testdata.astronauts", 357, 1, None),

        # Does the error tester work
        ("THIS IS NOT VALID SQL", None, None, SqlError),

        # Unary minus on a column expression must not crash the planner [regression]
        ("SELECT -id FROM $planets", 9, 1, None),
        ("SELECT -id AS neg FROM $planets", 9, 1, None),
        ("SELECT +id FROM $planets", 9, 1, None),
        ("SELECT -(id + 1) FROM $planets", 9, 1, None),
        ("SELECT -gravity FROM $planets", 9, 1, None),
        ("SELECT -5 AS a", 1, 1, None),

        # PAGING OF DATASETS AFTER A GROUP BY [#179]
        ("SELECT * FROM (SELECT COUNT(*), name FROM testdata.astronauts GROUP BY name ORDER BY COUNT(*)) AS SQ LIMIT 5", 5, 2, None),
        # FILTER CREATION FOR 3 OR MORE ANDED PREDICATES FAILS [#182]
        ("SELECT * FROM testdata.astronauts WHERE name LIKE '%o%' AND `year` > 1900 AND gender ILIKE '%ale%' AND group IN (1,2,3,4,5,6)", 41, 19, None),

        # Additional basic query patterns - LIMIT and OFFSET
        ("SELECT * FROM $planets LIMIT 5", 5, 20, None),
        ("SELECT * FROM $planets LIMIT 0", 0, 20, None),
        ("SELECT * FROM $planets LIMIT 1", 1, 20, None),
        ("SELECT * FROM $planets OFFSET 5", 4, 20, None),
        ("SELECT * FROM $planets LIMIT 3 OFFSET 2", 3, 20, None),
        ("SELECT * FROM $planets LIMIT 100", 9, 20, None),

        # ORDER BY variations
        ("SELECT * FROM $planets ORDER BY id", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY id DESC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY name ASC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY id, name", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY id DESC, name ASC", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY LENGTH(name)", 9, 20, None),
        ("SELECT * FROM $planets ORDER BY LENGTH(name) LIMIT 2", 2, 20, None),

        # Positional ORDER BY (SQL-92)
        ("SELECT name FROM $planets ORDER BY 1", 9, 1, None),
        ("SELECT name, mass FROM $planets ORDER BY 2 DESC", 9, 2, None),
        ("SELECT name FROM $planets ORDER BY 99", None, None, UnsupportedSyntaxError),

        # DISTINCT variations
        ("SELECT DISTINCT id FROM $planets", 9, 1, None),
        ("SELECT DISTINCT ON (id) * FROM $planets", 9, 20, None),
        ("SELECT DISTINCT name FROM $planets", 9, 1, None),
        ("SELECT DISTINCT id, name FROM $planets", 9, 2, None),
        # ORDER BY column must appear in the SELECT DISTINCT list — the ordering
        # value is ambiguous once rows collapse into a DISTINCT group.
        ("SELECT DISTINCT name FROM $planets ORDER BY id DESC", None, None, UnsupportedSyntaxError),
        ("SELECT DISTINCT name FROM $planets ORDER BY name DESC", 9, 1, None),

        # Basic aggregations
        ("SELECT COUNT(*) FROM $planets", 1, 1, None),
        ("SELECT COUNT(id) FROM $planets", 1, 1, None),
        ("SELECT COUNT(DISTINCT id) FROM $planets", 1, 1, None),
        ("SELECT SUM(id) FROM $planets", 1, 1, None),
        ("SELECT AVG(id) FROM $planets", 1, 1, None),
        ("SELECT MIN(id) FROM $planets", 1, 1, None),
        ("SELECT MAX(id) FROM $planets", 1, 1, None),

        # GROUP BY with aggregations
        ("SELECT COUNT(*) FROM testdata.satellites GROUP BY planetId", 7, 1, None),
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY planetId", 7, 2, None),
        ("SELECT planetId, COUNT(*), MAX(id) FROM testdata.satellites GROUP BY planetId", 7, 3, None),

        # WHERE clause variations
        ("SELECT * FROM $planets WHERE id = 1", 1, 20, None),
        ("SELECT * FROM $planets WHERE ~id = -2", 1, 20, None),
        ("SELECT * FROM $planets WHERE id != 1", 8, 20, None),
        ("SELECT * FROM $planets WHERE id > 5", 4, 20, None),
        ("SELECT * FROM $planets WHERE id >= 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE id < 5", 4, 20, None),
        ("SELECT * FROM $planets WHERE id <= 5", 5, 20, None),
        ("SELECT * FROM $planets WHERE id BETWEEN 3 AND 6", 4, 20, None),
        ("SELECT * FROM $planets WHERE name LIKE 'M%'", 2, 20, None),
        ("SELECT * FROM $planets WHERE name ILIKE 'm%'", 2, 20, None),
        ("SELECT * FROM $planets WHERE id IN (1, 3, 5)", 3, 20, None),
        ("SELECT * FROM $planets WHERE id NOT IN (1, 3, 5)", 6, 20, None),
        ("SELECT name FROM testdata.satellites WHERE planetId = 3", 1, 1, None),

        # NULL handling
        ("SELECT * FROM $planets WHERE name IS NULL", 0, 20, None),
        ("SELECT * FROM $planets WHERE name IS NOT NULL", 9, 20, None),
        ("SELECT name FROM testdata.satellites WHERE magnitude = 'NaN'::DOUBLE", 6, 1, None),

        # Combining conditions
        ("SELECT * FROM $planets WHERE id > 3 AND id < 7", 3, 20, None),
        ("SELECT * FROM $planets WHERE id < 3 OR id > 7", 4, 20, None),
        ("SELECT * FROM $planets WHERE (id > 3 AND id < 7) OR id = 1", 4, 20, None),

        # Column selection variations
        ("SELECT id FROM $planets", 9, 1, None),
        ("SELECT id, name FROM $planets", 9, 2, None),
        ("SELECT name, id FROM $planets", 9, 2, None),
        ("SELECT id, name, id FROM $planets", 9, 3, AmbiguousIdentifierError),

        # Expressions in SELECT
        ("SELECT id * 2 FROM $planets", 9, 1, None),
        ("SELECT id + 1 FROM $planets", 9, 1, None),
        ("SELECT id - 1, id + 1 FROM $planets", 9, 2, None),

        # Subqueries
        ("SELECT * FROM (SELECT * FROM $planets) AS subquery", 9, 20, None),
        ("SELECT * FROM (SELECT id, name FROM $planets) AS subquery", 9, 2, None),
        ("SELECT COUNT(*) FROM (SELECT * FROM $planets WHERE id > 5) AS subquery", 1, 1, None),

        # Mixed case identifiers
        ("SELECT ID FROM $planets", 9, 1, None),
        ("SELECT Id, NAME FROM $planets", 9, 2, None),
        ("SELECT * FROM $planets WHERE ID = 1", 1, 20, None),
        ("SELECT * FROM $planets ORDER BY NAME", 9, 20, None),
        ("SELECT COUNT(ID) FROM $planets", 1, 1, None),
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY PLANETID", 7, 2, None),
        ("SELECT S.id, P.name FROM testdata.satellites AS S JOIN $planets AS P ON S.PLANETID = P.ID", 177, 2, None),
        ("SELECT * FROM (SELECT ID, Name FROM $planets) AS subquery", 9, 2, None),
        ("SELECT name FROM $planets WHERE id IN (1, 3, 5) ORDER BY NAME DESC", 3, 1, None),
        ("SELECT ID, Name, id FROM $planets", 9, 3, AmbiguousIdentifierError),

        ("SELECT 1::TIMESTAMP[ms]", 1, 1, None),
        ("SELECT 1::TIMESTAMP[s]", 1, 1, None),
        ("SELECT 1::TIMESTAMP[us]", 1, 1, None),
        ("SELECT 1::TIMESTAMP[ns]", 1, 1, None),
        ("SELECT 1::DATE", 1, 1, None),
        ("SELECT 1::TIMESTAMP[d]", 1, 1, None),
        ("SELECT 1::TIMESTAMP", 1, 1, UnsupportedSyntaxError),
        ("SELECT 1::TIMESTAMP[]", 1, 1, UnsupportedSyntaxError),
        ("SELECT 1::TIMESTAMP[milliseconds]", 1, 1, UnsupportedSyntaxError),

        # Temporal comparison validation tests
        # Temporal column + cast literal - should pass (column is already temporal, doesn't need cast)
        ("SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at >= '1957-10-04'::DATE", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at >= '1957-10-04'::TIMESTAMP[ms]", 1, 1, None),

        # Literal without cast - should fail with IncompatibleTypesError
        ("SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at >= '1957-10-04'", None, None, IncompatibleTypesError),

        # Different temporal types but literal is cast - should pass
        ("SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at >= '1957-10-04'::DATE", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.missions WHERE Lauched_at >= '2024-12-31'::TIMESTAMP[ms]", 1, 1, None),

        # Set operations - UNION (SQL92 compatibility) - NEW
        # NOTE: Opteryx requires dataset aliases in set operations when the same dataset is referenced
        # in multiple branches, even though SQL doesn't require this. This is a known limitation of the
        # context merging logic in the binder's traversal phase.
        # Baseline: Single UNION removes duplicates
        ("SELECT * FROM (SELECT name, id FROM $planets AS A UNION SELECT name, id FROM $planets AS B) AS C WHERE name = 'Earth'", 1, 2, None),
        # Baseline: UNION ALL keeps duplicates
        ("SELECT * FROM (SELECT name, id FROM $planets AS A UNION ALL SELECT name, id FROM $planets AS B) AS C WHERE name = 'Earth'", 2, 2, None),
        # FROM-less SELECT branches in set operations (regression: schema must be
        # registered under the renamed $union- relation even when the inner SELECT
        # has no FROM clause).
        ("SELECT 1 UNION ALL SELECT 2", 2, 1, None),
        ("SELECT 1, 'a' UNION ALL SELECT 2, 'b'", 2, 2, None),
        # Chained UNION (three+ relations). Two bind-time bugs were fixed here:
        #   1. rename_relations did not remap the relation-name lists on nested
        #      Union/Intersect/Except nodes (only Join nodes) — a nested set op
        #      kept stale scan aliases -> KeyError(['$union-...']) at bind.
        #   2. _columns_for_side discarded a resolvable side when a sibling
        #      relation had been collapsed by a nested set op, and its graph-walk
        #      fallback stopped at a column-less DISTINCT wrapper (FROM-less legs).
        # Wrapped in a subquery (named relation legs).
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id = 1 UNION SELECT name, id FROM $planets AS B WHERE id = 2 UNION SELECT name, id FROM $planets AS C WHERE id = 3) D", 3, 2, None),
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id = 1 UNION SELECT name, id FROM $planets AS B WHERE id = 2 UNION SELECT name, id FROM $planets AS C WHERE id = 3) D WHERE id > 1", 2, 2, None),
        # Top-level chained UNION (distinct) — the original repro.
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 2 UNION SELECT name FROM $planets WHERE id = 3", 3, 1, None),
        ("SELECT id, name FROM $planets WHERE id = 1 UNION SELECT id, name FROM $planets WHERE id = 2 UNION SELECT id, name FROM $planets WHERE id = 3", 3, 2, None),
        # Four and five legs.
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 2 UNION SELECT name FROM $planets WHERE id = 3 UNION SELECT name FROM $planets WHERE id = 4", 4, 1, None),
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 2 UNION SELECT name FROM $planets WHERE id = 3 UNION SELECT name FROM $planets WHERE id = 4 UNION SELECT name FROM $planets WHERE id = 5", 5, 1, None),
        # Chained UNION ALL keeps duplicates across all legs.
        ("SELECT name FROM $planets WHERE id = 1 UNION ALL SELECT name FROM $planets WHERE id = 2 UNION ALL SELECT name FROM $planets WHERE id = 3", 3, 1, None),
        # Chained distinct UNION dedups across all legs.
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 2", 2, 1, None),
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 1", 1, 1, None),
        # Mixed distinct / ALL in a chain: (id=1 UNION id=1) -> 1 row, then UNION ALL id=2.
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 1 UNION ALL SELECT name FROM $planets WHERE id = 2", 2, 1, None),
        # Chained UNION with a trailing ORDER BY / LIMIT.
        ("SELECT name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 2 UNION SELECT name FROM $planets WHERE id = 3 ORDER BY name LIMIT 2", 2, 1, None),
        # FROM-less chained UNION (legs collapse $no_table into $project): distinct + ALL.
        ("SELECT 1 UNION SELECT 2 UNION SELECT 3", 3, 1, None),
        ("SELECT 1 UNION SELECT 1 UNION SELECT 2", 2, 1, None),
        ("SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 2", 3, 1, None),
        ("SELECT 1, 'a' UNION SELECT 2, 'b' UNION SELECT 3, 'c'", 3, 2, None),
        ("SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4", 4, 1, None),
        # Chained UNION with mismatched column counts must still raise.
        ("SELECT id, name FROM $planets WHERE id = 1 UNION SELECT name FROM $planets WHERE id = 2 UNION SELECT name FROM $planets WHERE id = 3", None, None, ValueError),

        # Set operations - INTERSECT (SQL92 compatibility) - NEW, BLOCKED: Operators not compiled
        # INTERSECT keeps only rows in both inputs
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id <= 5 INTERSECT SELECT name, id FROM $planets AS B WHERE id >= 3) C", 3, 2, None),
        # INTERSECT with no matches should return 0 rows
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id < 3 INTERSECT SELECT name, id FROM $planets AS B WHERE id > 7) C", 0, 2, None),
        # INTERSECT with same input (all rows match)
        ("SELECT * FROM (SELECT name, id FROM $planets AS A INTERSECT SELECT name, id FROM $planets AS B) C", 9, 2, None),

        # Set operations - EXCEPT (SQL92 compatibility) - NEW, BLOCKED: Operators not compiled
        # EXCEPT keeps rows from first input not in second
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id <= 5 EXCEPT SELECT name, id FROM $planets AS B WHERE id >= 3) C", 2, 2, None),
        # EXCEPT with empty right side returns all from left
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id < 3 EXCEPT SELECT name, id FROM $planets AS B WHERE id > 7) C", 2, 2, None),
        # EXCEPT with same input should return 0 rows
        ("SELECT * FROM (SELECT name, id FROM $planets AS A EXCEPT SELECT name, id FROM $planets AS B) C", 0, 2, None),

        # Chained INTERSECT / EXCEPT (three+ legs). INTERSECT/EXCEPT are lowered to
        # left semi / left anti joins; the ON-condition must reference only the
        # relation that survives a nested set op, not the over-reported scan list
        # (otherwise the outer join references a consumed relation -> bind error).
        # Wrapped in a subquery (named relation legs).
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id <= 5 INTERSECT SELECT name, id FROM $planets AS B WHERE id >= 2 INTERSECT SELECT name, id FROM $planets AS C WHERE id <= 3) D", 2, 2, None),
        ("SELECT * FROM (SELECT name, id FROM $planets AS A WHERE id <= 6 EXCEPT SELECT name, id FROM $planets AS B WHERE id = 1 EXCEPT SELECT name, id FROM $planets AS C WHERE id = 2) D", 4, 2, None),
        # Top-level chained INTERSECT (the original repro family).
        ("SELECT name FROM $planets WHERE id < 5 INTERSECT SELECT name FROM $planets WHERE id < 4 INTERSECT SELECT name FROM $planets WHERE id < 3", 2, 1, None),
        ("SELECT name FROM $planets WHERE id < 6 INTERSECT SELECT name FROM $planets WHERE id < 5 INTERSECT SELECT name FROM $planets WHERE id < 4 INTERSECT SELECT name FROM $planets WHERE id < 3", 2, 1, None),
        # Top-level chained EXCEPT.
        ("SELECT name FROM $planets WHERE id < 6 EXCEPT SELECT name FROM $planets WHERE id = 1 EXCEPT SELECT name FROM $planets WHERE id = 2", 3, 1, None),
        ("SELECT name FROM $planets WHERE id < 6 EXCEPT SELECT name FROM $planets WHERE id = 1 EXCEPT SELECT name FROM $planets WHERE id = 2 EXCEPT SELECT name FROM $planets WHERE id = 3", 2, 1, None),
        # Mixed INTERSECT/EXCEPT: INTERSECT binds tighter than EXCEPT.
        # (id<5 INTERSECT id<4) EXCEPT id=1.
        ("SELECT name FROM $planets WHERE id < 5 INTERSECT SELECT name FROM $planets WHERE id < 4 EXCEPT SELECT name FROM $planets WHERE id = 1", 2, 1, None),
        # id<6 EXCEPT (id=1 INTERSECT id<4)  -> exercises right-side reduction.
        ("SELECT name FROM $planets WHERE id < 6 EXCEPT SELECT name FROM $planets WHERE id = 1 INTERSECT SELECT name FROM $planets WHERE id < 4", 4, 1, None),
        # Two columns.
        ("SELECT id, name FROM $planets WHERE id < 5 INTERSECT SELECT id, name FROM $planets WHERE id < 4 INTERSECT SELECT id, name FROM $planets WHERE id < 3", 2, 2, None),

        # INTERSECT ALL / EXCEPT ALL — multiset semantics via ROW_NUMBER + semi/anti
        # join. min(count_left, count_right) for INTERSECT ALL, max(left - right, 0)
        # for EXCEPT ALL. Over $planets (distinct rows) the ALL forms coincide with the
        # distinct forms; multiset counting is exercised by the result-checking suite.
        ("SELECT name FROM $planets WHERE id < 5 INTERSECT ALL SELECT name FROM $planets WHERE id < 3", 2, 1, None),
        ("SELECT name FROM $planets WHERE id < 5 EXCEPT ALL SELECT name FROM $planets WHERE id = 1", 3, 1, None),
        ("SELECT name FROM $planets WHERE id < 5 INTERSECT ALL SELECT name FROM $planets WHERE id < 4 INTERSECT ALL SELECT name FROM $planets WHERE id < 3", 2, 1, None),
        # Multiset counting (testdata.satellites.planetId has duplicates): planetId 5
        # appears only on the left, 6..9 on both. INTERSECT ALL keeps the min per value
        # (drops 5 entirely); EXCEPT ALL keeps the left surplus (only the 67 planetId=5).
        ("SELECT planetId FROM testdata.satellites WHERE planetId >= 5 INTERSECT ALL SELECT planetId FROM testdata.satellites WHERE planetId >= 6", 107, 1, None),
        ("SELECT planetId FROM testdata.satellites WHERE planetId >= 5 EXCEPT ALL SELECT planetId FROM testdata.satellites WHERE planetId >= 6", 67, 1, None),

        # IN subquery
        ("SELECT name FROM testdata.satellites WHERE id IN (SELECT id FROM $planets)", 9, 1, None),
        ("SELECT name FROM testdata.satellites WHERE id NOT IN (SELECT id FROM $planets)", 168, 1, None),

        # EXISTS / NOT EXISTS subquery
        ("SELECT name FROM $planets WHERE EXISTS (SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)", 7, 1, None),
        ("SELECT name FROM $planets WHERE NOT EXISTS (SELECT 1 FROM testdata.satellites WHERE testdata.satellites.planetId = $planets.id)", 2, 1, None),

        # REGEXP_REPLACE
        ("SELECT COUNT(*), cve FROM (SELECT REGEXP_REPLACE(cve_id, '^CVE-([^-]+)-.*$', r'\\1') AS cve FROM testdata.nvd) GROUP BY cve;", 28, 2, None),

        # CAST to VARCHAR — array column (list<string>) and integer column
        ("SELECT CAST(missions AS VARCHAR) FROM testdata.astronauts", 357, 1, None),
        ("SELECT CAST(space_flights AS VARCHAR) FROM testdata.astronauts", 357, 1, None),

        # WINDOW FUNCTIONS (PARTITION BY aggregates — rewritten to CTE + inner join)
        # Single aggregate window, unique partition key (each planet has a unique id)
        ("SELECT name, SUM(gravity) OVER (PARTITION BY id) FROM $planets", 9, 2, None),
        # Explicit alias
        ("SELECT name, COUNT(id) OVER (PARTITION BY id) AS cnt FROM $planets", 9, 2, None),
        # Multiple window functions with the same PARTITION BY — shared CTE
        ("SELECT name, SUM(gravity) OVER (PARTITION BY id), AVG(mass) OVER (PARTITION BY id) FROM $planets", 9, 3, None),
        # Non-unique partition key: each satellite belongs to a planet
        ("SELECT name, COUNT(name) OVER (PARTITION BY planetId) FROM testdata.satellites", 177, 2, None),
        # Unsupported: ORDER BY inside window spec
        ("SELECT id, SUM(gravity) OVER (PARTITION BY id ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        # Unsupported: window function combined with GROUP BY
        ("SELECT id, SUM(gravity) OVER (PARTITION BY id) FROM $planets GROUP BY id", None, None, UnsupportedSyntaxError),

        # RANKING WINDOW FUNCTIONS (ROW_NUMBER / RANK / DENSE_RANK) — blocking sort path.
        # User-facing ranking functions REQUIRE an OVER (...) with ORDER BY.
        ("SELECT name, ROW_NUMBER() OVER (PARTITION BY id ORDER BY name) AS rn FROM $planets", 9, 2, None),
        ("SELECT name, RANK() OVER (ORDER BY id) AS rk FROM $planets", 9, 2, None),
        ("SELECT name, DENSE_RANK() OVER (ORDER BY id) AS dr FROM $planets", 9, 2, None),
        # Several ranking functions sharing one PARTITION BY + ORDER BY (one sort).
        ("SELECT id, ROW_NUMBER() OVER (ORDER BY id) rn, RANK() OVER (ORDER BY id) rk, DENSE_RANK() OVER (ORDER BY id) dr FROM $planets", 9, 4, None),
        # PARTITION BY a real grouping column (numbering resets per partition).
        ("SELECT planetId, ROW_NUMBER() OVER (PARTITION BY planetId ORDER BY id) rn FROM testdata.satellites", 177, 2, None),
        ("SELECT planetId, RANK() OVER (PARTITION BY planetId ORDER BY id DESC) rk FROM testdata.satellites", 177, 2, None),
        # Ranking function without ORDER BY — must raise (numbering would be undefined).
        ("SELECT ROW_NUMBER() OVER (PARTITION BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        # Ranking function without OVER — must raise (it is window-only).
        ("SELECT RANK() FROM $planets", None, None, UnsupportedSyntaxError),

        # casting array vectors segfaulted
        ("SELECT CAST(missions AS VARCHAR) FROM testdata.astronauts", 357, 1, None),

        # CAST: literal string → numeric types (exercises BOOL take fix + constant-shape cast path)
        ("SELECT CAST('42' AS INTEGER) FROM $planets LIMIT 1", 1, 1, None),
        ("SELECT CAST('3.14' AS DOUBLE) FROM $planets LIMIT 1", 1, 1, None),

        # CAST: VARCHAR column → numeric types (round-trip via CAST(col AS VARCHAR) first)
        ("SELECT CAST(CAST(year AS VARCHAR) AS INTEGER) FROM testdata.astronauts LIMIT 3", 3, 1, None),
        ("SELECT CAST(CAST(space_walks_hours AS VARCHAR) AS DOUBLE) FROM testdata.astronauts LIMIT 3", 3, 1, None),

        # ASOF JOIN — basic shape checks
        # Self-join: every planet matches itself or nearest lower gravity (LEFT semantics = 9 rows)
        ("SELECT p.name, p2.name AS match_name FROM $planets AS p ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity >= p2.gravity)", 9, 2, None),
        # ASOF with <= operator (find nearest-after match)
        ("SELECT p.name, p2.name AS match_name FROM $planets AS p ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity <= p2.gravity)", 9, 2, None),
        # LEFT semantics: right side filtered to id >= 5; planets with id < 5 emit null right columns but still appear
        ("SELECT p.id, p.name, p2.name AS match_name FROM $planets AS p ASOF JOIN (SELECT id, name FROM $planets WHERE id >= 5) AS p2 MATCH_CONDITION(p.id >= p2.id)", 9, 3, None),
        # ASOF with constant column on the right — exercises align_tables constant-vector + negative-indices path
        ("SELECT p.name, p2.marker FROM $planets AS p ASOF JOIN (SELECT gravity, 1 AS marker FROM $planets WHERE id >= 5) AS p2 MATCH_CONDITION(p.gravity >= p2.gravity)", 9, 2, None),
        # ASOF rejects equality in MATCH_CONDITION — must raise UnsupportedSyntaxError
        ("SELECT p.name FROM $planets AS p ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity = p2.gravity)", None, None, UnsupportedSyntaxError),
        # ASOF rejects not-equal in MATCH_CONDITION — must raise UnsupportedSyntaxError
        ("SELECT p.name FROM $planets AS p ASOF JOIN $planets AS p2 MATCH_CONDITION(p.gravity != p2.gravity)", None, None, UnsupportedSyntaxError),

        # Array-literal operand to @>/@>> in a WHERE clause (regression: constant
        # folding used to hand vector_contains_any/all the Cython shim Vector
        # instead of the raw nanobind Vector, crashing with a TypeError from
        # draken_vector_unwrap).
        ("SELECT * FROM $planets WHERE ['a', 'b', 'c'] @> ['a']", 9, 20, None),
        ("SELECT * FROM $planets WHERE ['a', 'b', 'c'] @>> ['a', 'b']", 9, 20, None),
        ("SELECT ['a', 'b', 'c'] @> ['a']", 1, 1, None),
        ("SELECT ['a', 'b', 'c'] @>> ['a', 'b']", 1, 1, None),
]

# fmt:on


def _assert_sql_battery_shape(
    statement: str, rows: int, columns: int, exception: Optional[Exception]
):
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)

    try:
        session = opteryx.session(memberships=["Apollo 11", "opteryx"])
        morsels = list(session.execute_to_morsels(statement))
        actual_rows = sum(morsel.num_rows for morsel in morsels)
        assert rows == actual_rows, (
            f"\n\033[38;5;203mQuery returned {actual_rows} rows but {rows} were expected.\033[0m\n{statement}"
        )
        if morsels:
            actual_columns = len(morsels[0].column_names)
            assert columns == actual_columns, (
                f"\n\033[38;5;203mQuery returned {actual_columns} cols but {columns} were expected.\033[0m\n{statement}"
            )
        else:
            # Empty morsel streams currently do not expose output schema metadata.
            assert rows == 0, f"Query returned no morsels but expected {rows} rows.\n{statement}"
        assert exception is None, (
            f"Exception {exception} not raised but expected\n{format_sql(statement)}"
        )
    except AssertionError as error:
        raise error
    except Exception as error:
        if not type(error) == exception:
            raise ValueError(
                f"{format_sql(statement)}\nQuery failed with error {type(error)} but error {exception} was expected"
            ) from error


@pytest.mark.parametrize("statement, rows, columns, exception", STATEMENTS)
def test_sql_battery(statement: str, rows: int, columns: int, exception: Optional[Exception]):
    """
    Test a battery of statements
    """
    _assert_sql_battery_shape(statement, rows, columns, exception)


def test_sql_battery_pass2_sequence_regression():
    """
    Replay the early shapes sequence with fresh sessions to guard the
    masked parquet pass-2 path against sequence-sensitive crashes.
    """
    for statement, rows, columns, exception in STATEMENTS[:20]:
        _assert_sql_battery_shape(statement, rows, columns, exception)


def test_bool_group_by_key_values():
    """
    VALUE-level regression: BOOL GROUP BY keys must reconstruct with the
    correct labels, not collapse to all-False.

    The shape battery is deliberately shape-only, which is precisely why a
    BOOL group-by key emitting the right COUNTs against the WRONG key values
    (every group labelled False) went undetected. Guards
    emit_fixed_column's DRAKEN_BOOL bit-packed arm in
    src/cpp/engine/native_group_sinks.hpp.
    """
    from opteryx.connectors import DiskConnector

    opteryx.register_workspace("testdata", DiskConnector)

    statement = (
        "SELECT user_verified, COUNT(*) AS c "
        "FROM testdata.tweets GROUP BY user_verified"
    )
    session = opteryx.session(memberships=["Apollo 11", "opteryx"])
    morsels = list(session.execute_to_morsels(statement))

    counts = {}
    for morsel in morsels:
        keys = morsel.column("user_verified").to_pylist()
        vals = morsel.column("c").to_pylist()
        for key, val in zip(keys, vals):
            counts[key] = counts.get(key, 0) + val

    assert counts == {False: 99335, True: 665}, (
        f"BOOL group-by keys reconstructed wrong: {counts} "
        "(expected {False: 99335, True: 665})"
    )


if __name__ == "__main__":  # pragma: no cover
    import shutil
    import time

    from tests import trunc_printable

    # Change to repo root so testdata paths resolve correctly
    os.chdir(os.path.join(os.path.dirname(__file__), "../../../"))

    start_suite = time.monotonic_ns()
    width = shutil.get_terminal_size((80, 20))[0] - 15
    passed: int = 0
    failed: int = 0
    nl: str = "\n"
    failures = []

    print(f"RUNNING BATTERY OF {len(STATEMENTS)} BASIC SHAPE TESTS")
    for index, (statement, rows, cols, err) in enumerate(STATEMENTS):
        printable = statement
        if hasattr(printable, "decode"):
            printable = printable.decode()
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(printable), width - 1)}",
            end="",
            flush=True,
        )
        try:
            start = time.monotonic_ns()
            test_sql_battery(statement, rows, cols, err)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(f" \033[0;31m{failed}\033[0m")
            else:
                print()
        except Exception as err:
            failed += 1
            print(
                f"\033[0;31m{str(int((time.monotonic_ns() - start) / 1e6)).rjust(4)}ms ❌ {failed}\033[0m"
            )
            print(">", err)
            failures.append((statement, err))

    print("--- ✅ \033[0;32mdone\033[0m")

    # VALUE-level regressions (the battery above is shape-only).
    print("RUNNING VALUE-LEVEL REGRESSIONS")
    for name, fn in (("bool group-by key values", test_bool_group_by_key_values),):
        print(f"\033[38;2;255;184;108m{name}\033[0m ", end="", flush=True)
        try:
            fn()
            print("✅")
            passed += 1
        except Exception as err:
            failed += 1
            print(f"\033[0;31m❌ {failed}\033[0m")
            print(">", err)
            failures.append((name, err))

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")
        for statement, err in failures:
            print(err)

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )

    # Exit with appropriate code to signal success/failure to parent process
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)
