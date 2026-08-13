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
    QueryParseError,
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
        # `SHOW VARIABLES` is the SINGLE surface for session variables. The
        # `$variables` relation backing it is internal-only and must not be
        # addressable by name, by any route.
        # This battery runs without entitlements, so RESTRICTED variables are
        # withheld — the full list is asserted in tests/unit/security/.
        # 22 since `build` joined `version` as an UNRESTRICTED engine-identity row,
        # +1 for `write_coalesce_rows` (INSERT/CTAS coalescing threshold, USER/UNRESTRICTED).
        ("SHOW VARIABLES", 23, 5, None),
        ("SELECT * FROM $variables", None, None, UnsupportedSyntaxError),
        ("SELECT name FROM $variables", None, None, UnsupportedSyntaxError),
        ("SELECT * FROM $VARIABLES", None, None, UnsupportedSyntaxError),
        ("SELECT * FROM (SELECT * FROM $variables) AS x", None, None, UnsupportedSyntaxError),
        ("SELECT v.name FROM $planets p CROSS JOIN $variables v", None, None, UnsupportedSyntaxError),
        # `SHOW USER` reaches the same ShowVariable builder and is planned as a scan
        # of `$user`. Unlike `$variables`, `$user` stays directly addressable — the
        # two are deliberately two surfaces onto one reader.
        # This battery builds its session with two memberships and no user or
        # entitlements, so the rows are those two plus the billing account
        # query_session always supplies.
        ("SHOW USER", 3, 3, None),
        # Every other `SHOW <words>` form parses to the same ShowVariable node and is
        # rejected rather than silently answered as if it were SHOW VARIABLES.
        ("SHOW TIME ZONE", None, None, UnsupportedSyntaxError),
        ("SHOW ALL", None, None, UnsupportedSyntaxError),
        # The parser DISCARDS the LIKE pattern, so honouring it is impossible — this
        # must fail rather than return the unfiltered list.
        ("SHOW VARIABLES LIKE '%trace%'", None, None, UnsupportedSyntaxError),
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
        # QueryParseError, not the general SqlError: nothing parsed, so there is no
        # clause to name and the error carries a position instead. It is a SqlError
        # subclass, but this battery pins the exact type.
        ("THIS IS NOT VALID SQL", None, None, QueryParseError),

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

        # HAVING — a HAVING clause may reference aggregates and group keys that never
        # appear in the SELECT list (SQL-92). These shapes silently failed with
        # ColumnNotFoundError until the aggregate/pass-through hoist landed; every row
        # count below is confirmed against DuckDB reading the same parquet file.
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId HAVING COUNT(*) > 5", 4, 1, None),
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId HAVING MAX(id) > 100", 4, 1, None),
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId HAVING MIN(id) > 10 AND MAX(id) < 150", 1, 1, None),
        # group KEY referenced in HAVING but not projected
        ("SELECT COUNT(*) FROM testdata.satellites GROUP BY planetId HAVING planetId > 4", 5, 1, None),
        # HAVING aggregate differs from the projected aggregate
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY planetId HAVING MAX(id) > 100", 4, 2, None),
        # HAVING + ORDER BY, both hoisting; the shared-expression case must not
        # double-emit the column (AmbiguousIdentifierError)
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId HAVING SUM(id) > 500 ORDER BY planetId", 5, 1, None),
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId HAVING MAX(id) > 100 ORDER BY MAX(id) DESC", 4, 1, None),
        # regression: HAVING referencing a SELECT alias still resolves at the Project
        ("SELECT planetId, COUNT(*) AS c FROM testdata.satellites GROUP BY planetId HAVING c > 5", 4, 2, None),

        # ORDER BY over an aggregate absent from the SELECT list — same class as the
        # HAVING cases above ("sort by a metric you don't display"). Shape only asserts
        # the hoisted aggregate does not leak into the output row; the ORDER itself is
        # asserted against DuckDB in results/order_by_agg_not_in_projection_01.slt.
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId ORDER BY COUNT(*) DESC", 7, 1, None),
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId ORDER BY MIN(id) DESC", 7, 1, None),
        ("SELECT planetId FROM testdata.satellites GROUP BY planetId HAVING MAX(id) > 100 ORDER BY MIN(id) DESC", 4, 1, None),

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
        ("SELECT name FROM testdata.satellites WHERE magnitude = 'NaN'::FLOAT64", 6, 1, None),

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
        # Window over a CTE / derived table. The source is a Subquery relation, not a
        # Scan: the CTE the rewrite builds is a copy of that sub-plan, and until
        # rename_relations re-aliased Subquery nodes the copy kept the original's alias
        # and the binder saw two relations of one name (it reported a SEMI-join error).
        ("WITH x AS (SELECT * FROM $planets) SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM x", 9, 2, None),
        ("SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM (SELECT * FROM $planets) AS s", 9, 2, None),
        ("SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM (SELECT * FROM (SELECT * FROM $planets) AS inner_s) AS s", 9, 2, None),
        # WHERE ABOVE the derived table, so the copied sub-plan is Filter -> Subquery.
        # This is the shape whose join legs the optimizer reordered; the legs were
        # unlabelled, so the compiler keyed the left column against the right leg.
        ("SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM (SELECT * FROM $planets) AS s WHERE id != 1", 8, 2, None),
        # A CTE is ONE relation however many tables its body joins.
        ("WITH x AS (SELECT p.name AS name, q.number_of_moons AS number_of_moons FROM $planets p INNER JOIN $planets q ON p.id = q.id) SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM x", 9, 2, None),
        # Unsupported: two relations directly under the window — the outer leg is rebuilt
        # as one qualified wildcard, so the second relation's columns would be dropped.
        ("SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM $planets CROSS JOIN (VALUES (1),(2)) AS v(x)", None, None, UnsupportedSyntaxError),
        # DISTINCT partition specs — one Window node each, rewritten as one chain: every
        # spec's CTE is built from the same source and ONE Project reconciles the result.
        # Rewriting them one at a time gave each its own `source.*` Project, and the
        # second one re-expanded the source relation — every source column arrived twice
        # and the query was rejected as ambiguous.
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity) AS c, COUNT(*) OVER (PARTITION BY id) AS d FROM $planets", 9, 3, None),
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity) AS c, COUNT(*) OVER (PARTITION BY id) AS d, MAX(mass) OVER (PARTITION BY number_of_moons) AS e FROM $planets", 9, 4, None),
        # Distinct specs mixed with a SHARED spec: the shared pair still rides one CTE.
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity) AS c, SUM(mass) OVER (PARTITION BY gravity) AS c2, COUNT(*) OVER (PARTITION BY id) AS d FROM $planets", 9, 4, None),
        # A filter below the chain applies to EVERY spec's CTE, not just the first.
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity) AS c, COUNT(*) OVER (PARTITION BY id) AS d FROM $planets WHERE id > 3", 6, 3, None),
        # Distinct specs over a derived table — the copied sub-plan is a Subquery, once per spec.
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity) AS c, COUNT(*) OVER (PARTITION BY id) AS d FROM (SELECT * FROM $planets) AS s", 9, 3, None),
        # Multi-column partition spec alongside a single-column one.
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity, id) AS c, COUNT(*) OVER (PARTITION BY id) AS d FROM $planets", 9, 3, None),
        ("SELECT name, COUNT(name) OVER (PARTITION BY planetId) AS c, MIN(id) OVER (PARTITION BY magnitude) AS d FROM testdata.satellites", 177, 3, None),
        # Unaliased windows are named by their rendered expression, so two that render
        # the same are two columns of one name — the same rejection an unaliased
        # expression written twice gets. See test_window_functions_are_named_for_their_expression.
        ("SELECT name, COUNT(*) OVER (PARTITION BY gravity), COUNT(*) OVER (PARTITION BY gravity) FROM $planets", None, None, AmbiguousIdentifierError),
        # OVER () — one partition holding every row. The CTE is an UNGROUPED aggregate,
        # which yields exactly one row, and the join becomes a CROSS join that attaches
        # that row to every outer row. With no ON to read, the binder cannot back-fill
        # the join's left relation names, so the rewrite states them.
        ("SELECT name, COUNT(*) OVER () FROM $planets", 9, 2, None),
        ("SELECT name, COUNT(*) OVER () AS c FROM $planets", 9, 2, None),
        ("SELECT name, SUM(id) OVER () AS s, AVG(gravity) OVER () AS a FROM $planets", 9, 3, None),
        # WHERE applies before the window, so the CTE aggregates the surviving rows.
        ("SELECT name, COUNT(*) OVER () AS c FROM $planets WHERE id > 4", 5, 2, None),
        # Empty input: no outer rows to attach the aggregate row to, so no output rows.
        ("SELECT name, COUNT(*) OVER () AS c FROM $planets WHERE id > 100", 0, 2, None),
        # OVER () alongside a PARTITION BY spec — two Window nodes, one chain, and the
        # partition-less one groups under the empty spec key.
        ("SELECT name, COUNT(*) OVER () AS t, COUNT(*) OVER (PARTITION BY number_of_moons) AS c FROM $planets", 9, 3, None),
        ("SELECT name, COUNT(*) OVER () AS t, SUM(mass) OVER () AS s FROM $planets", 9, 3, None),
        # OVER () over a CTE / derived table source.
        ("WITH x AS (SELECT * FROM $planets) SELECT name, COUNT(*) OVER () AS c FROM x", 9, 2, None),
        ("SELECT name, COUNT(*) OVER () AS c FROM (SELECT * FROM $planets) AS s", 9, 2, None),
        # QUALIFY on a partition-less window.
        ("SELECT name FROM $planets QUALIFY COUNT(*) OVER () > 5", 9, 1, None),
        ("SELECT name FROM $planets QUALIFY COUNT(*) OVER () > 500", 0, 1, None),
        # The refusals still apply to OVER () — it is a window like any other.
        ("SELECT id, SUM(gravity) OVER () FROM $planets GROUP BY id", None, None, UnsupportedSyntaxError),
        ("SELECT ROW_NUMBER() OVER () FROM $planets", None, None, UnsupportedSyntaxError),

        # WINDOW BESIDE A PLAIN AGGREGATE — refused, for the same reason a window beside
        # an explicit GROUP BY is: the Window step is planned UNDER the aggregate, so the
        # window is computed over the rows the aggregate collapses and can never see the
        # aggregated result. A bare aggregate is an implicit single group, so it hits the
        # same wall — it just had no guard of its own and fell through to the generic
        # "must appear in the GROUP BY clause" error, which named the MINTED `$win_` key.
        # The message is asserted in test_window_beside_aggregate_is_refused_by_name.
        ("SELECT COUNT(*), COUNT(*) OVER () FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT MAX(id) OVER (PARTITION BY gravity), COUNT(*) FROM $planets", None, None, UnsupportedSyntaxError),
        # The ranking path mints the same way and is refused the same way.
        ("SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        # The aggregate need not be SELECTed — ORDER BY puts one in `_aggregates` too.
        ("SELECT COUNT(*) OVER () FROM $planets ORDER BY COUNT(*)", None, None, UnsupportedSyntaxError),
        # QUALIFY is refused on the same terms: its Filter is planned below the aggregate
        # step too, so these filtered the PRE-aggregation rows and then counted them —
        # and ran without complaint.
        ("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER () > 1", None, None, UnsupportedSyntaxError),
        ("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1", None, None, UnsupportedSyntaxError),
        ("SELECT COUNT(*) FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1", None, None, UnsupportedSyntaxError),
        # NOT refused: the aggregate under the window is the window's own, not a plain
        # one, so these stay legal and must not be caught by the guard above.
        ("SELECT COUNT(*) OVER (), SUM(mass) OVER () FROM $planets", 9, 2, None),
        ("SELECT name, COUNT(*) OVER () FROM $planets", 9, 2, None),
        # A QUALIFY window with no plain aggregate beside it — nothing collapses it away.
        ("SELECT name FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY mass) = 1", 1, 1, None),
        # `SELECT *` BESIDE QUALIFY — the window column QUALIFY appends must NOT be one of
        # the columns the wildcard expands to. A bare `SELECT *` builds no Project at all
        # (see logical_planner), so the Exit node expands it against the relations in
        # scope — which include the Window node's output relation (ranking) or the
        # aggregate CTE the window-to-join rewrite builds — and the minted `$win_` column
        # rode out to the caller as a 21st column of $planets. The names are asserted in
        # test_qualify_does_not_leak_its_window_column.
        ("SELECT * FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 2", 2, 20, None),
        ("SELECT * FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1", 2, 20, None),
        # The qualified form always pruned correctly (its Project expands one relation) —
        # it is here so the two forms cannot drift apart again.
        ("SELECT p.* FROM $planets AS p QUALIFY ROW_NUMBER() OVER (ORDER BY p.id) <= 2", 2, 20, None),
        # `SELECT * EXCEPT (...)` DOES build a Project, and leaked by the same route
        # through its own bare-wildcard expansion.
        ("SELECT * EXCEPT (mass) FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 2", 2, 19, None),
        # A narrower source, so a leaked column is not lost in twenty real ones.
        ("SELECT * FROM (SELECT id, name FROM $planets) AS s QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 2", 2, 2, None),
        # Two windows in one QUALIFY — both are hidden, not just the first.
        ("SELECT * FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 4 AND RANK() OVER (ORDER BY mass) >= 2", 4, 20, None),
        # A window the caller DID ask for is still returned — hiding is scoped to the
        # columns QUALIFY appended, not to window columns in general.
        ("SELECT * FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $planets) AS s", 9, 2, None),
        # The remedies the two refusals advise must actually run.
        ("SELECT c, SUM(c) OVER () FROM (SELECT COUNT(*) AS c FROM $planets) AS s", 1, 2, None),
        ("SELECT COUNT(*) FROM (SELECT gravity FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1) AS s", 1, 1, None),

        # WINDOW INSIDE AN AGGREGATE'S ARGUMENT — `SUM(COUNT(*) OVER ())`. Forbidden by
        # the standard ("aggregate function calls cannot contain window function calls"),
        # and made reachable by the nested-window hoist: the window is lifted out and the
        # aggregate is left over its output, which is a plan the engine can build.
        #
        # Refused by its OWN guard rather than by the beside-aggregate one above, because
        # the arrangement is the other way round and so is the remedy — the window goes in
        # the subquery here, not the aggregate. The message is asserted in
        # test_aggregate_over_window_is_refused_by_name.
        ("SELECT SUM(COUNT(*) OVER ()) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT MAX(ROW_NUMBER() OVER (ORDER BY id)) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT SUM(COUNT(*) OVER (PARTITION BY gravity)) FROM $planets", None, None, UnsupportedSyntaxError),
        # The window need not be the whole argument, and the aggregate need not be the
        # whole projection item — ancestry is what is tested, not adjacency.
        ("SELECT SUM(mass + COUNT(*) OVER ()) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT SUM(COUNT(*) OVER ()) + 1 FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT CAST(SUM(COUNT(*) OVER ()) AS VARCHAR) FROM $planets", None, None, UnsupportedSyntaxError),
        # QUALIFY reaches it too, and had NO guard at all: only the window is borrowed
        # into the projection, so the wrapping aggregate was seen by nothing and the
        # statement planned, then died in the engine with a raw KeyError naming a
        # `$derived_` column.
        ("SELECT name FROM $planets QUALIFY SUM(COUNT(*) OVER ()) > 1", None, None, UnsupportedSyntaxError),
        ("SELECT name FROM $planets QUALIFY MAX(ROW_NUMBER() OVER (ORDER BY id)) > 1", None, None, UnsupportedSyntaxError),
        # NOT this shape: siblings, not ancestor-and-descendant. These must still be
        # caught by the BESIDE guard, whose remedy is the opposite one.
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", None, None, UnsupportedSyntaxError),
        # NOT refused at all: an aggregate window is not an aggregate ancestral to itself,
        # and a non-aggregate wrapper is not an aggregate.
        ("SELECT CAST(COUNT(*) OVER () AS VARCHAR) FROM $planets", 9, 1, None),
        ("SELECT COUNT(*) OVER () + 0 FROM $planets", 9, 1, None),
        # The remedy this refusal advises must actually run — and answer what the
        # refused statement was asking for (9 rows of 9, summed = 81).
        ("SELECT SUM(x) FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS t", 1, 1, None),

        # WINDOW INSIDE A WINDOW — `SUM(COUNT(*) OVER ()) OVER ()`. Forbidden by the
        # standard on the same terms as the aggregate case, and reached by the same
        # hoist. Every one of these used to fail with a message that named nothing the
        # caller wrote: a vague NotSupportedError, a raw internal KeyError, a
        # ColumnNotFoundError for a column called *COUNT*, or — through QUALIFY — the
        # beside-aggregate refusal printing `SUM(COUNT(*)) OVER ()`, dropping the inner
        # OVER and calling the inner window an aggregate. Asserted in
        # test_nested_window_is_refused_by_name.
        ("SELECT SUM(COUNT(*) OVER ()) OVER () FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT SUM(COUNT(*) OVER (PARTITION BY gravity)) OVER (PARTITION BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT SUM(ROW_NUMBER() OVER (ORDER BY id)) OVER () FROM $planets", None, None, UnsupportedSyntaxError),
        # Was a raw KeyError on a `$derived_` column — the window is only part of the
        # argument, so nothing downstream recognised the shape.
        ("SELECT SUM(mass + COUNT(*) OVER ()) OVER () FROM $planets", None, None, UnsupportedSyntaxError),
        # Nested three deep. The middle window is the honest complaint, and it must
        # render with the innermost OVER intact.
        ("SELECT SUM(SUM(COUNT(*) OVER ()) OVER ()) OVER () FROM $planets", None, None, UnsupportedSyntaxError),
        # An inner window that is ITSELF malformed is refused for the NESTING, not for
        # the missing ORDER BY — adding one would not make it legal.
        ("SELECT SUM(ROW_NUMBER() OVER ()) OVER () FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT name FROM $planets QUALIFY SUM(COUNT(*) OVER ()) OVER () > 1", None, None, UnsupportedSyntaxError),
        # A window in the OVER SPEC rather than in the argument. Invisible to the
        # expression walk — the spec is the parser's dict, not a child node — so it is
        # caught where the spec becomes nodes. Both spec clauses, both window forms.
        ("SELECT SUM(mass) OVER (PARTITION BY COUNT(*) OVER ()) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT ROW_NUMBER() OVER (ORDER BY COUNT(*) OVER ()) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT ROW_NUMBER() OVER (PARTITION BY COUNT(*) OVER () ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        # NOT refused: windows CHAINED across a subquery boundary are the legal way to
        # write this, and all four combinations run. The values are pinned in
        # test_chained_windows_across_a_subquery.
        ("SELECT SUM(r) OVER () FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS r FROM $planets) AS t", 9, 1, None),
        ("SELECT ROW_NUMBER() OVER (ORDER BY c) FROM (SELECT COUNT(*) OVER () AS c FROM $planets) AS t", 9, 1, None),
        ("SELECT RANK() OVER (ORDER BY r) FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS r FROM $planets) AS t", 9, 1, None),

        # TWO AGGREGATE WINDOWS IN A CHAIN — one per scope. Each scope is its own window
        # chain, and the rewrite COPIES the chain's whole source sub-plan once per
        # partition spec, so copying a source that still holds an un-rewritten Window
        # would bring back the exponential source duplication the chain rewrite exists to
        # remove. That was refused outright — `InvalidInternalStateError: an aggregate
        # Window node was left below a window chain` — on perfectly legal SQL. Rewriting
        # the INNERMOST chain first leaves a join there instead.
        ("SELECT SUM(x) OVER () FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS t", 9, 1, None),
        ("WITH c AS (SELECT COUNT(*) OVER () AS x FROM $planets) SELECT SUM(x) OVER () FROM c", 9, 1, None),
        ("SELECT SUM(x) OVER () FROM (SELECT * FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS a) AS t", 9, 1, None),
        # Three scopes deep.
        ("SELECT SUM(y) OVER () FROM (SELECT SUM(x) OVER () AS y FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS a) AS t", 9, 1, None),
        # A PARTITIONED outer window over a windowing subquery. This one needed the
        # qualified-wildcard rename fix as well: the chain rewrite's reconciling Project
        # is a qualified `source.*`, and the copy taken for the CTE renamed the relation
        # without renaming the wildcard, so it expanded to NOTHING and the partition
        # column "could not be found".
        ("SELECT SUM(x) OVER (PARTITION BY number_of_moons) FROM (SELECT number_of_moons, COUNT(*) OVER () AS x FROM $planets) AS t", 9, 1, None),
        ("SELECT SUM(c) OVER (PARTITION BY number_of_moons) FROM (SELECT number_of_moons, COUNT(*) OVER (PARTITION BY gravity) AS c FROM $planets) AS t", 9, 1, None),
        # More than one spec in the inner scope — a chain of length > 1 BELOW another chain.
        ("SELECT SUM(a + b) OVER () FROM (SELECT COUNT(*) OVER () AS a, COUNT(*) OVER (PARTITION BY number_of_moons) AS b FROM $planets) AS t", 9, 1, None),

        # WINDOW IN HAVING — refused. The standard forbids it and the reason is the
        # evaluation order: HAVING filters GROUPS, and windows are computed AFTER
        # grouping, so the value does not exist yet when the filter runs.
        #
        # It had no guard because the hoist walks the PROJECTION (plus the windows QUALIFY
        # borrows into it) and HAVING is neither: its aggregates went straight into
        # `_aggregates`, where nothing reads `over`, so the SPEC WAS DISCARDED and the
        # window silently became a plain aggregate. `HAVING COUNT(*) OVER () > 100`
        # compared 9 > 100 and returned no rows — no error, and an answer that looks
        # right. Asserted in test_window_in_having_is_refused_by_name.
        ("SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 100", None, None, UnsupportedSyntaxError),
        ("SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 0", None, None, UnsupportedSyntaxError),
        # A GROUP BY beside it did not help — that combination is refused everywhere else,
        # but the guard for it never saw HAVING either.
        ("SELECT number_of_moons, COUNT(*) FROM $planets GROUP BY number_of_moons HAVING COUNT(*) OVER () > 0", None, None, UnsupportedSyntaxError),
        # Only part of the predicate.
        ("SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () + 0 > 100", None, None, UnsupportedSyntaxError),
        # Ranking functions, with and without an OVER — window-only wherever written. The
        # bare form was reported as "the aggregate function ROW_NUMBER is not supported".
        ("SELECT COUNT(*) FROM $planets HAVING ROW_NUMBER() OVER (ORDER BY 1) > 0", None, None, UnsupportedSyntaxError),
        ("SELECT COUNT(*) FROM $planets HAVING RANK() > 0", None, None, UnsupportedSyntaxError),
        # Every scope, not just the outermost.
        ("SELECT * FROM (SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 0) AS t", None, None, UnsupportedSyntaxError),
        # NOT refused: a HAVING with no window in it is untouched.
        ("SELECT number_of_moons, COUNT(*) FROM $planets GROUP BY number_of_moons HAVING COUNT(*) > 1", 1, 2, None),
        ("SELECT COUNT(*) FROM $planets HAVING COUNT(*) > 100", 0, 1, None),
        # The advised remedy runs.
        ("SELECT name FROM $planets QUALIFY COUNT(*) OVER () > 0", 9, 1, None),
        ("SELECT number_of_moons FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY number_of_moons) > 1", 2, 1, None),
        # NOT this shape: HAVING naming a window's SELECT ALIAS is an identifier, not a
        # window node, so no spec is dropped and nothing is refused. It filters on the
        # computed window column — the value is 9 on every row — which is what QUALIFY
        # would do. Pinned because it is the boundary of the refusal above, not because
        # the extension is being ruled on here.
        ("SELECT COUNT(*) OVER () AS c FROM $planets HAVING c > 5", 9, 1, None),
        ("SELECT COUNT(*) OVER () AS c FROM $planets HAVING c > 500", 0, 1, None),

        # QUALIFIED WILDCARD INSIDE A COPIED SUB-PLAN. `rename_relations` renames every
        # relation in a copy and remaps the column references onto the new alias, but a
        # qualified wildcard names its relation as a plain string in `value`, which
        # nothing remapped — so it expanded to nothing and the copy silently lost every
        # source column. Nothing to do with windows: a CTE body was enough, and it died
        # with a raw `ValueError: not enough values to unpack` out of the binder.
        ("WITH c AS (SELECT p.* FROM $planets AS p) SELECT name FROM c", 9, 1, None),
        ("WITH c AS (SELECT p.* FROM $planets AS p) SELECT COUNT(*) FROM c", 1, 1, None),
        ("SELECT COUNT(*) OVER () FROM (SELECT p.* FROM $planets AS p) AS d", 9, 1, None),
        # The un-copied form always worked; pinned so the two cannot diverge again.
        ("SELECT name FROM (SELECT p.* FROM $planets AS p) AS d", 9, 1, None),

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

        # WINDOWS NESTED INSIDE AN EXPRESSION — the window is hoisted out and the rest of
        # the expression is computed over its output, so the row count is the WINDOW's,
        # not an aggregate's. `+ 0` used to discard the OVER spec and collapse these to
        # one row holding a plain global aggregate (see test_window_nested_in_expression).
        ("SELECT COUNT(*) OVER (PARTITION BY gravity) + 0 AS c FROM $planets", 9, 1, None),
        ("SELECT COUNT(*) OVER (PARTITION BY gravity) + 0 FROM $planets", 9, 1, None),
        ("SELECT SUM(mass) OVER () + 1 FROM $planets", 9, 1, None),
        ("SELECT CAST(COUNT(*) OVER () AS VARCHAR) FROM $planets", 9, 1, None),
        ("SELECT ROW_NUMBER() OVER (ORDER BY mass) + 1 FROM $planets", 9, 1, None),
        # A base column and a window in the SAME expression, and in a sibling projection.
        ("SELECT mass / SUM(mass) OVER () FROM $planets", 9, 1, None),
        ("SELECT name, SUM(mass) OVER () + 1 FROM $planets", 9, 2, None),
        # Two windows in one expression — same spec, and two different specs (one chain).
        ("SELECT SUM(mass) OVER () / COUNT(*) OVER () FROM $planets", 9, 1, None),
        ("SELECT SUM(mass) OVER (PARTITION BY gravity) + COUNT(*) OVER () FROM $planets", 9, 1, None),
        # Deeper than one level: inside CASE, a function call, and a unary operator.
        ("SELECT CASE WHEN COUNT(*) OVER (PARTITION BY gravity) > 1 THEN 'dup' ELSE 'uniq' END FROM $planets", 9, 1, None),
        ("SELECT COALESCE(SUM(mass) OVER (PARTITION BY gravity), 0) FROM $planets", 9, 1, None),
        ("SELECT -SUM(mass) OVER () FROM $planets", 9, 1, None),
        # A nested window in QUALIFY, which hoists by the same mechanism.
        ("SELECT name FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY mass) + 0 = 1", 1, 1, None),
        # The refusals reach a nested window too — they used to be internal errors there.
        ("SELECT COUNT(*) OVER (PARTITION BY gravity) + 0 FROM $planets GROUP BY gravity", None, None, UnsupportedSyntaxError),
        ("SELECT SUM(mass) OVER (ORDER BY id) + 1 FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT ROW_NUMBER() + 1 FROM $planets", None, None, UnsupportedSyntaxError),

        # casting array vectors segfaulted
        ("SELECT CAST(missions AS VARCHAR) FROM testdata.astronauts", 357, 1, None),

        # CAST: literal string → numeric types (exercises BOOL take fix + constant-shape cast path)
        ("SELECT CAST('42' AS INTEGER) FROM $planets LIMIT 1", 1, 1, None),
        ("SELECT CAST('3.14' AS FLOAT64) FROM $planets LIMIT 1", 1, 1, None),

        # CAST: VARCHAR column → numeric types (round-trip via CAST(col AS VARCHAR) first)
        ("SELECT CAST(CAST(year AS VARCHAR) AS INTEGER) FROM testdata.astronauts LIMIT 3", 3, 1, None),
        ("SELECT CAST(CAST(space_walks_hours AS VARCHAR) AS FLOAT64) FROM testdata.astronauts LIMIT 3", 3, 1, None),

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


def test_window_aggregate_respects_where():
    """
    VALUE-level regression: a window aggregate must be computed over the rows the
    WHERE clause left behind, not over the whole table.

    SQL applies WHERE before window functions. The rewrite that lowers a window to a
    GROUP BY + join built its CTE from the base Scan alone, so the aggregate ran over
    every row in the relation while the outer leg kept the filter — the right number of
    rows, each carrying a value counted from rows the query had discarded. Shape-only
    coverage cannot see that: the answer differs from the truth in the VALUE and in
    nothing else. Guards the sub-plan copy in
    opteryx/planner/plan_rewriter/strategies/window_to_join.py.
    """
    session = opteryx.session()

    # Mercury and Venus are the only planets sharing a partition (0 moons). Filtering
    # Mercury out must drop Venus's partition to one member; counting over the
    # unfiltered scan answers 2.
    statement = (
        "SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS c "
        "FROM $planets WHERE id != 1"
    )
    counts = {}
    for morsel in session.execute_to_morsels(statement):
        for name, count in zip(morsel.column("name").to_pylist(), morsel.column("c").to_pylist()):
            counts[name] = count

    assert counts.get("Venus") == 1, (
        f"window COUNT(*) ignored the WHERE clause: Venus counted {counts.get('Venus')}, "
        "expected 1 (Mercury shares its partition but is filtered out)"
    )
    assert len(counts) == 8, f"expected 8 rows after the filter, got {len(counts)}"

    # SUM over a filtered partition: without the filter each of these would carry the
    # whole-table sum for its partition.
    statement = "SELECT name, SUM(id) OVER (PARTITION BY number_of_moons) AS s FROM $planets WHERE id > 4"
    sums = {}
    for morsel in session.execute_to_morsels(statement):
        for name, total in zip(morsel.column("name").to_pylist(), morsel.column("s").to_pylist()):
            sums[name] = total

    assert sums == {"Jupiter": 5, "Saturn": 6, "Uranus": 7, "Neptune": 8, "Pluto": 9}, (
        f"window SUM ignored the WHERE clause: {sums}"
    )


def test_window_aggregate_over_subquery_source():
    """
    VALUE-level regression: a window aggregate over a CTE or a derived table must
    answer exactly what the same window over the base table answers.

    Two defects sat behind this, and shape-only coverage sees neither.

    1. `rename_relations` re-aliased Scan and FunctionDataset nodes but not Subquery
       nodes, so the copy of the source sub-plan kept the original's subquery alias.
       The binder saw two relations of one name and reported a SEMI-join error.
    2. The rewrite never labelled its join's legs. The native compiler falls back to
       in-edge ORDER when a join is unlabelled, `left_columns`/`right_columns` do not
       move with it, and the optimizer reorders those edges once the redundant Subquery
       wrappers over a derived-table source are removed — the join then compiled its
       left key against the right leg. The direct-Scan shapes only ever passed because
       the order happened to come out right, so the fix has to be pinned by a shape the
       optimizer actually reorders: WHERE sitting ABOVE the derived table.

    Guards opteryx/planner/plan_rewriter/strategies/window_to_join.py and the Subquery
    arm of rename_relations in opteryx/planner/relation_resolver/__init__.py.
    """
    session = opteryx.session()

    def counts(statement):
        result = {}
        for morsel in session.execute_to_morsels(statement):
            for name, count in zip(
                morsel.column("name").to_pylist(), morsel.column("c").to_pylist()
            ):
                result[name] = count
        return result

    # Mercury and Venus are the only planets sharing a partition (0 moons).
    truth = {
        "Mercury": 2, "Venus": 2, "Earth": 1, "Mars": 1, "Jupiter": 1,
        "Saturn": 1, "Uranus": 1, "Neptune": 1, "Pluto": 1,
    }
    window = "COUNT(*) OVER (PARTITION BY number_of_moons) AS c"

    for label, statement in (
        ("base table", f"SELECT name, {window} FROM $planets"),
        ("CTE", f"WITH x AS (SELECT * FROM $planets) SELECT name, {window} FROM x"),
        ("derived table", f"SELECT name, {window} FROM (SELECT * FROM $planets) AS s"),
        (
            "nested derived tables",
            f"SELECT name, {window} FROM (SELECT * FROM (SELECT * FROM $planets) AS i) AS s",
        ),
        (
            "CTE joining two tables",
            "WITH x AS (SELECT p.name AS name, q.number_of_moons AS number_of_moons "
            "FROM $planets p INNER JOIN $planets q ON p.id = q.id) "
            f"SELECT name, {window} FROM x",
        ),
    ):
        assert counts(statement) == truth, f"window over {label}: {counts(statement)}"

    # WHERE outside the derived table: the filter applies before the window, so Venus
    # loses the only other member of its partition. This is the leg-ordering shape.
    filtered = counts(
        f"SELECT name, {window} FROM (SELECT * FROM $planets) AS s WHERE id != 1"
    )
    assert filtered == {
        "Venus": 1, "Earth": 1, "Mars": 1, "Jupiter": 1,
        "Saturn": 1, "Uranus": 1, "Neptune": 1, "Pluto": 1,
    }, f"window over a filtered derived table: {filtered}"

    # WHERE inside the derived table must give the same answer.
    assert (
        counts(f"SELECT name, {window} FROM (SELECT * FROM $planets WHERE id != 1) AS s")
        == filtered
    ), "the filter moving inside the derived table changed the window's answer"


def test_window_aggregates_with_distinct_partition_specs():
    """
    VALUE-level regression: N window aggregates with N DIFFERENT partition specs must
    each answer over their OWN partitioning, and must not disturb one another.

    The logical planner emits one Window node per distinct spec, stacked. Rewriting them
    one at a time was wrong twice over: each rewrite copied the sub-plan below its Window
    node, which by then held the previous rewrite's join (the source was duplicated
    exponentially in the number of specs), and each added its own `source.*` Project, so
    the second one expanded the source relation a second time and the query was rejected
    as ambiguous. Both are fixed by rewriting the chain as a unit — one CTE and one join
    per spec over ONE source, one Project on top.

    Shape-only coverage would not see a spec answering against the wrong partitioning,
    so the counts are asserted against the partitions themselves. Guards
    opteryx/planner/plan_rewriter/strategies/window_to_join.py.
    """
    session = opteryx.session()

    statement = (
        "SELECT name, "
        "COUNT(*) OVER (PARTITION BY number_of_moons) AS moons, "
        "COUNT(*) OVER (PARTITION BY gravity) AS grav, "
        "SUM(id) OVER (PARTITION BY number_of_moons) AS moon_sum, "
        "MIN(id) OVER (PARTITION BY id) AS own_id "
        "FROM $planets"
    )
    rows = {}
    for morsel in session.execute_to_morsels(statement):
        for name, moons, grav, moon_sum, own_id in zip(
            morsel.column("name").to_pylist(),
            morsel.column("moons").to_pylist(),
            morsel.column("grav").to_pylist(),
            morsel.column("moon_sum").to_pylist(),
            morsel.column("own_id").to_pylist(),
        ):
            rows[name] = (moons, grav, moon_sum, own_id)

    # Mercury and Venus share a partition on number_of_moons (both 0); Mercury and Mars
    # share one on gravity (both 3.7). The two specs cut the table differently, which is
    # the point: a spec answering against the other's partitioning would still return
    # nine rows of plausible-looking counts.
    assert rows == {
        "Mercury": (2, 2, 3, 1),
        "Venus": (2, 1, 3, 2),
        "Earth": (1, 1, 3, 3),
        "Mars": (1, 2, 4, 4),
        "Jupiter": (1, 1, 5, 5),
        "Saturn": (1, 1, 6, 6),
        "Uranus": (1, 1, 7, 7),
        "Neptune": (1, 1, 8, 8),
        "Pluto": (1, 1, 9, 9),
    }, f"windows with distinct partition specs: {rows}"

    # The same specs under a WHERE: the filter feeds EVERY spec's CTE, not just the first.
    filtered = {}
    for morsel in session.execute_to_morsels(
        "SELECT name, COUNT(*) OVER (PARTITION BY number_of_moons) AS moons, "
        "COUNT(*) OVER (PARTITION BY gravity) AS grav FROM $planets WHERE id != 1"
    ):
        for name, moons, grav in zip(
            morsel.column("name").to_pylist(),
            morsel.column("moons").to_pylist(),
            morsel.column("grav").to_pylist(),
        ):
            filtered[name] = (moons, grav)

    # Mercury is filtered out, so Venus loses its moons partner and Mars its gravity one.
    assert filtered == {
        "Venus": (1, 1), "Earth": (1, 1), "Mars": (1, 1), "Jupiter": (1, 1),
        "Saturn": (1, 1), "Uranus": (1, 1), "Neptune": (1, 1), "Pluto": (1, 1),
    }, f"windows with distinct partition specs under a filter: {filtered}"


def test_window_over_whole_relation():
    """
    VALUE-level regression: `agg OVER ()` — no PARTITION BY — is the aggregate over the
    window's whole input, attached unchanged to every row.

    The rewrite lowers this to an UNGROUPED aggregate cross-joined onto the outer
    relation. Two things can only be checked by value. The cross join must BROADCAST:
    one row in, one row out, carrying the aggregate — if the CTE ever returned more than
    one row it would MULTIPLY the outer rows instead, and if it returned the wrong rows
    the count would simply be wrong. And WHERE applies before the window, so the
    aggregate covers the surviving rows, not the whole table.
    """
    session = opteryx.session()

    # Every row carries the same total, and the row count is untouched by the join.
    rows = {}
    for morsel in session.execute_to_morsels("SELECT name, COUNT(*) OVER () AS c FROM $planets"):
        for name, count in zip(morsel.column("name").to_pylist(), morsel.column("c").to_pylist()):
            rows[name] = count

    assert len(rows) == 9, f"OVER () changed the row count: {len(rows)} rows, expected 9"
    assert set(rows.values()) == {9}, f"OVER () did not broadcast one value: {sorted(set(rows.values()))}"

    # The filter is below the window, so both the outer rows AND the aggregate see it.
    filtered = {}
    for morsel in session.execute_to_morsels(
        "SELECT name, COUNT(*) OVER () AS c, SUM(id) OVER () AS s FROM $planets WHERE id > 4"
    ):
        for name, count, total in zip(
            morsel.column("name").to_pylist(),
            morsel.column("c").to_pylist(),
            morsel.column("s").to_pylist(),
        ):
            filtered[name] = (count, total)

    # ids 5..9 survive: five rows, summing to 35.
    assert filtered == {
        "Jupiter": (5, 35), "Saturn": (5, 35), "Uranus": (5, 35),
        "Neptune": (5, 35), "Pluto": (5, 35),
    }, f"OVER () ignored the WHERE clause: {filtered}"

    # A partition-less window beside a partitioned one: one chain, two CTEs, and the
    # partition-less leg must not disturb the partitioned leg's keys.
    mixed = {}
    for morsel in session.execute_to_morsels(
        "SELECT name, COUNT(*) OVER () AS t, COUNT(*) OVER (PARTITION BY number_of_moons) AS c "
        "FROM $planets"
    ):
        for name, total, count in zip(
            morsel.column("name").to_pylist(),
            morsel.column("t").to_pylist(),
            morsel.column("c").to_pylist(),
        ):
            mixed[name] = (total, count)

    # Mercury and Venus are the only planets sharing a partition (0 moons).
    assert mixed == {
        "Mercury": (9, 2), "Venus": (9, 2), "Earth": (9, 1), "Mars": (9, 1),
        "Jupiter": (9, 1), "Saturn": (9, 1), "Uranus": (9, 1), "Neptune": (9, 1),
        "Pluto": (9, 1),
    }, f"OVER () mixed with a partition spec: {mixed}"


def test_window_functions_are_named_for_their_expression():
    """
    NAME-level regression: an unaliased window function is named by the expression it
    renders to, like every other unaliased projection expression.

    The planner mints `$win_<random>` as the INTERNAL join key that names the aggregate
    inside the CTE the window rewrite builds and the reference to it above the join.
    Nothing restored a user-facing name, so that minted key reached the caller — and it
    is random per execution, so the column name was not even stable across runs. The
    shape battery above cannot see this: the column count is right either way.

    The two names are separate things and stay separate: the display name is carried on
    the outer reference's `query_column`, the minted key stays on `source_column` where
    the rewrite reads it. Guards the `_win_alias` / `_win_display` split in
    opteryx/planner/logical_planner/logical_planner.py — both the aggregate-window path
    and the ranking path, which mint the same way.
    """
    session = opteryx.session()

    def _names(statement):
        names = None
        for morsel in session.execute_to_morsels(statement):
            names = [
                n.decode() if isinstance(n, bytes) else n for n in morsel.column_names
            ]
        return names

    # Aggregate window (rewritten to a CTE + join).
    assert _names("SELECT name, COUNT(*) OVER (PARTITION BY gravity) FROM $planets") == [
        "name",
        "COUNT(*) OVER (PARTITION BY gravity)",
    ]
    # Ranking window (the blocking-sort operator path) — same mint, same symptom.
    assert _names("SELECT name, ROW_NUMBER() OVER (ORDER BY id) FROM $planets") == [
        "name",
        "ROW_NUMBER() OVER (ORDER BY id)",
    ]
    # The whole spec is part of the name: partition list, order list and direction.
    assert _names(
        "SELECT name, RANK() OVER (PARTITION BY gravity, id ORDER BY id DESC) FROM $planets"
    ) == ["name", "RANK() OVER (PARTITION BY gravity, id ORDER BY id DESC)"]
    # Two windows differing ONLY in their spec must not collide on one name.
    assert _names(
        "SELECT COUNT(*) OVER (PARTITION BY gravity), COUNT(*) OVER (PARTITION BY id) "
        "FROM $planets"
    ) == ["COUNT(*) OVER (PARTITION BY gravity)", "COUNT(*) OVER (PARTITION BY id)"]
    # An explicit alias still wins, and still wins for only its own column.
    assert _names(
        "SELECT name, SUM(id) OVER (PARTITION BY gravity) AS t, "
        "MAX(id) OVER (PARTITION BY gravity) FROM $planets"
    ) == ["name", "t", "MAX(id) OVER (PARTITION BY gravity)"]


def test_window_beside_aggregate_is_refused_by_name():
    """
    MESSAGE-level regression: mixing a window function with a plain aggregate is refused,
    and the refusal names the window the caller WROTE.

    The rejection itself is architectural and deliberate — the Window step is planned
    UNDER the aggregate step, so the window is computed over the rows the aggregate
    collapses and can never see the aggregated result. That is already refused by name
    for an explicit GROUP BY ("Window functions cannot be combined with GROUP BY"). A
    bare aggregate is an implicit single group and hits the same wall, but had no guard:
    it fell through to the generic "must appear in the `GROUP BY` clause" error, which
    printed the MINTED `$win_<random>` join key — a column the caller never wrote, and
    random per execution, so the message was not even stable across runs.

    Both halves are asserted, because either alone can regress: the shape battery above
    sees only that SOMETHING raised, and the display split guarded by
    test_window_functions_are_named_for_their_expression is about column names, not
    messages.
    """
    session = opteryx.session()

    def _message(statement):
        with pytest.raises(UnsupportedSyntaxError) as raised:
            for _ in session.execute_to_morsels(statement):
                pass
        return str(raised.value)

    # (statement, the window's display form that must be named)
    for statement, window in (
        ("SELECT COUNT(*), COUNT(*) OVER () FROM $planets", "COUNT(*) OVER ()"),
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", "SUM(mass) OVER ()"),
        (
            "SELECT MAX(id) OVER (PARTITION BY gravity), COUNT(*) FROM $planets",
            "MAX(id) OVER (PARTITION BY gravity)",
        ),
        # The ranking path mints the same way, so it leaked the same way.
        (
            "SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY id) FROM $planets",
            "ROW_NUMBER() OVER (ORDER BY id)",
        ),
        # The aggregate reaches `_aggregates` from ORDER BY, not the SELECT list.
        ("SELECT COUNT(*) OVER () FROM $planets ORDER BY COUNT(*)", "COUNT(*) OVER ()"),
    ):
        message = _message(statement)
        assert "$win_" not in message, f"minted alias leaked: {statement} -> {message}"
        assert window in message, f"window not named: {statement} -> {message}"
        assert "**SELECT**" in message, f"clause not named: {statement} -> {message}"

    # QUALIFY reaches the same wall — its Filter is planned below the aggregate too, so
    # these filtered the PRE-aggregation rows and then counted them, and ran clean. The
    # refusal must name QUALIFY, not SELECT: the window is not in the caller's SELECT
    # list, and it must offer the remedy that fits (window and filter together in the
    # subquery, aggregate outside) rather than the SELECT one, which does not apply.
    for statement, window in (
        ("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER () > 1", "COUNT(*) OVER ()"),
        (
            "SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1",
            "COUNT(*) OVER (PARTITION BY gravity)",
        ),
        (
            "SELECT COUNT(*) FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1",
            "ROW_NUMBER() OVER (ORDER BY id)",
        ),
    ):
        message = _message(statement)
        assert "$win_" not in message, f"minted alias leaked: {statement} -> {message}"
        assert window in message, f"window not named: {statement} -> {message}"
        assert "**QUALIFY**" in message, f"clause not named: {statement} -> {message}"
        assert "in the same **SELECT**" not in message, f"wrong clause: {statement}"

    # A statement carrying BOTH names the SELECT one — the caller can see that window in
    # their output list, where the QUALIFY one is invisible to them.
    _both = _message(
        "SELECT COUNT(*), SUM(id) OVER () FROM $planets QUALIFY COUNT(*) OVER () > 1"
    )
    assert "SUM(id) OVER ()" in _both and "in the same **SELECT**" in _both, _both

    # The generic error this used to fall through to still fires for a real column, and
    # names it — the fix narrows WHICH name it prints, it does not remove the error.
    with pytest.raises(SqlError) as raised:
        for _ in session.execute_to_morsels("SELECT name, COUNT(*) FROM $planets"):
            pass
    assert "Column 'name' must appear in the `GROUP BY` clause" in str(raised.value)


def test_aggregate_over_window_is_refused_by_name():
    """
    MESSAGE-level regression: a window inside an aggregate's ARGUMENT is refused, and the
    refusal names both halves as the caller wrote them.

    `SUM(COUNT(*) OVER ())` is forbidden by the standard — an aggregate call cannot
    contain a window call — and it became reachable when the nested-window hoist landed:
    the window is lifted out of the argument and the aggregate is left over its output
    column, which is a plan the engine can build.

    The window-beside-aggregate guard did catch it, but described it wrongly twice over,
    which is why this shape has a guard of its own:

    * it rendered the aggregate through `format_expression` AFTER the hoist, so the
      window inside it read as the minted `$win_<random>` join key — a column the caller
      never wrote, and different on every execution;
    * its remedy is the reverse of the one this shape needs. It advises putting the
      AGGREGATE in a subquery, but the caller wrote the window inside the aggregate, and
      what runs is the window in the subquery with the aggregate outside.

    Both halves are asserted because either can regress independently, and the shape
    battery above sees only that SOMETHING raised — it cannot tell the two guards apart.
    """
    session = opteryx.session()

    def _message(statement):
        with pytest.raises(UnsupportedSyntaxError) as raised:
            for _ in session.execute_to_morsels(statement):
                pass
        return str(raised.value)

    # (statement, the window's display form, the aggregate as written)
    for statement, window, aggregate in (
        (
            "SELECT SUM(COUNT(*) OVER ()) FROM $planets",
            "COUNT(*) OVER ()",
            "SUM(COUNT(*) OVER ())",
        ),
        # The ranking path mints its alias the same way, so it leaked the same way.
        (
            "SELECT MAX(ROW_NUMBER() OVER (ORDER BY id)) FROM $planets",
            "ROW_NUMBER() OVER (ORDER BY id)",
            "MAX(ROW_NUMBER() OVER (ORDER BY id))",
        ),
        # The spec is part of what the window IS, so it is part of both renderings.
        (
            "SELECT SUM(COUNT(*) OVER (PARTITION BY gravity)) FROM $planets",
            "COUNT(*) OVER (PARTITION BY gravity)",
            "SUM(COUNT(*) OVER (PARTITION BY gravity))",
        ),
        # The window is only PART of the argument — the whole argument is rendered.
        (
            "SELECT SUM(mass + COUNT(*) OVER ()) FROM $planets",
            "COUNT(*) OVER ()",
            "SUM(mass + COUNT(*) OVER ())",
        ),
        # The aggregate is nested inside a larger item; the AGGREGATE is what is named,
        # not the whole projection item, because the aggregate is what cannot hold it.
        (
            "SELECT SUM(COUNT(*) OVER ()) + 1 FROM $planets",
            "COUNT(*) OVER ()",
            "SUM(COUNT(*) OVER ())",
        ),
        # QUALIFY had no guard: only the window is borrowed into the projection, so the
        # wrapping aggregate stayed in the predicate where nothing looked at it, and the
        # statement planned and then died in the engine with a raw KeyError.
        (
            "SELECT name FROM $planets QUALIFY SUM(COUNT(*) OVER ()) > 1",
            "COUNT(*) OVER ()",
            "SUM(COUNT(*) OVER ())",
        ),
    ):
        message = _message(statement)
        assert "$win_" not in message, f"minted alias leaked: {statement} -> {message}"
        assert window in message, f"window not named: {statement} -> {message}"
        assert aggregate in message, f"aggregate not named: {statement} -> {message}"
        # The remedy must be the one that fits THIS arrangement. The beside-aggregate
        # refusal advises the opposite rewrite, and advising it here sends the caller to
        # a statement that is refused all over again.
        assert "Compute the window in a subquery" in message, f"wrong remedy: {message}"
        assert "cannot be combined with" not in message, f"wrong guard fired: {message}"

    # The remedy runs, and answers what the refused statement asked for: the window
    # gives 9 to each of nine rows, and the aggregate sums them.
    values = []
    for morsel in session.execute_to_morsels(
        "SELECT SUM(x) AS s FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS t"
    ):
        morsel.materialize()
        values.extend(morsel.column("s").to_pylist())
    assert values == [81], f"the advised rewrite does not answer the question: {values!r}"

    # The BESIDE guard must still own the sibling arrangement — the two refusals describe
    # opposite shapes and give opposite remedies, so neither may swallow the other.
    beside = _message("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets")
    assert "cannot be combined with" in beside, beside
    assert "Compute the aggregate in a subquery" in beside, beside


def test_nested_window_is_refused_by_name():
    """
    MESSAGE-level regression: a window written inside another window is refused, and the
    refusal names both windows exactly as the caller wrote them.

    `SUM(COUNT(*) OVER ()) OVER ()` is forbidden by the standard on the same terms as the
    aggregate case — window calls cannot be nested — and reached the engine by the same
    hoist. Every route to it produced a message that named nothing the caller had
    written:

    * the argument nesting → `NotSupportedError: SUM over a column the engine could not
      resolve here`, or a raw `KeyError` on a `$derived_` column when the window was only
      part of the argument;
    * the OVER SPEC nesting → `ColumnNotFoundError: Column *COUNT* cannot be found`,
      pointing at a table that was never the problem;
    * through QUALIFY → the beside-aggregate refusal, printing `SUM(COUNT(*)) OVER ()` —
      the inner OVER silently dropped, so it named a DIFFERENT statement, and called the
      inner window an aggregate.

    Rendering is the fragile half and is asserted hardest: `format_expression` sees no
    OVER anywhere (the spec is the parser's dict, not a child node), so both halves have
    to be rendered by substituting each nested window for its own display form, and that
    has to recurse or a triple nest prints `SUM(COUNT(*)) OVER ()` again.
    """
    session = opteryx.session()

    def _message(statement):
        with pytest.raises(UnsupportedSyntaxError) as raised:
            for _ in session.execute_to_morsels(statement):
                pass
        return str(raised.value)

    # (statement, inner window, the enclosing window as written)
    for statement, inner, outer in (
        (
            "SELECT SUM(COUNT(*) OVER ()) OVER () FROM $planets",
            "COUNT(*) OVER ()",
            "SUM(COUNT(*) OVER ()) OVER ()",
        ),
        # Both specs are part of what each window IS, so both are rendered.
        (
            "SELECT SUM(COUNT(*) OVER (PARTITION BY gravity)) OVER (PARTITION BY id) FROM $planets",
            "COUNT(*) OVER (PARTITION BY gravity)",
            "SUM(COUNT(*) OVER (PARTITION BY gravity)) OVER (PARTITION BY id)",
        ),
        # The window is only PART of the argument — this one was a raw KeyError.
        (
            "SELECT SUM(mass + COUNT(*) OVER ()) OVER () FROM $planets",
            "COUNT(*) OVER ()",
            "SUM(mass + COUNT(*) OVER ()) OVER ()",
        ),
        # Three deep: the NEAREST enclosing window is the honest complaint, and the
        # innermost OVER must survive both renderings.
        (
            "SELECT SUM(SUM(COUNT(*) OVER ()) OVER ()) OVER () FROM $planets",
            "SUM(COUNT(*) OVER ()) OVER ()",
            "SUM(SUM(COUNT(*) OVER ()) OVER ()) OVER ()",
        ),
        # QUALIFY reached it through the beside-aggregate guard, which named a statement
        # the caller did not write.
        (
            "SELECT name FROM $planets QUALIFY SUM(COUNT(*) OVER ()) OVER () > 1",
            "COUNT(*) OVER ()",
            "SUM(COUNT(*) OVER ()) OVER ()",
        ),
    ):
        message = _message(statement)
        assert "$win_" not in message, f"minted alias leaked: {statement} -> {message}"
        assert inner in message, f"inner window not named: {statement} -> {message}"
        assert outer in message, f"outer window not named: {statement} -> {message}"
        assert "cannot be nested" in message, f"wrong guard fired: {statement} -> {message}"
        # The inner window is a window, not an aggregate — calling it one was the old
        # message's second error, and it points at the wrong remedy.
        assert "inside the aggregate" not in message, f"named as an aggregate: {message}"

    # An inner window that is itself malformed is refused for the NESTING. Fixing the
    # missing ORDER BY would land the caller straight back here.
    _malformed = _message("SELECT SUM(ROW_NUMBER() OVER ()) OVER () FROM $planets")
    assert "cannot be nested" in _malformed, _malformed
    assert "requires an **ORDER BY**" not in _malformed, _malformed

    # A window in the OVER SPEC is invisible to the expression walk and is caught where
    # the spec becomes nodes. The spec clause is load-bearing: the two remedies differ.
    for statement, clause, remedy in (
        (
            "SELECT SUM(mass) OVER (PARTITION BY COUNT(*) OVER ()) FROM $planets",
            "**PARTITION BY**",
            "partition by its result",
        ),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY COUNT(*) OVER ()) FROM $planets",
            "**ORDER BY**",
            "order by its result",
        ),
    ):
        message = _message(statement)
        assert "COUNT(*) OVER ()" in message, f"inner window not named: {message}"
        assert clause in message, f"spec clause not named: {statement} -> {message}"
        assert remedy in message, f"wrong remedy: {statement} -> {message}"
        assert "Column *COUNT* cannot be found" not in message, message

    # The remedy is advised for EVERY combination. It was once withheld when the inner
    # window was an aggregate one, because an aggregate window over a subquery computing
    # an aggregate window died with "an aggregate Window node was left below a window
    # chain" — a plan-rewrite ordering defect, since fixed. A refusal that withholds a
    # working rewrite is as wrong as one that advises a broken one, so both are asserted.
    for statement in (
        "SELECT SUM(ROW_NUMBER() OVER (ORDER BY id)) OVER () FROM $planets",
        "SELECT SUM(COUNT(*) OVER ()) OVER () FROM $planets",
    ):
        assert "Compute the inner window in a subquery" in _message(statement), statement

    # The advised rewrite must actually run, for every combination — they are what the
    # refusal points at.
    for statement, expected in (
        (
            "SELECT SUM(r) OVER () AS s FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS r FROM $planets) AS t",
            45,
        ),
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY c) AS s FROM (SELECT COUNT(*) OVER () AS c FROM $planets) AS t",
            1,
        ),
        # The combination that used to have no working rewrite: nine rows of nine,
        # summed and broadcast back over all nine.
        (
            "SELECT SUM(x) OVER () AS s FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS t",
            81,
        ),
    ):
        values = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            values.extend(morsel.column("s").to_pylist())
        assert values[0] == expected, f"{statement} -> {values[:3]!r}"


def test_window_in_having_is_refused_by_name():
    """
    MESSAGE-level regression: a window function in HAVING is refused, named as written.

    This one was a SILENT WRONG ANSWER rather than a bad message. The hoist walks the
    PROJECTION (plus the windows QUALIFY borrows into it), and HAVING is neither — its
    aggregates were collected straight into `_aggregates`, where nothing reads `over`, so
    the OVER spec was DISCARDED and the window became a plain aggregate.
    `SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 100` therefore compared
    9 > 100 and returned no rows: no error, and an answer that looks entirely reasonable
    until you notice the window never ran. Flipping the comparison shows it — `> 0`
    returned the row.

    Refusing is the fix rather than a gap. The standard forbids a window in HAVING
    because HAVING filters GROUPS and windows are computed AFTER grouping, so there is
    no value to filter on yet and no semantics to implement.

    The shape battery above sees only that SOMETHING raised, so the naming, the reason
    and the remedy are asserted here.
    """
    session = opteryx.session()

    def _message(statement):
        with pytest.raises(UnsupportedSyntaxError) as raised:
            for _ in session.execute_to_morsels(statement):
                pass
        return str(raised.value)

    # (statement, the window as the caller wrote it)
    for statement, window in (
        ("SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () > 100", "COUNT(*) OVER ()"),
        (
            "SELECT number_of_moons, COUNT(*) FROM $planets GROUP BY number_of_moons "
            "HAVING COUNT(*) OVER (PARTITION BY number_of_moons) > 0",
            "COUNT(*) OVER (PARTITION BY number_of_moons)",
        ),
        # Only part of the predicate is the window.
        ("SELECT COUNT(*) FROM $planets HAVING COUNT(*) OVER () + 0 > 100", "COUNT(*) OVER ()"),
        (
            "SELECT COUNT(*) FROM $planets HAVING ROW_NUMBER() OVER (ORDER BY 1) > 0",
            "ROW_NUMBER() OVER (ORDER BY 1)",
        ),
        # A ranking function with NO over clause is still window-only, and must be named
        # as WRITTEN — not given an `OVER ()` the caller never typed. It used to be
        # reported as "the aggregate function ROW_NUMBER is not supported", which names it
        # as the one thing it is not.
        ("SELECT COUNT(*) FROM $planets HAVING RANK() > 0", "RANK()"),
    ):
        message = _message(statement)
        assert "$win_" not in message, f"minted alias leaked: {statement} -> {message}"
        assert window in message, f"window not named: {statement} -> {message}"
        assert "**HAVING**" in message, f"clause not named: {statement} -> {message}"
        assert "**QUALIFY**" in message, f"remedy missing: {statement} -> {message}"
        assert "aggregate function" not in message, f"named as an aggregate: {message}"

    # `RANK()` must be named bare — the assertion above would also pass on
    # "RANK() OVER ()", which contains it.
    assert "OVER" not in _message("SELECT COUNT(*) FROM $planets HAVING RANK() > 0").split(
        "cannot appear"
    )[0], "a bare ranking function was given an OVER clause it was not written with"

    # The advised remedy runs, and answers the question the refused statement was asking:
    # nine rows each carrying the global count of nine.
    values = []
    for morsel in session.execute_to_morsels(
        "SELECT COUNT(*) OVER () AS s FROM $planets QUALIFY COUNT(*) OVER () > 0"
    ):
        morsel.materialize()
        values.extend(morsel.column("s").to_pylist())
    assert values == [9] * 9, values

    # A HAVING with no window in it is untouched — the guard narrows what is refused, it
    # does not refuse HAVING.
    plain = []
    for morsel in session.execute_to_morsels(
        "SELECT COUNT(*) AS s FROM $planets HAVING COUNT(*) > 1"
    ):
        morsel.materialize()
        plain.extend(morsel.column("s").to_pylist())
    assert plain == [9], plain


def test_chained_windows_across_a_subquery():
    """
    VALUE-level regression: a window over a subquery that itself computes a window.

    Each SCOPE is its own window chain, and `_rewrite_window_chain` COPIES the chain's
    whole source sub-plan once per partition spec — so copying a source that still holds
    an un-rewritten aggregate Window would reinstate the exponential source duplication
    the chain rewrite exists to remove. It refused rather than do that, which made every
    statement here fail with `InvalidInternalStateError: an aggregate Window node was
    left below a window chain` — an internal-state error on legal SQL. The fix is
    ordering: rewrite the INNERMOST chain first, and the source holds a join by the time
    the chain above it is copied.

    The counts alone would pass on a wrong answer — a broadcast that multiplied rows, or
    an inner window computed over the wrong row set, both keep the shape — so the values
    are computed here from the data rather than written as literals. $planets has nine
    rows, so `COUNT(*) OVER ()` is nine on each of them and `SUM` of that is 81; deriving
    it keeps the test honest if the fixture ever changes.
    """
    session = opteryx.session()

    def _values(statement, name="s"):
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            out.extend(morsel.column(name).to_pylist())
        return out

    planets = len(_values("SELECT id AS s FROM $planets"))
    moons = _values("SELECT number_of_moons AS s FROM $planets")
    per_partition = {m: moons.count(m) for m in moons}

    for statement, expected in (
        # The shape the refusal was written for: nine rows of nine, summed to 81 and
        # broadcast back over all nine. A cross join that multiplied instead of
        # broadcasting would give 81 ROWS, which the length check catches.
        (
            "SELECT SUM(x) OVER () AS s FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS t",
            [planets * planets] * planets,
        ),
        # The same through a CTE — a different route to one relation.
        (
            "WITH c AS (SELECT COUNT(*) OVER () AS x FROM $planets) SELECT SUM(x) OVER () AS s FROM c",
            [planets * planets] * planets,
        ),
        # Three scopes: 9 -> 81 -> 729.
        (
            "SELECT SUM(y) OVER () AS s FROM (SELECT SUM(x) OVER () AS y FROM (SELECT COUNT(*) OVER () AS x FROM $planets) AS a) AS t",
            [planets**3] * planets,
        ),
        # WHERE inside the subquery: SQL applies it BEFORE the window, so the inner
        # window must see eight rows, not nine. This is the silent-wrong-answer axis —
        # the row count is identical either way.
        (
            "SELECT SUM(x) OVER () AS s FROM (SELECT COUNT(*) OVER () AS x FROM $planets WHERE id != 1) AS t",
            [(planets - 1) ** 2] * (planets - 1),
        ),
        # Ranking window inner, aggregate window outer: 1..9 summed.
        (
            "SELECT SUM(r) OVER () AS s FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS r FROM $planets) AS t",
            [sum(range(1, planets + 1))] * planets,
        ),
        # Two specs in the inner scope — a chain of length > 1 below another chain.
        (
            "SELECT SUM(a + b) OVER () AS s FROM (SELECT COUNT(*) OVER () AS a, COUNT(*) OVER (PARTITION BY number_of_moons) AS b FROM $planets) AS t",
            [sum(planets + per_partition[m] for m in moons)] * planets,
        ),
    ):
        got = _values(statement)
        assert got == expected, f"{statement}\n  got      {got!r}\n  expected {expected!r}"

    # A PARTITIONED outer window over a windowing subquery. Order is not guaranteed
    # without an ORDER BY, so this compares as a multiset.
    partitioned = _values(
        "SELECT SUM(x) OVER (PARTITION BY number_of_moons) AS s "
        "FROM (SELECT number_of_moons, COUNT(*) OVER () AS x FROM $planets) AS t"
    )
    assert sorted(partitioned) == sorted(planets * per_partition[m] for m in moons), partitioned

    # The qualified-wildcard half of the same fix, with no window anywhere: the copy a
    # CTE expansion takes renamed the relation but not the `p.*` naming it, so the
    # wildcard expanded to nothing and the whole body silently lost its columns.
    assert _values("WITH c AS (SELECT p.* FROM $planets AS p) SELECT COUNT(*) AS s FROM c") == [
        planets
    ]
    assert _values("WITH c AS (SELECT p.* FROM $planets AS p) SELECT name AS s FROM c") == _values(
        "SELECT name AS s FROM $planets"
    )


def test_qualify_does_not_leak_its_window_column():
    """
    NAME-level regression: the window column QUALIFY appends must never reach the caller.

    QUALIFY filters on a window's OUTPUT, so the planner appends that window to the
    projection to get a Window node built for it, then drops it again once the Filter is
    pointed at it. Dropping it from the projection list only covers a projection that
    NAMES its columns. A wildcard names none: it is expanded at BIND time from the
    relations in scope, and the Window node's output relation (ranking windows) or the
    aggregate CTE the window-to-join rewrite builds (aggregate windows) is one of them —
    so `SELECT *` picked the minted column straight back up and returned it.

    That column is `$win_` + six random characters, minted per execution. A caller could
    not have named it, could not rely on it, and did not ask for it.

    The shape battery above pins the column COUNT for these statements; the count alone
    would also pass if the leaked column merely displaced a real one, so the names are
    asserted here. Guards the wildcard expansion in opteryx/planner/binder/project.py
    (both visit_exit and visit_project) and the `hidden_columns` the logical planner
    hands them.
    """
    session = opteryx.session()

    def _names(statement):
        out = None
        for morsel in session.execute_to_morsels(statement):
            out = [name.decode() for name in morsel.column_names]
        assert out is not None, f"no morsel returned: {statement}"
        return out

    planets = _names("SELECT * FROM $planets")

    for statement, expected in (
        # Ranking window — the Window node's own output relation.
        ("SELECT * FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 2", planets),
        # Aggregate window — lowered to a join against a minted CTE by the plan rewriter,
        # a different relation by a different route, leaking the same way.
        ("SELECT * FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1", planets),
        # The qualified wildcard never leaked; pinned so the two forms cannot diverge.
        ("SELECT p.* FROM $planets AS p QUALIFY ROW_NUMBER() OVER (ORDER BY p.id) <= 2", planets),
        # EXCEPT builds a Project, so this leaks through visit_project's expansion
        # rather than visit_exit's — both sites have to honour the exclusion.
        (
            "SELECT * EXCEPT (mass) FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 2",
            [name for name in planets if name != "mass"],
        ),
        # Two windows in one QUALIFY: both minted columns are hidden, not just the first.
        (
            "SELECT * FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 4 AND RANK() OVER (ORDER BY mass) >= 2",
            planets,
        ),
        # A narrow source, so a leak cannot hide among twenty real columns.
        (
            "SELECT * FROM (SELECT id, name FROM $planets) AS s QUALIFY ROW_NUMBER() OVER (ORDER BY id) <= 2",
            ["id", "name"],
        ),
    ):
        names = _names(statement)
        assert not any(name.startswith("$win") for name in names), (
            f"minted window column leaked: {statement} -> {names}"
        )
        assert names == expected, f"wrong columns: {statement} -> {names}"

    # The exclusion is scoped to the columns QUALIFY appended — a window the caller DID
    # write is still theirs, under the name they gave it. Hiding by relation (rather than
    # by the specific minted names) would have taken this one with it.
    assert _names(
        "SELECT * FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $planets) AS s"
    ) == ["id", "rn"]


def test_window_nested_in_expression():
    """
    VALUE-level regression: a window function inside a larger expression keeps its
    window semantics — the expression is computed over the window's output, per row.

    This was a P0 SILENT WRONG ANSWER. The planner's window detection tested only the
    top level of each projection item, so a window one level down kept its OVER clause,
    fell through to the plain-aggregate path, and was computed as an aggregate with the
    spec DISCARDED: `COUNT(*) OVER (PARTITION BY gravity) + 0` answered ONE row holding
    a global count of 9, where the un-nested form answers one value per row. No error.

    The shape battery above catches the row count now, but not the pairing: a wrong
    hoist could return nine rows with the value attached to the wrong ones. The
    comparisons here are against the UN-NESTED form of the same window, which is what
    "the nesting changed nothing" actually means. Guards the hoist in
    opteryx/planner/logical_planner/logical_planner.py.
    """
    session = opteryx.session()

    def _pairs(statement, value_column):
        out = {}
        for morsel in session.execute_to_morsels(statement):
            for name, value in zip(
                morsel.column("name").to_pylist(), morsel.column(value_column).to_pylist()
            ):
                out[name] = value
        return out

    # Aggregate window: nested must agree with un-nested, row for row.
    plain = _pairs("SELECT name, COUNT(*) OVER (PARTITION BY gravity) AS c FROM $planets", "c")
    nested = _pairs(
        "SELECT name, COUNT(*) OVER (PARTITION BY gravity) + 0 AS c FROM $planets", "c"
    )
    assert len(plain) == 9, f"un-nested window changed the row count: {plain}"
    # Mercury and Mars are the only planets sharing a gravity (3.7).
    assert plain == {
        "Mercury": 2, "Mars": 2, "Venus": 1, "Earth": 1, "Jupiter": 1,
        "Saturn": 1, "Uranus": 1, "Neptune": 1, "Pluto": 1,
    }, f"un-nested window is wrong, the comparison below is worthless: {plain}"
    assert nested == plain, f"nesting the window changed its answer: {nested} != {plain}"

    # The arithmetic is applied, not ignored — `+ 0` alone cannot prove that.
    scaled = _pairs(
        "SELECT name, COUNT(*) OVER (PARTITION BY gravity) * 10 AS c FROM $planets", "c"
    )
    assert scaled == {k: v * 10 for k, v in plain.items()}, f"expression not applied: {scaled}"

    # Ranking window: the whole numbering must shift by one, not collapse or re-order.
    ranked = _pairs("SELECT name, ROW_NUMBER() OVER (ORDER BY mass) AS r FROM $planets", "r")
    ranked_plus = _pairs(
        "SELECT name, ROW_NUMBER() OVER (ORDER BY mass) + 1 AS r FROM $planets", "r"
    )
    assert sorted(ranked.values()) == list(range(1, 10)), f"ranking window is wrong: {ranked}"
    assert ranked_plus == {k: v + 1 for k, v in ranked.items()}, f"nested ranking: {ranked_plus}"

    # A base column and the window in one expression: the fractions must sum to 1.
    fractions = _pairs("SELECT name, mass / SUM(mass) OVER () AS f FROM $planets", "f")
    assert len(fractions) == 9, f"base column beside a window changed the shape: {fractions}"
    assert abs(sum(fractions.values()) - 1.0) < 1e-9, f"fractions do not sum to 1: {fractions}"

    # WHERE is below the window, so a nested window sees the filtered rows too.
    filtered = _pairs(
        "SELECT name, COUNT(*) OVER (PARTITION BY gravity) + 0 AS c FROM $planets WHERE id > 4",
        "c",
    )
    # ids 5..9 survive and no two of them share a gravity.
    assert filtered == {
        "Jupiter": 1, "Saturn": 1, "Uranus": 1, "Neptune": 1, "Pluto": 1,
    }, f"nested window ignored the WHERE clause: {filtered}"

    # Unaliased, the item is named for what it renders to — NOT the minted `$win_...`,
    # which is random per execution. See test_window_functions_are_named_for_their_expression.
    names = None
    for morsel in session.execute_to_morsels("SELECT SUM(mass) OVER () + 1 FROM $planets"):
        names = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
    assert names == ["SUM(mass) OVER () + 1"], f"nested window naming: {names}"


def test_humanize_modes():
    """
    VALUE-level regression: HUMANIZE's scale systems and the two defects the
    mode work uncovered.

    Shape-only coverage cannot see any of this — every case here is one row of
    one VARCHAR column, so a wrong ladder, a dropped sign or a NULL that should
    be a string all pass a shape check. Guards
    draken/ops/kernels/string_humanize.cpp and the bind-time mode lowering in
    opteryx/compiled/expression/compiled_expression.pyx.

    The negatives cases are the regression proper: the bucket test used to be on
    the signed value, so a negative could never reach +0.9 and HUMANIZE(-2.5e9)
    rendered "-2,500,000,000".
    """
    cases = [
        # default ladder — unchanged by the mode parameter
        ("SELECT HUMANIZE(1000000)", "1.0 million"),
        ("SELECT HUMANIZE(0)", "0"),
        # negatives abbreviate
        ("SELECT HUMANIZE(-2500000000)", "-2.5 billion"),
        ("SELECT HUMANIZE(-1500)", "-1.5 thousand"),
        # bytes: 1024-based, IEC labels
        ("SELECT HUMANIZE(512, 'bytes')", "512 B"),
        ("SELECT HUMANIZE(1536, 'bytes')", "1.5 KiB"),
        ("SELECT HUMANIZE(1500000000, 'bytes')", "1.4 GiB"),
        # si: 1000-based, both directions
        ("SELECT HUMANIZE(1500, 'si')", "1.5k"),
        ("SELECT HUMANIZE(0.0000012, 'si')", "1.2µ"),
        # time: mixed radix, inflected labels, seconds base
        ("SELECT HUMANIZE(90, 'time')", "1.5 minutes"),
        ("SELECT HUMANIZE(3600, 'time')", "1.0 hour"),
        ("SELECT HUMANIZE(31557600000, 'time')", "1.0 millennium"),
        ("SELECT HUMANIZE(0.0025, 'time')", "2.5 milliseconds"),
        # clock / percent / compact
        ("SELECT HUMANIZE(5025, 'clock')", "01:23:45"),
        ("SELECT HUMANIZE(1000000, 'clock')", "277:46:40"),
        ("SELECT HUMANIZE(0.4212, 'percent')", "42.1%"),
        ("SELECT HUMANIZE(1500000, 'compact')", "1.5M"),
        # odds: defined on (0, 1]; outside it the row is NULL, never clamped
        ("SELECT HUMANIZE(0.000001, 'odds')", "1 in 1 million"),
        ("SELECT HUMANIZE(0.5, 'odds')", "1 in 2"),
        ("SELECT HUMANIZE(0, 'odds')", None),
        ("SELECT HUMANIZE(2, 'odds')", None),
        # A mode's own arithmetic can overflow a FINITE input — odds' 1/x on a
        # subnormal, percent's x100 near DBL_MAX. Ryu spells infinity "Infinity",
        # which comma-grouping would mangle to "In,fin,ity" if it ever reached
        # the formatter.
        ("SELECT HUMANIZE(CAST(5e-324 AS FLOAT64), 'odds')", None),
        ("SELECT HUMANIZE(CAST(1e307 AS FLOAT64), 'percent')", "Infinity"),
        ("SELECT HUMANIZE(CAST(-1e307 AS FLOAT64), 'percent')", "-Infinity"),
        ("SELECT HUMANIZE(CAST(1e300 AS FLOAT64), 'clock')", None),
        # mode spelling is case-insensitive but the SET is closed
        ("SELECT HUMANIZE(1536, 'BYTES')", "1.5 KiB"),
    ]

    for statement, expected in cases:
        session = opteryx.session()
        values = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            values.extend(morsel.column(morsel.column_names[0]).to_pylist())
        assert values == [expected], f"{statement} -> {values!r}, expected [{expected!r}]"

    # An unknown mode must fail at PLAN time, not produce a wrong rendering.
    for bad in ("SELECT HUMANIZE(1000, 'kilobytes')", "SELECT HUMANIZE(1000, 'bits')"):
        session = opteryx.session()
        try:
            list(session.execute_to_morsels(bad))
        except InvalidFunctionParameterError:
            continue
        raise AssertionError(f"{bad} was accepted; the mode set is meant to be closed")

    # A FLOAT64 large enough to make Ryu emit ~290 digits used to overflow a
    # 40-byte stack buffer and abort the process. It must render, not crash.
    session = opteryx.session()
    lengths = []
    for morsel in session.execute_to_morsels(
        "SELECT LENGTH(HUMANIZE(CAST(1e300 AS FLOAT64))) AS n"
    ):
        morsel.materialize()
        lengths.extend(morsel.column("n").to_pylist())
    assert lengths == [396], f"huge-double rendering changed length: {lengths!r}"


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
    for name, fn in (
        ("bool group-by key values", test_bool_group_by_key_values),
        ("window aggregate respects WHERE", test_window_aggregate_respects_where),
        ("window aggregate over a subquery source", test_window_aggregate_over_subquery_source),
        (
            "window aggregates with distinct partition specs",
            test_window_aggregates_with_distinct_partition_specs,
        ),
        ("window over the whole relation", test_window_over_whole_relation),
        (
            "window functions are named for their expression",
            test_window_functions_are_named_for_their_expression,
        ),
        ("window nested inside an expression", test_window_nested_in_expression),
        (
            "window beside an aggregate is refused by name",
            test_window_beside_aggregate_is_refused_by_name,
        ),
        (
            "aggregate over a window is refused by name",
            test_aggregate_over_window_is_refused_by_name,
        ),
        (
            "nested window is refused by name",
            test_nested_window_is_refused_by_name,
        ),
        (
            "window in HAVING is refused by name",
            test_window_in_having_is_refused_by_name,
        ),
        (
            "chained windows across a subquery",
            test_chained_windows_across_a_subquery,
        ),
        (
            "qualify does not leak its window column",
            test_qualify_does_not_leak_its_window_column,
        ),
        ("humanize scale systems", test_humanize_modes),
    ):
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
