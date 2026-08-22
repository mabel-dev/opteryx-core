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

import decimal
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
    DataError,
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
    NotSupportedError,
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

        # GROUP BY ROLLUP — the grouping-set chain `(a,b), (a), ()`. Row counts are the
        # per-set group counts summed: satellites has 7 planetIds, 7+1 for ROLLUP(planetId).
        # Value-level assertions (which rows, where the NULLs land, data-NULL vs
        # rolled-up NULL) live in tests/sql/test_group_by_rollup.py — these pin the shape.
        # Confirmed against DuckDB reading the same parquet file.
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY ROLLUP(planetId)", 8, 2, None),
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY ROLLUP(planetId) HAVING COUNT(*) > 5", 5, 2, None),
        ("SELECT i_category, i_class, COUNT(*) FROM testdata.tpcds_001.item GROUP BY ROLLUP(i_category, i_class)", 83, 3, None),
        # GROUPING(col) — ROLLUP's companion, 1 on the row where `col` was rolled up
        # (the grand total) and 0 everywhere else. Value-level assertions (including
        # the data-NULL-vs-rolled-up-NULL split) live in tests/sql/test_group_by_rollup.py.
        ("SELECT planetId, COUNT(*), GROUPING(planetId) FROM testdata.satellites GROUP BY ROLLUP(planetId)", 8, 3, None),
        # GROUPING() outside ROLLUP/CUBE/GROUPING SETS, and over a non-key column,
        # are both refused rather than answered.
        ("SELECT planetId, GROUPING(planetId) FROM testdata.satellites GROUP BY planetId", None, None, UnsupportedSyntaxError),
        ("SELECT planetId, GROUPING(id) FROM testdata.satellites GROUP BY ROLLUP(planetId)", None, None, UnsupportedSyntaxError),
        # A no-aggregate ROLLUP would collapse onto a DISTINCT, losing the rows two
        # different grouping sets produce identically — refused, not answered short.
        ("SELECT planetId FROM testdata.satellites GROUP BY ROLLUP(planetId)", None, None, NotSupportedError),
        # Same family, no lowering yet: refused by name rather than half-understood.
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY CUBE(planetId, id)", None, None, UnsupportedSyntaxError),
        ("SELECT planetId, COUNT(*) FROM testdata.satellites GROUP BY GROUPING SETS ((planetId), ())", None, None, UnsupportedSyntaxError),

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

        # RELATION ALIASES ARE SCOPED TO THE DERIVED TABLE THAT DECLARES THEM.
        # `d` below is private to each subquery, so the two are not in conflict — this
        # raised a false AmbiguousDatasetError and took seven TPC-DS queries with it
        # (Q02, Q28, Q59, Q61, Q65, Q88, Q90). Values are pinned in
        # tests/sql/test_derived_table_alias_scope.py; these are the shapes.
        ("SELECT y.a, x.a FROM (SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) y, (SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) x", 81, 2, None),
        ("SELECT a1.a FROM (SELECT d.id AS a FROM $planets d) a1, (SELECT d.id AS a FROM $planets d) a2, (SELECT d.id AS a FROM $planets d) a3", 729, 1, None),
        ("SELECT y.a FROM (SELECT d.id AS a FROM $planets d) y INNER JOIN (SELECT d.id AS a FROM $planets d) x ON y.a = x.a", 9, 1, None),
        ("SELECT o.a FROM (SELECT i.a FROM (SELECT d.id AS a FROM $planets d) i, (SELECT d.id AS a FROM $planets d) j WHERE i.a = j.a) o", 9, 1, None),
        # An enclosing alias is not visible inside a derived table, so the inner `p`
        # shadows the outer one rather than colliding with it.
        ("SELECT p.name FROM $planets p, (SELECT p.id AS i FROM $planets p) q WHERE p.id = q.i", 9, 1, None),
        ("SELECT y.id FROM (SELECT y.id FROM $planets y) y", 9, 1, None),
        # Union legs are independent scopes too — the planner gives each leg's scans
        # its own `$union-` alias, so the aliases written here never meet.
        ("SELECT y.a FROM (SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) y UNION ALL SELECT y.a FROM (SELECT p.id AS a FROM $planets p, $planets d WHERE d.id = p.id) y", 18, 1, None),
        ("SELECT a.id FROM $planets a INNER JOIN $planets b ON a.id = b.id UNION ALL SELECT a.id FROM $planets a INNER JOIN $planets b ON a.id = b.id", 18, 1, None),
        # A subquery in WHERE is its own scope as well - twice over, and reusing the
        # outer name.
        ("SELECT p.name FROM $planets p WHERE p.id IN (SELECT d.id FROM $planets d) AND p.id IN (SELECT d.id FROM $planets d WHERE d.id < 5)", 4, 1, None),
        ("SELECT p.name FROM $planets p WHERE p.id IN (SELECT p.id FROM $planets p WHERE p.id < 4)", 3, 1, None),
        ("SELECT p.name FROM $planets p, $planets d WHERE d.id = p.id AND EXISTS (SELECT d.id FROM $planets d WHERE d.id = p.id)", 9, 1, None),
        # A SET-OPERATION leg is not one relation, and its columns are matched by
        # POSITION. Any INTERSECT/EXCEPT with a leg over more than one relation used
        # to fail to bind — the ON was the cross product of the two sides' relation
        # names (TPC-DS Q87). Values and the every-column-compared property are
        # pinned in tests/sql/test_set_operation_multi_relation_legs.py.
        ("SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id INTERSECT SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id", 9, 1, None),
        ("SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id EXCEPT SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id AND p.id > 4", 4, 1, None),
        ("SELECT p.id AS i, d.name AS n FROM $planets p, $planets d WHERE d.id = p.id INTERSECT SELECT p.id AS i, d.name AS n FROM $planets p, $planets d WHERE d.id = p.id", 9, 2, None),
        ("SELECT p.id AS i FROM $planets p, $planets d, $planets e WHERE d.id = p.id AND e.id = p.id EXCEPT SELECT id FROM $planets WHERE id > 4", 4, 1, None),
        ("SELECT p.id AS i FROM $planets p, $planets d WHERE d.id = p.id INTERSECT SELECT id FROM $planets", 9, 1, None),
        # Legs are paired by position, so differing column names are not a barrier.
        ("SELECT id AS a FROM $planets WHERE id < 5 INTERSECT SELECT id AS b FROM $planets WHERE id > 2", 2, 1, None),
        # ...and an unequal pairing is still refused.
        ("SELECT id, name FROM $planets INTERSECT SELECT id FROM $planets", None, None, ValueError),

        # ...and what is still genuinely ambiguous: one scope, one name, two relations.
        ("SELECT * FROM $planets, $planets", None, None, AmbiguousDatasetError),
        ("SELECT * FROM $planets AS a, $planets AS a", None, None, AmbiguousDatasetError),
        ("SELECT y.a FROM (SELECT id AS a FROM $planets) y, (SELECT id AS a FROM $planets) y", None, None, AmbiguousDatasetError),
        ("SELECT p.name FROM $planets p, (SELECT id AS a FROM $planets) p", None, None, AmbiguousDatasetError),
        ("WITH c AS (SELECT id FROM $planets) SELECT * FROM c, c", None, None, AmbiguousDatasetError),

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

        # IN-list against a DATE column - each element individually cast should pass,
        # same as the scalar `=`/BETWEEN cases above (TPC-DS Q83 regression: the
        # temporal-cast validator was reading the IN-list literal's own ARRAY
        # category instead of its element type, and predicate_rewriter's
        # single-element IN->Eq rewrite left a stale ARRAY-typed schema_column
        # behind it - both rejected an already-cast IN-list as an uncast literal).
        # date_dim is unscaled in TPC-DS - the SF0.01 and SF1 files are byte-identical
        # (73,049 rows both), so these read the committed tpcds_001 fixture.
        ("SELECT COUNT(*) FROM testdata.tpcds_001.date_dim WHERE d_date IN (CAST('2000-06-30' AS DATE))", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.tpcds_001.date_dim WHERE d_date IN (CAST('2000-06-30' AS DATE), CAST('2000-07-01' AS DATE))", 1, 1, None),
        ("SELECT COUNT(*) FROM testdata.tpcds_001.date_dim WHERE d_date IN ('2000-06-30'::DATE, '2000-07-01'::DATE, '2000-07-02'::DATE)", 1, 1, None),
        # A bare, uncast literal in the list must still be refused - the explicit-cast
        # requirement is not loosened for IN-lists.
        ("SELECT COUNT(*) FROM testdata.tpcds_001.date_dim WHERE d_date IN ('2000-06-30')", None, None, IncompatibleTypesError),

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
        # A UNION leg KEEPS its Project (a lone aggregate's is folded away), so a
        # computed GROUP BY key is the one shape where the key's own identity has to
        # survive projection_pushdown's liveness set. Under-collecting it ruled the key
        # dead, the groupby sink dropped its value store, and the surviving Project
        # tried to recompute TRUNC over an `id` the aggregate no longer carried. Only
        # the SECOND leg failed — the first escaped because the union's output
        # identities ARE the first leg's, so its key looked live by accident.
        ("SELECT name AS k, TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY ALL UNION ALL SELECT name AS k, TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY ALL", 18, 3, None),
        ("SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY TRUNC(id, 1) UNION ALL SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY TRUNC(id, 1)", 18, 2, None),
        # Distinct UNION over the same shape, and a third leg (the second leg is not a
        # special case — every non-first leg has independently-minted identities).
        ("SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1 UNION SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1", 9, 2, None),
        ("SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1 UNION ALL SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1 UNION ALL SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1", 27, 2, None),
        # UNION legs whose types differ only in WIDTH. Every integer width shares
        # LogicalCategory.INTEGER (and FLOAT32/FLOAT64 share FLOAT), and the leg-cast
        # guard compared CATEGORY — so the correct target was computed and then no cast
        # was inserted, leaving draken's concat to reject two different physical tags.
        ("SELECT id AS n FROM $planets UNION ALL SELECT CAST(id AS INT64) AS n FROM $planets", 18, 1, None),
        ("SELECT density AS n FROM $planets UNION ALL SELECT id AS n FROM $planets", 18, 1, None),
        ("SELECT diameter AS n FROM $planets UNION ALL SELECT density AS n FROM $planets", 18, 1, None),
        ("SELECT perihelion AS n FROM $planets UNION ALL SELECT mass AS n FROM $planets", 18, 1, None),
        # The everyday shape of the same bug: COUNT(*) is always INT64, MAX/MIN keep
        # the source column's width.
        ("SELECT name AS k, COUNT(*) AS n FROM $planets GROUP BY name UNION ALL SELECT name AS k, MAX(id) AS n FROM $planets GROUP BY name", 18, 2, None),
        ("SELECT COUNT(*) AS n FROM $planets UNION ALL SELECT MIN(id) AS n FROM $planets", 2, 1, None),
        # BOOL promotes to INT64, so the INT8 leg needed the same widening cast.
        ("SELECT id AS n FROM $planets UNION ALL SELECT id > 3 AS n FROM $planets", 18, 1, None),
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
        # A leg whose SUBQUERY is wider than the leg itself. The set op takes its output
        # shape from the left leg, which used to mean "the first Project node in that
        # leg's graph" — the subquery's, once a leg has one. The union then declared two
        # columns for a leg projecting one and died with "a UNION leg narrower than the
        # union schema". Both operand positions, both operators, and a nested subquery,
        # because only the OUTERMOST projection is the leg's output.
        ("SELECT id FROM (SELECT id, name FROM $planets) AS x UNION ALL SELECT id FROM $planets", 18, 1, None),
        ("SELECT id FROM $planets UNION ALL SELECT id FROM (SELECT id, name FROM $planets) AS x", 18, 1, None),
        ("SELECT id FROM (SELECT id, name FROM $planets) AS x INTERSECT SELECT id FROM $planets", 9, 1, None),
        ("SELECT id FROM (SELECT id, name FROM $planets) AS x EXCEPT SELECT id FROM $planets WHERE id > 4", 4, 1, None),
        ("SELECT id FROM (SELECT id FROM (SELECT id, name FROM $planets) AS a) AS b UNION ALL SELECT id FROM $planets", 18, 1, None),
        ("SELECT id, name FROM (SELECT id, name, mass FROM $planets) AS x UNION ALL SELECT id, name FROM $planets", 18, 2, None),
        # Mismatched column counts must raise for EVERY set operator, not just UNION.
        # Only UNION reached the binder's check: plan_rewriter turns non-wildcard
        # INTERSECT/EXCEPT into semi/anti joins pre-bind, and that rewrite builds its ON
        # condition from the LEFT side's names alone — so the wider right side's extra
        # columns were silently ignored and the query ANSWERED (9 rows, and 0 for
        # EXCEPT) instead of refusing. Both operand orders, and the ALL forms, which
        # take a different path again.
        ("SELECT id FROM $planets INTERSECT SELECT id, name FROM $planets", None, None, ValueError),
        ("SELECT id, name FROM $planets INTERSECT SELECT id FROM $planets", None, None, ValueError),
        ("SELECT id FROM $planets EXCEPT SELECT id, name FROM $planets", None, None, ValueError),
        ("SELECT id, name FROM $planets EXCEPT SELECT id FROM $planets", None, None, ValueError),
        ("SELECT id FROM $planets INTERSECT ALL SELECT id, name FROM $planets", None, None, ValueError),
        ("SELECT id FROM $planets EXCEPT ALL SELECT id, name FROM $planets", None, None, ValueError),

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
        # Navigation windows (LAG/LEAD): shapes; values are pinned in
        # test_navigation_window_values.
        ("SELECT name, LAG(name) OVER (ORDER BY id) FROM $planets", 9, 2, None),
        ("SELECT name, LEAD(id, 2) OVER (PARTITION BY planetId ORDER BY id) FROM testdata.satellites", 177, 2, None),
        ("SELECT name, LAG(id) OVER (ORDER BY id), LEAD(id) OVER (ORDER BY id), ROW_NUMBER() OVER (ORDER BY id) FROM $planets", 9, 4, None),
        # Refusals: 3-argument default form, bad offsets, missing OVER / ORDER BY,
        # and arguments on the argument-less ranking functions.
        ("SELECT LAG(id, 1, 42) OVER (ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT LAG(id, -1) OVER (ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT LAG(id, id) OVER (ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT LAG(id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT LAG(id) OVER (PARTITION BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT LAG() OVER (ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        ("SELECT ROW_NUMBER(id) OVER (ORDER BY id) FROM $planets", None, None, UnsupportedSyntaxError),
        # SUM(...) OVER (... ORDER BY ...) is a framed (running) window — see
        # tests/sql/test_window_over_group_by.py's cumulative-window coverage. Each
        # partition here is one row (PARTITION BY the unique key `id`), so the
        # (default) running total is just that row's own gravity.
        ("SELECT id, SUM(gravity) OVER (PARTITION BY id ORDER BY id) FROM $planets", 9, 2, None),
        # A window IS supported over a GROUP BY, but only over the grouped result — and
        # `gravity` is read raw inside the window, at a level where only the group keys
        # and the aggregates exist. DuckDB and PostgreSQL refuse it in the same terms.
        ("SELECT id, SUM(gravity) OVER (PARTITION BY id) FROM $planets GROUP BY id", None, None, SqlError),
        # The same statement with the column aggregated is the supported idiom.
        ("SELECT id, SUM(SUM(gravity)) OVER (PARTITION BY id) FROM $planets GROUP BY id", 9, 2, None),
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
        ("SELECT id, SUM(gravity) OVER () FROM $planets GROUP BY id", None, None, SqlError),
        ("SELECT id, SUM(SUM(gravity)) OVER () FROM $planets GROUP BY id", 9, 2, None),
        ("SELECT ROW_NUMBER() OVER () FROM $planets", None, None, UnsupportedSyntaxError),

        # WINDOW BESIDE A PLAIN AGGREGATE — a bare aggregate is an implicit single group,
        # so the window runs over that ONE grouped row. These used to be refused wholesale
        # (the Window step was planned UNDER the aggregate, so the window would have been
        # computed over the rows the aggregate collapses); the grouped-window lowering
        # plans the aggregate first and the window over its output, so the arrangement is
        # now answered rather than refused. Every value below is DuckDB's.
        #
        # One row out, and the window over it counts ONE row, not nine.
        ("SELECT COUNT(*), COUNT(*) OVER () FROM $planets", 1, 2, None),
        # Still refused, and this is the rule that always governed it: `mass` is read raw
        # beside an aggregate, so it must be grouped by or aggregated. DuckDB refuses this
        # one too, in the same words.
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", None, None, SqlError),
        ("SELECT MAX(id) OVER (PARTITION BY gravity), COUNT(*) FROM $planets", None, None, SqlError),
        # The aggregated spellings of those two run.
        ("SELECT SUM(SUM(mass)) OVER () + SUM(mass) FROM $planets", 1, 1, None),
        # The ranking path takes the same route — over one grouped row, ROW_NUMBER is 1.
        ("SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY id) FROM $planets", None, None, SqlError),
        ("SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY COUNT(*)) FROM $planets", 1, 2, None),
        # The aggregate need not be SELECTed — ORDER BY puts one in `_aggregates` too.
        ("SELECT COUNT(*) OVER () FROM $planets ORDER BY COUNT(*)", 1, 1, None),
        # QUALIFY filters the grouped rows on the window's value: one row, whose
        # COUNT(*) OVER () is 1, so `> 1` keeps nothing.
        ("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER () > 1", 0, 1, None),
        ("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1", None, None, SqlError),
        ("SELECT COUNT(*) FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1", None, None, SqlError),
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
        # NOT this shape: siblings, not ancestor-and-descendant. The nesting guard must
        # not claim them — they are governed by the GROUP BY rule instead.
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", None, None, SqlError),
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
        # The same partition key on BOTH sides, and the partition column projected
        # through the boundary — the single-table fuzzer's repro for this, in both
        # spellings, deleted from single_table_known_gaps once it stopped reproducing.
        ("SELECT gravity, COUNT(w) OVER (PARTITION BY gravity) AS w2 FROM (SELECT gravity, MIN(mass) OVER (PARTITION BY gravity) AS w FROM $planets) AS s", 9, 2, None),
        ("WITH c AS (SELECT gravity, MIN(mass) OVER (PARTITION BY gravity) AS w FROM $planets) SELECT gravity, COUNT(w) OVER (PARTITION BY gravity) AS w2 FROM c", 9, 2, None),
        # More than one spec in the inner scope — a chain of length > 1 BELOW another chain.
        ("SELECT SUM(a + b) OVER () FROM (SELECT COUNT(*) OVER () AS a, COUNT(*) OVER (PARTITION BY number_of_moons) AS b FROM $planets) AS t", 9, 1, None),

        # WINDOW IN ORDER BY — SUPPORTED. Legal SQL: windows are computed before the sort,
        # so ordering on one is well defined. It used to fall through to the aggregate walk
        # as a PLAIN aggregate with its OVER discarded, which made the statement look like
        # an aggregate query and produced "Column 'name' must appear in the `GROUP BY`
        # clause" — a rule that was never the problem, naming a column the caller could not
        # act on. Values and column sets are pinned in test_window_in_order_by.
        ("SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id) DESC", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY COUNT(*) OVER ()", 9, 1, None),
        ("SELECT name FROM $planets ORDER BY COUNT(*) OVER (PARTITION BY number_of_moons)", 9, 1, None),
        # The ordering column must NOT reach the caller — one column, not two.
        ("SELECT name FROM $planets ORDER BY COUNT(*) OVER (PARTITION BY number_of_moons), name", 9, 1, None),
        # …and a wildcard must not pick it up either.
        ("SELECT * FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)", 9, 20, None),
        # The SAME window selected AND ordered by is ONE column, computed once. This case
        # produced the most misleading message of the lot before the shared dedup: the
        # beside-aggregate refusal, naming the ALIAS as the window and the window as the
        # aggregate.
        ("SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)", 9, 2, None),
        ("SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $planets ORDER BY rn", 9, 2, None),
        # Positional ORDER BY onto a selected window.
        ("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $planets ORDER BY 1", 9, 1, None),
        # With LIMIT — the sort still drives which rows survive.
        ("SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id) LIMIT 3", 3, 1, None),
        # Still refused, and for reasons that are NOT this one. A malformed window is
        # malformed wherever it is written.
        ("SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER ()", None, None, UnsupportedSyntaxError),
        ("SELECT name FROM $planets ORDER BY RANK()", None, None, UnsupportedSyntaxError),
        ("SELECT name FROM $planets ORDER BY SUM(COUNT(*) OVER ()) OVER ()", None, None, UnsupportedSyntaxError),
        # A window in ORDER BY over a GROUPED result: eight groups, ordered by a ranking
        # window computed over those eight rows.
        ("SELECT COUNT(*) FROM $planets GROUP BY gravity ORDER BY ROW_NUMBER() OVER (ORDER BY gravity)", 8, 1, None),
        # Beside a plain aggregate: one grouped row, and the window over it.
        ("SELECT COUNT(*) FROM $planets ORDER BY COUNT(*) OVER ()", 1, 1, None),
        # DISTINCT makes the ordering value ambiguous once rows collapse — the existing
        # rule applies to a window exactly as it does to any other unselected sort key.
        ("SELECT DISTINCT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)", None, None, UnsupportedSyntaxError),
        # The remedy the beside-aggregate refusal advises for ORDER BY must run.
        ("SELECT c FROM (SELECT COUNT(*) AS c FROM $planets) AS s ORDER BY COUNT(*) OVER ()", 1, 1, None),

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
        # A nested window over a GROUPED result: eight groups, each its own partition, so
        # the count within each is 1.
        ("SELECT COUNT(*) OVER (PARTITION BY gravity) + 0 FROM $planets GROUP BY gravity", 8, 1, None),
        # The refusals reach a nested window too — they used to be internal errors there.
        # SUM(...) OVER (ORDER BY ...) is a framed (running) window now — see
        # test_window_over_group_by.py's cumulative-window coverage — so this is a
        # nested-window SHAPE check (still hoists correctly inside `+ 1`), not a refusal.
        ("SELECT SUM(mass) OVER (ORDER BY id) + 1 FROM $planets", 9, 1, None),
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


def test_union_leg_width_coercion():
    """
    VALUE-level regression: UNION legs whose types differ only in WIDTH must be cast to
    one common type, and legs that ALREADY agree must not be widened.

    `find_compatible_type` picked the right target all along; `_cast_leg_columns_to`
    then skipped the cast whenever the leg's CATEGORY already matched the target's.
    Every integer width shares LogicalCategory.INTEGER, so `INT8 ∪ INT64` computed
    INT64 and cast neither leg. Shape-only coverage is enough to catch the failure
    itself (it raised), but not to catch the two ways of "fixing" it wrongly: casting
    to a NARROWER type would silently truncate values, and widening legs that already
    agree costs 8x the memory for nothing. Both are asserted here.
    """
    session = opteryx.session()

    # Widen to the wider leg, preserving values from BOTH legs. 9999 does not fit in
    # the INT8 leg's type, so a narrowing coercion would corrupt it.
    values = []
    for morsel in session.execute_to_morsels(
        "SELECT id AS n FROM $planets UNION ALL SELECT CAST(9999 AS INT64) AS n"
    ):
        result_type = morsel.column("n").type
        values += morsel.column("n").to_pylist()
    assert sorted(values) == list(range(1, 10)) + [9999], (
        f"UNION of INT8 and INT64 legs lost or truncated values: {sorted(values)}"
    )
    assert "INT64" in str(result_type), f"expected the wider INT64, got {result_type}"

    # Legs that already agree must keep their own type — find_compatible_type widens
    # INT8/16/32 to INT64 when resolving a MIXED set, which must not fire here.
    for morsel in session.execute_to_morsels(
        "SELECT id AS n FROM $planets UNION ALL SELECT id AS n FROM $planets"
    ):
        assert "INT8" in str(morsel.column("n").type), (
            f"UNION of two identical INT8 legs was widened to {morsel.column('n').type}"
        )
        break


def test_chained_union_coerces_every_leg_and_declares_what_it_delivers():
    """
    VALUE-level regression: a CHAINED (3+ leg) UNION must coerce every leg, and a
    set-op must DECLARE the type it delivers.

    `A UNION B UNION C` is nested binary unions. `_cast_leg_columns_to` rewrote each
    leg's Project but left the union node's own output column bound to the FIRST
    leg's ORIGINAL, pre-cast type — so when the outer union reconciled C against
    `union(A, B)` it read that stale type, picked its target from it, and skipped the
    cast the legs actually needed. draken's concat then refused the mismatched legs
    ("all inputs must share one type").

    The stale type was never only a chaining problem: the two-leg form below RAN,
    while reporting INT8 for a column it delivered as FLOAT64 — a lie to anything
    reading the declared shape (an editor's `check`, or an enclosing query binding
    an expression over it). Both forms are asserted, shape and values, because
    getting the declared type right by widening the DATA to match the lie would
    satisfy a type-only assertion while corrupting every row.
    """
    session = opteryx.session()

    gravities = []
    for morsel in session.execute_to_morsels(
        "SELECT CAST(gravity AS FLOAT64) AS g FROM $planets"
    ):
        gravities += morsel.column("g").to_pylist()

    chained = (
        "SELECT id AS n FROM $planets "
        "UNION ALL SELECT CAST(id AS INT64) AS n FROM $planets "
        "UNION ALL SELECT CAST(gravity AS FLOAT64) AS n FROM $planets"
    )
    values = []
    result_type = None
    for morsel in session.execute_to_morsels(chained):
        result_type = morsel.column("n").type
        values += morsel.column("n").to_pylist()

    expected = sorted([float(i) for i in range(1, 10)] * 2 + gravities)
    assert sorted(values) == expected, (
        f"chained UNION lost or corrupted values: {sorted(values)} != {expected}"
    )
    assert "FLOAT64" in str(result_type), (
        f"chained UNION of INT8, INT64 and FLOAT64 legs delivered {result_type}"
    )

    # What the binder DECLARES has to be what the query delivers — the same lie is
    # reachable with two legs, where it never raised.
    two_leg = (
        "SELECT id AS n FROM $planets "
        "UNION ALL SELECT CAST(gravity AS FLOAT64) AS n FROM $planets"
    )
    for statement in (chained, two_leg):
        checked = session.check(statement)
        assert checked.ok, f"{statement} failed to bind: {checked.error}"
        assert [(column.name, column.type) for column in checked.columns] == [("n", "FLOAT64")], (
            "UNION declared a type it does not deliver: "
            f"{[(column.name, column.type) for column in checked.columns]}"
        )


def test_literals_are_not_interned_across_types():
    """
    VALUE-level regression: two literals that render the same and hold the same value
    must not share one ConstantColumn when their TYPES differ.

    `inner_binder` interns literals by rendered name, guarded on value being
    byte-identical and on the alias agreeing — neither of which separates two NULLs.
    `COUNT(*) FILTER (WHERE p)` lowers to `COUNT(IIF(p, 1, NULL))` with a deliberately
    UNTYPED else-NULL, so when `p` was a BOOL-typed null the else adopted its column
    and came back BOOL: "IIF: literal 1 is INT64 but literal None is BOOL", refusing a
    query whose branches are really INT64 and untyped NULL.

    Asserted as answers, not just as "does not raise": FILTER admits a row only when
    the predicate is TRUE, so an all-UNKNOWN predicate admits none — COUNT is 0 and
    SUM over no rows is NULL. The TRUE/column-predicate forms are pinned alongside so
    a fix that broke interning generally would show up here.
    """
    session = opteryx.session()

    def one_value(statement):
        produced = []
        for morsel in session.execute_to_morsels(statement):
            produced += morsel.column("x").to_pylist()
        return produced[0]

    assert one_value("SELECT COUNT(*) FILTER (WHERE CAST(NULL AS BOOLEAN)) AS x FROM $planets") == 0
    assert one_value("SELECT SUM(id) FILTER (WHERE CAST(NULL AS BOOLEAN)) AS x FROM $planets") is None
    assert one_value("SELECT IIF(CAST(NULL AS BOOLEAN), 1, NULL) AS x FROM $planets") is None
    # Interning itself must still work — these never went through the new guard.
    assert one_value("SELECT COUNT(*) FILTER (WHERE TRUE) AS x FROM $planets") == 9
    assert one_value("SELECT COUNT(*) FILTER (WHERE id > 3) AS x FROM $planets") == 6

    # Two same-valued nulls of different declared types stay two columns.
    declared = [column.type for column in session.check(
        "SELECT CAST(NULL AS BOOLEAN) AS a, CAST(NULL AS INTEGER) AS b FROM $planets"
    ).columns]
    assert declared == ["BOOL", "INT64"], f"typed nulls collapsed onto one column: {declared}"


def test_case_when_condition_is_never_a_raw_type_error():
    """
    VALUE-level regression: a CASE condition that is not a live BOOLEAN vector must
    give an ANSWER when it has one, and a sentence when it does not — never a raw
    Cython `TypeError: Argument 'bv' has incorrect type`.

    `case_helpers.decide_one_branch` is declared `BoolVector bv` and reads the
    bit-packed layout through it. Every BOOL producer returns that wrapper except the
    constant materialiser, which returned a plain Vector — so `CASE WHEN CAST(NULL AS
    BOOLEAN)` and `CASE WHEN NULL` died in the argument conversion, as did `CASE WHEN
    1` and `CASE WHEN 'x'`, while the same conditions over a COLUMN were refused with
    a sentence naming the type.

    UNKNOWN never matches, so a NULL condition takes the ELSE branch — asserted as a
    value, because "does not raise" would also be satisfied by wrongly taking THEN.
    """
    session = opteryx.session()

    for statement, expected in (
        ("SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN 'a' ELSE 'b' END AS x FROM $planets", "b"),
        ("SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END AS x FROM $planets", "b"),
        # A NULL condition must not swallow a later WHEN that does match.
        ("SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN 'a' WHEN TRUE THEN 'b' ELSE 'c' END AS x FROM $planets", "b"),
        # ... and the live-row bookkeeping must survive it: id 1 fails `id > 3`, so it
        # falls past the NULL condition to ELSE.
        ("SELECT CASE WHEN id > 3 THEN 'a' WHEN CAST(NULL AS BOOLEAN) THEN 'b' ELSE 'c' END AS x FROM $planets WHERE id = 1", "c"),
        ("SELECT CASE WHEN id > 3 THEN 'a' WHEN CAST(NULL AS BOOLEAN) THEN 'b' ELSE 'c' END AS x FROM $planets WHERE id = 5", "a"),
    ):
        produced = []
        for morsel in session.execute_to_morsels(statement):
            produced += morsel.column("x").to_pylist()
        assert produced[0] == expected, f"{statement} produced {produced[0]!r}, expected {expected!r}"

    # A condition that is neither BOOLEAN nor NULL is refused, by name and by type.
    for statement, named_type in (
        ("SELECT CASE WHEN 1 THEN 'a' ELSE 'b' END AS x FROM $planets", "INT64"),
        ("SELECT CASE WHEN 'x' THEN 'a' ELSE 'b' END AS x FROM $planets", "VARCHAR"),
    ):
        with pytest.raises(Exception) as raised:  # noqa: PT011 - the message IS the assertion
            list(session.execute_to_morsels(statement))
        message = str(raised.value)
        assert "incorrect type" not in message, (
            f"a raw Cython argument error reached the caller: {message}"
        )
        assert "BOOLEAN" in message and named_type in message, (
            f"{statement} refused without naming the type: {message}"
        )


def test_cast_null_as_boolean_behaves_like_a_boolean():
    """
    VALUE-level regression: `CAST(NULL AS BOOLEAN)` must declare and carry BOOLEAN.

    The plan-time literal fold stamps the target type onto a folded NULL for VARCHAR,
    ARRAY and the INTEGER/FLOAT categories, and dropped every other target to an
    untyped NULL. BOOLEAN was not an exclusion with a reason — it was simply absent,
    and BOOLEAN is where a CONDITION comes from, so the paths that consume one
    type-check it rather than short-circuiting: as a sort key it was refused
    ("ORDER BY on *null* (type `NULL`)"), as an IIF condition refused, `MAX(...)`
    refused, and `CAST(NULL AS BOOLEAN) = TRUE` DECLARED NULL for a comparison that
    returns BOOLEAN.

    The measure of a typed null is that it behaves like its type, so each assertion
    pins the BOOLEAN-COLUMN behaviour: every one of these runs for `b_value`. Values
    are asserted, not just the absence of a raise — a null sort key must stay NULL,
    and an IIF on an UNKNOWN condition must take the ELSE branch.
    """
    session = opteryx.session()

    statement = "SELECT CAST(NULL AS BOOLEAN) AS x, id FROM $planets ORDER BY x DESC"
    assert [column.type for column in session.check(statement).columns] == ["BOOL", "INT8"], (
        f"CAST(NULL AS BOOLEAN) declared {[c.type for c in session.check(statement).columns]}"
    )
    values = []
    sort_key_type = None
    for morsel in session.execute_to_morsels(statement):
        sort_key_type = morsel.column("x").type
        values += morsel.column("x").to_pylist()
    assert values == [None] * 9, f"a NULL sort key stopped being NULL: {values}"
    assert "BOOL" in str(sort_key_type), f"sort key materialised as {sort_key_type}"

    # An UNKNOWN condition takes the ELSE branch, and MAX over an all-null BOOLEAN
    # column is NULL — both were refusals while the null was untyped.
    for expression, expected in (
        ("IIF(CAST(NULL AS BOOLEAN), 1, 2)", 2),
        ("MAX(CAST(NULL AS BOOLEAN))", None),
        ("(CAST(NULL AS BOOLEAN) = TRUE)", None),
        ("COALESCE(CAST(NULL AS BOOLEAN), TRUE)", True),
    ):
        produced = []
        for morsel in session.execute_to_morsels(f"SELECT {expression} AS x FROM $planets"):
            produced += morsel.column("x").to_pylist()
        assert produced[0] == expected, f"{expression} produced {produced[0]}, expected {expected}"


def test_filtered_aggregate_over_a_constant_folded_null_condition():
    """
    VALUE-level regression: a BOOL-typed NULL literal must materialise as a BOOL
    constant, not as an untyped DRAKEN_NULL.

    `AGG(x) FILTER (WHERE p)` lowers to `AGG(IIF(p, x, NULL))`, and `draken_iif`
    type-checks its CONDITION rather than short-circuiting on it — the one argument
    position where an untyped null is not interchangeable with a typed one. A `p`
    the optimizer can decide at plan time, whose surviving branch is NULL, folds to
    exactly that: `CASE WHEN ('beta' <= '0') THEN TRUE ELSE NULL END` is a BOOL null
    literal, and the query died with `draken_iif: condition must be BOOLEAN`.

    Found by the single-table fuzzer, so the assertion is on the ANSWER as well as
    on not raising: FILTER admits a row only when `p` is TRUE, and an all-UNKNOWN
    condition admits none — the same 0 the FALSE-condition form returns, which is
    asserted alongside it so a fix that made the query run by admitting rows would
    still fail here.
    """
    session = opteryx.session()

    for statement, expected in (
        ("SELECT COUNT(*) FILTER (WHERE (CASE WHEN ('beta' <= '0') THEN TRUE ELSE NULL END)) AS c FROM $planets", 0),
        ("SELECT COUNT(*) FILTER (WHERE (CASE WHEN ('beta' <= '0') THEN TRUE ELSE FALSE END)) AS c FROM $planets", 0),
        ("SELECT COUNT(*) FILTER (WHERE (CASE WHEN ('0' <= 'beta') THEN TRUE ELSE NULL END)) AS c FROM $planets", 9),
        # The same shape with a COLUMN condition, which never folded and so never
        # broke — here to keep the two paths' answers pinned together.
        ("SELECT COUNT(*) FILTER (WHERE (CASE WHEN (name <= 'M') THEN TRUE ELSE NULL END)) AS c FROM $planets", 2),
    ):
        counted = []
        for morsel in session.execute_to_morsels(statement):
            counted += morsel.column("c").to_pylist()
        assert counted == [expected], f"{statement} counted {counted}, expected [{expected}]"


def test_union_output_columns_that_share_one_source_column():
    """
    VALUE-level regression: two UNION output positions fed by ONE source column must
    stay two columns when the legs make them diverge.

    `SELECT id AS a, id AS b` is one column named twice, and the binder deliberately
    folds both onto a single SchemaColumn — which is right until a set operation gives
    the two positions different types. Identity is the engine's column key, so the
    union's `column_ids` named both legs' columns the same and the second resolved to
    the first: `b` came back holding `a`'s data, INT64 where the query says FLOAT64.

    Shape-only coverage cannot see this — the result has the right number of rows and
    columns, and the wrong VALUES in one of them. Asserted over both legs' rows,
    because the first leg's values alone would look right for `a` either way.
    """
    session = opteryx.session()

    statement = (
        "SELECT id AS a, id AS b FROM $planets "
        "UNION ALL "
        "SELECT CAST(id AS INT64) AS a, CAST(gravity AS FLOAT64) AS b FROM $planets"
    )
    a_values = []
    b_values = []
    b_type = None
    for morsel in session.execute_to_morsels(statement):
        b_type = morsel.column("b").type
        a_values += morsel.column("a").to_pylist()
        b_values += morsel.column("b").to_pylist()

    gravities = []
    for morsel in session.execute_to_morsels(
        "SELECT CAST(gravity AS FLOAT64) AS g FROM $planets"
    ):
        gravities += morsel.column("g").to_pylist()

    assert sorted(a_values) == sorted(list(range(1, 10)) * 2), (
        f"UNION column `a` lost values: {sorted(a_values)}"
    )
    # `b` is the one that used to be handed `a`'s data: one leg's ids as FLOAT64, the
    # other leg's gravities.
    assert sorted(b_values) == sorted([float(i) for i in range(1, 10)] + gravities), (
        f"UNION column `b` delivered the wrong column: {sorted(b_values)}"
    )
    assert "FLOAT64" in str(b_type), f"UNION column `b` delivered {b_type}"

    # The benign form must NOT be disturbed: both positions really are the same column,
    # and folding them onto one identity is what makes that work.
    values = []
    for morsel in session.execute_to_morsels(
        "SELECT id AS a, id AS b FROM $planets UNION ALL SELECT id AS a, id AS b FROM $planets"
    ):
        assert morsel.column("a").to_pylist() == morsel.column("b").to_pylist(), (
            "UNION of one column projected twice returned two different columns"
        )
        values += morsel.column("a").to_pylist()
    assert sorted(values) == sorted(list(range(1, 10)) * 2)


def test_union_leg_aliases_its_own_derived_table_case_insensitively():
    """
    VALUE-level regression: a UNION leg that aliases its OWN derived table and
    references that alias — in a DIFFERENT case — from its own SELECT list and WHERE
    clause must resolve and return the right values.

    `FROM (SELECT ...) AS Alias ... WHERE alias.col` is ordinary unquoted-identifier
    case folding, the same folding column names already get
    (`RelationSchema.find_column(..., case_insensitive=True)`). Relation ALIASES were
    exact-string matched everywhere instead of folded — reproducible with no UNION at
    all (`SELECT p.name FROM $planets P` raised `UnexpectedDatasetReferenceError`) —
    and a UNION leg additionally runs through `rename_relations`
    (relation_resolver/__init__.py), which remaps every reference in a spliced leg to
    a freshly minted synthetic alias; that remap was ALSO keyed by exact string, so a
    leg's own declaration (`AS CATALOG`) and its own reference (`catalog.x`) landed on
    two different synthetic names and the leg's WHERE clause failed to resolve its own
    FROM-clause alias — TPC-DS Q49's catalog leg, verbatim.
    """
    session = opteryx.session()

    statement = """
        SELECT chan, x FROM (
            SELECT 'a' AS chan, t.x
            FROM (SELECT id AS x FROM $planets WHERE id <= 3) t
            WHERE t.x > 0
            UNION
            SELECT 'b' AS chan, catalog.x
            FROM (SELECT id AS x FROM $planets WHERE id BETWEEN 4 AND 6) CATALOG
            WHERE catalog.x > 0
        ) sq1
        ORDER BY chan, x
    """

    chans = []
    xs = []
    for morsel in session.execute_to_morsels(statement):
        chans += morsel.column("chan").to_pylist()
        xs += morsel.column("x").to_pylist()

    assert chans == ["a", "a", "a", "b", "b", "b"], (
        f"UNION leg's own-case-mismatched alias reference lost rows: {chans}"
    )
    assert xs == [1, 2, 3, 4, 5, 6], (
        f"UNION leg's own-case-mismatched alias reference returned wrong values: {xs}"
    )


def test_union_of_aggregates_with_a_computed_group_key():
    """
    VALUE-level regression: each leg of a UNION over aggregates with a COMPUTED
    GROUP BY key must emit that key's VALUES, not just hash on it.

    A lone aggregate has its Project folded away by redundant_operators, so nothing
    above it ever asks for the derived key as a column; a UNION leg KEEPS its Project,
    which is the only shape where the question is asked. projection_pushdown's
    collect_columns recorded a computed column's INPUT identifiers but not the computed
    column's own identity, so _group_key_emit ruled the key dead and the groupby sink
    dropped its per-group value store.

    The first leg escaped by accident — the union's output identities ARE the first
    leg's — so this must assert over BOTH legs' values, which is why the shape entries
    in the battery above are not sufficient on their own.
    """
    statement = (
        "SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1 "
        "UNION ALL "
        "SELECT TRUNC(id, 1) AS d, COUNT(*) AS n FROM $planets GROUP BY 1"
    )
    session = opteryx.session()

    counts = {}
    for morsel in session.execute_to_morsels(statement):
        for key, val in zip(morsel.column("d").to_pylist(), morsel.column("n").to_pylist()):
            counts[key] = counts.get(key, 0) + val

    # $planets has 9 rows with distinct ids 1..9; TRUNC(id, 1) is the identity here, so
    # every group is a single row, seen once per leg.
    assert counts == {float(i): 2 for i in range(1, 10)}, (
        f"UNION of aggregates over a computed GROUP BY key returned {counts} "
        f"(expected each of ids 1-9 counted twice)"
    )


def test_union_leg_computed_alias_matching_source_column_name():
    """
    VALUE-level regression: a UNION leg's OWN computed expression, aliased to the SAME
    name as a real column of the relation every leg reads FROM, must not make the
    union's declared output ambiguous — TPC-DS Q36's `results_rollup` CTE, verbatim
    shape: `results_rollup AS (SELECT gross_margin FROM results UNION SELECT
    (sum(...)/sum(...)) AS gross_margin FROM results ...)`, then `SELECT gross_margin
    FROM results_rollup` outside — one relation in the outer FROM clause, so this must
    not be ambiguous.

    Leg 1 passes `stats.ratio` straight through; leg 2 computes a NEW expression under
    the same alias. Once leg 2's own Project finishes, its alias lives in
    `context.schemas["$project"]` (`$derived` renamed at Project-exit — see
    project.py), and that scratch schema was not exempted the way `$derived` is when
    `visit_union` re-binds the union's own declared output columns (borrowed from leg
    1's shape — see `logical_planner.py`'s `plan_query`). So `stats`' real `ratio`
    column and leg 2's `$project` copy of `ratio` both showed up in one
    `context.schemas` dict, and `locate_identifier_in_loaded_schemas` raised
    AmbiguousIdentifierError even though the outer query's FROM clause names only
    `stats_rollup`.
    """
    session = opteryx.session()

    statement = """
        WITH stats AS (
            SELECT id AS grp, SUM(mass) AS mass_sum, SUM(mass) / SUM(gravity) AS ratio
            FROM $planets
            GROUP BY id
        ),
        stats_rollup AS (
            SELECT ratio, grp FROM stats
            UNION ALL
            SELECT SUM(mass_sum) / 1.0 AS ratio, NULL AS grp FROM stats
        )
        SELECT ratio, grp FROM stats_rollup
    """

    ratios = []
    grps = []
    for morsel in session.execute_to_morsels(statement):
        ratios += morsel.column("ratio").to_pylist()
        grps += morsel.column("grp").to_pylist()

    assert len(ratios) == 10, f"expected 9 per-planet rows + 1 rollup row, got {len(ratios)}"
    assert sorted(g for g in grps if g is not None) == list(range(1, 10)), (
        f"UNION leg 1's passthrough of `stats.grp` lost values: {grps}"
    )
    assert grps.count(None) == 1, f"UNION leg 2's `NULL AS grp` did not survive: {grps}"

    mass = []
    gravity = []
    for morsel in session.execute_to_morsels("SELECT id, mass, gravity FROM $planets ORDER BY id"):
        mass += morsel.column("mass").to_pylist()
        gravity += morsel.column("gravity").to_pylist()

    expected_per_planet = sorted(float(m) / float(g) for m, g in zip(mass, gravity))
    expected_rollup = sum(float(m) for m in mass) / 1.0

    rollup_idx = grps.index(None)
    per_planet_ratios = ratios[:rollup_idx] + ratios[rollup_idx + 1 :]

    assert sorted(per_planet_ratios) == pytest.approx(expected_per_planet), (
        f"UNION leg 1's passthrough of `stats.ratio` returned wrong values: {per_planet_ratios}"
    )
    assert ratios[rollup_idx] == pytest.approx(expected_rollup), (
        f"UNION leg 2's own computed `ratio` returned the wrong value: {ratios[rollup_idx]}"
    )

    # A name that really IS ambiguous — two relations providing it — must still be
    # refused. The fix narrows what counts as a relation ($project scratch does not
    # count); it does not stop counting relations that genuinely both provide the name.
    with pytest.raises(AmbiguousIdentifierError):
        for _ in session.execute_to_morsels(
            "SELECT name FROM $planets INNER JOIN testdata.satellites ON planetId = $planets.id"
        ):
            pass


def test_chained_union_of_having_filtered_aggregates():
    """
    VALUE-level regression: a 3+-leg `UNION ALL` whose legs each end
    `GROUP BY ... HAVING ...` must resolve every leg's OWN projection, not the
    HAVING predicate's operand columns.

    `A UNION ALL B UNION ALL C` parses as nested binary unions, so the OUTER union's
    left child is the INNER union `A UNION ALL B` — and when a leg's top node is a
    HAVING Filter sitting directly below the set-op (no Project in between), the
    binder's `_branch_project_columns` (opteryx/planner/binder/set_ops.py) walked
    UP from that Filter and accepted the FIRST node with a non-empty `.columns` list
    as "the leg's projection". A Filter's `.columns` are the identifiers its
    predicate REFERENCES (here, `mass`, from `HAVING sum(mass) > 0`), not an output
    list — so a 6-column leg reported as 1 column, and the outer union's arity check
    against a correctly-resolved sibling raised "UNION: column count mismatch".
    Found via TPC-DS Q14, whose `y` derived table has exactly this shape (three
    `GROUP BY ... HAVING ...` legs chained with `UNION ALL`); this reproduces the
    defect without TPC-DS data or ROLLUP, which turned out to be unrelated to the
    root cause. Guards the same restriction to `_PROJECTING_STEPS` that its sibling
    `_setop_leg_columns` already applied.
    """
    statement = (
        "SELECT 'a' AS channel, name, id, gravity, SUM(mass) AS total_mass, COUNT(*) AS n "
        "FROM $planets GROUP BY name, id, gravity HAVING SUM(mass) > 0 "
        "UNION ALL "
        "SELECT 'b' AS channel, name, id, gravity, SUM(mass) AS total_mass, COUNT(*) AS n "
        "FROM $planets GROUP BY name, id, gravity HAVING SUM(mass) > 0 "
        "UNION ALL "
        "SELECT 'c' AS channel, name, id, gravity, SUM(mass) AS total_mass, COUNT(*) AS n "
        "FROM $planets GROUP BY name, id, gravity HAVING SUM(mass) > 0"
    )
    session = opteryx.session()

    by_channel: dict = {}
    for morsel in session.execute_to_morsels(statement):
        for channel, name, total_mass, n in zip(
            morsel.column("channel").to_pylist(),
            morsel.column("name").to_pylist(),
            morsel.column("total_mass").to_pylist(),
            morsel.column("n").to_pylist(),
        ):
            by_channel.setdefault(channel, {})[name] = (total_mass, n)

    # Every leg is the SAME query over $planets (9 rows, distinct name/id/gravity, so
    # each row is its own group and HAVING keeps all of them): three channels, each
    # with exactly the 9 planets, each planet's own mass as its group total.
    assert set(by_channel) == {"a", "b", "c"}, (
        f"expected channels a/b/c, got {sorted(by_channel)} "
        "(a wrong leg-column count either raises at bind time or silently drops/misaligns a leg)"
    )
    expected_masses = {}
    for morsel in session.execute_to_morsels("SELECT name, mass FROM $planets"):
        for name, mass in zip(morsel.column("name").to_pylist(), morsel.column("mass").to_pylist()):
            expected_masses[name] = mass

    for channel, rows in by_channel.items():
        assert set(rows) == set(expected_masses), (
            f"channel {channel!r} has planets {sorted(rows)}, expected {sorted(expected_masses)}"
        )
        for name, (total_mass, n) in rows.items():
            assert n == 1, f"channel {channel!r} planet {name!r}: expected count 1, got {n}"
            assert total_mass == expected_masses[name], (
                f"channel {channel!r} planet {name!r}: total_mass {total_mass} != mass {expected_masses[name]}"
            )


def test_chained_union_of_having_scalar_subquery_filtered_aggregates():
    """
    VALUE-level regression: a `UNION ALL` whose legs each end `GROUP BY ...
    HAVING agg(...) > (SELECT scalar subquery)` must resolve every leg's OWN
    projection, not just the columns the HAVING predicate itself reads.

    Sibling of `test_chained_union_of_having_filtered_aggregates` above, but for
    the deeper defect that survived the fix that test guards: a scalar subquery
    in HAVING is decorrelated (opteryx/planner/optimizer/strategies/
    decorrelate_subquery.py `_decorrelate`) into a CROSS JOIN against the
    subquery's one-row result, with the join's value column feeding the Filter.
    That join's physical output is build-side first
    (`_Compiler._compile_join`: CROSS builds the right/inner leg), so the scalar
    value lands BEFORE the leg's real columns rather than after. UNION aligns
    legs by raw POSITION (compiler.py's UnionNode: `add_select(lp,
    range(len(ids)), ids)`, never by identity), so this silently shifts every
    column in a leg wide enough to survive (values land under the wrong output
    name) and, for a leg projection pushdown cannot demand-match by identity —
    any leg but the union's own left/identity-donor leg — drops columns
    outright ("a UNION leg narrower than the union schema").

    Found via TPC-DS Q14's `y` CTE (three such legs `UNION ALL`ed together,
    each with a correlated-free `HAVING SUM(...) > (SELECT AVG(...) ...)`), but
    reproduces without TPC-DS data or ROLLUP — both turned out unrelated to the
    root cause, which is decorrelation leaving its rewrite non-transparent, not
    anything union- or rollup-specific. Guards the narrow-back Project
    `_decorrelate` now inserts immediately above the Filter it builds.
    """
    statement = (
        "SELECT 'a' AS channel, name, id, gravity, SUM(mass) AS total_mass, COUNT(*) AS n "
        "FROM $planets GROUP BY name, id, gravity "
        "HAVING SUM(mass) > (SELECT AVG(mass) FROM $planets) "
        "UNION ALL "
        "SELECT 'b' AS channel, name, id, gravity, SUM(mass) AS total_mass, COUNT(*) AS n "
        "FROM $planets GROUP BY name, id, gravity "
        "HAVING SUM(mass) > (SELECT AVG(mass) FROM $planets)"
    )
    session = opteryx.session()

    by_channel: dict = {}
    for morsel in session.execute_to_morsels(statement):
        for channel, name, total_mass, n in zip(
            morsel.column("channel").to_pylist(),
            morsel.column("name").to_pylist(),
            morsel.column("total_mass").to_pylist(),
            morsel.column("n").to_pylist(),
        ):
            # A shifted/scrambled leg would put a float (the subquery's AVG) or
            # a name into `channel`, or a planet name into `total_mass` — this
            # would fail immediately rather than silently pass a corrupted row.
            assert channel in ("a", "b"), f"channel column holds {channel!r}, not 'a'/'b'"
            by_channel.setdefault(channel, {})[name] = (total_mass, n)

    assert set(by_channel) == {"a", "b"}, (
        f"expected channels a/b, got {sorted(by_channel)} "
        "(a wrong leg-column count either raises at bind time or silently drops/misaligns a leg)"
    )

    # Independently compute which planets have above-average mass, and by how much.
    masses = {}
    for morsel in session.execute_to_morsels("SELECT name, mass FROM $planets"):
        for name, mass in zip(morsel.column("name").to_pylist(), morsel.column("mass").to_pylist()):
            masses[name] = mass
    average_mass = sum(masses.values()) / len(masses)
    expected_masses = {name: mass for name, mass in masses.items() if mass > average_mass}
    assert 0 < len(expected_masses) < len(masses), (
        "test fixture assumption broken: expected a proper (non-empty, non-total) subset "
        f"of planets above the average mass, got {len(expected_masses)} of {len(masses)}"
    )

    for channel, rows in by_channel.items():
        assert set(rows) == set(expected_masses), (
            f"channel {channel!r} has planets {sorted(rows)}, expected {sorted(expected_masses)}"
        )
        for name, (total_mass, n) in rows.items():
            assert n == 1, f"channel {channel!r} planet {name!r}: expected count 1, got {n}"
            assert total_mass == masses[name], (
                f"channel {channel!r} planet {name!r}: total_mass {total_mass} != mass {masses[name]}"
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


def test_window_beside_an_aggregate_runs_over_the_grouped_rows():
    """
    VALUE-level regression: a window written beside a plain aggregate is computed over the
    GROUPED rows, and answers what the standard says it should.

    This arrangement used to be refused outright, and for a real reason: the Window step
    was planned UNDER the aggregate step, so the window would have been computed over the
    rows the aggregate collapses and could never see the aggregated result. A bare
    aggregate is an implicit single group and hit the same wall. The grouped-window
    lowering plans the aggregate first and the windows over its output, so the answer is
    now the one the refusal used to tell the caller to get by hand.

    Values are DuckDB's, on the same nine rows.

    The refusal that DID govern the raw-column half of these statements is still here, and
    is now the only one: a column read beside an aggregate must be grouped by or
    aggregated. `SUM(mass) OVER () + SUM(mass)` reads `mass` at a level where only the
    aggregates exist, and DuckDB refuses it in the same terms.
    """
    session = opteryx.session()

    def _rows(statement):
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            names = list(morsel.column_names)
            out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
        return out

    def _message(statement):
        with pytest.raises(SqlError) as raised:
            for _ in session.execute_to_morsels(statement):
                pass
        return str(raised.value)

    # One group, one row, and the window counts THAT row — not the nine below it.
    assert _rows("SELECT COUNT(*), COUNT(*) OVER () FROM $planets") == [(9, 1)]
    # The aggregate reaches `_aggregates` from ORDER BY, not the SELECT list.
    assert _rows("SELECT COUNT(*) OVER () FROM $planets ORDER BY COUNT(*)") == [(1,)]
    assert _rows("SELECT COUNT(*) FROM $planets ORDER BY COUNT(*) OVER ()") == [(9,)]
    # The ranking path takes the same route: rank 1 of one grouped row.
    assert _rows("SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY COUNT(*)) FROM $planets") == [
        (9, 1)
    ]
    # QUALIFY filters the grouped rows on the window's value. The one row has
    # COUNT(*) OVER () = 1, so `> 1` keeps nothing and `>= 1` keeps it.
    assert _rows("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER () > 1") == []
    assert _rows("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER () >= 1") == [(9,)]
    # An aggregate nested in the window's argument is the group-level aggregate, and the
    # window's own call is the aggregate over the group results. Over one group they are
    # the same number, and the arithmetic around them still runs.
    assert _rows("SELECT SUM(id), SUM(SUM(id)) OVER () FROM $planets") == [(45, 45)]
    assert _rows("SELECT SUM(SUM(id)) OVER () + SUM(id) FROM $planets") == [(90,)]

    # Still refused, by the rule that always governed it. `mass`/`id`/`gravity` are read
    # RAW at a level where only the aggregates exist.
    for statement, column in (
        ("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets", "mass"),
        ("SELECT MAX(id) OVER (PARTITION BY gravity), COUNT(*) FROM $planets", "id"),
        ("SELECT COUNT(*), ROW_NUMBER() OVER (ORDER BY id) FROM $planets", "id"),
        ("SELECT COUNT(*) FROM $planets QUALIFY COUNT(*) OVER (PARTITION BY gravity) > 1", "gravity"),
        ("SELECT COUNT(*) FROM $planets QUALIFY ROW_NUMBER() OVER (ORDER BY id) = 1", "id"),
        ("SELECT id, SUM(gravity) OVER (PARTITION BY id) FROM $planets GROUP BY id", "gravity"),
    ):
        message = _message(statement)
        assert "$win_" not in message, f"minted alias leaked: {statement} -> {message}"
        assert f"Column '{column}' must appear in the `GROUP BY` clause" in message, (
            f"{statement} -> {message}"
        )

    # The same error still fires for a plain un-grouped column, with no window in sight —
    # the lowering reuses the rule, it does not replace it.
    assert "Column 'name' must appear in the `GROUP BY` clause" in _message(
        "SELECT name, COUNT(*) FROM $planets"
    )


def test_window_over_group_by_result():
    """
    VALUE-level regression: a window computed over the GROUPED rows answers the numbers
    the standard says it should, not merely "does not raise".

    This is the idiom ten TPC-DS queries are written in and Opteryx refused wholesale
    ("Window functions cannot be combined with GROUP BY"): GROUP BY collapses the rows,
    the aggregates are computed per group, and the window then runs over those group
    results. An aggregate NESTED inside the window's argument is the crux, not an oddity —
    in `SUM(SUM(x)) OVER (PARTITION BY k)` the inner SUM is the GROUP BY aggregate and the
    outer one is the window over the group results.

    Every expected value below was taken from DuckDB on the same nine rows.

    Rows are sorted before comparison: the grouped result reaches the windows through a
    join, which does not promise an order, and the statements that pin an order do so with
    an explicit ORDER BY.
    """
    session = opteryx.session()

    def _rows(statement):
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            names = list(morsel.column_names)
            out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
        return out

    def _rounded(statement, places=4):
        return sorted(
            tuple(round(value, places) if isinstance(value, float) else value for value in row)
            for row in _rows(statement)
        )

    # FLAVOUR (a) — ratio to the partition total. Three buckets of three planets; each
    # planet's mass as a percentage of its bucket's total. This is TPC-DS Q12/Q20/Q98.
    assert _rounded(
        "SELECT id % 3 AS bucket, name, SUM(mass) AS m, "
        "SUM(SUM(mass)) OVER (PARTITION BY id % 3) AS bucket_total, "
        "SUM(mass) * 100.0 / SUM(SUM(mass)) OVER (PARTITION BY id % 3) AS pct "
        "FROM $planets GROUP BY id % 3, name"
    ) == [
        (0, "Earth", 5.97, 573.9846, 1.0401),
        (0, "Pluto", 0.0146, 573.9846, 0.0025),
        (0, "Saturn", 568.0, 573.9846, 98.9574),
        (1, "Mars", 0.642, 87.7720, 0.7314),
        (1, "Mercury", 0.33, 87.7720, 0.376),
        (1, "Uranus", 86.8, 87.7720, 98.8926),
        (2, "Jupiter", 1898.0, 2004.87, 94.6695),
        (2, "Neptune", 102.0, 2004.87, 5.0876),
        (2, "Venus", 4.87, 2004.87, 0.2429),
    ]

    # AVG over the group results — TPC-DS Q47/Q53/Q57/Q63/Q89's spelling. One partition,
    # so every row carries the mean of the three bucket totals.
    assert _rounded(
        "SELECT id % 3 AS bucket, SUM(mass) AS m, AVG(SUM(mass)) OVER () AS avg_group "
        "FROM $planets GROUP BY id % 3"
    ) == [(0, 573.9846, 888.8755), (1, 87.772, 888.8755), (2, 2004.87, 888.8755)]

    # A RANKING window over the grouped rows — the other half of Q47/Q57. The ORDER BY
    # is over the AGGREGATE, which only exists after the grouping.
    assert _rows(
        "SELECT number_of_moons, COUNT(*) AS c, "
        "RANK() OVER (ORDER BY COUNT(*) DESC, number_of_moons) AS r "
        "FROM $planets GROUP BY number_of_moons ORDER BY r"
    ) == [(0, 2, 1), (1, 1, 2), (2, 1, 3), (5, 1, 4), (14, 1, 5), (27, 1, 6), (79, 1, 7), (82, 1, 8)]

    # HAVING is evaluated BEFORE the window functions, so the window sees only the groups
    # that survived it. Planned above the windows instead, this answered 45 — the total
    # over ALL nine groups — where DuckDB and the standard say 35.
    assert _rows(
        "SELECT number_of_moons, SUM(id) AS s, SUM(SUM(id)) OVER () AS t "
        "FROM $planets GROUP BY number_of_moons HAVING SUM(id) > 4 ORDER BY number_of_moons"
    ) == [(5, 9, 35), (14, 8, 35), (27, 7, 35), (79, 5, 35), (82, 6, 35)]

    # FLAVOUR (b) — a CUMULATIVE window over the grouped rows (TPC-DS Q51): a window
    # FRAME (native_window_frame.hpp's FramedWindowSink), combined with the GROUP BY
    # boundary above. PARTITION BY and ORDER BY are the same key here (one row per
    # partition, same as `test_two_partition_specs_over_one_grouping`'s shape), so the
    # "running total" degenerates to that partition's own SUM(id) — still a genuine
    # exercise of the framed-window-over-grouped-rows plan shape, not merely PARTITION
    # BY passthrough.
    assert _rows(
        "SELECT number_of_moons, SUM(SUM(id)) OVER ("
        "PARTITION BY number_of_moons ORDER BY number_of_moons "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c "
        "FROM $planets GROUP BY number_of_moons ORDER BY number_of_moons"
    ) == [(0, 3), (1, 3), (2, 4), (5, 9), (14, 8), (27, 7), (79, 5), (82, 6)]


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

    # The nesting guard must NOT claim the sibling arrangement. `SUM(mass) OVER () +
    # SUM(mass)` is two calls side by side, not one inside the other; it is refused by the
    # GROUP BY rule (`mass` is read raw beside an aggregate) and must say so, with no
    # mention of nesting. See test_window_beside_an_aggregate_runs_over_the_grouped_rows.
    with pytest.raises(SqlError) as raised:
        for _ in session.execute_to_morsels("SELECT SUM(mass) OVER () + SUM(mass) FROM $planets"):
            pass
    beside = str(raised.value)
    assert "Column 'mass' must appear in the `GROUP BY` clause" in beside, beside
    assert "cannot appear inside" not in beside, f"nesting guard claimed a sibling: {beside}"


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


def test_window_in_order_by():
    """
    VALUE-level regression: ORDER BY a window function.

    Legal SQL — windows are computed before the sort — and it used to be refused with
    "Column 'name' must appear in the `GROUP BY` clause". The window fell through to the
    aggregate walk as a PLAIN aggregate with its OVER discarded, which made the statement
    look like an aggregate query, so the error named a rule that was never the problem and
    a column the caller had no way to act on.

    The shape battery pins the row and column COUNTS, which is what catches the ordering
    column leaking into the result. It cannot catch a wrong ORDER, so the actual sequence
    is asserted here against an independently ordered query.
    """
    session = opteryx.session()

    def _run(statement):
        columns, values = None, {}
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            columns = [name.decode() for name in morsel.column_names]
            for name in morsel.column_names:
                values.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
        return columns, values

    by_id = _run("SELECT name FROM $planets ORDER BY id")[1]["name"]

    # A ranking window over `id` must order exactly as `id` does — and must NOT return the
    # column it ordered by.
    columns, values = _run("SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)")
    assert columns == ["name"], f"the ordering column reached the caller: {columns}"
    assert values["name"] == by_id, values["name"]

    # DESC reverses it, so the sort key is genuinely being read rather than the rows
    # arriving in scan order and looking right by accident.
    columns, values = _run(
        "SELECT name FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id) DESC"
    )
    assert columns == ["name"], columns
    assert values["name"] == by_id[::-1], values["name"]

    # The SAME window selected AND ordered by is ONE column, computed once. Before the
    # shared dedup this was the worst message in the family — the beside-aggregate
    # refusal, naming the ALIAS as the window and the window as the aggregate.
    columns, values = _run(
        "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $planets "
        "ORDER BY ROW_NUMBER() OVER (ORDER BY id)"
    )
    assert columns == ["name", "rn"], columns
    assert values["name"] == by_id, values["name"]
    assert values["rn"] == list(range(1, len(by_id) + 1)), values["rn"]

    # An aggregate window as the sort key: rows come out grouped by partition size.
    moons = _run("SELECT number_of_moons AS n FROM $planets")[1]["n"]
    per_partition = {m: moons.count(m) for m in moons}
    columns, values = _run(
        "SELECT name FROM $planets ORDER BY COUNT(*) OVER (PARTITION BY number_of_moons)"
    )
    assert columns == ["name"], columns
    assert sorted(values["name"]) == sorted(by_id), values["name"]
    sizes = [per_partition[moons[by_id.index(n)]] for n in values["name"]]
    assert sizes == sorted(sizes), f"not ordered by partition size: {sizes}"

    # A wildcard must not pick the hidden ordering column up — it is expanded from the
    # relations in scope at bind time, and the Window node's output is one of them.
    columns, _ = _run("SELECT * FROM $planets ORDER BY ROW_NUMBER() OVER (ORDER BY id)")
    planets = _run("SELECT * FROM $planets")[0]
    assert columns == planets, f"wildcard leaked the ordering column: {columns}"


def test_positional_order_by_over_select_star():
    """
    VALUE-level regression: positional ORDER BY (`ORDER BY 2`) over a bare `SELECT *`.

    `_projection` is a single WILDCARD placeholder at plan time -- `SELECT *` has no
    fixed column list until the source schema is bound -- so validating a position
    against `len(_projection)` there rejects every position but 1. That miscount was a
    genuine regression from making windows work alongside GROUP BY: resolving ORDER BY
    early enough to collect a window/GROUP BY aggregate from it (see the comment on
    `_order_by` in `inner_query_planner`) moved the positional check ahead of the point
    where a bare wildcard used to be left alone. `SELECT * FROM v ... ORDER BY 1, 2, ...,
    9` is exactly the shape TPC-DS Q47/Q57/Q89 write, and all three raised
    "ORDER BY position 2 is out of range - SELECT has 1 column(s)" even though the real
    output is far wider than one column.

    The fix defers a wildcard's positions to bind time (`binder/order.py`), where the
    schema is real, rather than trying to validate them against a placeholder.

    A window computed OVER a GROUP BY result is included because that is the exact
    plan shape (Aggregate -> Project -> Subquery -> Window) the regression came from --
    a plain, non-windowed `SELECT *` alone would not have exercised the code path that
    broke.

    NOTE: `ORDER BY <expr>, 1, 2` (an expression ahead of the positions, TPC-DS's own
    spelling) is deliberately NOT exercised here for the CTE case. That combination
    used to reach a separate defect: a `SELECT *` whose ORDER BY needs a computed
    pass-through key, over the window-over-GROUP-BY Subquery boundary, crashed the
    native Sort operator (`gather_rows`, draken/morsels/sort.hpp) with a column/name
    count mismatch. It reproduced with zero positional references, so it was unrelated
    to the miscount this test targets — see
    `test_computed_order_by_over_window_group_by_select_star` below for that one
    (fixed at its actual source, `ExprMultiProjectOperator`/`JsonExtractMultiOperator`
    in `src/cpp/engine/native_expression.hpp`, not in the Sort operator itself).
    """
    session = opteryx.session()

    def _run(statement):
        columns, values = None, {}
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            columns = [name.decode() for name in morsel.column_names]
            for name in morsel.column_names:
                values.setdefault(name.decode(), []).extend(morsel.column(name).to_pylist())
        return columns, values

    # Plain table, no CTE, no window -- this alone used to fail identically, since the
    # miscount has nothing to do with windows or GROUP BY.
    expected_columns, expected_values = _run(
        "SELECT name, mass FROM $planets ORDER BY name, mass"
    )
    columns, values = _run("SELECT * FROM $planets ORDER BY 2, 3")
    assert values["name"] == expected_values["name"], values["name"]
    assert values["mass"] == expected_values["mass"], values["mass"]

    # A position past the real (bound) column count must still be refused -- the fix
    # defers the check, it does not remove it.
    try:
        _run("SELECT * FROM $planets ORDER BY 999")
        raise AssertionError("out-of-range position over SELECT * did not raise")
    except UnsupportedSyntaxError as err:
        assert "out of range" in str(err), err

    # Explicit projections are untouched: positional ORDER BY still resolves at plan
    # time, and a genuinely out-of-range position still raises.
    explicit_columns, explicit_values = _run("SELECT name, mass FROM $planets ORDER BY 2 DESC")
    reference_columns, reference_values = _run("SELECT name, mass FROM $planets ORDER BY mass DESC")
    assert explicit_values["mass"] == reference_values["mass"], explicit_values["mass"]
    try:
        _run("SELECT id, name FROM $planets ORDER BY 5")
        raise AssertionError("out-of-range position over an explicit projection did not raise")
    except UnsupportedSyntaxError as err:
        assert "out of range" in str(err), err

    # The regression's actual shape: `SELECT *` over a CTE whose window runs OVER a
    # GROUP BY result (Aggregate -> Project -> Subquery -> Window), ordered positionally.
    window_group_by_cte = """
        WITH v1 AS (
            SELECT id, name, sum(gravity) sg, avg(sum(gravity)) OVER (PARTITION BY id) avg_g
            FROM $planets GROUP BY id, name
        )
        SELECT {select_list} FROM v1 ORDER BY {order_by}
    """
    explicit_columns, explicit_values = _run(
        window_group_by_cte.format(select_list="id, name, sg, avg_g", order_by="1, 2")
    )
    star_columns, star_values = _run(
        window_group_by_cte.format(select_list="*", order_by="1, 2")
    )
    assert star_columns == ["id", "name", "sg", "avg_g"], star_columns
    assert star_values["id"] == explicit_values["id"], star_values["id"]
    assert star_values["sg"] == explicit_values["sg"], star_values["sg"]
    assert star_values["avg_g"] == explicit_values["avg_g"], star_values["avg_g"]
    # Positions actually sort, they are not merely accepted: `2, 1` (name-major,
    # id-minor) must differ in ORDER from `1, 2` (id-major) for this dataset.
    reordered_columns, reordered_values = _run(
        window_group_by_cte.format(select_list="*", order_by="2, 1")
    )
    assert reordered_values["name"] == sorted(star_values["name"]), reordered_values["name"]
    assert reordered_values["id"] != star_values["id"], "position order was not honoured"


def test_computed_order_by_over_window_group_by_select_star():
    """
    VALUE-level regression: TPC-DS Q89's exact plan shape — `SELECT *` over a
    window-over-GROUP-BY subquery, `ORDER BY <computed expression> LIMIT n` — used to
    crash the native engine with

        RuntimeError: [1]: gather_rows: input name list is neither empty nor parallel
        to the input columns — cannot narrow it to the emit subset

    This was newly exposed (not pre-existing) once `test_positional_order_by_over_select_star`'s
    fix let Q89 progress past planning and into execution.

    Root cause was NOT the window-over-GROUP-BY lowering the ORDER BY fix touched. It is
    a latent bug in `ExprMultiProjectOperator`/`JsonExtractMultiOperator`
    (`src/cpp/engine/native_expression.hpp`), the operators that append one or more
    computed columns to a morsel: they unconditionally did `m->names = in->names;` and
    then `push_back`ed one name per new column. `CxxMorsel::names` is positional-only
    and usually EMPTY between native operators (see
    `[[morsel_names_are_not_maintained_inside_the_engine]]`) — so appending onto an
    empty input list left `names` holding ONLY the new computed column's name, shorter
    than (and not empty relative to) `columns`. `gather_rows` requires `names` to be
    either fully empty or fully parallel to `columns` before it can narrow to an emit
    subset (draken/morsels/sort.hpp) and fails loud otherwise.

    That mismatch stayed invisible for two reasons that both have to hold at once:
    1. the sort/window sink's input must flow straight from a computed-column operator
       whose own input carried no names (the ordinary case), and
    2. the sink's `_narrow_sink_input` (compiler.py) must decide NOTHING needs
       dropping before buffering — i.e. every layout column is either a sort key or
       wanted in the emit set — which only happens when the projection above is (or is
       close to) `SELECT *`. A narrower explicit SELECT list inserts a `ColumnSelect`
       ahead of the sink, which repopulates `names` correctly and hides the bug.

    Q89's `ORDER BY sum_sales - avg_monthly_sales, s_store_name, 1, 2, ... LIMIT 100`
    over `SELECT * FROM (window-over-GROUP-BY) tmp1` hits exactly that combination: the
    computed ORDER BY key is added by `_add_computed` right before the TopN/HeapSort
    sink, and `SELECT *` wants virtually every column, so no narrowing `ColumnSelect` is
    inserted ahead of it.

    Fixed by making both operators carry `names` forward only when the input already
    has one entry per input column (matching `gather_rows`' own contract), otherwise
    leaving `names` empty — never partially populated.
    """
    session = opteryx.session()

    def _rows(statement):
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            names = [n.decode() for n in morsel.column_names]
            out.append((names, list(zip(*(morsel.column(n).to_pylist() for n in names)))))
        return out

    # Same GROUP BY / window shape as `test_window_over_group_by_result`'s FLAVOUR (a)
    # (TPC-DS Q12/Q20/Q98's ratio-to-partition-total idiom, which is also Q89's), but
    # wrapped in a CTE and read back through `SELECT *` with a computed ORDER BY key and
    # a LIMIT — the TopN/HeapSort sink Q89 actually compiles to.
    cte = """
        WITH v1 AS (
            SELECT id % 3 AS bucket, name, SUM(mass) AS m,
                   AVG(SUM(mass)) OVER (PARTITION BY id % 3) AS avg_m
            FROM $planets GROUP BY id % 3, name
        )
        SELECT {select_list} FROM v1 ORDER BY {order_by} LIMIT 9
    """

    # This used to raise; it must now not only survive but answer correctly. Compare
    # against the explicit-column form (which never touched the buggy code path — a
    # narrower select forces the `ColumnSelect` that hides the bug) as the source of
    # truth for both the column set and the sort order.
    [(explicit_columns, explicit_rows)] = [
        (cols, rows)
        for cols, rows in _rows(cte.format(select_list="bucket, name, m, avg_m", order_by="m - avg_m"))
    ]
    [(star_columns, star_rows)] = [
        (cols, rows) for cols, rows in _rows(cte.format(select_list="*", order_by="m - avg_m"))
    ]
    assert sorted(star_columns) == sorted(explicit_columns), star_columns
    # Reorder each row to the explicit column order so the two are directly comparable
    # regardless of the layout position `SELECT *` happens to emit them in.
    star_reordered = [
        tuple(row[star_columns.index(c)] for c in explicit_columns) for row in star_rows
    ]
    assert star_reordered == explicit_rows, star_reordered

    # The values themselves, pinned independently of either query above: three buckets
    # of three planets, ordered by (this group's mass sum - its partition's average).
    assert explicit_rows == [
        (2, "Venus", 4.87, 668.29),
        (2, "Neptune", 102.0, 668.29),
        (0, "Pluto", 0.0146, 191.3282),
        (0, "Earth", 5.97, 191.3282),
        (1, "Mercury", 0.33, 29.25733333333333),
        (1, "Mars", 0.642, 29.25733333333333),
        (1, "Uranus", 86.8, 29.25733333333333),
        (0, "Saturn", 568.0, 191.3282),
        (2, "Jupiter", 1898.0, 668.29),
    ], explicit_rows


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


def test_alias_shadowing_the_column_it_aggregates():
    """
    VALUE-level regression: `AGG(col) AS col` — an output alias spelled the same as the
    column being aggregated — beside a SECOND reference to that column.

    `$derived` holds what the scope is computing, and the projection registers each alias
    there AS IT BINDS. Counting that alias as a relation made the name self-ambiguous: by
    the time the second reference to `mass` bound, `mass` matched both `$planets` and the
    alias just minted from it, and name resolution refused a statement with exactly one
    relation in it ("more than one relation in this query has a column with that name").

    Two spellings reached it by different routes, so both are asserted:

      * a second aggregate over the same column (`MAX(mass) AS mass, MIN(mass)`) binds by
        the mint-a-new-column path and RAISED the ambiguity, and
      * the same aggregate repeated in HAVING or ORDER BY (`MAX(mass) AS mass ... HAVING
        MAX(mass) > 1`) matches an already-bound expression, and `inner_binder`'s fast
        path for that re-binds sub-trees under `suppress(Exception)`. The ambiguity was
        swallowed there, leaving the aggregate's OPERAND with no schema_column, and the
        aggregate binder then read `.identity` off it: `AttributeError: 'NoneType' object
        has no attribute 'identity'` — an internal crash, on legal SQL, that named
        nothing. Reported through a view over a window function; both were incidental.

    An alias is not a relation, so a name it shares with a real column is not ambiguous:
    it binds to the INPUT column, which is what PostgreSQL does when an output name and
    an input name collide. The values are therefore checked against the same statements
    written with a non-colliding alias — the answer the collision must not change.
    """
    session = opteryx.session()

    def _rows(statement):
        """Rows as tuples of values — the output NAMES are what differs between the two
        spellings, so they are deliberately not compared."""
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
            out.extend(zip(*columns))
        return out

    # (statement with the shadowing alias, same statement with a safe alias)
    for shadowed_sql, safe_sql in (
        # HAVING repeats the aggregate whose alias shadows its operand.
        (
            "SELECT gravity, MAX(mass) AS mass FROM $planets GROUP BY gravity HAVING MAX(mass) > 1",
            "SELECT gravity, MAX(mass) AS mx FROM $planets GROUP BY gravity HAVING MAX(mass) > 1",
        ),
        # ORDER BY repeats it instead — same fast path, same swallowed error.
        (
            "SELECT gravity, MAX(mass) AS mass FROM $planets GROUP BY gravity ORDER BY MAX(mass)",
            "SELECT gravity, MAX(mass) AS mx FROM $planets GROUP BY gravity ORDER BY MAX(mass)",
        ),
        # HAVING names a DIFFERENT aggregate over the shadowed column: no fast path, so
        # this one surfaced the ambiguity as an error rather than as a crash.
        (
            "SELECT gravity, MAX(mass) AS mass FROM $planets GROUP BY gravity HAVING MIN(mass) > 1",
            "SELECT gravity, MAX(mass) AS mx FROM $planets GROUP BY gravity HAVING MIN(mass) > 1",
        ),
        # No HAVING and no ORDER BY at all — the projection alone is enough.
        (
            "SELECT MAX(mass) AS mass, MIN(mass) AS smallest FROM $planets",
            "SELECT MAX(mass) AS mx, MIN(mass) AS smallest FROM $planets",
        ),
        # The shadowed column read BARE beside the alias that shadows it: `gravity` here
        # is the input column and the group key, not a reference to the aggregate.
        (
            "SELECT gravity, MAX(mass) AS gravity_max FROM $planets GROUP BY gravity",
            "SELECT gravity, MAX(mass) AS mx FROM $planets GROUP BY gravity",
        ),
    ):
        shadowed_rows = _rows(shadowed_sql)
        safe_rows = _rows(safe_sql)
        assert shadowed_rows, f"no rows: {shadowed_sql}"
        assert len(shadowed_rows) == len(safe_rows), (
            f"row count changed with the alias: {shadowed_sql} -> "
            f"{len(shadowed_rows)} vs {len(safe_rows)}"
        )
        # Same values, whatever the output column is called.
        assert sorted(map(str, shadowed_rows)) == sorted(
            map(str, safe_rows)
        ), f"values changed with the alias: {shadowed_sql}"

    # A name that really IS ambiguous — two relations providing it — must still be
    # refused. The fix narrows what counts as a relation; it does not stop counting.
    with pytest.raises(AmbiguousIdentifierError):
        for _ in session.execute_to_morsels(
            "SELECT name FROM $planets INNER JOIN testdata.satellites ON planetId = $planets.id"
        ):
            pass


def test_having_over_an_ungrouped_aggregate():
    """
    VALUE-level regression: HAVING over an aggregate with a COLUMN operand and no
    GROUP BY — `SELECT MAX(mass) FROM $planets HAVING MAX(mass) > 1`.

    Predicate pushdown had an arm for AggregateAndGroup (the grouped aggregate, where
    it folds the condition on as `having_condition`) and NO arm at all for Aggregate,
    the UNGROUPED one. With no arm, the HAVING predicate kept flowing down and was
    parked above the Scan — because the only identity its condition resolves against
    down there is the aggregate's OPERAND (`mass`), which is what let it match the
    scan in the first place. The compile then died on the aggregate's own output
    identity: `KeyError: expression references column b'$derived_...' which the stream
    does not carry`.

    `HAVING COUNT(*) > 1` escaped it only because COUNT(*) references no column at
    all, so the Scan had nothing to match and the predicate was restored above the
    aggregate — the shape every statement here now gets.

    An ungrouped aggregate collapses every input row into one and emits only its
    results, so a filter above it can never be pushed below it. The counts alone would
    not catch that: `MIN(mass) > 1` is FALSE ($planets' lightest is 0.0146) and must
    return no rows, but the same predicate applied pre-aggregation keeps only the
    heavy planets and MIN over those IS greater than 1 — one row, wrong answer. Both
    halves are asserted, and the values are derived from the data rather than written
    as literals.
    """
    session = opteryx.session()

    def _rows(statement):
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
            out.extend(zip(*columns))
        return out

    masses = [row[0] for row in _rows("SELECT mass FROM $planets")]
    heaviest = max(masses)
    lightest = min(masses)
    assert lightest < 1 < heaviest, "test data no longer straddles the threshold"

    # The aggregate is named in HAVING as well as in the SELECT list. Every spelling
    # crashed: aliased, unaliased, and a different aggregate over the same column.
    for statement in (
        "SELECT MAX(mass) AS m FROM $planets HAVING MAX(mass) > 1",
        "SELECT MAX(mass) FROM $planets HAVING MAX(mass) > 1",
        "SELECT MAX(mass) AS m FROM $planets HAVING m > 1",
    ):
        assert _rows(statement) == [(heaviest,)], statement

    # TRUE post-aggregation, and the value is the whole-relation minimum.
    assert _rows("SELECT MIN(mass) FROM $planets HAVING MIN(mass) < 1") == [(lightest,)]

    # FALSE post-aggregation. Pushed below the aggregate each of these becomes a
    # per-row predicate the relation partly satisfies, and the query returns a row.
    for statement in (
        "SELECT MIN(mass) FROM $planets HAVING MIN(mass) > 1",
        "SELECT MAX(mass) AS m FROM $planets HAVING MAX(mass) > 100000",
        "SELECT COUNT(DISTINCT gravity) AS c FROM $planets HAVING COUNT(DISTINCT gravity) > 100",
    ):
        assert _rows(statement) == [], statement

    # A HAVING naming two aggregates, and one combined with a WHERE below it — the
    # WHERE belongs under the aggregate, the HAVING above it, and the two must not
    # collapse into each other.
    assert _rows("SELECT COUNT(*) AS s FROM $planets HAVING COUNT(*) > 1 AND MAX(mass) > 1") == [
        (len(masses),)
    ]
    heavy = [mass for mass in masses if mass > 1]
    assert _rows("SELECT COUNT(*) AS s FROM $planets WHERE mass > 1 HAVING COUNT(*) > 1") == [
        (len(heavy),)
    ]

    # The grouped aggregate keeps its own (folded) HAVING path — unchanged by this.
    assert len(
        _rows("SELECT gravity, MAX(mass) FROM $planets GROUP BY gravity HAVING MAX(mass) > 1")
    ) == len({row for row in _rows("SELECT gravity FROM $planets WHERE mass > 1")})


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

    # An aggregate window PARTITIONED BY THE SAME KEY on both sides of the boundary,
    # with that key projected through it — the single-table fuzzer's own repro, in both
    # spellings. `w` is a MIN over the partition, so it is non-null on every row and
    # COUNT(w) is the partition's size; a broadcast that attached the wrong partition's
    # row, or one that multiplied, changes those numbers.
    gravities = _values("SELECT gravity AS s FROM $planets")
    per_gravity = {g: gravities.count(g) for g in gravities}
    for statement in (
        "SELECT COUNT(w) OVER (PARTITION BY gravity) AS s "
        "FROM (SELECT gravity, MIN(mass) OVER (PARTITION BY gravity) AS w FROM $planets) AS d",
        "WITH c AS (SELECT gravity, MIN(mass) OVER (PARTITION BY gravity) AS w FROM $planets) "
        "SELECT COUNT(w) OVER (PARTITION BY gravity) AS s FROM c",
    ):
        got = _values(statement)
        assert sorted(got) == sorted(per_gravity[g] for g in gravities), f"{statement}\n  {got!r}"

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


def test_ranking_window_values():
    """
    VALUE-level regression: the rank numbers themselves.

    Every ranking entry in the shape battery orders by a UNIQUE key, so no shape
    test can tell RANK from DENSE_RANK — the 1,1,3 vs 1,1,2 tie behaviour in
    WindowSink (src/cpp/engine/native_sort.hpp), partition reset, DESC ordering,
    and the NULL-partition rule were all unasserted until here.
    """
    session = opteryx.session()

    def _rows(statement, *columns):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            cols = [morsel.column(c).to_pylist() for c in columns]
            collected.extend(zip(*cols))
        return collected

    # Mercury and Venus tie at 0 moons. RANK shares then skips (1,1,3);
    # DENSE_RANK shares then continues (1,1,2); ROW_NUMBER never shares.
    # Three functions on one OVER spec also exercises the shared-node path
    # with values, not just column counts.
    by_name = {
        name: (rn, rk, dr)
        for name, rn, rk, dr in _rows(
            "SELECT name, "
            "ROW_NUMBER() OVER (ORDER BY number_of_moons) AS rn, "
            "RANK() OVER (ORDER BY number_of_moons) AS rk, "
            "DENSE_RANK() OVER (ORDER BY number_of_moons) AS dr "
            "FROM $planets",
            "name", "rn", "rk", "dr",
        )
    }
    # The tied pair: distinct row numbers (either order — SQL does not say which
    # of two peers is row 1), shared rank, shared dense rank.
    assert sorted((by_name["Mercury"][0], by_name["Venus"][0])) == [1, 2], by_name
    assert by_name["Mercury"][1:] == (1, 1), by_name
    assert by_name["Venus"][1:] == (1, 1), by_name
    # After the tie: RANK skips to 3, DENSE_RANK continues at 2 — and the offset
    # persists to the end of the partition.
    assert by_name["Earth"] == (3, 3, 2), by_name
    assert by_name["Saturn"] == (9, 9, 8), by_name

    # Partition reset and DESC: numbering restarts at 1 in every partition and
    # follows the declared direction (highest v first).
    numbered = dict(
        _rows(
            "SELECT x, ROW_NUMBER() OVER (PARTITION BY p ORDER BY v DESC) AS rn "
            "FROM (VALUES ('g1','a',1),('g1','b',2),('g2','c',3),('g2','d',4),('g2','e',5)) "
            "AS t(p,x,v)",
            "x", "rn",
        )
    )
    assert numbered == {"b": 1, "a": 2, "e": 1, "d": 2, "c": 3}, numbered

    # NULL partition keys: win_keys_equal treats two NULLs as equal, so NULL keys
    # form ONE real partition on the ranking path.
    numbered = dict(
        _rows(
            "SELECT x, ROW_NUMBER() OVER (PARTITION BY p ORDER BY x) AS rn "
            "FROM (VALUES ('k','c'), (NULL,'a'), (NULL,'b')) AS t(p,x)",
            "x", "rn",
        )
    )
    assert numbered == {"a": 1, "b": 2, "c": 1}, numbered

    # The same rule on a scan source: the four gas giants share a NULL
    # surface_pressure and are ranked as one partition of four.
    numbered = dict(
        _rows(
            "SELECT name, ROW_NUMBER() OVER (PARTITION BY surface_pressure ORDER BY name) AS rn "
            "FROM $planets",
            "name", "rn",
        )
    )
    assert numbered["Jupiter"] == 1 and numbered["Uranus"] == 4, numbered
    assert len(numbered) == 9, numbered

    # KNOWN DIVERGENCE, pinned deliberately: the AGGREGATE window path lowers to an
    # inner join on the partition key (Eq, not IS NOT DISTINCT FROM — see
    # window_to_join.py), so rows with a NULL partition key are DROPPED there,
    # while the ranking path above keeps them. If either side changes, this test
    # is the notice — the fix is a ruling, not a silent edit here.
    counted = dict(
        _rows(
            "SELECT name, COUNT(*) OVER (PARTITION BY surface_pressure) AS c FROM $planets",
            "name", "c",
        )
    )
    assert len(counted) == 5 and "Jupiter" not in counted, counted


def test_navigation_window_values():
    """
    VALUE-level regression: LAG/LEAD — the shifted-permutation gather.

    Covers each piece of the navigation path: partition-edge NULLs (the
    kGatherNullRow rows), explicit and default offsets, DESC ordering, a STRING
    argument (the binder's output-typing path — the planner mints INT64 and the
    binder must overwrite it with the argument's type), a computed argument
    (the compiler's _add_computed route), offset 0, ranking and navigation
    sharing one window node, and the top-K fusion gate (a `LAG(...) <= K`
    filter must NOT be fused as a top-K — it is an ordinary value filter).
    """
    session = opteryx.session()

    def _rows(statement, *columns):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            cols = [morsel.column(c).to_pylist() for c in columns]
            collected.extend(zip(*cols))
        return collected

    # String argument, both directions, default offset 1. Typing: prev/nxt are
    # VARCHAR because `name` is — INT64 placeholders leaking through would fail
    # loudly here.
    rows = _rows(
        "SELECT name, LAG(name) OVER (ORDER BY id) AS prev, "
        "LEAD(name) OVER (ORDER BY id) AS nxt FROM $planets",
        "name", "prev", "nxt",
    )
    by_name = {name: (prev, nxt) for name, prev, nxt in rows}
    assert by_name["Mercury"] == (None, "Venus"), by_name
    assert by_name["Earth"] == ("Venus", "Mars"), by_name
    assert by_name["Pluto"] == ("Neptune", None), by_name

    # Explicit offset: rows closer to the partition edge than the offset are NULL.
    lagged = dict(_rows(
        "SELECT name, LAG(id, 2) OVER (ORDER BY id) AS l2 FROM $planets",
        "name", "l2",
    ))
    assert lagged["Mercury"] is None and lagged["Venus"] is None, lagged
    assert lagged["Earth"] == 1 and lagged["Pluto"] == 7, lagged

    # Offset 0 is the current row.
    same = dict(_rows(
        "SELECT name, LAG(id, 0) OVER (ORDER BY id) AS l0 FROM $planets",
        "name", "l0",
    ))
    assert same == {n: i for n, i in _rows("SELECT name, id FROM $planets", "name", "id")}, same

    # Partition reset + DESC: LAG follows the DECLARED direction, and restarts
    # (NULL) at every partition edge.
    lagged = dict(_rows(
        "SELECT x, LAG(v) OVER (PARTITION BY p ORDER BY v DESC) AS lg "
        "FROM (VALUES ('g1','a',1),('g1','b',2),('g2','c',3),('g2','d',4),('g2','e',5)) "
        "AS t(p,x,v)",
        "x", "lg",
    ))
    assert lagged == {"b": None, "a": 2, "e": None, "d": 5, "c": 4}, lagged

    # Computed argument — projected to a stream column by the compiler first.
    computed = dict(_rows(
        "SELECT name, LAG(id + 100) OVER (ORDER BY id) AS lc FROM $planets",
        "name", "lc",
    ))
    assert computed["Mercury"] is None and computed["Venus"] == 101, computed

    # Two navigation functions with DIFFERENT arguments over one spec are two
    # distinct columns (the dedup key includes the argument), and ranking +
    # navigation share a single window node.
    mixed = dict(_rows(
        "SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn, "
        "LAG(name) OVER (ORDER BY id) AS lg FROM $planets",
        "name", "rn",
    ))
    assert mixed["Mercury"] == 1 and mixed["Pluto"] == 9, mixed
    both = dict(_rows(
        "SELECT name, LAG(name) OVER (ORDER BY id) AS a, LAG(id) OVER (ORDER BY id) AS b "
        "FROM $planets",
        "name", "b",
    ))
    assert both["Venus"] == 1, both

    # Fusion gate: `LAG(...) <= K` is a value filter, not a top-K — the optimizer
    # must not fuse it. Values prove it filtered on the VALUE (NULL excluded);
    # telemetry proves fusing did not fire.
    gate_session = opteryx.session()
    kept = []
    for morsel in gate_session.execute_to_morsels(
        "SELECT name FROM $planets QUALIFY LAG(id) OVER (ORDER BY id) <= 3"
    ):
        kept.extend(morsel.column("name").to_pylist())
    assert sorted(kept) == ["Earth", "Mars", "Venus"], kept
    assert dict(gate_session.telemetry).get("optimization_window_topk_fuse", 0) == 0, (
        "a LAG filter was fused as a top-K — silent wrong answer gate failed"
    )

    # NULL partition keys form one partition on this path too.
    lagged = dict(_rows(
        "SELECT name, LAG(name) OVER (PARTITION BY surface_pressure ORDER BY name) AS lg "
        "FROM $planets",
        "name", "lg",
    ))
    # The four gas giants share a NULL surface_pressure: alphabetical within it.
    assert lagged["Jupiter"] is None and lagged["Neptune"] == "Jupiter", lagged
    assert lagged["Saturn"] == "Neptune" and lagged["Uranus"] == "Saturn", lagged


def test_framed_window_general_shapes():
    """
    VALUE-level regression: SUM/COUNT/AVG/MIN/MAX OVER (... ROWS/RANGE BETWEEN ...) —
    native_window_frame.hpp's FramedWindowSink, a SEPARATE native sink from the ranking
    WindowSink above (see that header's comment for why: a sliding-window reduction with
    a per-function OUTPUT TYPE, not "one value per row from the sort order itself").

    Covers, all id-ordered so every value is hand-checkable: a growing (cumulative)
    frame, a fixed-size sliding frame (tests the MIN/MAX monotonic-deque eviction path,
    not just the SUM/COUNT/AVG two-pointer accumulation), a FOLLOWING bound (frame
    extends past the current row, including a partition-edge case with fewer rows than
    the frame asks for), the standard's default frame (ORDER BY with no explicit
    ROWS/RANGE — RANGE UNBOUNDED PRECEDING AND CURRENT ROW), and DECIMAL128 exactness
    (CAST to a >18-digit-precision DECIMAL forces the int128 accumulation domain, not
    just the int64-backed DECIMAL(p<=18) one).
    """
    session = opteryx.session()

    def _rows(statement):
        out = []
        for morsel in session.execute_to_morsels(statement):
            morsel.materialize()
            names = list(morsel.column_names)
            out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
        return sorted(out)

    # Cumulative (growing) frame — id is 1..9 in $planets, so the running sum is
    # id's own triangular numbers.
    assert _rows(
        "SELECT id, SUM(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c FROM $planets"
    ) == [(1, 1), (2, 3), (3, 6), (4, 10), (5, 15), (6, 21), (7, 28), (8, 36), (9, 45)]

    # Fixed-size sliding frame (current + 1 preceding) — MAX/MIN exercise the
    # monotonic-deque eviction path (values must be forgotten once they slide out).
    assert _rows(
        "SELECT id, MAX(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c FROM $planets"
    ) == [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9)]
    assert _rows(
        "SELECT id, MIN(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) c FROM $planets"
    ) == [(1, 1), (2, 1), (3, 2), (4, 3), (5, 4), (6, 5), (7, 6), (8, 7), (9, 8)]
    assert _rows(
        "SELECT id, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) c FROM $planets"
    ) == [
        (1, 1.5), (2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0), (6, 6.0), (7, 7.0), (8, 8.0), (9, 8.5),
    ]

    # A FOLLOWING bound: frame extends AHEAD of the current row. The last row's frame
    # (CURRENT ROW AND 1 FOLLOWING) has no following row — clipped to the partition,
    # not an error and not NULL (COUNT(*) counts 1, not 2).
    assert _rows(
        "SELECT id, COUNT(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) c FROM $planets"
    ) == [(1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2), (7, 2), (8, 2), (9, 1)]

    # No explicit frame — the standard's default (RANGE UNBOUNDED PRECEDING AND CURRENT
    # ROW) — answers the same running total as the explicit ROWS form above, since id
    # has no ties (RANGE's peer-group CURRENT ROW degenerates to ROWS' single row).
    assert _rows("SELECT id, SUM(id) OVER (ORDER BY id) c FROM $planets") == [
        (1, 1), (2, 3), (3, 6), (4, 10), (5, 15), (6, 21), (7, 28), (8, 36), (9, 45),
    ]

    # DECIMAL128 (precision > 18, forcing the int128 accumulation domain — a
    # precision<=18 DECIMAL is int64-backed and exercised by test_window_over_group_by's
    # Q51 coverage) exactness: no accumulated float rounding across nine additions.
    assert _rows(
        "SELECT id, SUM(CAST(mass AS DECIMAL(28,4))) OVER (ORDER BY id "
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) c FROM $planets"
    ) == [
        (1, decimal.Decimal("0.3300")),
        (2, decimal.Decimal("5.2000")),
        (3, decimal.Decimal("11.1700")),
        (4, decimal.Decimal("11.8120")),
        (5, decimal.Decimal("1909.8120")),
        (6, decimal.Decimal("2477.8120")),
        (7, decimal.Decimal("2564.6120")),
        (8, decimal.Decimal("2666.6120")),
        (9, decimal.Decimal("2666.6266")),
    ]

    # Scope refusals, not silent wrong answers: a RANGE frame with a numeric offset
    # (value-distance semantics — a materially different feature, see
    # native_window_frame.hpp's header comment); a FRAME with no ORDER BY; and a
    # non-SUM/COUNT/AVG/MIN/MAX aggregate (STDDEV) with a window ORDER BY.
    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels(
            "SELECT SUM(id) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM $planets"
        ))
    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels(
            "SELECT SUM(id) OVER (PARTITION BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM $planets"
        ))
    with pytest.raises(UnsupportedSyntaxError):
        list(session.execute_to_morsels(
            "SELECT STDDEV(id) OVER (ORDER BY id) FROM $planets"
        ))


def test_uncorrelated_scalar_subquery_single_row_proofs():
    """
    VALUE-level regression for uncorrelated-scalar-subquery cardinality
    enforcement in `decorrelate_subquery.py` (`_guard_scalar_cardinality` /
    the engine's ScalarGuardSource).

    An uncorrelated scalar subquery decorrelates into a CROSS JOIN of one value
    against every outer row — sound only if the subquery yields ONE value.
    Unless the plan proves exactly one row statically (an ungrouped aggregate),
    a ScalarSubqueryGuard step enforces SQL's scalar semantics at runtime, at
    the subquery's materialization boundary in the engine:

      - more than one row -> the SQL-standard "more than one row returned by a
        subquery used as an expression", raised as DataError. This replaced the
        old compile-time refusal ("must return exactly one row"): shapes whose
        single-row property is a fact about the DATA — DISTINCT, a bare
        filtered SELECT (TPC-DS Q06/Q54/Q58) — now run, and error only when
        the data genuinely breaks the property. The STATIC proof was never
        widened to accept them (that would multiply outer rows silently the
        day the data changes); the runtime check is what admits them honestly.
      - zero rows -> NULL, per SQL. Without the guard an empty subquery
        emptied the cross join, so `WHERE (subq) IS NULL` wrongly returned
        nothing; the guard emits one all-NULL row instead.
      - exactly one row -> passes through untouched.
    """
    session = opteryx.session()

    def _names(statement):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            collected.extend(morsel.column("name").to_pylist())
        return collected

    # Jupiter (id=5) has 79 moons; Saturn is the only planet with more (82).
    # GROUP BY id, filtered by `id = 5` — one possible group.
    assert _names(
        "SELECT name FROM $planets WHERE number_of_moons > "
        "(SELECT avg(number_of_moons) FROM $planets WHERE id = 5 GROUP BY id)"
    ) == ["Saturn"]

    # Saturn (82 moons) is also the top row by this ordering — LIMIT 1 with no
    # GROUP BY or aggregate at all.
    assert _names(
        "SELECT name FROM $planets WHERE number_of_moons >= "
        "(SELECT number_of_moons FROM $planets ORDER BY number_of_moons DESC LIMIT 1)"
    ) == ["Saturn"]

    # A DISTINCT that is GENUINELY single-valued for the data (TPC-DS Q06/Q54's
    # shape): filter pins a different column than the one selected, so this is
    # a data fact the plan cannot prove — the runtime guard admits it.
    assert _names(
        "SELECT name FROM $planets WHERE number_of_moons = "
        "(SELECT DISTINCT number_of_moons FROM $planets WHERE name = 'Saturn')"
    ) == ["Saturn"]

    # A bare filtered projection, no DISTINCT/LIMIT/aggregate (TPC-DS Q58's
    # shape) — same data-fact single row, same runtime admission.
    assert _names(
        "SELECT name FROM $planets WHERE number_of_moons = "
        "(SELECT number_of_moons FROM $planets WHERE name = 'Saturn')"
    ) == ["Saturn"]

    # Genuinely >1 row: `mass > 1` matches several planets, so GROUP BY id has
    # several groups — the guard raises SQL's cardinality violation at runtime.
    with pytest.raises(DataError, match="more than one row returned by a subquery"):
        _names(
            "SELECT name FROM $planets WHERE number_of_moons > "
            "(SELECT avg(number_of_moons) FROM $planets WHERE mass > 1 GROUP BY id)"
        )

    # Genuinely >1 row through DISTINCT: every planet has a different moon
    # count, so this DISTINCT returns nine rows.
    with pytest.raises(DataError, match="more than one row returned by a subquery"):
        _names(
            "SELECT name FROM $planets WHERE number_of_moons = "
            "(SELECT DISTINCT number_of_moons FROM $planets WHERE mass > 0)"
        )

    # Zero rows -> NULL. The comparison with NULL is unknown, so no outer row
    # survives ...
    assert _names(
        "SELECT name FROM $planets WHERE number_of_moons = "
        "(SELECT number_of_moons FROM $planets WHERE name = 'Krypton' LIMIT 1)"
    ) == []

    # ... while IS NULL is TRUE for every outer row. Before the guard, the
    # empty subquery emptied the cross join and this wrongly returned nothing —
    # the LIMIT 1 shape had exactly this bug.
    assert len(_names(
        "SELECT name FROM $planets WHERE "
        "(SELECT number_of_moons FROM $planets WHERE name = 'Krypton' LIMIT 1) IS NULL"
    )) == 9


def test_select_list_scalar_subquery():
    """
    VALUE-level regression for uncorrelated scalar subqueries used as a SELECT-list
    value (`_decorrelate_projection` / `_find_subquery_in_columns` in
    decorrelate_subquery.py).

    This used to be refused outright, pre-bind, in logical_planner.py: "Scalar
    subqueries are supported in the WHERE clause but not yet in the SELECT list."
    The WHERE-clause rewrite (`_decorrelate`) already proved an uncorrelated
    single-row scalar subquery can become a CROSS JOIN of one value against every
    outer row; the gap was that nothing applied the same rewrite to a Project's own
    column list — the SUBQUERY node just rode through binding unresolved and failed
    deep in the planner. TPC-DS Q09 needs exactly this shape, repeated inside CASE
    branches — see `test_select_list_case_scalar_subquery` for that.

    $planets' number_of_moons sums to 210 across 9 planets: avg == 23.33...
    """
    session = opteryx.session()

    def _col(statement, column):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            collected.extend(morsel.column(column).to_pylist())
        return collected

    # A bare scalar subquery, aliased, alongside an ordinary column — the same
    # value attached to every outer row via a cross join.
    assert _col(
        "SELECT name, (SELECT AVG(number_of_moons) FROM $planets) AS avg_moons "
        "FROM $planets WHERE id IN (1, 5) ORDER BY id",
        "avg_moons",
    ) == pytest.approx([210 / 9, 210 / 9])
    assert _col(
        "SELECT name, (SELECT AVG(number_of_moons) FROM $planets) AS avg_moons "
        "FROM $planets WHERE id IN (1, 5) ORDER BY id",
        "name",
    ) == ["Mercury", "Jupiter"]

    # The subquery's value participates in an enclosing expression — the join
    # still has to expose it as an ordinary column the arithmetic can read.
    assert _col(
        "SELECT (SELECT COUNT(*) FROM $planets WHERE number_of_moons > 0) * 2 AS doubled "
        "FROM $planets WHERE id = 1",
        "doubled",
    ) == [14]

    # Still refused: correlated (references the outer row) — SELECT-list
    # correlation needs a LEFT OUTER join this rewrite does not build.
    with pytest.raises(UnsupportedSyntaxError, match="must be uncorrelated"):
        _col(
            "SELECT (SELECT AVG(number_of_moons) FROM $planets p2 WHERE p2.id = p1.id) "
            "AS x FROM $planets p1",
            "x",
        )

    # Not provably one row (no aggregate, no GROUP BY, no LIMIT): no longer a
    # compile-time refusal. The runtime ScalarSubqueryGuard admits the shape and
    # enforces SQL's scalar semantics at the materialization boundary — nine
    # planets come back, so this raises the standard cardinality violation.
    # (Same contract as the WHERE-clause shapes proved above in
    # test_scalar_subquery_single_row_proofs.)
    with pytest.raises(DataError, match="more than one row returned by a subquery"):
        _col(
            "SELECT (SELECT number_of_moons FROM $planets) AS x FROM $planets WHERE id = 1",
            "x",
        )

    # A LIMIT-headed subquery binds too (LIMIT / ORDER BY / DISTINCT head the
    # subplan as column-less pass-through steps — bind_correlated_subquery must
    # descend to the Project to type the value). Zero rows -> NULL per outer
    # row, never an emptied result.
    assert _col(
        "SELECT (SELECT number_of_moons FROM $planets WHERE name = 'Krypton' LIMIT 1) AS x "
        "FROM $planets WHERE id = 1",
        "x",
    ) == [None]

    # EXISTS in the SELECT list is now a supported VALUE. Uncorrelated, it is a
    # COUNT(*) > 0 cross joined on — the count emits exactly one row structurally,
    # so no cardinality guard is involved. See test_select_list_existence.
    assert _col(
        "SELECT EXISTS(SELECT 1 FROM $planets) AS x FROM $planets WHERE id = 1", "x"
    ) == [True]
    assert _col(
        "SELECT EXISTS(SELECT 1 FROM $planets WHERE id > 100) AS x "
        "FROM $planets WHERE id = 1",
        "x",
    ) == [False]


def test_select_list_case_scalar_subquery():
    """
    VALUE-level regression matching TPC-DS Q09's actual shape: a CASE expression
    whose WHEN test and THEN/ELSE results are each their own uncorrelated scalar
    subquery — not just a bare `SELECT (subquery)`.

    `_find` (decorrelate_subquery.py) only walked left/right/centre/parameters, so
    it never reached a subquery buried inside a CASE's conditions/results/
    else_result; Q09 has three subqueries per CASE (WHEN, THEN, ELSE), repeated
    for five buckets, so every one of them has to be found and replaced, not just
    the first.

    7 of 9 $planets have `number_of_moons > 0` (Earth, Mars, Jupiter, Saturn,
    Uranus, Neptune, Pluto), summing to 210: avg == 30. All 9 have
    `number_of_moons >= 0`, so `COUNT(*) > 8` is true and the THEN branch fires;
    `COUNT(*) > 100` is false and the ELSE branch fires instead.
    """
    session = opteryx.session()

    def _col(statement, column):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            collected.extend(morsel.column(column).to_pylist())
        return collected

    # THEN branch: the WHEN subquery's count (9) clears the threshold (8).
    assert _col(
        "SELECT CASE "
        "WHEN (SELECT COUNT(*) FROM $planets WHERE number_of_moons >= 0) > 8 "
        "THEN (SELECT AVG(number_of_moons) FROM $planets WHERE number_of_moons > 0) "
        "ELSE (SELECT AVG(mass) FROM $planets) "
        "END AS bucket FROM $planets WHERE id = 1",
        "bucket",
    ) == pytest.approx([30.0])

    # ELSE branch: same CASE, threshold raised past what the WHEN subquery
    # returns, so it falls through to the ELSE subquery instead.
    assert _col(
        "SELECT CASE "
        "WHEN (SELECT COUNT(*) FROM $planets WHERE number_of_moons >= 0) > 100 "
        "THEN (SELECT AVG(number_of_moons) FROM $planets WHERE number_of_moons > 0) "
        "ELSE (SELECT AVG(mass) FROM $planets) "
        "END AS bucket FROM $planets WHERE id = 1",
        "bucket",
    ) == pytest.approx([sum([0.33, 4.87, 5.97, 0.642, 1898.0, 568.0, 86.8, 102.0, 0.0146]) / 9])

    # Several buckets in one SELECT list, each its own three subqueries — the
    # round-based rewrite in `_rewrite_projects` has to drain all of them, not
    # just the first CASE found.
    rows = list(
        session.execute_to_morsels(
            "SELECT "
            "CASE WHEN (SELECT COUNT(*) FROM $planets WHERE number_of_moons >= 0) > 8 "
            "THEN (SELECT AVG(number_of_moons) FROM $planets WHERE number_of_moons > 0) "
            "ELSE (SELECT AVG(mass) FROM $planets) END AS bucket1, "
            "CASE WHEN (SELECT COUNT(*) FROM $planets WHERE number_of_moons >= 0) > 100 "
            "THEN (SELECT AVG(number_of_moons) FROM $planets WHERE number_of_moons > 0) "
            "ELSE (SELECT AVG(mass) FROM $planets) END AS bucket2 "
            "FROM $planets WHERE id = 1"
        )
    )
    bucket1 = [v for morsel in rows for v in morsel.column("bucket1").to_pylist()]
    bucket2 = [v for morsel in rows for v in morsel.column("bucket2").to_pylist()]
    assert bucket1 == pytest.approx([30.0])
    assert bucket2 == pytest.approx(
        [sum([0.33, 4.87, 5.97, 0.642, 1898.0, 568.0, 86.8, 102.0, 0.0146]) / 9]
    )


def test_correlated_scalar_subquery_or_factored_equality():
    """
    VALUE-level regression for `_factor_common_or_correlation` in
    `decorrelate_subquery.py`.

    TPC-DS Q41 correlates a scalar subquery like this:

        (i_manufact = i1.i_manufact AND <local A>)
        OR (i_manufact = i1.i_manufact AND <local B>)

    The correlation is a plain equality, but it sits inside a top-level OR, not
    a top-level AND — `_split_correlations`'s AND-only walk never saw it, so
    the whole OR was treated as an unresolvable residual and the query was
    rejected as a "non-equality correlation" even though every occurrence of
    the outer column is inside an `=`. The fix factors an equality correlation
    that is common to EVERY branch of the OR out of it: `(A AND X) OR (A AND
    Y)` becomes `A AND (X OR Y)`, which the existing AND walk handles.

    $planets ids are unique (1..9), so `id = p1.id` inside the subquery pins
    it to exactly the outer row itself: the correlated COUNT(*) is 1 when that
    planet's own moon count is 0 or > 50, else 0. Mercury/Venus have 0 moons;
    Jupiter/Saturn have 79/82 — the only planets `> 0` should select.
    """
    session = opteryx.session()

    def _names(statement):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            collected.extend(morsel.column("name").to_pylist())
        return sorted(collected)

    # The Q41 shape: same equality correlation common to both OR branches.
    assert _names(
        "SELECT name FROM $planets p1 WHERE ("
        "SELECT COUNT(*) FROM $planets "
        "WHERE (id = p1.id AND number_of_moons = 0) "
        "OR (id = p1.id AND number_of_moons > 50)"
        ") > 0"
    ) == ["Jupiter", "Mercury", "Saturn", "Venus"]

    # Sanity: the already-working plain-AND equality correlation (no OR) is
    # unaffected — same predicate, no OR to factor.
    assert _names(
        "SELECT name FROM $planets p1 WHERE ("
        "SELECT COUNT(*) FROM $planets WHERE id = p1.id AND number_of_moons = 0"
        ") > 0"
    ) == ["Mercury", "Venus"]

    # Still refused: the two OR branches correlate through DIFFERENT
    # predicates (`id = p1.id` vs `id > p1.id`), so no equality is common to
    # every branch and there is nothing to factor — the non-equality
    # correlation in the second branch is the genuine, still-unsupported case.
    with pytest.raises(UnsupportedSyntaxError, match="non-equality"):
        _names(
            "SELECT name FROM $planets p1 WHERE ("
            "SELECT COUNT(*) FROM $planets "
            "WHERE (id = p1.id AND number_of_moons = 0) "
            "OR (id > p1.id AND number_of_moons > 50)"
            ") > 0"
        )


def test_comma_join_equality_buried_in_or():
    """
    VALUE-level regression for `DisjunctionSimplificationStrategy` reaching an
    OR that is not the top-level node of a Filter's condition.

    TPC-DS Q13/Q48 comma-join `store_sales` to `household_demographics` /
    `customer_address` through equalities buried inside three-way ORs, each
    ANDed with unrelated per-branch filters and themselves ANDed alongside
    other top-level predicates:

        d_year = 2001 AND (
            (ss_hdemo_sk=hd_demo_sk AND cd_marital_status='M' AND ...)
            OR (ss_hdemo_sk=hd_demo_sk AND cd_marital_status='S' AND ...)
            OR (ss_hdemo_sk=hd_demo_sk AND cd_marital_status='W' AND ...)
        )

    `DisjunctionSimplificationStrategy` already implements exactly this
    factoring — `(J AND A) OR (J AND B) OR (J AND C)` -> `J AND (A OR B OR
    C)` — but ran BEFORE `SplitConjunctivePredicatesStrategy` and only ever
    inspected whether a Filter's WHOLE condition was an OR, so it silently
    no-opped whenever the OR sat inside a top-level AND instead (as it does
    here, and in Q13/Q48). The join-key equality then stayed invisible to
    every downstream join-key-detection site (`cross_join_chain_reorder`,
    the DPccp adapter, `cross_join_filter_pushdown`), and the comma join fell
    back to a genuine, unfiltered cross join — a 20+ minute hang on TPC-DS
    Q13 at SF1 (see tests/performance/tpcds/runner.py's docstring).

    $planets x testdata.satellites, comma-joined with the connecting equality
    (`p.id = s.planetId`) buried inside a three-branch OR (mirroring Q13's
    shape), each branch ANDed with a different, mutually-exclusive-and-
    exhaustive `s.id` band, and the whole OR ANDed alongside an unrelated
    top-level predicate. Since the three bands partition every satellite,
    this is logically equivalent to the plain join `p.id = s.planetId`.
    """
    session = opteryx.session()
    or_buried_sql = (
        "SELECT p.name AS planet_name, COUNT(*) AS n FROM $planets p, testdata.satellites s "
        "WHERE 1 = 1 AND ("
        "(p.id = s.planetId AND s.id < 60) "
        "OR (p.id = s.planetId AND s.id >= 60 AND s.id < 120) "
        "OR (p.id = s.planetId AND s.id >= 120)"
        ") GROUP BY p.name ORDER BY p.name"
    )

    def _rows(sess, sql):
        collected = []
        for morsel in sess.execute_to_morsels(sql):
            collected.extend(
                zip(morsel.column("planet_name").to_pylist(), morsel.column("n").to_pylist())
            )
        return sorted(collected)

    or_buried_rows = _rows(session, or_buried_sql)

    telemetry = dict(session.telemetry)
    assert telemetry.get("optimization_disjunction_simplification", 0) >= 1, (
        "disjunction simplification did not fire — this shape no longer "
        "exercises the OR-buried join-key detection path, and the test "
        "proves nothing"
    )

    reference_session = opteryx.session()
    reference_rows = _rows(
        reference_session,
        "SELECT p.name AS planet_name, COUNT(*) AS n FROM $planets p, testdata.satellites s "
        "WHERE p.id = s.planetId GROUP BY p.name ORDER BY p.name",
    )

    assert or_buried_rows == reference_rows
    assert or_buried_rows, "join produced no rows — this shape is not actually testing a join"

    # Sanity: a plain top-level AND (no OR to factor) is unaffected.
    plain_session = opteryx.session()
    plain_rows = _rows(
        plain_session,
        "SELECT p.name AS planet_name, COUNT(*) AS n FROM $planets p, testdata.satellites s "
        "WHERE p.id = s.planetId AND s.id >= 0 GROUP BY p.name ORDER BY p.name",
    )
    assert plain_rows == reference_rows


def test_window_topk_fusion_parity():
    """
    The fused `rank <= K` path must answer exactly what the unfused path answers.

    WindowTopKFusionStrategy folds a downstream `<rank> <= K` filter into the
    Window node (telemetry: optimization_window_topk_fuse). The COMPILER then
    independently picks WindowTopKSink (only for a single ROW_NUMBER over one
    fixed-width ORDER BY key) or WindowSink's post-rank filter — the optimizer
    counter fires for BOTH sink choices, so all three shapes below assert
    counter-on-fused and value parity; the sink split is invisible to telemetry
    and is covered by the shapes chosen.

    The A/B uses config.features (read live by the optimizer) with save/restore —
    the same harness as test_topn_manifest_pruning. Without the telemetry
    assertion this test proves nothing: a shape that silently fails to qualify
    "passes" while fusing never runs.
    """
    from opteryx import config

    statements = [
        # Qualifies for WindowTopKSink: single ROW_NUMBER, one INT ORDER BY key.
        (
            "SELECT name, r FROM (SELECT name, "
            "ROW_NUMBER() OVER (PARTITION BY planetId ORDER BY id) AS r "
            "FROM testdata.satellites) AS t WHERE r <= 2"
        ),
        # Fuses at the optimizer, but RANK falls back to WindowSink's post-rank
        # filter (ties need every row's exact rank first).
        (
            "SELECT name, r FROM (SELECT name, "
            "RANK() OVER (PARTITION BY planetId ORDER BY id) AS r "
            "FROM testdata.satellites) AS t WHERE r <= 2"
        ),
        # Fuses at the optimizer; two ORDER BY keys also fall back to WindowSink.
        (
            "SELECT name, r FROM (SELECT name, "
            "ROW_NUMBER() OVER (PARTITION BY planetId ORDER BY id, name) AS r "
            "FROM testdata.satellites) AS t WHERE r <= 2"
        ),
    ]

    def _rows(session, statement):
        collected = []
        for morsel in session.execute_to_morsels(statement):
            collected.extend(
                zip(morsel.column("name").to_pylist(), morsel.column("r").to_pylist())
            )
        return sorted(collected)

    for statement in statements:
        fused_session = opteryx.session()
        fused = _rows(fused_session, statement)
        fused_telemetry = dict(fused_session.telemetry)
        assert fused_telemetry.get("optimization_window_topk_fuse", 0) >= 1, (
            f"fusion did not fire — the shape no longer qualifies and this "
            f"case is testing nothing: {statement}"
        )

        original = config.features.disable_window_topk_fusion
        try:
            config.features.disable_window_topk_fusion = True
            unfused_session = opteryx.session()
            unfused = _rows(unfused_session, statement)
            unfused_telemetry = dict(unfused_session.telemetry)
        finally:
            config.features.disable_window_topk_fusion = original

        assert unfused_telemetry.get("optimization_window_topk_fuse", 0) == 0, (
            "disable flag did not disable fusing"
        )
        assert fused == unfused, (
            f"fused and unfused paths disagree for: {statement}\n"
            f"fused:   {fused}\nunfused: {unfused}"
        )
        assert fused, f"parity holds but the query returned nothing: {statement}"


def test_cross_join_output_mixes_raw_and_computed_columns():
    """
    VALUE-level regression: `SELECT a, b, a + b FROM (leg1) x, (leg2) y` — a raw
    passthrough column from EACH cross-joined leg plus an expression combining
    both — must actually compute the expression, not silently drop it.

    `ProjectionPushdownStrategy` rebuilds a Join node's `.columns` from
    `node.schemas`, which `binder/join.py` sets to a REFERENCE to the live
    binder `context.schemas` dict, not a snapshot. `context.schemas["$derived"]`
    is the query-wide scratch registry every computed expression is minted
    into as binding proceeds — so by the time the optimizer runs (after
    binding finishes), it holds every derived column the WHOLE query minted,
    including `a + b`, computed by the OUTER Project sitting ABOVE this join.
    Treating "identity present in $derived" as "this join can emit this
    column" pulled `a + b`'s identity onto the join, which made
    RedundantOperationsStrategy think the Project computing `a + b` was a
    no-op reselection (provider already produces every column asked for) and
    deleted it — TPC-DS Q61's shape. compile_to_native then had no operator
    left to compute `a + b` and refused with "an output column the engine
    could not resolve here". Only fires with a column from EACH leg present
    alongside the cross-leg expression; any two of the three alone compiled
    fine, which is why the shape battery (shape-only, not value-level) never
    caught it.
    """
    statement = (
        "SELECT a, b, a + b FROM "
        "(SELECT SUM(id) AS a FROM $planets) x, "
        "(SELECT SUM(id) AS b FROM $planets) y"
    )
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(statement))

    rows = []
    for morsel in morsels:
        rows.extend(
            zip(
                morsel.column("a").to_pylist(),
                morsel.column("b").to_pylist(),
                morsel.column("a + b").to_pylist(),
            )
        )

    assert rows == [(45, 45, 90)], f"expected [(45, 45, 90)], got {rows}"


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
        (
            "union of aggregates with a computed group key",
            test_union_of_aggregates_with_a_computed_group_key,
        ),
        (
            "union leg computed alias matching source column name",
            test_union_leg_computed_alias_matching_source_column_name,
        ),
        ("union leg width coercion", test_union_leg_width_coercion),
        (
            "chained union coerces every leg",
            test_chained_union_coerces_every_leg_and_declares_what_it_delivers,
        ),
        (
            "union columns sharing one source column",
            test_union_output_columns_that_share_one_source_column,
        ),
        (
            "union leg aliases its own derived table case-insensitively",
            test_union_leg_aliases_its_own_derived_table_case_insensitively,
        ),
        (
            "chained union of having-filtered aggregates",
            test_chained_union_of_having_filtered_aggregates,
        ),
        (
            "chained union of having-scalar-subquery-filtered aggregates",
            test_chained_union_of_having_scalar_subquery_filtered_aggregates,
        ),
        (
            "filtered aggregate over a folded null condition",
            test_filtered_aggregate_over_a_constant_folded_null_condition,
        ),
        (
            "cast null as boolean behaves like a boolean",
            test_cast_null_as_boolean_behaves_like_a_boolean,
        ),
        (
            "case when condition is never a raw type error",
            test_case_when_condition_is_never_a_raw_type_error,
        ),
        (
            "literals are not interned across types",
            test_literals_are_not_interned_across_types,
        ),
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
            "window beside an aggregate runs over the grouped rows",
            test_window_beside_an_aggregate_runs_over_the_grouped_rows,
        ),
        (
            "window over a GROUP BY result",
            test_window_over_group_by_result,
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
            "window in ORDER BY",
            test_window_in_order_by,
        ),
        (
            "positional ORDER BY over SELECT *",
            test_positional_order_by_over_select_star,
        ),
        (
            "computed ORDER BY over window-over-GROUP-BY SELECT *",
            test_computed_order_by_over_window_group_by_select_star,
        ),
        (
            "window in HAVING is refused by name",
            test_window_in_having_is_refused_by_name,
        ),
        (
            "alias shadowing the column it aggregates",
            test_alias_shadowing_the_column_it_aggregates,
        ),
        (
            "having over an ungrouped aggregate",
            test_having_over_an_ungrouped_aggregate,
        ),
        (
            "chained windows across a subquery",
            test_chained_windows_across_a_subquery,
        ),
        (
            "qualify does not leak its window column",
            test_qualify_does_not_leak_its_window_column,
        ),
        ("ranking window values", test_ranking_window_values),
        ("navigation window values", test_navigation_window_values),
        ("framed window general shapes", test_framed_window_general_shapes),
        (
            "uncorrelated scalar subquery single-row proofs",
            test_uncorrelated_scalar_subquery_single_row_proofs,
        ),
        ("select-list scalar subquery", test_select_list_scalar_subquery),
        ("select-list CASE scalar subquery", test_select_list_case_scalar_subquery),
        (
            "correlated scalar subquery OR-factored equality",
            test_correlated_scalar_subquery_or_factored_equality,
        ),
        (
            "comma join equality buried in OR",
            test_comma_join_equality_buried_in_or,
        ),
        ("window top-k fusion parity", test_window_topk_fusion_parity),
        (
            "cross join output mixes raw and computed columns",
            test_cross_join_output_mixes_raw_and_computed_columns,
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
