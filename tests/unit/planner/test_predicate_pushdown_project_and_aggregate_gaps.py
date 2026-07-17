# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression tests for two predicate-pushdown gaps in
opteryx/planner/optimizer/strategies/predicate_pushdown.py:

- Gap A/B: a predicate on a passthrough/renamed column, collected against an
  outer view/subquery alias, must still reach the underlying Scan even after
  the planner inlines nested views/subqueries down onto it (identity-based
  matching, not relation-name matching).
- The AggregateAndGroup gap: a non-HAVING predicate on a GROUP BY key column
  must be free to keep flowing past the aggregate (and any Project below it)
  down to the Scan, rather than being pinned immediately above/below the
  aggregate. HAVING predicates (predicates with aggregators) are unaffected.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _find_node_by_class_fragment(plan, fragment: str):
    """Return the nid of the first node whose class name contains `fragment`."""
    for nid in plan.nodes():
        if fragment in type(plan[nid]).__name__:
            return nid
    return None


def _plan_for(sql: str):
    session = opteryx.session()
    try:
        list(session.execute_to_morsels(sql))
        return session._plan
    finally:
        session.close()


def test_passthrough_filter_reaches_scan_through_nested_aggregate_and_project():
    """The repro from the predicate-pushdown gap report: a WHERE on a
    passthrough-renamed column, sitting above a nested GROUP BY and an inner
    Project, must end up immediately adjacent to the Scan/Reader rather than
    stuck above the whole chain."""
    sql = """
    SELECT billing_account, TRUNC(billing_date, 'day') AS billing_date2, SUM(total_queries) AS total_queries
    FROM (
        SELECT billing_account, TRUNC(billing_hour, 'day') AS billing_date, COUNT(*) AS total_queries
        FROM (SELECT name AS billing_account, FROM_UNIXTIME(id * 3600) AS billing_hour FROM $planets) AS inner_t
        GROUP BY billing_account, TRUNC(billing_hour, 'day')
    ) AS mid_t
    WHERE billing_account = 'Earth'
    GROUP BY ALL
    """
    plan = _plan_for(sql)

    reader_nid = _find_node_by_class_fragment(plan, "Read")
    assert reader_nid is not None, "expected a Read/Scan node in the physical plan"

    # A Filter must feed directly into the reader's only consumer, i.e. sit
    # immediately above the Scan -- proof it was pushed all the way down
    # through both the inner Project and the inner GROUP BY, rather than
    # being restored near its original position at the top of the plan.
    outgoing = list(plan.outgoing_edges(reader_nid))
    assert len(outgoing) == 1, f"expected the reader to feed exactly one consumer, got {outgoing}"
    consumer_nid = outgoing[0][1]
    assert "Filter" in type(plan[consumer_nid]).__name__, (
        f"expected a Filter directly above the Scan, found "
        f"{type(plan[consumer_nid]).__name__}"
    )


def test_passthrough_filter_correctness_through_nested_aggregate_and_project():
    """Same shape as above; check the actual row values, not just the plan."""
    sql = """
    SELECT billing_account, SUM(total_queries) AS total_queries
    FROM (
        SELECT billing_account, TRUNC(billing_hour, 'day') AS billing_date, COUNT(*) AS total_queries
        FROM (SELECT name AS billing_account, FROM_UNIXTIME(id * 3600) AS billing_hour FROM $planets) AS inner_t
        GROUP BY billing_account, TRUNC(billing_hour, 'day')
    ) AS mid_t
    WHERE billing_account = 'Earth'
    GROUP BY ALL
    """
    session = opteryx.session()
    try:
        morsels = list(session.execute_to_morsels(sql))
        rows = sum(m.num_rows for m in morsels)
        assert rows == 1
        morsel = morsels[0]
        billing_account_col = morsel.column_names.index(b"billing_account")
        assert morsel.column(morsel.column_names[billing_account_col])[0] == "Earth"
    finally:
        session.close()


def test_having_predicate_still_attaches_to_aggregate():
    """HAVING (a predicate with an aggregator) must keep working exactly as
    before -- it's handled by the earlier has_agg branch, untouched by the
    new GROUP BY key gating."""
    sql = """
    SELECT planetId, COUNT(*) AS moons
    FROM testdata.satellites
    GROUP BY planetId
    HAVING COUNT(*) > 1
    """
    session = opteryx.session()
    try:
        morsels = list(session.execute_to_morsels(sql))
        moons_col_name = b"moons"
        for morsel in morsels:
            idx = morsel.column_names.index(moons_col_name)
            col = morsel.column(moons_col_name)
            for i in range(morsel.num_rows):
                assert col[i] > 1, "HAVING COUNT(*) > 1 must exclude groups with <= 1 row"
    finally:
        session.close()


def test_group_by_key_filter_correctness():
    """A plain WHERE on a GROUP BY key column (no view inlining involved)
    must still produce the same result as filtering post-aggregation."""
    pre_filtered = opteryx.session()
    post_filtered = opteryx.session()
    try:
        pre = list(
            pre_filtered.execute_to_morsels(
                "SELECT planetId, COUNT(*) AS moons FROM testdata.satellites "
                "WHERE planetId = 5 GROUP BY planetId"
            )
        )
        post = list(
            post_filtered.execute_to_morsels(
                "SELECT planetId, moons FROM ("
                "SELECT planetId, COUNT(*) AS moons FROM testdata.satellites GROUP BY planetId"
                ") AS t WHERE planetId = 5"
            )
        )
        pre_rows = sum(m.num_rows for m in pre)
        post_rows = sum(m.num_rows for m in post)
        assert pre_rows == post_rows == 1
    finally:
        pre_filtered.close()
        post_filtered.close()


def test_trunc_alias_predicate_pushes_through_project_to_scan():
    """WHERE on a TRUNC(col, unit) alias defined by a plain Project must be
    substituted for the underlying TRUNC expression and folded (via the
    existing rewrite_date_trunc_to_range) into a range predicate on the raw
    column, landing directly above the Scan/Reader."""
    sql = """
    SELECT billing_account, billing_date, total_queries
    FROM (SELECT name AS billing_account, TRUNC(FROM_UNIXTIME(id * 3600), 'day') AS billing_date, id AS total_queries FROM $planets) AS t
    WHERE billing_date >= CAST('1970-01-01' AS TIMESTAMP)
    """
    session = opteryx.session()
    try:
        morsels = list(session.execute_to_morsels(sql))
        assert sum(m.num_rows for m in morsels) == 9

        plan = session._plan
        reader_nid = _find_node_by_class_fragment(plan, "Read")
        assert reader_nid is not None
        outgoing = list(plan.outgoing_edges(reader_nid))
        assert len(outgoing) == 1
        assert "Filter" in type(plan[outgoing[0][1]]).__name__, (
            "expected the TRUNC-alias predicate to be rewritten onto the raw "
            "column and pushed directly above the Scan"
        )

        telemetry = session._telemetry
        assert telemetry.optimization_predicate_pushdown_trunc_alias_inline >= 1
        assert telemetry.optimization_predicate_pushdown_trunc_alias_inline_declined == 0
    finally:
        session.close()


def test_trunc_alias_range_pushes_below_nested_aggregate():
    """The nested-aggregate variant: the TRUNC alias is a GROUP BY key of an
    inner aggregate, wrapped by a passthrough Project. The date-range WHERE on
    it must be substituted for a unit-aligned range on the raw underlying
    column and deep-restored to PRE-AGGREGATION position (directly above the
    Project that emits that raw column, below the inner aggregate) -- not left
    stranded above the aggregate. This is the production billing-query shape."""
    sql = """
    SELECT
        billing_account,
        TRUNC(billing_date, 'days') AS billing_date,
        SUM(total_queries) AS total_queries
    FROM (
        SELECT billing_account, TRUNC(billing_hour, 'day') AS billing_date, COUNT(*) AS total_queries
        FROM (SELECT name AS billing_account, FROM_UNIXTIME(id * 3600) AS billing_hour FROM $planets) AS inner_t
        GROUP BY billing_account, TRUNC(billing_hour, 'day')
    ) AS query_count_billing
    WHERE billing_account = 'Earth'
        AND billing_date >= CAST('1970-01-01' AS TIMESTAMP)
        AND billing_date < CAST('1970-02-01' AS TIMESTAMP)
    GROUP BY ALL
    ORDER BY billing_date
    """
    session = opteryx.session()
    try:
        morsels = list(session.execute_to_morsels(sql))
        assert sum(m.num_rows for m in morsels) == 1

        telemetry = session._telemetry
        assert telemetry.optimization_predicate_pushdown_trunc_alias_inline >= 1
        assert telemetry.optimization_predicate_pushdown_deep_restore >= 1
        assert telemetry.optimization_predicate_pushdown_trunc_alias_inline_declined == 0

        # The deep-restored range filter must sit directly below the inner
        # aggregate (pre-aggregation), i.e. the inner GroupedAggregate's only
        # provider is a Filter.
        plan = session._plan
        inner_aggs = [
            nid for nid in plan.nodes() if "Aggregate" in type(plan[nid]).__name__
        ]
        assert inner_aggs, "expected at least one aggregate in the plan"
        # The deepest aggregate (closest to the reader) is the inner one.
        reader_nid = _find_node_by_class_fragment(plan, "Read")

        def _depth_from_reader(nid):
            # crude BFS distance from reader following provider->consumer edges
            seen = {reader_nid}
            frontier = [reader_nid]
            d = 0
            while frontier:
                nxt = []
                for n in frontier:
                    for _p, tgt, _r in plan.outgoing_edges(n):
                        if tgt == nid:
                            return d + 1
                        if tgt not in seen:
                            seen.add(tgt)
                            nxt.append(tgt)
                frontier = nxt
                d += 1
            return 10**9

        inner_agg = min(inner_aggs, key=_depth_from_reader)
        provider = list(plan.ingoing_edges(inner_agg))
        assert len(provider) == 1
        assert "Filter" in type(plan[provider[0][0]]).__name__, (
            "expected the rewritten date-range filter directly below the inner "
            f"aggregate, found {type(plan[provider[0][0]]).__name__}"
        )
    finally:
        session.close()


def test_trunc_alias_range_below_aggregate_correctness():
    """Discriminating (multi-day) range: the pre-aggregation-pushed result must
    equal the same range applied directly to the raw column."""
    pushed_sql = """
    SELECT billing_account, billing_date, total_queries
    FROM (
        SELECT billing_account, TRUNC(billing_hour, 'day') AS billing_date, COUNT(*) AS total_queries
        FROM (SELECT name AS billing_account, FROM_UNIXTIME(id * 86400) AS billing_hour FROM $planets) AS inner_t
        GROUP BY billing_account, TRUNC(billing_hour, 'day')
    ) AS query_count_billing
    WHERE billing_date >= CAST('1970-01-04' AS TIMESTAMP) AND billing_date < CAST('1970-01-07' AS TIMESTAMP)
    """
    reference_sql = """
    SELECT billing_account, TRUNC(billing_hour, 'day') AS billing_date, COUNT(*) AS total_queries
    FROM (SELECT name AS billing_account, FROM_UNIXTIME(id * 86400) AS billing_hour FROM $planets) AS inner_t
    WHERE billing_hour >= CAST('1970-01-04' AS TIMESTAMP) AND billing_hour < CAST('1970-01-07' AS TIMESTAMP)
    GROUP BY billing_account, TRUNC(billing_hour, 'day')
    """

    def _rows(sql):
        session = opteryx.session()
        try:
            out = []
            for m in session.execute_to_morsels(sql):
                for i in range(m.num_rows):
                    out.append(tuple(str(m.column(n)[i]) for n in m.column_names))
            return sorted(out)
        finally:
            session.close()

    assert _rows(pushed_sql) == _rows(reference_sql)


if __name__ == "__main__":  # pragma: no cover
    test_passthrough_filter_reaches_scan_through_nested_aggregate_and_project()
    test_passthrough_filter_correctness_through_nested_aggregate_and_project()
    test_having_predicate_still_attaches_to_aggregate()
    test_group_by_key_filter_correctness()
    test_trunc_alias_predicate_pushes_through_project_to_scan()
    test_trunc_alias_range_pushes_below_nested_aggregate()
    test_trunc_alias_range_below_aggregate_correctness()
    print("All predicate-pushdown Project/Aggregate gap tests passed.")
