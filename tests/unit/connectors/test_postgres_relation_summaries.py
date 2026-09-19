"""`PostgresConnector.relation_summaries` - the `Summarisable` contract.

`information_schema.tables` reads its metadata columns off a dataset document's
snapshot. A workspace bound to a PostgreSQL server has neither, so all of them
read NULL for relations that query perfectly. These pin what the connector
answers instead, and - just as much - what it refuses to answer:

* ONE statement for a whole schema. A statement per relation is the cost the
  batch contract exists to avoid, and a listing is where it would bite hardest.
* `reltuples` is the planner's ESTIMATE and travels flagged as one, because the
  native path's record count in that same column is an exact committed total.
* `-1` (never analysed) and `0` are 'unknown', not 'empty' - the same reading
  `_row_estimate` already gives them. A zero SIZE is kept: that is a real
  measurement.
* sort order means the CLUSTER order or nothing. An index is an access path,
  not an ordering of the heap.
* `updated_at` stays None. PostgreSQL records no per-relation modification
  time, and the available proxies answer a different question.
* a server that cannot be reached yields an empty mapping, never an exception:
  listing a workspace must not fail because its server is down.
* a non-PostgreSQL wire-compatible dialect is not answered at all, rather than
  answered with a table of zeroes that look like measurements.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.connectors.postgres_connector import DIALECT_COCKROACH
from opteryx.connectors.postgres_connector import PostgresConnector


def _gateway(dialect="postgres"):
    gateway = PostgresConnector(
        host="h", dbname="d", user="u", password="p", dialect=dialect
    )
    gateway._matched_prefix = "health"
    return gateway


def _wire(rows, recorder=None):
    """`_pg_helpers` answering the summary statement with `rows`."""

    def query_text(config, statement, params):
        if recorder is not None:
            recorder.append((statement, list(params)))
        return rows

    def unused(*_args, **_kwargs):  # pragma: no cover - not reached here
        raise AssertionError("not part of the summary path")

    return lambda: (unused, query_text, unused, unused)


def _install(monkeypatch, rows, recorder=None):
    monkeypatch.setattr(
        "opteryx.connectors.postgres_connector._pg_helpers", _wire(rows, recorder)
    )


def test_the_whole_schema_is_read_in_one_statement(monkeypatch):
    calls = []
    _install(
        monkeypatch,
        [
            ("results", "14820", "2129920", "checked_at DESC"),
            ("runs", "42", "8192", None),
        ],
        calls,
    )

    summaries = _gateway().relation_summaries("probe", ["results", "runs"])

    assert len(calls) == 1
    statement, params = calls[0]
    assert params == ["probe"]
    assert "pg_catalog.pg_class" in statement
    assert set(summaries) == {"results", "runs"}


def test_the_columns_a_server_can_answer(monkeypatch):
    _install(monkeypatch, [("results", "14820", "2129920", "checked_at DESC")])

    summary = _gateway().relation_summaries("probe", ["results"])["results"]

    assert summary.record_count == 14820
    assert summary.record_count_is_estimate is True
    assert summary.byte_count == 2129920
    assert summary.sort_order == "checked_at DESC"


def test_the_row_count_is_flagged_as_an_estimate(monkeypatch):
    """It is `pg_class.reltuples`, and it lands in the column the native path
    fills with an exact committed count. A reader has no other way to tell."""
    _install(monkeypatch, [("results", "14820", "1", None)])

    summary = _gateway().relation_summaries("probe", ["results"])["results"]

    assert summary.record_count_is_estimate is True


def test_an_unanalysed_relation_is_unknown_not_empty(monkeypatch):
    """`-1` is 'never analysed' and `0` is 'no estimate'. Reported as zero rows,
    either would say a populated table is empty."""
    _install(
        monkeypatch,
        [("never_analysed", "-1", "16384", None), ("no_estimate", "0", "16384", None)],
    )

    summaries = _gateway().relation_summaries("probe", ["never_analysed", "no_estimate"])

    assert summaries["never_analysed"].record_count is None
    assert summaries["no_estimate"].record_count is None
    # The SIZE is still a measurement, and both relations really do occupy that.
    assert summaries["never_analysed"].byte_count == 16384


def test_a_zero_size_is_a_measurement(monkeypatch):
    """A truncated relation, or a partitioned parent whose partitions hold
    everything, genuinely occupies nothing."""
    _install(monkeypatch, [("empty", "-1", "0", None)])

    assert _gateway().relation_summaries("probe", ["empty"])["empty"].byte_count == 0


def test_a_relation_with_no_storage_reports_no_size(monkeypatch):
    """The statement's CASE returns NULL for a view or foreign table rather than
    calling `pg_total_relation_size` on one."""
    _install(monkeypatch, [("a_view", "-1", None, None)])

    summary = _gateway().relation_summaries("probe", ["a_view"])["a_view"]

    assert summary.byte_count is None
    assert summary.record_count is None


def test_updated_at_is_never_claimed(monkeypatch):
    """PostgreSQL keeps no per-relation modification time. The usual stand-in -
    `last_autoanalyze` - says when the statistics collector ran, which moves
    without the data changing and stays still while it does."""
    _install(monkeypatch, [("results", "14820", "2129920", "checked_at DESC")])

    assert _gateway().relation_summaries("probe", ["results"])["results"].updated_at is None


def test_a_relation_that_was_not_asked_about_is_dropped(monkeypatch):
    """The statement reads the whole schema in one pass - one round trip is the
    point - so the requested names are the filter on the way out."""
    _install(
        monkeypatch,
        [("results", "1", "1", None), ("someone_elses_table", "1", "1", None)],
    )

    summaries = _gateway().relation_summaries("probe", ["results"])

    assert set(summaries) == {"results"}


def test_names_are_matched_case_insensitively(monkeypatch):
    """Opteryx lowercases relation names at bind; PostgreSQL folds unquoted
    identifiers the same way, but a quoted mixed-case relation does not."""
    _install(monkeypatch, [("Results", "7", "1", None)])

    summaries = _gateway().relation_summaries("probe", ["results"])

    assert summaries["results"].record_count == 7


def test_an_unreachable_server_yields_nothing_rather_than_raising(monkeypatch):
    def exploding():
        def query_text(*_args, **_kwargs):
            raise RuntimeError("[08006] connection failure")

        return None, query_text, None, None

    monkeypatch.setattr("opteryx.connectors.postgres_connector._pg_helpers", exploding)

    assert _gateway().relation_summaries("probe", ["results"]) == {}


def test_a_non_postgres_dialect_is_not_answered(monkeypatch):
    """CockroachDB leaves `pg_class.reltuples` NULL for every relation and keeps
    no PostgreSQL-shaped catalogue statistics. Running the statement there would
    produce a table of nulls and zeroes shaped exactly like measurements."""
    calls = []
    _install(monkeypatch, [("results", "1", "1", None)], calls)

    assert _gateway(DIALECT_COCKROACH).relation_summaries("probe", ["results"]) == {}
    assert calls == []


def test_nothing_is_asked_for_an_empty_name_list(monkeypatch):
    calls = []
    _install(monkeypatch, [], calls)

    assert _gateway().relation_summaries("probe", []) == {}
    assert calls == []


def test_the_capability_is_declared(monkeypatch):
    """`information_schema` gates on the capability, never on the class."""
    assert _gateway().provides_relation_summaries is True
