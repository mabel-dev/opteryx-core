# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Every DML/DDL statement says what it DID, in `session.messages`.

`rowcount` cannot answer this on its own: a count of 1 is the same number for
"one row deleted" and "table created", and a semicolon-separated batch keeps
only the last statement's count. The receipt names the verb and the object, and
is surfaced per statement - so a batch reports one line per statement.

The zero cases are asserted as hard as the acting ones: "no table(s) dropped"
distinguishes an IF EXISTS that matched nothing from a statement that never
ran, and suppressing it would make those two indistinguishable.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx  # noqa: E402

from tests.storage.test_ctas import _setup_workspace  # noqa: E402


def _messages(sql: str):
    session = opteryx.session()
    list(session.execute_to_morsels(sql))
    return session.messages


def test_create_table_names_what_it_created(tmp_path):
    _setup_workspace(tmp_path)
    assert _messages("CREATE TABLE ws.src (a BIGINT)") == ["created table `ws.src`"]


def test_insert_counts_the_rows(tmp_path):
    _setup_workspace(tmp_path)
    _messages("CREATE TABLE ws.src (a BIGINT)")
    assert _messages("INSERT INTO ws.src VALUES (-1), (1), (2)") == [
        "3 rows inserted into `ws.src`"
    ]


def test_insert_of_one_row_is_singular(tmp_path):
    _setup_workspace(tmp_path)
    _messages("CREATE TABLE ws.src (a BIGINT)")
    assert _messages("INSERT INTO ws.src VALUES (1)") == ["1 row inserted into `ws.src`"]


def test_ctas_reports_the_create_and_the_rows(tmp_path):
    _setup_workspace(tmp_path)
    assert _messages("CREATE TABLE ws.dst AS SELECT 1 AS a, 2 AS b") == [
        "created table `ws.dst`, 1 row written"
    ]


def test_column_ddl_names_the_column(tmp_path):
    _setup_workspace(tmp_path)
    _messages("CREATE TABLE ws.src (a BIGINT)")
    assert _messages("ALTER TABLE ws.src ADD COLUMN b VARCHAR") == [
        "added column *b* to `ws.src`"
    ]
    assert _messages("ALTER TABLE ws.src RENAME COLUMN b TO c") == [
        "renamed column *b* to *c* in `ws.src`"
    ]


def test_drop_view_names_every_view(tmp_path):
    _setup_workspace(tmp_path)
    _messages("CREATE TABLE ws.src (a BIGINT)")
    _messages("CREATE VIEW ws.v AS SELECT a FROM ws.src")
    assert _messages("DROP VIEW ws.v") == ["dropped 1 view(s): `ws.v`"]


def test_if_exists_that_matched_nothing_still_reports(tmp_path):
    """The statement ran and did nothing. Silence here would read the same as a
    statement that never ran at all."""
    _setup_workspace(tmp_path)
    assert _messages("DROP TABLE IF EXISTS ws.gone") == ["no table(s) dropped"]


def test_a_batch_reports_every_statement(tmp_path):
    """`_execute_statements` returns only the LAST statement's result, so this
    is the case a receipt read off the result object could never cover."""
    _setup_workspace(tmp_path)
    assert _messages(
        "CREATE TABLE ws.one (a BIGINT); CREATE TABLE ws.two (a BIGINT);"
    ) == ["created table `ws.one`", "created table `ws.two`"]
