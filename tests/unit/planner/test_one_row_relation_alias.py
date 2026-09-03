# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`$one_row` — the one-row source behind a statement with no FROM clause.

It was called `$no_table` until Sep 2026. The name is SQL-reachable, so the old
one still resolves; what must NOT survive is the old name leaking past the
binder, because everything downstream — the plan diagram, telemetry, billing —
identifies the relation by the name bound here.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.connectors.virtual_data_connector import canonical_dataset_name
from opteryx.managers.virtual_datasets import one_row_data


def test_the_relation_is_named_for_what_it_emits():
    assert one_row_data.schema().name == "$one_row"
    assert one_row_data.schema().row_count_metric == 1


def test_the_old_name_still_resolves():
    assert canonical_dataset_name("$no_table") == "$one_row"
    assert canonical_dataset_name("$one_row") == "$one_row"
    assert canonical_dataset_name("$planets") == "$planets"


def _rows(statement):
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(statement))


def test_both_names_read_the_same_single_row():
    assert _rows("SELECT * FROM $one_row") == 1
    assert _rows("SELECT * FROM $no_table") == 1


def test_the_old_name_does_not_survive_binding():
    """A query written against the old name must still bind to the canonical one.

    Everything downstream identifies the relation by the bound name, so an
    un-normalized `$no_table` would reach the plan diagram and the billing
    event as a second name for one relation. The unaliased form is the one that
    matters: it carries the typed name as its alias too, and would otherwise
    print as "$one_row AS $no_table".
    """
    session = opteryx.session()
    rendered = "".join(
        str(morsel) for morsel in session.execute_to_morsels("EXPLAIN SELECT * FROM $no_table")
    )
    assert "$one_row" in rendered, rendered
    assert "$no_table" not in rendered, rendered


def test_a_user_chosen_alias_is_left_alone():
    """Only the old name is rewritten — an alias the user picked is theirs."""
    session = opteryx.session()
    rendered = "".join(
        str(morsel)
        for morsel in session.execute_to_morsels("EXPLAIN SELECT * FROM $no_table AS t")
    )
    assert "$one_row AS t" in rendered, rendered


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
