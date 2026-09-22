# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
"""
Non tabular results are usually the result of management tasks, like CREATE or DELETE
they tend to be the result of actions that operate on the structure of the data rather
than data elements themselves.
"""

from typing import Optional

from opteryx.constants import QueryStatus
from opteryx.exceptions import md_code


class NonTabularResult:
    """
    Class to encapsulate non-tabular query results.
    """

    def __init__(
        self,
        record_count: int = None,
        status: QueryStatus = QueryStatus._UNDEFINED,
        message: Optional[str] = None,
    ):
        self.record_count = record_count
        self.status = status
        # What the statement DID, in a sentence for the person who ran it.
        # `record_count` alone cannot say it: a count of 1 is the same number
        # for "one row deleted" and "table created", and a batch keeps only the
        # last statement's count. Surfaced through telemetry.messages by
        # managers/execution/__init__.py - the one place every non-tabular
        # result passes through - so no operator has to reach for telemetry.
        self.message = message


def row_count_phrase(count: int, verb: str) -> str:
    """`14 rows deleted` - the count half of a receipt, with no relation on it.

    Separate from `rows_message` because MERGE reports its three arms in one
    sentence ("3 rows inserted, 1 row deleted in `x`") and naming the relation
    once per arm would be unreadable. Both spellings of the count come from
    here, so the singular can never be right in one and wrong in the other.
    """
    noun = "row" if count == 1 else "rows"
    return f"{count:,} {noun} {verb}"


def rows_message(count: int, verb: str, relation: str, preposition: str = "in") -> str:
    """`14 rows deleted in `x`` - the receipt for a statement that moved rows.

    `count` is what the engine COUNTED, never an estimate, so a zero is
    reported rather than suppressed: "0 rows deleted" is the answer to a
    predicate that matched nothing, and saying nothing at all would leave the
    reader unable to tell that from a statement that never ran.
    """
    return f"{row_count_phrase(count, verb)} {preposition} {md_code(relation)}"


def object_message(verb: str, kind: str, name: str, detail: Optional[str] = None) -> str:
    """``created table `x`` - the receipt for a statement that acted on an object.

    `kind` names what the object IS to the reader (table, view, task), which is
    not always what the engine calls the action - DROP MATERIALIZED VIEW is a
    flagged `drop_relation`, and reporting it as a table would describe the
    wrong thing.
    """
    # `kind` is optional, not always-present-but-sometimes-blank: COMMENT ON
    # acts on a view or a table and the statement does not say which, so the
    # receipt names the object and claims nothing about what it is.
    parts = [part for part in (verb, kind, md_code(name), detail) if part]
    return " ".join(parts)
