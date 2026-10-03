# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Show Indexes Node

This is a SQL Query Execution Plan Node.

Answers `SHOW INDEXES FROM <table>` (docs/VECTOR_INDEX_DESIGN.md §7A, D-15): one row
per vector index - its definition, how many of the head snapshot's live data files it
covers (an async index's lag), and the logical bytes of its files (what is billed,
§5.5). Read from the catalog and the head manifest; no index file is opened. Rows in
index-name order; no index, no rows.
"""

import datetime

from opteryx.exceptions import DatasetNotFoundError
from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.

_COLUMNS = (
    ("name", _draken_native.DrakenType.VARCHAR),
    ("column", _draken_native.DrakenType.VARCHAR),
    ("method", _draken_native.DrakenType.VARCHAR),
    ("metric", _draken_native.DrakenType.VARCHAR),
    ("build", _draken_native.DrakenType.VARCHAR),
    # 0 = the builder's own choice per file (sqrt of its rows).
    ("clusters", _draken_native.DrakenType.INT64),
    ("embedding", _draken_native.DrakenType.VARCHAR),
    ("files_indexed", _draken_native.DrakenType.INT64),
    ("files_total", _draken_native.DrakenType.INT64),
    ("index_bytes", _draken_native.DrakenType.INT64),
    ("created_by", _draken_native.DrakenType.VARCHAR),
    ("created_at", _draken_native.DrakenType.TIMESTAMP64),
)


def _row(status: dict) -> dict:
    return {
        "name": status["name"],
        "column": status["column"],
        "method": status["method"],
        "metric": status["metric"],
        "build": status["build"],
        "clusters": status["clusters"],
        "embedding": status["embedding-identity"],
        "files_indexed": status["files_indexed"],
        "files_total": status["files_total"],
        "index_bytes": status["index_bytes"],
        "created_by": status["created-by"],
        "created_at": datetime.datetime.fromtimestamp(
            status["created-at-ms"] / 1000, tz=datetime.timezone.utc
        ),
    }


class ShowIndexesNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step):
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.relation = step.object_name
        # Bound by visit_show, which authorizes the read first.
        self.connector = step.connector

    @property
    def name(self):  # pragma: no cover
        return "Show Indexes"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        if not self.connector.relation_exists(self.relation):
            raise DatasetNotFoundError(dataset=self.relation, connector="TABLE")
        rows = [_row(status) for status in self.connector.vector_index_status(self.relation)]
        names = [name for name, _ in _COLUMNS]
        vectors = [
            vector_from_sequence([row[name] for row in rows], dtype=dtype) for name, dtype in _COLUMNS
        ]
        yield Morsel.from_vectors(names, vectors)
