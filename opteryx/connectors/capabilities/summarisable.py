# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Summarisable
------------

Capability for a connector that can describe its OWN relations - how many rows,
how much storage, how they are ordered - without a metastore being asked.

`information_schema.tables` derives every metadata column from the catalog: it
loads each dataset document and reads its current snapshot. A workspace bound
to a connector with NO metastore (`postgres`, say - see the resolution chain's
KINDS allowlist) has no dataset document and no snapshot, so every one of those
columns came back NULL while the relation itself queried perfectly. The gap was
structural rather than an oversight, and this capability is the second path
that closes it: the connector already talks to the server that owns the
relation, so it is the thing that can answer.

The contract is deliberately BATCHED - one call per schema, not per relation.
`information_schema.tables` over a workspace with hundreds of relations must
not become hundreds of round trips, and every source this is likely to serve
(a PostgreSQL `pg_class`, a MySQL `information_schema.TABLES`) can answer for a
whole schema in one statement.

Everything on `RelationSummary` is OPTIONAL and independently degradable. A
field a source cannot answer honestly stays None; None means "this source does
not say", which is the only truthful rendering, and is never to be filled with
a plausible-looking substitute. `snapshot_id`, `snapshot_sequence_id` and
`table_file_count` have no place here at all - they describe a snapshot-based
store, and a remote relation has no snapshots and no files of ours - so they
are not fields and `information_schema` keeps reporting them NULL.

Nothing here is authoritative for reading. A query resolves its schema and its
rows through the connector's ordinary path; this is a LISTING projection,
gathered when someone asks the catalogue what it holds.
"""

import dataclasses
import datetime
from typing import Dict
from typing import Optional
from typing import Sequence


@dataclasses.dataclass
class RelationSummary:
    """One relation's listing metadata, as its own source reports it.

    Every field is None unless the source really knows it - see the module
    docstring on why a None here is a better answer than an approximation.
    """

    # Rows. `record_count_is_estimate` travels WITH the number rather than being
    # implied by the connector type, because the same column on the native path
    # carries an exact committed count and a reader comparing the two has no
    # other way to tell them apart.
    record_count: Optional[int] = None
    record_count_is_estimate: bool = False
    # Storage the relation occupies at its source, including its indexes and
    # TOAST where the source counts those - it is the source's notion of the
    # relation's size, not a byte count of rows we would read.
    byte_count: Optional[int] = None
    # Rendered the way `information_schema.tables` renders the native column:
    # "<column> ASC" / "<column> DESC".
    sort_order: Optional[str] = None
    # When the relation's data last changed, and ONLY when the source records
    # that. Most SQL servers do not - see PostgresConnector for why the
    # available proxies were refused there.
    updated_at: Optional[datetime.datetime] = None


class Summarisable:
    """Capability for connectors that can summarise their own relations.

    Declared by the CONNECTOR (the long-lived gateway), not the per-query
    table reader, because the contract is per-schema batch.
    """

    provides_relation_summaries = True

    def __init__(self, **kwargs):
        pass

    def relation_summaries(
        self, schema_name: str, relation_names: Sequence[str]
    ) -> Dict[str, "RelationSummary"]:
        """Summarise `relation_names` within `schema_name`, in ONE round trip.

        Returns a mapping keyed by relation name. A name the source cannot
        answer for is simply absent from the mapping - the caller renders it
        exactly as it renders a relation with no source at all - so a partial
        answer is always preferable to raising.
        """
        raise NotImplementedError("relation_summaries must be implemented by subclasses.")
