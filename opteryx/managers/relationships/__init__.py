# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Declared column relationships - the write seam, deliberately unwired.

=======================================================================
THIS IS A SEAM. The store it writes to does not exist yet, and where it
should live is an OPEN DESIGN QUESTION, not an oversight.
=======================================================================

`ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ... NOT ENFORCED`
records that two columns hold corresponding values. Nothing is enforced: a
write that breaks the relationship succeeds. The declaration exists for tooling
and discovery, and the engine never acts on it.

Parsing, planning and authorization for that statement are complete and land
here. Persistence does not, because the proposal is one account-scoped table
that is deliberately NOT addressable as a dataset - it lives in no workspace's
namespace and cannot be named in a query - and the visibility model depends on
exactly that. Choosing a location for it is a catalog-service decision that has
not been taken (RELATIONSHIP-GRAPH-DESIGN.md, sections 4.1 and 12.1), and the
wrong location is not a detail: put it anywhere queryable and the visibility
guarantee is gone.

So this raises rather than writing somewhere plausible. A statement that
planned, bound and authorized and then said it had nowhere to go is honest; one
that invented a home for the row would be a decision made by omission, in the
place least likely to be reviewed.

Two things settle before this is wired:

  - Where the store lives (section 4.1). Account-scoped and engine-internal is
    proposed.
  - Whether each relationship is stored twice, `forward` and `mirror`, so that
    a store clustered on the near dataset also answers "what points at this
    dataset" (section 3.3). That doubles the rows and makes every consumer
    filter on direction. It is a storage-layer decision and belongs on this
    side of the seam, which is why nothing above it knows about `direction`.

Names arrive here already split into their parts. They are never re-joined into
a dotted string for storage: a dataset name can contain dots and consumers do
not agree where the boundaries are, so the split the parser made is the split
that is kept.
"""

from typing import List


def declare_relationship(
    *,
    relation_parts: List[str],
    column_name: str,
    references_relation_parts: List[str],
    references_column_name: str,
    constraint_name: str,
    cardinality: str,
    author,
) -> None:
    """Record one declared relationship. Not yet implemented - see module docstring."""
    raise NotImplementedError(
        "ALTER TABLE ... ADD CONSTRAINT ... NOT ENFORCED is parsed, planned and authorized, "
        "but the relationship store it writes to does not exist yet: where it physically "
        "lives is an open design decision (RELATIONSHIP-GRAPH-DESIGN.md 4.1/12.1) and "
        "picking a location here would make that decision by accident."
    )


def drop_relationship(
    *,
    relation_parts: List[str],
    constraint_name: str,
    if_exists: bool,
    author,
) -> None:
    """Remove one declared relationship by name. Not yet implemented - see module docstring."""
    raise NotImplementedError(
        "ALTER TABLE ... DROP CONSTRAINT is parsed, planned and authorized, but the "
        "relationship store it removes from does not exist yet: where it physically lives "
        "is an open design decision (RELATIONSHIP-GRAPH-DESIGN.md 4.1/12.1)."
    )
