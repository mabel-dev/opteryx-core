# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
May the pass-1 (Q24 latmat) predicate be evaluated on rugo's decode workers?

rugo runs the pushed pass-1 predicate on the decode worker that produced the row
group (`pass1_run_predicate` / `pass1_build_dv_view`, io_pipeline.hpp) by wrapping
each predicate column's decoded buffers in a NON-owning DrakenVector. It tags that
vector with the buffers' own physical type — the only thing it can know, since rugo
is opteryx-free by contract.

For most columns that is exactly the tag the consumer would use. For some it is
not: the scan RETAGS a column after decode, from plan state rugo has no access to.

* DATE / TIMESTAMP decode as a physical int stream and are retagged DRAKEN_DATE32 /
  DRAKEN_TIMESTAMP64 (the latter with a mandatory unit descriptor).
* DECIMAL (int64-backed) and DECIMAL128 carry precision/scale in a logical
  descriptor attached OUTSIDE the DrakenVector.
* NVARCHAR / VARBINARY share VARCHAR's byte layout but not its semantics — case
  folding, LENGTH and regex all dispatch on the tag.

A worker-side view of one of those would answer a different question than the
serial fallback: not a fast path, a wrong one (CLAUDE.md §11). rugo cannot detect
it, so the decision is made here, where the schema is: the predicate is pushed to
the workers only when EVERY predicate column lands on its natural physical tag.
Refusing is free — the consumer already evaluates the identical program itself
whenever the survivor mask comes back empty.

Both registration sites call this: the native `LatmatScanSource` plan
(`managers/execution/compiler.py::_latmat_scan_plan`) and the trampoline scan
(`operators/parquet_read/parquet_read.pyx`). One rule, not two.
"""

from __future__ import annotations

from draken.draken_native import DrakenType

# Physical types the scan hands downstream with NO retag and NO logical descriptor,
# so the tag rugo derives from the decoded buffers is the tag the consumer uses.
#
# Integers and floats: `_classify_scan_columns` records logical_coerce 0 for every
# width and signedness, and the consumer wraps them with `draken_type_for(dk)` —
# the same physical-kind→type mapping `pass1_natural_type` applies in rugo.
# TIME32/TIME64: parquet TIME decodes as a plain int stream and no TIME coercion is
# modelled on either scan path, so it reaches output as that int — natural too.
# BOOL: self-describing, bit-packed, no descriptor.
# VARCHAR: the default string tag, which is what rugo stamps on a string view.
_NATURAL_TAG_TYPES = frozenset(
    (
        DrakenType.INT8,
        DrakenType.INT16,
        DrakenType.INT32,
        DrakenType.INT64,
        DrakenType.UINT8,
        DrakenType.UINT16,
        DrakenType.UINT32,
        DrakenType.UINT64,
        DrakenType.FLOAT32,
        DrakenType.FLOAT64,
        DrakenType.TIME32,
        DrakenType.TIME64,
        DrakenType.BOOL,
        DrakenType.VARCHAR,
    )
)


def pass1_worker_predicate_admissible(column_types) -> bool:
    """True iff the pass-1 predicate may be pushed to rugo's decode workers.

    `column_types` are the `ColumnType`s of the predicate's columns (any iterable;
    a None entry — an untyped column — refuses). Fail-closed: an unknown or
    descriptor-carrying physical type means no push, and the consumer evaluates the
    predicate serially exactly as it does today for a shape rugo declines.
    """
    for ct in column_types:
        if ct is None or ct.physical not in _NATURAL_TAG_TYPES:
            return False
    return True
