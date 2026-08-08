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

That tag is no longer trusted. `Pass1PredCtx.col_type` carries the PLAN's DrakenType
for every predicate column, and `opteryx_pass1_predicate_eval` stamps it on the view
before running the program, so the worker evaluates over exactly the operands the
serial fallback would. A tag rugo cannot derive is therefore no longer a reason to
refuse.

What IS still a reason to refuse: a type whose meaning does not fit in the
DrakenVector at all, because it lives in a logical descriptor the scan attaches
alongside the column.

* DECIMAL (int64-backed) and DECIMAL128 carry precision and scale outside the
  vector, so a view of them is a decimal with no scale.
* TIMESTAMP carries a mandatory unit descriptor, same problem.
* DATE32 has no descriptor and would now be admissible; it is left out because this
  gate has only ever been exercised for the types below and widening it wants its
  own measurement, not a free ride on this one.

Refusing is free — the consumer already evaluates the identical program itself
whenever the survivor mask comes back empty.

Both registration sites call this: the native `LatmatScanSource` plan
(`managers/execution/compiler.py::_latmat_scan_plan`) and the trampoline scan
(`operators/parquet_read/parquet_read.pyx`). One rule, not two.
"""

from __future__ import annotations

from draken.draken_native import DrakenType

# Physical types whose whole meaning is the DrakenVector's own type tag, so stamping
# the plan's tag on a worker-side view makes it answer the consumer's question
# exactly. Nothing here carries a logical descriptor.
#
# Integers and floats: `_classify_scan_columns` records logical_coerce 0 for every
# width and signedness, and the consumer wraps them with `draken_type_for(dk)` —
# the same physical-kind→type mapping `pass1_natural_type` applies in rugo.
# TIME32/TIME64: parquet TIME decodes as a plain int stream and no TIME coercion is
# modelled on either scan path, so it reaches output as that int.
# BOOL: self-describing, bit-packed, no descriptor.
# VARCHAR / VARBINARY: one byte layout, two tags, and the tag alone selects the
# semantics (byte-length ops either way; character ops throw on VARBINARY, and the
# stamp is what makes them throw on the worker exactly as they do on the main
# thread). VARBINARY admitted by architect ruling 2026-08-08 — the downloaded
# ClickBench files declare URL as parquet `binary` with no UTF8 annotation, so it
# binds VARBINARY and Q24's whole predicate was running serially. NVARCHAR is
# mechanically safe under the stamp too, and is deliberately NOT added here: it was
# not part of that ruling.
_DESCRIPTOR_FREE_TYPES = frozenset(
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
        DrakenType.VARBINARY,
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
        if ct is None or ct.physical not in _DESCRIPTOR_FREE_TYPES:
            return False
    return True
