# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
A dictionary-encoded DATE column reads back as real dates.

This used to call `ParquetReadNode._cast_table_to_schema(table, schema)` — a
PyArrow `Table` → `pyarrow.schema` cast helper on the read node. That helper is
gone: PyArrow is banned from the engine (CLAUDE.md §4), and the temporal retag is
now native (`vector_reinterpret_as_date32` in parquet_read.pyx), applied per
vector rather than per Arrow table. There is no private method left to unit-test.

Re-pointed at the behaviour that actually mattered — dictionary encoding on a date
column must not corrupt the values on the way out — asserted end-to-end through a
real read, which is a stronger statement than the old helper-level one. PyArrow is
used only to WRITE the fixture, which is sanctioned in tests.
"""

import datetime
import os
import sys
import tempfile

import pyarrow
import pyarrow.parquet as pq

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx

_EPOCH = datetime.date(1970, 1, 1)
# the repeated 19001 forces a real dictionary code reuse rather than a 1:1 map
_DAYS = [19000, 19001, 19002, 19001]


def test_dictionary_encoded_date_column_reads_as_dates():
    dictionary = pyarrow.DictionaryArray.from_arrays(
        pyarrow.array([0, 1, 2, 1], type=pyarrow.int32()),
        pyarrow.array([19000, 19001, 19002], type=pyarrow.int32()),
    )
    table = pyarrow.table({"birth_date": dictionary.cast(pyarrow.date32())})

    with tempfile.TemporaryDirectory() as tmp:
        dataset = os.path.join(tmp, "t")
        os.makedirs(dataset)
        pq.write_table(table, os.path.join(dataset, "p.parquet"), use_dictionary=True)

        session = opteryx.session()
        values, types = [], None
        for morsel in session.execute_to_morsels("SELECT birth_date FROM '%s'" % dataset):
            if morsel.num_rows:
                types = morsel.column_types
                values += morsel.column(b"birth_date").to_pylist()

    import draken.draken_native as dn

    assert types == [dn.DrakenType.DATE32], types
    assert values == [_EPOCH + datetime.timedelta(days=d) for d in _DAYS], values


if __name__ == "__main__":  # pragma: no cover
    test_dictionary_encoded_date_column_reads_as_dates()
    print("✅ okay")
