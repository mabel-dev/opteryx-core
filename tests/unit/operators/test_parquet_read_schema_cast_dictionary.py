import os
import sys

import pyarrow

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.operators._operators import ParquetReadNode


def test_dictionary_int32_casts_to_date64_via_date32():
    dictionary = pyarrow.DictionaryArray.from_arrays(
        pyarrow.array([0, 1, 2], type=pyarrow.int32()),
        pyarrow.array([19000, 19001, 19002], type=pyarrow.int32()),
    )
    table = pyarrow.table({"birth_date": pyarrow.chunked_array([dictionary])})
    schema = pyarrow.schema([("birth_date", pyarrow.date64())])

    casted = ParquetReadNode._cast_table_to_schema(table, schema)

    assert casted.column("birth_date").type == pyarrow.date64()
    assert casted.column("birth_date").to_pylist() == [
        pyarrow.scalar(19000, type=pyarrow.date32()).as_py(),
        pyarrow.scalar(19001, type=pyarrow.date32()).as_py(),
        pyarrow.scalar(19002, type=pyarrow.date32()).as_py(),
    ]

