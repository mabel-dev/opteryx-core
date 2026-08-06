"""P0.3 — uint64 compare validity-helper contract.

u64c_copy_validity must mask the partial last byte (padding bits from the
source buffer must not leak into result validity) and u64c_and_validity must
normalise an all-valid AND to the no-nulls representation, mirroring
cmp_copy_validity / cmp_and_validity in int64_compare.h.

Exercised end-to-end: nullable UINT64 parquet columns at 9 and 17 rows (one
and two full validity bytes plus a 1-bit tail), with values above INT64_MAX so
the unsigned kernels — not a signed fallback — answer the predicate.
"""

import os
import sys
import tempfile

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pyarrow as pa
import pyarrow.parquet as pq

import opteryx

BIG = 18446744073709551615  # UINT64_MAX — unrepresentable as INT64
MID = 9223372036854775808   # INT64_MAX + 1


def _write(dataset_dir, columns):
    os.makedirs(dataset_dir, exist_ok=True)
    arrays = {name: pa.array(vals, type=typ) for name, (typ, vals) in columns.items()}
    pq.write_table(pa.table(arrays), os.path.join(dataset_dir, "part.parquet"))
    return dataset_dir


def _count(sql):
    session = opteryx.session()
    try:
        vals = []
        for morsel in session.execute_to_morsels(sql):
            col = morsel.column("r")
            vals.extend(col.to_pylist())
        assert len(vals) == 1, vals
        return vals[0]
    finally:
        session.close()


def test_nullable_uint64_predicates_odd_row_counts():
    with tempfile.TemporaryDirectory() as tmp:
        # 9 rows: validity = 1 full byte + 1-bit tail. 2 nulls.
        nine = _write(
            os.path.join(tmp, "nine"),
            {"u": (pa.uint64(), [1, None, 3, 4, None, MID, 7, 8, BIG])},
        )
        # 17 rows: 2 full bytes + 1-bit tail; null in the tail position.
        seventeen = _write(
            os.path.join(tmp, "seventeen"),
            {"u": (pa.uint64(), [1, 2, 3, 4, 5, 6, 7, 8,
                                  9, 10, 11, 12, 13, MID, 15, BIG, None])},
        )

        assert _count(f"SELECT COUNT(*) AS r FROM '{nine}' WHERE u > 4") == 4
        assert _count(f"SELECT COUNT(*) AS r FROM '{nine}' WHERE u >= 9223372036854775808") == 2
        assert _count(f"SELECT COUNT(*) AS r FROM '{nine}' WHERE u IS NULL") == 2
        assert _count(f"SELECT COUNT(*) AS r FROM '{seventeen}' WHERE u > 8") == 8
        assert _count(f"SELECT COUNT(*) AS r FROM '{seventeen}' WHERE u <= 8") == 8
        assert _count(f"SELECT COUNT(*) AS r FROM '{seventeen}' WHERE u IS NULL") == 1
        # BETWEEN drives u64_between (both bound compares AND'd).
        assert _count(
            f"SELECT COUNT(*) AS r FROM '{seventeen}' WHERE u BETWEEN 4 AND 9223372036854775808"
        ) == 12  # 4..13, 15, and MID itself


if __name__ == "__main__":
    test_nullable_uint64_predicates_odd_row_counts()
    print("✅ okay")
