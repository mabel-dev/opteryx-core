# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Regression: rewrite_date_trunc_to_range (predicate_rewriter.py) must correctly
interpret a folded temporal literal.

Once ConstantFolding evaluates `CAST('...' AS TIMESTAMP)`, the literal is an
INTEGER in the column's native unit (microseconds since epoch). parse_iso would
misread such an integer as epoch SECONDS -- fine near 1970 by coincidence, but
for any realistic (post-2000) date the value is ~1e15+, read-as-seconds lands
tens of millions of years out and overflows datetime, so parse_iso returned
None and the whole TRUNC->range rewrite silently aborted. These tests exercise a
~2023 date, where the seconds/microseconds distinction actually bites.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx


def _rows(sql):
    session = opteryx.session()
    try:
        out = []
        for morsel in session.execute_to_morsels(sql):
            for i in range(morsel.num_rows):
                out.append(tuple(str(morsel.column(n)[i]) for n in morsel.column_names))
        return sorted(out), session._telemetry
    finally:
        session.close()


# Synthesize distinct days in ~2023 (large microsecond literals after folding):
# FROM_UNIXTIME(1700000000 + id*86400), id 1..9 -> 2023-11-14 .. 2023-11-22.
_RANGE_SQL = """
SELECT TRUNC(ts, 'day') AS d, COUNT(*) AS c
FROM (SELECT FROM_UNIXTIME(1700000000 + id * 86400) AS ts FROM $planets) AS t
WHERE TRUNC(ts, 'day') >= CAST('2023-11-16' AS TIMESTAMP)
  AND TRUNC(ts, 'day') <  CAST('2023-11-19' AS TIMESTAMP)
GROUP BY TRUNC(ts, 'day')
"""

# Same rows, but filtering the raw column directly (no TRUNC on the bound).
_REFERENCE_SQL = """
SELECT TRUNC(ts, 'day') AS d, COUNT(*) AS c
FROM (SELECT FROM_UNIXTIME(1700000000 + id * 86400) AS ts FROM $planets) AS t
WHERE ts >= CAST('2023-11-16' AS TIMESTAMP) AND ts < CAST('2023-11-19' AS TIMESTAMP)
GROUP BY TRUNC(ts, 'day')
"""


def test_date_trunc_range_rewrite_fires_for_realistic_timestamp():
    """The rewrite must actually fire (not silently decline) on a large,
    already-folded microsecond literal."""
    _, telemetry = _rows(_RANGE_SQL)
    # one increment per rewritten bound (>= and <)
    assert telemetry.optimization_predicate_rewriter_date_trunc_to_range >= 2


def test_date_trunc_range_rewrite_correctness_for_realistic_timestamp():
    """TRUNC(col,'day') in [a, b) must select exactly the days a raw-column
    range on the same bounds selects."""
    range_rows, _ = _rows(_RANGE_SQL)
    reference_rows, _ = _rows(_REFERENCE_SQL)
    assert range_rows == reference_rows
    assert len(range_rows) == 3  # 2023-11-16, -17, -18


if __name__ == "__main__":  # pragma: no cover
    test_date_trunc_range_rewrite_fires_for_realistic_timestamp()
    test_date_trunc_range_rewrite_correctness_for_realistic_timestamp()
    print("date-trunc range unit tests passed.")
