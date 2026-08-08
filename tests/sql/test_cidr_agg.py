"""CIDR_AGG — the minimal CIDR cover of the addresses in a column.

The aggregate collects into a Roaring bitmap (src/cpp/engine/native_roaring32.hpp)
and emits the minimal list of CIDR blocks covering exactly the addresses seen
(src/cpp/engine/native_cidr_emit.hpp). It works grouped AND ungrouped.

WHY THE ASSERTIONS LOOK LIKE THIS. The minimal exact cover of a set of addresses
is UNIQUE — it is the set of maximal full nodes of the binary trie over the
address space — so there is one right answer and it can be checked by property
rather than by pinning a literal list:

  * the block sizes must sum to the DISTINCT address count (covers exactly, no
    more and no less), and
  * no two adjacent blocks may be mergeable buddies (every block is maximal).

Those two together mean the output IS the minimal cover. Pinning a hand-written
list instead would pass just as well for an implementation that emitted a
correct-but-not-minimal cover.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx

# 10.0.0.0 — lets an integer column stand in for addresses without CONCAT
# (which is not in the native kernel set).
BASE = 0x0A000000


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _parse(block):
    """'10.0.0.8/29' -> (base_int, prefix)."""
    address, _, prefix = block.partition("/")
    octets = [int(part) for part in address.split(".")]
    value = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3]
    return value, int(prefix)


def _assert_minimal_cover(blocks, expected_addresses):
    parsed = [_parse(b) for b in blocks]

    covered = sum(1 << (32 - prefix) for _, prefix in parsed)
    assert covered == expected_addresses, (covered, expected_addresses, blocks)

    for base, prefix in parsed:
        size = 1 << (32 - prefix)
        assert base % size == 0, f"{base}/{prefix} is not aligned to its own size"

    previous_end = None
    for base, prefix in parsed:
        size = 1 << (32 - prefix)
        if previous_end is not None:
            assert base > previous_end, f"blocks overlap or are unsorted: {blocks}"
        previous_end = base + size - 1

    for (base_a, prefix_a), (base_b, prefix_b) in zip(parsed, parsed[1:]):
        if prefix_a != prefix_b or prefix_a == 0:
            continue
        size = 1 << (32 - prefix_a)
        mergeable = base_b == base_a + size and base_a % (size * 2) == 0
        assert not mergeable, f"{blocks} contains a mergeable pair — not maximal"


def test_ungrouped_covers_the_whole_column():
    """No GROUP BY: the entire column collapses to one CIDR list.

    This is the case ARRAY_AGG cannot do — its list lives in the fixed-width
    ungrouped AggCell. CIDR_AGG's Roaring state sits in a side-vector parallel to
    the cells instead, the same shape MEDIAN and the sketches already use.
    """
    rows = _rows(f"SELECT CIDR_AGG(CAST(id + {BASE} AS IPV4)) FROM $planets")
    assert len(rows) == 1, rows
    _assert_minimal_cover(rows[0][0], 9)   # $planets has ids 1..9


def test_contiguous_run_folds():
    """Addresses 0..8 fold to a /29 plus a /32, not nine /32s."""
    rows = _rows(
        f"SELECT CIDR_AGG(CAST(v + {BASE} AS IPV4)) "
        f"FROM (SELECT id - 1 AS v FROM $planets) AS t"
    )
    assert rows[0][0] == ["10.0.0.0/29", "10.0.0.8/32"], rows[0][0]
    _assert_minimal_cover(rows[0][0], 9)


def test_grouped_is_minimal_per_group():
    rows = _rows(
        f"SELECT planetId, COUNT(*) AS n, CIDR_AGG(CAST(id + {BASE} AS IPV4)) AS blocks "
        f"FROM testdata.satellites GROUP BY planetId"
    )
    assert len(rows) > 1, rows
    for _, n, blocks in rows:
        # satellite ids are distinct, so the row count IS the distinct count
        _assert_minimal_cover(blocks, n)


def test_duplicate_addresses_are_free():
    """The set dedups on insert, so repeats change neither the answer nor the cost.

    A CROSS JOIN multiplies every address nine-fold; the cover must be identical
    to the un-multiplied one.
    """
    plain = _rows(f"SELECT CIDR_AGG(CAST(id + {BASE} AS IPV4)) FROM $planets")[0][0]
    repeated = _rows(
        f"SELECT CIDR_AGG(CAST(p.id + {BASE} AS IPV4)) "
        f"FROM $planets AS p CROSS JOIN $planets AS q"
    )[0][0]
    assert plain == repeated, (plain, repeated)


def test_nulls_are_not_members():
    """A NULL is not an address, so it is not in the set.

    This differs from ARRAY_AGG, which keeps NULLs as elements — a list has a
    slot to hold one, a set does not.
    """
    rows = _rows(
        f"SELECT CIDR_AGG(CAST(CASE WHEN id > 6 THEN id + {BASE} END AS IPV4)) "
        f"FROM $planets"
    )
    _assert_minimal_cover(rows[0][0], 3)   # ids 7, 8, 9 only


def test_non_ipv4_operand_is_refused():
    """A plain integer column is refused rather than folded into fiction.

    The IPV4 descriptor is the only thing separating an address from any other
    32-bit unsigned value; without it CIDR_AGG would emit well-formed, confident,
    entirely invented network ranges.
    """
    from opteryx.exceptions import NotSupportedError

    try:
        _rows("SELECT CIDR_AGG(id) FROM $planets")
    except NotSupportedError as err:
        assert "IPV4" in str(err), str(err)
        return
    raise AssertionError("CIDR_AGG accepted a non-IPV4 operand")


def test_budgets_are_discoverable():
    """Two ceilings, both reported — the state budget bounds the address sets,
    the emit budget bounds the CIDR text, and neither follows from the other."""
    shown = {row[0]: row[1] for row in _rows("SHOW VARIABLES")}
    assert "cidr_agg_state_budget_bytes" in shown, sorted(shown)
    assert "cidr_agg_emit_budget_bytes" in shown, sorted(shown)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
