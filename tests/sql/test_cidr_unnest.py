"""CROSS JOIN CIDR_UNNEST — expand CIDR blocks into one row per address.

The inverse of CIDR_AGG. A resumable native operator
(src/cpp/engine/native_cidr_unnest.hpp): one input morsel can cover billions of
addresses, so it emits bounded batches and the executor re-drives it. Memory is
flat at one morsel whatever the prefix length, which is why it has no memory
budget of its own — what it produces is ROWS, governed by sql_select_limit and
the result-size guard.

The strongest assertion available is the ROUND TRIP. The minimal CIDR cover of a
set of addresses is unique, so expanding a block and aggregating the addresses
back MUST return the identical block. That checks both directions at once and
would catch an off-by-one at either end, a lost IPV4 descriptor, or a wrong
netmask — none of which a row count alone would notice.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i])
    return out


def _expand(block, extra=""):
    return _rows(f"SELECT ip FROM $no_table CROSS JOIN CIDR_UNNEST('{block}') AS ip {extra}")


def test_expands_to_one_row_per_address():
    assert [r[0] for r in _expand("10.0.0.0/30")] == [
        "10.0.0.0", "10.0.0.1", "10.0.0.2", "10.0.0.3"
    ]


def test_prefix_lengths_give_the_right_counts():
    """2^(32-prefix) rows, checked across three orders of magnitude.

    The /16 matters beyond arithmetic: it is larger than one output batch, so it
    only returns the right count if the HAVE_MORE cursor resumes correctly
    instead of restarting or stopping at the batch boundary.
    """
    for prefix, expected in ((32, 1), (30, 4), (24, 256), (16, 65536)):
        got = _rows(
            f"SELECT COUNT(*) FROM $no_table "
            f"CROSS JOIN CIDR_UNNEST('172.16.0.0/{prefix}') AS ip"
        )[0][0]
        assert got == expected, (prefix, got, expected)


def test_single_host():
    assert [r[0] for r in _expand("8.8.8.8/32")] == ["8.8.8.8"]


def test_round_trip_through_cidr_agg():
    """Expand a block, aggregate the addresses back, get the same block.

    The unaligned-looking bases are the interesting cases: 10.1.2.4/30 folds back
    to one block only if the buddy alignment is right, and a base whose low bits
    are set would fold into several blocks if the expansion started in the wrong
    place.
    """
    for block in ("10.0.0.0/29", "10.1.2.4/30", "192.168.1.0/24", "8.8.8.8/32"):
        got = _rows(
            f"SELECT CIDR_AGG(ip) FROM $no_table "
            f"CROSS JOIN CIDR_UNNEST('{block}') AS ip"
        )[0][0]
        assert got == [block], (block, got)


def test_output_is_ipv4_typed():
    """The emitted column carries the IPV4 descriptor, not bare UINT32.

    Load-bearing rather than cosmetic: without the descriptor the values render
    as integers and CIDR_AGG refuses the column outright, so the round trip above
    could not pass. Asserted directly so a regression names the cause instead of
    surfacing as an unrelated CIDR_AGG type error.
    """
    session = opteryx.session()
    list(session.execute_to_morsels(
        "SELECT ip FROM $no_table CROSS JOIN CIDR_UNNEST('10.0.0.0/30') AS ip"
    ))
    assert [str(c.column_type) for c in session._schema.columns] == ["IPV4"]
    session.close()


def test_composes_with_containment():
    """The expanded column is a real IPV4, so the IP operators apply to it."""
    got = _rows(
        "SELECT COUNT(*) FROM $no_table CROSS JOIN CIDR_UNNEST('10.0.0.0/24') AS ip "
        "WHERE ip <<= '10.0.0.0/28'"
    )[0][0]
    assert got == 16, got


def test_fans_out_across_parent_rows():
    """Each parent row is repeated once per address — the CROSS JOIN contract."""
    got = _rows(
        "SELECT COUNT(*) FROM $planets CROSS JOIN CIDR_UNNEST('191.42.0.0/24') AS ip"
    )[0][0]
    assert got == 9 * 256, got


def test_strict_parsing_rejects_rather_than_guesses():
    """Malformed blocks raise; they never expand to nothing.

    Leading zeros and shorthand are refused deliberately — an access list and a
    parser disagreeing on what '010.1' means is a known security bug class, so
    the engine will not pick a convention. Silently yielding zero rows would be
    worse than either: an allowlist that matched nothing would look empty rather
    than broken.
    """
    for bad in ("010.0.0.0/24", "10.0.0.0/33", "10.0.0.0", "10.0.0.0/", "not-an-ip/24"):
        try:
            _expand(bad)
        except Exception:
            continue
        raise AssertionError(f"CIDR_UNNEST accepted the malformed block {bad!r}")


def test_non_text_source_is_rejected():
    from opteryx.exceptions import IncorrectTypeError

    try:
        _rows("SELECT ip FROM $planets CROSS JOIN CIDR_UNNEST(id) AS ip")
    except IncorrectTypeError as err:
        assert "CIDR_UNNEST" in str(err), str(err)
        return
    raise AssertionError("CIDR_UNNEST accepted a non-text source")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
