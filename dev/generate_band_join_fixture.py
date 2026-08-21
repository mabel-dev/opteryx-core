"""Generate the band-join correlated-filter fixture (`testdata/band_join/`).

CorrelatedFiltersStrategy only fires on a real SCAN — it needs a connector, a
manifest with footer min/max, and propagated statistics — so a VALUES-based
fixture cannot exercise it at all. It also only fires when the two legs' ranges
GENUINELY DIFFER, because `_tightens` (correctly) rejects a push that is a
tautology against the target's own manifest bounds. Both facts shape this data:

* `flows` is NARROW — five rows inside a three-day window, plus one outlier in
  June that sets the upper bound.
* `lookups` is WIDE — a full year — so a range transported from `flows` really
  does exclude rows the lookups scan would otherwise return.

The rows that matter are the WINDOW EDGES. A shifted bound that is off by one
tick, or shifted the wrong way, DROPS JOINED ROWS silently; no row-count test
would notice and no error is raised. Each edge row below sits one microsecond /
one day either side of a bound the optimizer computes, so an arithmetic slip
shows up as a named tag appearing or disappearing.

Two of the boundaries deliberately CROSS A DAY: `flows.a` starts at 00:00:10, so
the -20s shift lands the derived lower bound on the PREVIOUS day at 23:59:50, and
`flows.d` starts at 00:00:05 with lookups on the evening before. A conversion that
went through a date rather than a timestamp gets those wrong.

Regenerate with:  python dev/generate_band_join_fixture.py
"""

import datetime
import os

import pyarrow
import pyarrow.parquet

TARGET = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "testdata", "band_join"
)


def ts(text):
    return datetime.datetime.strptime(text, "%Y-%m-%d %H:%M:%S.%f")


def day(text):
    return datetime.datetime.strptime(text, "%Y-%m-%d").date()


# client, flow_start, flow_day
#
# min(flow_start) = 2024-03-10 00:00:10 -> a -20s band shifts the derived lower
# bound to 2024-03-09 23:59:50, on the PREVIOUS DAY.
# max(flow_start) = 2024-06-01 00:00:00 (the `e` outlier).
# min(flow_day)   = 2024-03-10        -> a -2 day band shifts it to 2024-03-08.
FLOWS = [
    ("a", ts("2024-03-10 00:00:10.000000"), day("2024-03-10")),
    ("b", ts("2024-03-10 12:00:00.000000"), day("2024-03-10")),
    ("c", ts("2024-03-11 23:59:55.000000"), day("2024-03-11")),
    ("d", ts("2024-03-12 00:00:05.000000"), day("2024-03-12")),
    ("e", ts("2024-06-01 00:00:00.000000"), day("2024-06-01")),
]

# client, event_time, event_day, tag
#
# `tag` names what the row is testing, so a failure reads as a sentence rather
# than as a count that moved.
LOOKUPS = [
    # Far outside, both ends — these WIDEN the lookups scan's own range, which is
    # what makes the transported bound a genuine tightening rather than a no-op.
    ("a", ts("2024-01-05 00:00:00.000000"), day("2024-01-05"), "far_below"),
    ("e", ts("2024-12-20 00:00:00.000000"), day("2024-12-20"), "far_above"),
    # Around the -20s shifted lower bound, 2024-03-09 23:59:50.
    ("a", ts("2024-03-09 23:59:49.999999"), day("2024-03-09"), "lo_minus1us"),
    ("a", ts("2024-03-09 23:59:50.000000"), day("2024-03-09"), "lo_exact"),
    ("a", ts("2024-03-09 23:59:50.000001"), day("2024-03-09"), "lo_plus1us"),
    ("a", ts("2024-03-09 23:59:55.000000"), day("2024-03-09"), "lo_inside"),
    # Around flow `a`'s own closed upper edge.
    ("a", ts("2024-03-10 00:00:09.999999"), day("2024-03-10"), "a_minus1us"),
    ("a", ts("2024-03-10 00:00:10.000000"), day("2024-03-10"), "a_exact"),
    ("a", ts("2024-03-10 00:00:10.000001"), day("2024-03-10"), "a_plus1us"),
    # Flow `b`, comfortably interior.
    ("b", ts("2024-03-10 11:59:50.000001"), day("2024-03-10"), "b_lo_plus1us"),
    ("b", ts("2024-03-10 11:59:59.000000"), day("2024-03-10"), "b_inside"),
    ("b", ts("2024-03-10 12:00:00.000001"), day("2024-03-10"), "b_plus1us"),
    # Flow `c` and flow `d`: `d`'s window opens on the PREVIOUS evening.
    ("c", ts("2024-03-11 23:59:40.000000"), day("2024-03-11"), "c_inside"),
    ("d", ts("2024-03-11 23:59:45.000001"), day("2024-03-11"), "d_lo_plus1us"),
    ("d", ts("2024-03-11 23:59:50.000000"), day("2024-03-11"), "d_prev_evening"),
    ("d", ts("2024-03-12 00:00:05.000000"), day("2024-03-12"), "d_exact"),
    # Around the transported UPPER bound, max(flow_start) = 2024-06-01 00:00:00.
    ("e", ts("2024-05-31 23:59:59.999999"), day("2024-05-31"), "hi_minus1us"),
    ("e", ts("2024-06-01 00:00:00.000000"), day("2024-06-01"), "hi_exact"),
    ("e", ts("2024-06-01 00:00:00.000001"), day("2024-06-01"), "hi_plus1us"),
    # DATE-band edges around min(flow_day) - 2 days = 2024-03-08.
    ("a", ts("2024-03-07 06:00:00.000000"), day("2024-03-07"), "day_minus3"),
    ("a", ts("2024-03-08 06:00:00.000000"), day("2024-03-08"), "day_minus2"),
    ("a", ts("2024-03-09 06:00:00.000000"), day("2024-03-09"), "day_minus1"),
    # A client with lookups but no flow at all.
    ("z", ts("2024-03-10 00:00:00.000000"), day("2024-03-10"), "z_no_flow"),
]


def main():
    os.makedirs(TARGET, exist_ok=True)

    flows = pyarrow.table(
        {
            "client": pyarrow.array([r[0] for r in FLOWS], pyarrow.string()),
            "flow_start": pyarrow.array([r[1] for r in FLOWS], pyarrow.timestamp("us")),
            "flow_day": pyarrow.array([r[2] for r in FLOWS], pyarrow.date32()),
        }
    )
    lookups = pyarrow.table(
        {
            "client": pyarrow.array([r[0] for r in LOOKUPS], pyarrow.string()),
            "event_time": pyarrow.array([r[1] for r in LOOKUPS], pyarrow.timestamp("us")),
            "event_day": pyarrow.array([r[2] for r in LOOKUPS], pyarrow.date32()),
            "tag": pyarrow.array([r[3] for r in LOOKUPS], pyarrow.string()),
        }
    )

    for name, table in (("flows", flows), ("lookups", lookups)):
        folder = os.path.join(TARGET, name)
        os.makedirs(folder, exist_ok=True)
        pyarrow.parquet.write_table(table, os.path.join(folder, f"{name}.parquet"))
        print(f"wrote {folder}/{name}.parquet ({table.num_rows} rows)")


if __name__ == "__main__":
    main()
