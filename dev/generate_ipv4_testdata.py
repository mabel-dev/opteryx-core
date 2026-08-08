"""Generate testdata/flat/ipv4/hosts.parquet — a UINT32 address column.

An IPv4 column IS `DrakenType.UINT32` refined by a `LogicalKind.IPV4` descriptor,
and Parquet cannot carry the descriptor (rugo writes the physical `uint32`), so the
column reads back as a plain UINT32 and every IP predicate over it arrives wrapped
in a `::IPV4` cast. That is the shape `rewrite_cidr_to_range` has to push through,
and it needs a REAL scan to exercise: the pushdown only happens at a Scan node, so
a VALUES-style inline relation cannot test it.

Rows are laid out as two well-separated clusters across several row groups so that
min/max pruning has something to bite on rather than every group qualifying.

pyarrow is used here and only here — `dev/` is test-data generation, never imported
by production code (CLAUDE.md §4).

    python dev/generate_ipv4_testdata.py
"""

import os

import pyarrow as pa
import pyarrow.parquet as pq

OUTPUT = os.path.join("testdata", "flat", "ipv4", "hosts.parquet")

IN_NETWORK = 1000  # 10.0.0.0  .. 10.0.3.231   -- inside 10.0.0.0/8
OUT_NETWORK = 3000  # 192.168.1.0 .. 192.168.12.183 -- outside it


def main() -> None:
    addresses = [167772160 + i for i in range(IN_NETWORK)]
    addresses += [3232235776 + i for i in range(OUT_NETWORK)]

    table = pa.table(
        {
            "addr": pa.array(addresses, type=pa.uint32()),
            "label": pa.array([f"host-{i}" for i in range(len(addresses))], type=pa.string()),
        }
    )

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    pq.write_table(table, OUTPUT, row_group_size=500)
    print(f"wrote {len(addresses)} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
