#!/usr/bin/env python3
"""
Run TPC-H queries against DuckDB on Parquet datasets and emit a results JSON file.

Usage:
    python run_duckdb.py --scale 1        # runs against testdata/tpch_1
    python run_duckdb.py --scale 001      # runs against testdata/tpch_001
    python run_duckdb.py --scale 001 --warm --iterations 3

Output:
    duckdb.sf{scale}.json    # eg duckdb.sf1.json, duckdb.sf001.json

The output JSON follows the same schema as the ClickBench baseline:
    {
        "system": "DuckDB (Parquet)",
        "date": "<ISO date>",
        "machine": "<hostname>",
        "scale_factor": 1,
        "result": [[run1_ms, run2_ms, run3_ms], ...]
    }
"""

import argparse
import datetime
import gc
import json
import os
import platform
import time

# ---------------------------------------------------------------------------
# DuckDB-native TPC-H queries  (standard SQL — no Opteryx syntax workarounds)
# ---------------------------------------------------------------------------


def queries(scale_path: str) -> list[tuple[str, str]]:
    """
    Return a list of (name, sql) pairs.

    `scale_path` is the FROM-clause prefix for tables, e.g.
        `testdata/tpch_001/`   or   `testdata/tpch_1/`
    """
    T = f"'{scale_path}/'"

    return [
        (
            "Q01",
            f"""
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity)                                       AS sum_qty,
    SUM(l_extendedprice)                                  AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)                                       AS avg_qty,
    AVG(l_extendedprice)                                  AS avg_price,
    AVG(l_discount)                                       AS avg_disc,
    COUNT(*)                                              AS count_order
FROM read_parquet({T} || 'lineitem/*.parquet')
WHERE l_shipdate <= DATE '1998-09-16'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
""",
        ),
        (
            "Q02",
            f"""
WITH q2_min_ps_supplycost AS (
    SELECT
        p_partkey AS min_p_partkey,
        MIN(ps_supplycost) AS min_ps_supplycost
    FROM
        read_parquet({T} || 'part/*.parquet') part,
        read_parquet({T} || 'partsupp/*.parquet') partsupp,
        read_parquet({T} || 'supplier/*.parquet') supplier,
        read_parquet({T} || 'nation/*.parquet') nation,
        read_parquet({T} || 'region/*.parquet') region
    WHERE
        p_partkey = ps_partkey
        AND s_suppkey = ps_suppkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'EUROPE'
    GROUP BY p_partkey
)
SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    read_parquet({T} || 'part/*.parquet') part,
    read_parquet({T} || 'supplier/*.parquet') supplier,
    read_parquet({T} || 'partsupp/*.parquet') partsupp,
    read_parquet({T} || 'nation/*.parquet') nation,
    read_parquet({T} || 'region/*.parquet') region,
    q2_min_ps_supplycost
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 37
    AND p_type LIKE '%COPPER'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = min_ps_supplycost
    AND p_partkey = min_p_partkey
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100
""",
        ),
        (
            "Q03",
            f"""
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    read_parquet({T} || 'customer/*.parquet') customer,
    read_parquet({T} || 'orders/*.parquet') orders,
    read_parquet({T} || 'lineitem/*.parquet') lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-22'
    AND l_shipdate > DATE '1995-03-22'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10
""",
        ),
        (
            "Q04",
            f"""
SELECT
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    read_parquet({T} || 'orders/*.parquet') AS o
WHERE
    o_orderdate >= DATE '1996-05-01'
    AND o_orderdate < DATE '1996-08-01'
    AND EXISTS (
        SELECT *
        FROM read_parquet({T} || 'lineitem/*.parquet') AS l
        WHERE l_orderkey = o.o_orderkey
          AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
""",
        ),
        (
            "Q05",
            f"""
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    read_parquet({T} || 'customer/*.parquet') customer,
    read_parquet({T} || 'orders/*.parquet') orders,
    read_parquet({T} || 'lineitem/*.parquet') lineitem,
    read_parquet({T} || 'supplier/*.parquet') supplier,
    read_parquet({T} || 'nation/*.parquet') nation,
    read_parquet({T} || 'region/*.parquet') region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'AFRICA'
    AND o_orderdate >= DATE '1993-01-01'
    AND o_orderdate < DATE '1994-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
""",
        ),
        (
            "Q06",
            f"""
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    read_parquet({T} || 'lineitem/*.parquet') lineitem
WHERE
    l_shipdate >= DATE '1993-01-01'
    AND l_shipdate < DATE '1994-01-01'
    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 25
""",
        ),
        (
            "Q07",
            f"""
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        EXTRACT(YEAR FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM
        read_parquet({T} || 'supplier/*.parquet') supplier,
        read_parquet({T} || 'lineitem/*.parquet') lineitem,
        read_parquet({T} || 'orders/*.parquet') orders,
        read_parquet({T} || 'customer/*.parquet') customer,
        read_parquet({T} || 'nation/*.parquet') n1,
        read_parquet({T} || 'nation/*.parquet') n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND (
            (n1.n_name = 'KENYA' AND n2.n_name = 'PERU')
            OR (n1.n_name = 'PERU' AND n2.n_name = 'KENYA')
        )
        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year
""",
        ),
        (
            "Q08",
            f"""
SELECT
    o_year,
    SUM(CASE WHEN nation = 'PERU' THEN volume ELSE 0 END)
    / SUM(volume) AS mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM
        read_parquet({T} || 'part/*.parquet') part,
        read_parquet({T} || 'supplier/*.parquet') supplier,
        read_parquet({T} || 'lineitem/*.parquet') lineitem,
        read_parquet({T} || 'orders/*.parquet') orders,
        read_parquet({T} || 'customer/*.parquet') customer,
        read_parquet({T} || 'nation/*.parquet') n1,
        read_parquet({T} || 'nation/*.parquet') n2,
        read_parquet({T} || 'region/*.parquet') region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type = 'ECONOMY BURNISHED NICKEL'
) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year
""",
        ),
        (
            "Q09",
            f"""
SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        EXTRACT(YEAR FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        read_parquet({T} || 'part/*.parquet') part,
        read_parquet({T} || 'supplier/*.parquet') supplier,
        read_parquet({T} || 'lineitem/*.parquet') lineitem,
        read_parquet({T} || 'partsupp/*.parquet') partsupp,
        read_parquet({T} || 'orders/*.parquet') orders,
        read_parquet({T} || 'nation/*.parquet') nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%plum%'
) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC
""",
        ),
        (
            "Q10",
            f"""
SELECT
    c_custkey,
    c_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    read_parquet({T} || 'customer/*.parquet') customer,
    read_parquet({T} || 'orders/*.parquet') orders,
    read_parquet({T} || 'lineitem/*.parquet') lineitem,
    read_parquet({T} || 'nation/*.parquet') nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-10-01'
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20
""",
        ),
        (
            "Q11",
            f"""
WITH q11_part_tmp_cached AS (
    SELECT
        ps_partkey,
        SUM(ps_supplycost * ps_availqty) AS part_value
    FROM
        read_parquet({T} || 'partsupp/*.parquet') partsupp,
        read_parquet({T} || 'supplier/*.parquet') supplier,
        read_parquet({T} || 'nation/*.parquet') nation
    WHERE
        ps_suppkey = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'GERMANY'
    GROUP BY ps_partkey
),
q11_sum_tmp_cached AS (
    SELECT SUM(part_value) AS total_value
    FROM q11_part_tmp_cached
)
SELECT
    ps_partkey,
    part_value AS value
FROM (
    SELECT
        ps_partkey,
        part_value,
        total_value
    FROM
        q11_part_tmp_cached,
        q11_sum_tmp_cached
) a
WHERE
    part_value > total_value * 0.0001
ORDER BY
    value DESC
""",
        ),
        (
            "Q12",
            f"""
SELECT
    l_shipmode,
    SUM(CASE
        WHEN o_orderpriority = '1-URGENT'
            OR o_orderpriority = '2-HIGH'
            THEN 1
        ELSE 0
    END) AS high_line_count,
    SUM(CASE
        WHEN o_orderpriority <> '1-URGENT'
            AND o_orderpriority <> '2-HIGH'
            THEN 1
        ELSE 0
    END) AS low_line_count
FROM
    read_parquet({T} || 'orders/*.parquet') orders,
    read_parquet({T} || 'lineitem/*.parquet') lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('REG AIR', 'MAIL')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1995-01-01'
    AND l_receiptdate < DATE '1996-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
""",
        ),
        (
            "Q13",
            f"""
SELECT
    c_count,
    COUNT(*) AS custdist
FROM (
    SELECT
        c_custkey,
        COUNT(o_orderkey) AS c_count
    FROM
        read_parquet({T} || 'customer/*.parquet') customer
        LEFT OUTER JOIN read_parquet({T} || 'orders/*.parquet') orders
            ON c_custkey = o_custkey
            AND o_comment NOT LIKE '%unusual%accounts%'
    GROUP BY
        c_custkey
) c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
""",
        ),
        (
            "Q14",
            f"""
SELECT
    100.00 * SUM(CASE
        WHEN p_type LIKE 'PROMO%'
            THEN l_extendedprice * (1 - l_discount)
        ELSE 0
    END) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    read_parquet({T} || 'lineitem/*.parquet') lineitem,
    read_parquet({T} || 'part/*.parquet') part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-08-01'
    AND l_shipdate < DATE '1995-09-01'
""",
        ),
        (
            "Q15",
            f"""
WITH revenue_cached AS (
    SELECT
        l_suppkey AS supplier_no,
        SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        read_parquet({T} || 'lineitem/*.parquet') lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-04-01'
    GROUP BY l_suppkey
),
max_revenue_cached AS (
    SELECT MAX(total_revenue) AS max_revenue
    FROM revenue_cached
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    read_parquet({T} || 'supplier/*.parquet') supplier,
    revenue_cached,
    max_revenue_cached
WHERE
    s_suppkey = supplier_no
    AND total_revenue = max_revenue
ORDER BY s_suppkey
""",
        ),
        (
            "Q16",
            f"""
SELECT
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    read_parquet({T} || 'partsupp/*.parquet') partsupp,
    read_parquet({T} || 'part/*.parquet') part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#34'
    AND p_type NOT LIKE 'ECONOMY BRUSHED%'
    AND p_size IN (22, 14, 27, 49, 21, 33, 35, 28)
    AND ps_suppkey NOT IN (
        SELECT s_suppkey
        FROM read_parquet({T} || 'supplier/*.parquet') supplier
        WHERE s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
""",
        ),
        (
            "Q17",
            f"""
SELECT
    CAST(SUM(l_extendedprice) / 7.0 AS DECIMAL(32,2)) AS avg_yearly
FROM
    read_parquet({T} || 'lineitem/*.parquet') lineitem,
    read_parquet({T} || 'part/*.parquet') part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * AVG(l_quantity)
        FROM read_parquet({T} || 'lineitem/*.parquet') lineitem2
        WHERE lineitem2.l_partkey = part.p_partkey
    )
""",
        ),
        (
            "Q18",
            f"""
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    SUM(l_quantity)
FROM
    read_parquet({T} || 'customer/*.parquet') customer,
    read_parquet({T} || 'orders/*.parquet') orders,
    read_parquet({T} || 'lineitem/*.parquet') lineitem
WHERE
    o_orderkey IN (
        SELECT l_orderkey
        FROM read_parquet({T} || 'lineitem/*.parquet') lineitem2
        GROUP BY l_orderkey
        HAVING SUM(l_quantity) > 300
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
""",
        ),
        (
            "Q19",
            f"""
SELECT
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    read_parquet({T} || 'lineitem/*.parquet') lineitem,
    read_parquet({T} || 'part/*.parquet') part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#32'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 7 AND l_quantity <= 7 + 10
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#35'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 15 AND l_quantity <= 15 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#24'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 26 AND l_quantity <= 26 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
""",
        ),
        (
            "Q20",
            f"""
SELECT
    s_name,
    s_address
FROM
    read_parquet({T} || 'supplier/*.parquet') supplier,
    read_parquet({T} || 'nation/*.parquet') nation
WHERE
    s_suppkey IN (
        SELECT ps_suppkey
        FROM read_parquet({T} || 'partsupp/*.parquet') partsupp
        WHERE
            ps_partkey IN (
                SELECT p_partkey
                FROM read_parquet({T} || 'part/*.parquet') part
                WHERE p_name LIKE 'forest%'
            )
            AND ps_availqty > (
                SELECT 0.5 * SUM(l_quantity)
                FROM read_parquet({T} || 'lineitem/*.parquet') lineitem
                WHERE
                    l_partkey = ps_partkey
                    AND l_suppkey = ps_suppkey
                    AND l_shipdate >= DATE '1994-01-01'
                    AND l_shipdate < DATE '1995-01-01'
            )
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'CANADA'
ORDER BY s_name
""",
        ),
        (
            "Q21",
            f"""
SELECT
    s_name,
    COUNT(*) AS numwait
FROM
    read_parquet({T} || 'supplier/*.parquet') supplier,
    read_parquet({T} || 'lineitem/*.parquet') l1,
    read_parquet({T} || 'orders/*.parquet') orders,
    read_parquet({T} || 'nation/*.parquet') nation
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT *
        FROM read_parquet({T} || 'lineitem/*.parquet') l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT *
        FROM read_parquet({T} || 'lineitem/*.parquet') l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100
""",
        ),
        (
            "Q22",
            f"""
WITH q22_customer_tmp_cached AS (
    SELECT
        c_acctbal,
        c_custkey,
        SUBSTRING(c_phone, 1, 2) AS cntrycode
    FROM
        read_parquet({T} || 'customer/*.parquet') customer
    WHERE
        SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
),
q22_customer_tmp1_cached AS (
    SELECT
        AVG(c_acctbal) AS avg_acctbal
    FROM
        q22_customer_tmp_cached
    WHERE
        c_acctbal > 0.00
),
q22_orders_tmp_cached AS (
    SELECT o_custkey
    FROM read_parquet({T} || 'orders/*.parquet') orders
    GROUP BY o_custkey
)
SELECT
    cntrycode,
    COUNT(1) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM (
    SELECT
        cntrycode,
        c_acctbal,
        avg_acctbal
    FROM
        q22_customer_tmp1_cached ct1
        CROSS JOIN (
            SELECT
                cntrycode,
                c_acctbal
            FROM
                q22_orders_tmp_cached ot
                RIGHT OUTER JOIN q22_customer_tmp_cached ct
                    ON ct.c_custkey = ot.o_custkey
            WHERE
                o_custkey IS NULL
        ) ct2
) a
WHERE
    c_acctbal > avg_acctbal
GROUP BY
    cntrycode
ORDER BY
    cntrycode
""",
        ),
    ]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def main():
    import duckdb

    parser = argparse.ArgumentParser(description="DuckDB TPC-H benchmark")
    parser.add_argument(
        "--scale", type=str, default="001", help="Scale factor suffix (001, 1, etc.)"
    )
    parser.add_argument(
        "--warm",
        action="store_true",
        default=True,
        help="Run warm-up iterations before measurement",
    )
    parser.add_argument(
        "--iterations", type=int, default=10, help="Number of timed iterations (default: 10)"
    )
    parser.add_argument(
        "--output", type=str, default=None, help="Output JSON path (default: duckdb.sf{scale}.json)"
    )
    args = parser.parse_args()

    # Determine paths
    repo_root = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
    repo_root = os.path.abspath(repo_root)
    scale_path = os.path.join(repo_root, "testdata", f"tpch_{args.scale}")

    if not os.path.isdir(scale_path):
        print(f"ERROR: Scale directory not found: {scale_path}")
        print("       Expected: testdata/tpch_001 or testdata/tpch_1 etc.")
        return 1

    output_path = args.output or os.path.join(
        os.path.dirname(__file__), f"results.sf{args.scale}.json"
    )

    # ------------------------------------------------------------------
    # Gather dataset metadata: per-table row count + column count
    # ------------------------------------------------------------------
    tables = {}
    for table_dir in sorted(os.listdir(scale_path)):
        table_path = os.path.join(scale_path, table_dir)
        if not os.path.isdir(table_path):
            continue
        parquet_files = sorted(f for f in os.listdir(table_path) if f.endswith(".parquet"))
        if not parquet_files:
            continue
        # Count rows from all parquet parts
        total_rows = 0
        col_count = 0
        for pf in parquet_files:
            pth = os.path.join(table_path, pf)
            info = duckdb.sql("SELECT COUNT(*) AS n FROM read_parquet(?)", params=[pth]).fetchone()
            total_rows += info[0]
            if col_count == 0:
                cols = duckdb.sql(
                    "SELECT COUNT(*) AS n FROM parquet_schema(?)", params=[pth]
                ).fetchone()
                col_count = cols[0]
        tables[table_dir] = {"rows": total_rows, "columns": col_count}

    total_rows_all = sum(t["rows"] for t in tables.values())

    print(f"🐤 DuckDB TPC-H Benchmark — SF {args.scale}")
    print(f"   path: {scale_path}")
    print(f"   iterations: {args.iterations}")
    print(f"   queries: {len(queries(scale_path))}")
    print()
    print("   Tables:")
    for tname, tinfo in tables.items():
        print(f"     {tname:12s}  {tinfo['rows']:>12,d} rows  {tinfo['columns']} cols")
    print(f"     {'TOTAL':12s}  {total_rows_all:>12,d} rows")
    print()

    # ------------------------------------------------------------------
    # Run queries
    # ------------------------------------------------------------------
    all_queries = queries(scale_path)
    results = []

    for name, sql in all_queries:
        times = []
        shape = None
        for i in range(args.iterations + (1 if args.warm else 0)):
            gc.collect()
            t0 = time.perf_counter()
            result = duckdb.sql(sql).fetchall()
            elapsed = (time.perf_counter() - t0) * 1000.0  # ms

            if i == 0 and args.warm:
                shape = (len(result), len(result[0]) if result else 0)
                print(f"   {name} warm: {elapsed:8.1f}ms", end="\r")
                continue

            times.append(elapsed)
            print(
                f"   {name} [{i:2d}]: {elapsed:8.1f}ms", end="\r" if i < args.iterations else "\n"
            )

        # Store structured entry with timings and result shape
        min_ms = min(times)
        max_ms = max(times)
        avg_ms = sum(times) / len(times)
        results.append(
            {
                "name": name,
                "min_ms": min_ms,
                "max_ms": max_ms,
                "avg_ms": avg_ms,
                "iterations": len(times),
                "times": times,
                "shape": list(shape) if shape else [0, 0],
            }
        )

    # Build output
    record = {
        "system": "DuckDB (Parquet, partitioned)",
        "date": datetime.date.today().isoformat(),
        "machine": platform.node(),
        "scale_factor": args.scale,
        "iterations": args.iterations,
        "data_path": f"testdata/tpch_{args.scale}",
        "tables": tables,
        "result": results,
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)

    # Print summary table
    print()
    print(
        f"    {'Query':<6} {'Min (ms)':>10} {'Max (ms)':>10} {'Avg (ms)':>10} {'Rows':>10} {'Cols':>6}"
    )
    print(f"    {'─' * 6} {'─' * 11} {'─' * 11} {'─' * 11} {'─' * 11} {'─' * 6}")
    for entry in results:
        rows, cols = entry["shape"]
        print(
            f"    {entry['name']:<6} {entry['min_ms']:>10.1f} {entry['max_ms']:>10.1f} {entry['avg_ms']:>10.1f} {rows:>10,d} {cols:>6d}"
        )

    # Totals
    all_mins = [r["min_ms"] for r in results]
    all_maxs = [r["max_ms"] for r in results]
    all_avgs = [r["avg_ms"] for r in results]
    print(f"    {'TOTAL':<6} {sum(all_mins):>10.1f} {sum(all_maxs):>10.1f} {sum(all_avgs):>10.1f}")
    print()
    print(f"✅ Results written to: {output_path}")


if __name__ == "__main__":
    main()
