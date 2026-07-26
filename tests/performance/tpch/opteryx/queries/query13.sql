/*
Canonical Q13 filters o_comment inside the LEFT OUTER JOIN's ON clause; Opteryx
only supports equality predicates in a JOIN ON clause today, so the extra
predicate can't go there directly. The previous rewrite moved it to WHERE
instead, which is NOT equivalent: WHERE runs after the LEFT JOIN, so it drops
every customer whose orders all fail the filter (or who have no orders at
all) instead of counting them in the c_count=0 bucket, undercounting custdist
(verified against the DuckDB oracle at SF0.01: 31 rows instead of the correct
32, missing custdist=500 at c_count=0).

Fix: pre-filter orders in a derived table before the join. This reproduces
the ON-clause semantics exactly (still an equality-only join) and matches
canonical results.

CANONICAL:

select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey) as c_count
        from
            testdata.tpch.customer left outer join testdata.tpch.orders on
                c_custkey = o_custkey
                and o_comment not like '%unusual%accounts%'
        group by
            c_custkey
    ) c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
*/

SELECT
  c_count,
  Count(*) AS custdist
FROM
  (
    SELECT
      c_custkey,
      Count(o_orderkey) AS c_count
    FROM
      testdata.tpch.customer
      LEFT OUTER JOIN (
        SELECT
          *
        FROM
          testdata.tpch.orders
        WHERE
          o_comment NOT LIKE '%unusual%accounts%'
      ) AS t ON c_custkey = t.o_custkey
    GROUP BY
      c_custkey
  ) c_orders
GROUP BY
  c_count
ORDER BY
  custdist DESC,
  c_count DESC;
