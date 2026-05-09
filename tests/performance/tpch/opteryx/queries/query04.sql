/*
We've replaced the EXISTS clause will a LEFT SEMI JOIN

ORIGINAL:

select
    o_orderpriority,
    count(*) as order_count
from
    testdata.tpch.orders as o
where
    o_orderdate >= '1996-05-01'::DATE
    and o_orderdate < '1996-08-01'::DATE
    and exists (
        select
            *
        from
            testdata.tpch.lineitem
        where
            l_orderkey = o.o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;
*/

SELECT
  o_orderpriority,
  Count(*) AS order_count
FROM
  testdata.tpch.orders AS o LEFT semi
  JOIN (
    SELECT
      *
    FROM
      testdata.tpch.lineitem AS l
    WHERE
      l_commitdate < l_receiptdate
  ) AS l ON l.l_orderkey = o.o_orderkey
WHERE
  o_orderdate >= '1996-05-01'::DATE
  AND o_orderdate < '1996-08-01'::DATE
GROUP BY
  o_orderpriority
ORDER BY
  o_orderpriority;
