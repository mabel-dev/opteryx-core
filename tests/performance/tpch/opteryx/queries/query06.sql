select
    sum(l_extendedprice * l_discount) as revenue
from
    testdata.tpch.lineitem
where
    l_shipdate >= '1993-01-01'::DATE
    and l_shipdate < '1994-01-01'::DATE
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 25;
