select
    cast(sum(l_extendedprice) / 7.0 as decimal(32,2)) as avg_yearly
from
    testdata.tpch.lineitem,
    testdata.tpch.part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'MED BOX'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            testdata.tpch.lineitem
        where
            l_partkey = p_partkey
    );
