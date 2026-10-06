SELECT SUM(lo_revenue) AS lo_revenue, d_year, p_brand1
FROM testdata.ssb.lineorder
INNER JOIN testdata.ssb.date ON lo_orderdate = d_datekey
INNER JOIN testdata.ssb.part ON lo_partkey = p_partkey
INNER JOIN testdata.ssb.supplier ON lo_suppkey = s_suppkey
WHERE p_brand1 BETWEEN 'MFGR#2221' AND 'MFGR#2228' AND s_region = 'ASIA'
GROUP BY d_year, p_brand1
ORDER BY d_year, p_brand1;
