SELECT SUM(lo_revenue) AS lo_revenue, d_year, p_brand1
FROM testdata.ssb.lineorder
INNER JOIN testdata.ssb.date ON lo_orderdate = d_datekey
INNER JOIN testdata.ssb.part ON lo_partkey = p_partkey
INNER JOIN testdata.ssb.supplier ON lo_suppkey = s_suppkey
WHERE p_category = 'MFGR#12' AND s_region = 'AMERICA'
GROUP BY d_year, p_brand1
ORDER BY d_year, p_brand1;
