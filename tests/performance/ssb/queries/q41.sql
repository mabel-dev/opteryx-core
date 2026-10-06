SELECT d_year, c_nation, SUM(lo_revenue - lo_supplycost) AS profit
FROM testdata.ssb.lineorder
INNER JOIN testdata.ssb.date ON lo_orderdate = d_datekey
INNER JOIN testdata.ssb.customer ON lo_custkey = c_custkey
INNER JOIN testdata.ssb.supplier ON lo_suppkey = s_suppkey
INNER JOIN testdata.ssb.part ON lo_partkey = p_partkey
WHERE c_region = 'AMERICA' AND s_region = 'AMERICA' AND (p_mfgr = 'MFGR#1' OR p_mfgr = 'MFGR#2')
GROUP BY d_year, c_nation
ORDER BY d_year, c_nation;
