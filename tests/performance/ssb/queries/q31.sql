SELECT c_nation, s_nation, d_year, SUM(lo_revenue) AS revenue
FROM testdata.ssb.lineorder
INNER JOIN testdata.ssb.date ON lo_orderdate = d_datekey
INNER JOIN testdata.ssb.customer ON lo_custkey = c_custkey
INNER JOIN testdata.ssb.supplier ON lo_suppkey = s_suppkey
WHERE c_region = 'ASIA' AND s_region = 'ASIA' AND d_year >= 1992 AND d_year <= 1997
GROUP BY c_nation, s_nation, d_year
ORDER BY d_year ASC, revenue DESC, c_nation, s_nation;
