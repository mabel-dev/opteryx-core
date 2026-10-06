SELECT c_city, s_city, d_year, SUM(lo_revenue) AS revenue
FROM testdata.ssb.lineorder
INNER JOIN testdata.ssb.date ON lo_orderdate = d_datekey
INNER JOIN testdata.ssb.customer ON lo_custkey = c_custkey
INNER JOIN testdata.ssb.supplier ON lo_suppkey = s_suppkey
WHERE c_city IN ('UNITED KI1', 'UNITED KI5') AND s_city IN ('UNITED KI1', 'UNITED KI5') AND d_year >= 1992 AND d_year <= 1997
GROUP BY c_city, s_city, d_year
ORDER BY d_year ASC, revenue DESC, c_city, s_city;
