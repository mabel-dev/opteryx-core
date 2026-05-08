-- H2O upstream uses median(v3) and stddev(v3); both are likely missing in
-- Opteryx today. Listed in README "known gaps".
SELECT id4, id5, median(v3) AS median_v3, stddev(v3) AS sd_v3
FROM x
GROUP BY id4, id5;
