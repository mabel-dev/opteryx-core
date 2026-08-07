-- NOTE: Opteryx STDDEV is population (N); DuckDB's stddev defaults to sample
-- (N-1) — the baselines differ by sqrt((n-1)/n) per group.
SELECT id4, id5, median(v3) AS median_v3, stddev(v3) AS sd_v3
FROM x
GROUP BY id4, id5;
