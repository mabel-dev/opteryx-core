-- Upstream H2O uses pow(corr(v1, v2), 2); Opteryx spells pow as POWER.
SELECT id2, id4, power(corr(v1, v2), 2) AS r2
FROM x
GROUP BY id2, id4;
