-- corr() and pow() may not exist in Opteryx today; flagged in README.
SELECT id2, id4, pow(corr(v1, v2), 2) AS r2
FROM x
GROUP BY id2, id4;
