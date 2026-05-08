SELECT id1, id2, sum(v1) AS v1
FROM x
GROUP BY id1, id2;
