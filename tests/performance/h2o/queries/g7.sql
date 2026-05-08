SELECT id3, max(v1) - min(v2) AS range_v1_v2
FROM x
GROUP BY id3;
