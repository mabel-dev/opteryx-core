SELECT a.id1, a.id2, a.id3, a.id4, a.id5, a.id6, a.v1,
       b.id4 AS small_id4,
       b.v2 AS v2
FROM x a
JOIN small b ON a.id1 = b.id1;
