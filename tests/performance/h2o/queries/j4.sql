SELECT a.id1, a.id2, a.id3, a.id4, a.id5, a.id6, a.v1,
       m.id1 AS medium_id1,
       m.id2 AS medium_id2,
       m.id4 AS medium_id4,
       m.v2 AS v2
FROM x a
JOIN medium m ON a.id5 = m.id5;
