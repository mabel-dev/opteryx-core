SELECT a.id1, a.id2, a.id3, a.id4, a.id5, a.id6, a.v1,
       g.id1 AS big_id1,
       g.id2 AS big_id2,
       g.id4 AS big_id4,
       g.id5 AS big_id5,
       g.id6 AS big_id6,
       g.v2 AS v2
FROM x a
JOIN big g ON a.id3 = g.id3;
