-- "Largest two v3 per id6". Upstream uses ROW_NUMBER() OVER (...).
SELECT id6, largest_two_v3
FROM (
    SELECT id6,
           v3 AS largest_two_v3,
           row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS order_v3
    FROM x
    WHERE v3 IS NOT NULL
) sub_query
WHERE order_v3 <= 2;
