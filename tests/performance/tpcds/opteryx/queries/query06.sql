SELECT a.ca_state state,
       count(*) cnt
FROM testdata.tpcds_tiny.customer_address a ,
     testdata.tpcds_tiny.customer c ,
     testdata.tpcds_tiny.store_sales s ,
     testdata.tpcds_tiny.date_dim d ,
     testdata.tpcds_tiny.item i
WHERE a.ca_address_sk = c.c_current_addr_sk
  AND c.c_customer_sk = s.ss_customer_sk
  AND s.ss_sold_date_sk = d.d_date_sk
  AND s.ss_item_sk = i.i_item_sk
  AND d.d_month_seq =
    (SELECT DISTINCT (d_month_seq)
     FROM testdata.tpcds_tiny.date_dim
     WHERE d_year = 2001
       AND d_moy = 1 )
  AND i.i_current_price > 1.2 *
    (SELECT avg(j.i_current_price)
     FROM testdata.tpcds_tiny.item j
     WHERE j.i_category = i.i_category)
GROUP BY a.ca_state
HAVING count(*) >= 10
ORDER BY cnt NULLS FIRST,
         a.ca_state NULLS FIRST
LIMIT 100;
