SELECT sum(ws_net_paid) AS total_sum ,
       i_category ,
       i_class ,
       grouping(i_category)+grouping(i_class) AS lochierarchy ,
       rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
                                  CASE
                                      WHEN grouping(i_class) = 0 THEN i_category
                                  END
                    ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
FROM testdata.tpcds_tiny.web_sales ,
     testdata.tpcds_tiny.date_dim d1 ,
     testdata.tpcds_tiny.item
WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
  AND d1.d_date_sk = ws_sold_date_sk
  AND i_item_sk = ws_item_sk
GROUP BY rollup(i_category,i_class)
ORDER BY lochierarchy DESC NULLS FIRST,
         CASE
             WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category
         END NULLS FIRST,
         rank_within_parent NULLS FIRST
LIMIT 100;
