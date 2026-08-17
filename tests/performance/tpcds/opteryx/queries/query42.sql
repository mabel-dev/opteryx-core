SELECT dt.d_year,
       testdata.tpcds_tiny.item.i_category_id,
       testdata.tpcds_tiny.item.i_category,
       sum(ss_ext_sales_price)
FROM testdata.tpcds_tiny.date_dim dt,
     testdata.tpcds_tiny.store_sales,
     testdata.tpcds_tiny.item
WHERE dt.d_date_sk = testdata.tpcds_tiny.store_sales.ss_sold_date_sk
  AND testdata.tpcds_tiny.store_sales.ss_item_sk = testdata.tpcds_tiny.item.i_item_sk
  AND testdata.tpcds_tiny.item.i_manager_id = 1
  AND dt.d_moy=11
  AND dt.d_year=2000
GROUP BY dt.d_year,
         testdata.tpcds_tiny.item.i_category_id,
         testdata.tpcds_tiny.item.i_category
ORDER BY sum(ss_ext_sales_price) DESC,dt.d_year,
                                      testdata.tpcds_tiny.item.i_category_id,
                                      testdata.tpcds_tiny.item.i_category
LIMIT 100 ;
