SELECT dt.d_year,
       testdata.tpcds_tiny.item.i_brand_id brand_id,
       testdata.tpcds_tiny.item.i_brand brand,
       sum(ss_ext_sales_price) ext_price
FROM testdata.tpcds_tiny.date_dim dt,
     testdata.tpcds_tiny.store_sales,
     testdata.tpcds_tiny.item
WHERE dt.d_date_sk = testdata.tpcds_tiny.store_sales.ss_sold_date_sk
  AND testdata.tpcds_tiny.store_sales.ss_item_sk = testdata.tpcds_tiny.item.i_item_sk
  AND testdata.tpcds_tiny.item.i_manager_id = 1
  AND dt.d_moy=11
  AND dt.d_year=2000
GROUP BY dt.d_year,
         testdata.tpcds_tiny.item.i_brand,
         testdata.tpcds_tiny.item.i_brand_id
ORDER BY dt.d_year,
         ext_price DESC,
         brand_id
LIMIT 100 ;
