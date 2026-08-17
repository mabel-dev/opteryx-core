SELECT count(*)
FROM testdata.tpcds_tiny.store_sales ,
     testdata.tpcds_tiny.household_demographics,
     testdata.tpcds_tiny.time_dim,
     testdata.tpcds_tiny.store
WHERE ss_sold_time_sk = testdata.tpcds_tiny.time_dim.t_time_sk
  AND ss_hdemo_sk = testdata.tpcds_tiny.household_demographics.hd_demo_sk
  AND ss_store_sk = s_store_sk
  AND testdata.tpcds_tiny.time_dim.t_hour = 20
  AND testdata.tpcds_tiny.time_dim.t_minute >= 30
  AND testdata.tpcds_tiny.household_demographics.hd_dep_count = 7
  AND testdata.tpcds_tiny.store.s_store_name = 'ese'
ORDER BY count(*)
LIMIT 100;
