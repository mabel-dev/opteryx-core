SELECT c_last_name,
       c_first_name,
       SUBSTRING(s_city,1,30),
       ss_ticket_number,
       amt,
       profit
FROM
  (SELECT ss_ticket_number ,
          ss_customer_sk ,
          testdata.tpcds_tiny.store.s_city ,
          sum(ss_coupon_amt) amt ,
          sum(ss_net_profit) profit
   FROM testdata.tpcds_tiny.store_sales,
        testdata.tpcds_tiny.date_dim,
        testdata.tpcds_tiny.store,
        testdata.tpcds_tiny.household_demographics
   WHERE testdata.tpcds_tiny.store_sales.ss_sold_date_sk = testdata.tpcds_tiny.date_dim.d_date_sk
     AND testdata.tpcds_tiny.store_sales.ss_store_sk = testdata.tpcds_tiny.store.s_store_sk
     AND testdata.tpcds_tiny.store_sales.ss_hdemo_sk = testdata.tpcds_tiny.household_demographics.hd_demo_sk
     AND (testdata.tpcds_tiny.household_demographics.hd_dep_count = 6
          OR testdata.tpcds_tiny.household_demographics.hd_vehicle_count > 2)
     AND testdata.tpcds_tiny.date_dim.d_dow = 1
     AND testdata.tpcds_tiny.date_dim.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND testdata.tpcds_tiny.store.s_number_employees BETWEEN 200 AND 295
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            testdata.tpcds_tiny.store.s_city) ms,
     testdata.tpcds_tiny.customer
WHERE ss_customer_sk = c_customer_sk
ORDER BY c_last_name  NULLS FIRST,
         c_first_name  NULLS FIRST,
         SUBSTRING(s_city,1,30)  NULLS FIRST,
         profit NULLS FIRST,
         ss_ticket_number
LIMIT 100;
