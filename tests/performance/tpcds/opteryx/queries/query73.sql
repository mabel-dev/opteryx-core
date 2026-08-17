SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          count(*) cnt
   FROM testdata.tpcds_tiny.store_sales,
        testdata.tpcds_tiny.date_dim,
        testdata.tpcds_tiny.store,
        testdata.tpcds_tiny.household_demographics
   WHERE testdata.tpcds_tiny.store_sales.ss_sold_date_sk = testdata.tpcds_tiny.date_dim.d_date_sk
     AND testdata.tpcds_tiny.store_sales.ss_store_sk = testdata.tpcds_tiny.store.s_store_sk
     AND testdata.tpcds_tiny.store_sales.ss_hdemo_sk = testdata.tpcds_tiny.household_demographics.hd_demo_sk
     AND testdata.tpcds_tiny.date_dim.d_dom BETWEEN 1 AND 2
     AND (testdata.tpcds_tiny.household_demographics.hd_buy_potential = 'Unknown'
          OR testdata.tpcds_tiny.household_demographics.hd_buy_potential = '>10000')
     AND testdata.tpcds_tiny.household_demographics.hd_vehicle_count > 0
     AND CASE
             WHEN testdata.tpcds_tiny.household_demographics.hd_vehicle_count > 0 THEN (testdata.tpcds_tiny.household_demographics.hd_dep_count*1.000)/ testdata.tpcds_tiny.household_demographics.hd_vehicle_count
             ELSE NULL
         END > 1
     AND testdata.tpcds_tiny.date_dim.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND testdata.tpcds_tiny.store.s_county IN ('Orange County',
                            'Bronx County',
                            'Franklin Parish',
                            'Williamson County')
   GROUP BY ss_ticket_number,
            ss_customer_sk) dj,
     testdata.tpcds_tiny.customer
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 1 AND 5
ORDER BY cnt DESC,
         c_last_name ASC;
