SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
FROM
  (SELECT ss_ticket_number,
          ss_customer_sk,
          ca_city bought_city,
          sum(ss_coupon_amt) amt,
          sum(ss_net_profit) profit
   FROM testdata.tpcds_tiny.store_sales,
        testdata.tpcds_tiny.date_dim,
        testdata.tpcds_tiny.store,
        testdata.tpcds_tiny.household_demographics,
        testdata.tpcds_tiny.customer_address
   WHERE testdata.tpcds_tiny.store_sales.ss_sold_date_sk = testdata.tpcds_tiny.date_dim.d_date_sk
     AND testdata.tpcds_tiny.store_sales.ss_store_sk = testdata.tpcds_tiny.store.s_store_sk
     AND testdata.tpcds_tiny.store_sales.ss_hdemo_sk = testdata.tpcds_tiny.household_demographics.hd_demo_sk
     AND testdata.tpcds_tiny.store_sales.ss_addr_sk = testdata.tpcds_tiny.customer_address.ca_address_sk
     AND (testdata.tpcds_tiny.household_demographics.hd_dep_count = 4
          OR testdata.tpcds_tiny.household_demographics.hd_vehicle_count= 3)
     AND testdata.tpcds_tiny.date_dim.d_dow IN (6,
                            0)
     AND testdata.tpcds_tiny.date_dim.d_year IN (1999,
                             1999+1,
                             1999+2)
     AND testdata.tpcds_tiny.store.s_city IN ('Fairview',
                          'Midway')
   GROUP BY ss_ticket_number,
            ss_customer_sk,
            ss_addr_sk,
            ca_city) dn,
     testdata.tpcds_tiny.customer,
     testdata.tpcds_tiny.customer_address current_addr
WHERE ss_customer_sk = c_customer_sk
  AND testdata.tpcds_tiny.customer.c_current_addr_sk = current_addr.ca_address_sk
  AND current_addr.ca_city <> bought_city
ORDER BY c_last_name NULLS FIRST,
         c_first_name NULLS FIRST,
         ca_city NULLS FIRST,
         bought_city NULLS FIRST,
         ss_ticket_number NULLS FIRST
LIMIT 100;
