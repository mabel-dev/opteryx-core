SELECT count(*)
FROM ((SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM testdata.tpcds_tiny.store_sales,
              testdata.tpcds_tiny.date_dim,
              testdata.tpcds_tiny.customer
         WHERE testdata.tpcds_tiny.store_sales.ss_sold_date_sk = testdata.tpcds_tiny.date_dim.d_date_sk
           AND testdata.tpcds_tiny.store_sales.ss_customer_sk = testdata.tpcds_tiny.customer.c_customer_sk
           AND d_month_seq BETWEEN 1200 AND 1200+11)
      EXCEPT
        (SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM testdata.tpcds_tiny.catalog_sales,
              testdata.tpcds_tiny.date_dim,
              testdata.tpcds_tiny.customer
         WHERE testdata.tpcds_tiny.catalog_sales.cs_sold_date_sk = testdata.tpcds_tiny.date_dim.d_date_sk
           AND testdata.tpcds_tiny.catalog_sales.cs_bill_customer_sk = testdata.tpcds_tiny.customer.c_customer_sk
           AND d_month_seq BETWEEN 1200 AND 1200+11)
      EXCEPT
        (SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM testdata.tpcds_tiny.web_sales,
              testdata.tpcds_tiny.date_dim,
              testdata.tpcds_tiny.customer
         WHERE testdata.tpcds_tiny.web_sales.ws_sold_date_sk = testdata.tpcds_tiny.date_dim.d_date_sk
           AND testdata.tpcds_tiny.web_sales.ws_bill_customer_sk = testdata.tpcds_tiny.customer.c_customer_sk
           AND d_month_seq BETWEEN 1200 AND 1200+11)) cool_cust ;
