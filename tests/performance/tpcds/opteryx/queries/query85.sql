SELECT SUBSTRING(r_reason_desc,1,20) ,
       avg(ws_quantity) avg1,
       avg(wr_refunded_cash) avg2,
       avg(wr_fee)
FROM testdata.tpcds_tiny.web_sales,
     testdata.tpcds_tiny.web_returns,
     testdata.tpcds_tiny.web_page,
     testdata.tpcds_tiny.customer_demographics cd1,
     testdata.tpcds_tiny.customer_demographics cd2,
     testdata.tpcds_tiny.customer_address,
     testdata.tpcds_tiny.date_dim,
     testdata.tpcds_tiny.reason
WHERE ws_web_page_sk = wp_web_page_sk
  AND ws_item_sk = wr_item_sk
  AND ws_order_number = wr_order_number
  AND ws_sold_date_sk = d_date_sk
  AND d_year = 2000
  AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
  AND cd2.cd_demo_sk = wr_returning_cdemo_sk
  AND ca_address_sk = wr_refunded_addr_sk
  AND r_reason_sk = wr_reason_sk
  AND ( ( cd1.cd_marital_status = 'M'
         AND cd1.cd_marital_status = cd2.cd_marital_status
         AND cd1.cd_education_status = 'Advanced Degree'
         AND cd1.cd_education_status = cd2.cd_education_status
         AND ws_sales_price BETWEEN 100.00 AND 150.00 )
       OR ( cd1.cd_marital_status = 'S'
           AND cd1.cd_marital_status = cd2.cd_marital_status
           AND cd1.cd_education_status = 'College'
           AND cd1.cd_education_status = cd2.cd_education_status
           AND ws_sales_price BETWEEN 50.00 AND 100.00 )
       OR ( cd1.cd_marital_status = 'W'
           AND cd1.cd_marital_status = cd2.cd_marital_status
           AND cd1.cd_education_status = '2 yr Degree'
           AND cd1.cd_education_status = cd2.cd_education_status
           AND ws_sales_price BETWEEN 150.00 AND 200.00 ) )
  AND ( ( ca_country = 'United States'
         AND ca_state IN ('IN',
                          'OH',
                          'NJ')
         AND ws_net_profit BETWEEN 100 AND 200)
       OR ( ca_country = 'United States'
           AND ca_state IN ('WI',
                            'CT',
                            'KY')
           AND ws_net_profit BETWEEN 150 AND 300)
       OR ( ca_country = 'United States'
           AND ca_state IN ('LA',
                            'IA',
                            'AR')
           AND ws_net_profit BETWEEN 50 AND 250) )
GROUP BY r_reason_desc
ORDER BY SUBSTRING(r_reason_desc,1,20) ,
         avg(ws_quantity) ,
         avg(wr_refunded_cash) ,
         avg(wr_fee)
LIMIT 100;
