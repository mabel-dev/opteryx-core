SELECT count(DISTINCT cs_order_number) AS "order count",
       sum(cs_ext_ship_cost) AS "total shipping cost",
       sum(cs_net_profit) AS "total net profit"
FROM testdata.tpcds_tiny.catalog_sales cs1,
     testdata.tpcds_tiny.date_dim,
     testdata.tpcds_tiny.customer_address,
     testdata.tpcds_tiny.call_center
WHERE d_date BETWEEN cast('2002-02-01' AS date) AND cast('2002-04-02' AS date)
  AND cs1.cs_ship_date_sk = d_date_sk
  AND cs1.cs_ship_addr_sk = ca_address_sk
  AND ca_state = 'GA'
  AND cs1.cs_call_center_sk = cc_call_center_sk
  AND cc_county = 'Williamson County'
  AND EXISTS
    (SELECT *
     FROM testdata.tpcds_tiny.catalog_sales cs2
     WHERE cs1.cs_order_number = cs2.cs_order_number
       AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
  AND NOT EXISTS
    (SELECT *
     FROM testdata.tpcds_tiny.catalog_returns cr1
     WHERE cs1.cs_order_number = cr1.cr_order_number)
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100;
