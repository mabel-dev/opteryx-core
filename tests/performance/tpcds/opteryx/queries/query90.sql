SELECT case when pmc=0 then null else cast(amc AS decimal(15,4))/cast(pmc AS decimal(15,4)) end am_pm_ratio
FROM
  (SELECT count(*) amc
   FROM testdata.tpcds_tiny.web_sales,
        testdata.tpcds_tiny.household_demographics,
        testdata.tpcds_tiny.time_dim,
        testdata.tpcds_tiny.web_page
   WHERE ws_sold_time_sk = testdata.tpcds_tiny.time_dim.t_time_sk
     AND ws_ship_hdemo_sk = testdata.tpcds_tiny.household_demographics.hd_demo_sk
     AND ws_web_page_sk = testdata.tpcds_tiny.web_page.wp_web_page_sk
     AND testdata.tpcds_tiny.time_dim.t_hour BETWEEN 8 AND 8+1
     AND testdata.tpcds_tiny.household_demographics.hd_dep_count = 6
     AND testdata.tpcds_tiny.web_page.wp_char_count BETWEEN 5000 AND 5200) at_,
  (SELECT count(*) pmc
   FROM testdata.tpcds_tiny.web_sales,
        testdata.tpcds_tiny.household_demographics,
        testdata.tpcds_tiny.time_dim,
        testdata.tpcds_tiny.web_page
   WHERE ws_sold_time_sk = testdata.tpcds_tiny.time_dim.t_time_sk
     AND ws_ship_hdemo_sk = testdata.tpcds_tiny.household_demographics.hd_demo_sk
     AND ws_web_page_sk = testdata.tpcds_tiny.web_page.wp_web_page_sk
     AND testdata.tpcds_tiny.time_dim.t_hour BETWEEN 19 AND 19+1
     AND testdata.tpcds_tiny.household_demographics.hd_dep_count = 6
     AND testdata.tpcds_tiny.web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER BY am_pm_ratio
LIMIT 100;
