SELECT 
         base.inflow_date
        ,CASE WHEN base.inflow_date = date'2023-01-01' THEN 202252 else YEAR(base.inflow_date)*100 + WEEK(base.inflow_date) END AS created_year_week
        ,IF(city_name in ('HCM City','Ha Noi City','Da Nang City'),city_name,'OTH') AS city_name
        ,CASE WHEN base.lt_submit_to_cancel < 1 and base.order_status = 'Cancelled' THEN '1. < 1 min'
              WHEN base.lt_submit_to_cancel < 2 and base.order_status = 'Cancelled' THEN '2. 1 - 2 mins'  
              WHEN base.lt_submit_to_cancel < 3 and base.order_status = 'Cancelled' THEN '3. 2 - 3 mins'  
              WHEN base.lt_submit_to_cancel < 4 and base.order_status = 'Cancelled' THEN '4. 3 - 4 mins'
              WHEN base.lt_submit_to_cancel < 5 and base.order_status = 'Cancelled' THEN '5. 4 - 5 mins'                 
              WHEN base.lt_submit_to_cancel <= 10 and base.order_status = 'Cancelled' THEN '6. 5 - 10 mins'
              WHEN base.lt_submit_to_cancel > 10 and base.order_status = 'Cancelled' THEN '7. > 10 mins'
              END AS submit_to_cancel_range
        ,count(DISTINCT base.id) AS gross_orders
        ,count(DISTINCT CASE WHEN base.order_status = 'Delivered' THEN base.id ELSE NULL END ) AS net_orders
        ,count(DISTINCT CASE WHEN base.order_status = 'Cancelled' THEN base.id ELSE NULL END ) AS cancel_orders
        ,count(DISTINCT CASE WHEN base.order_status = 'Cancelled' and cancel_reason = 'No driver' THEN base.id ELSE NULL END ) AS cancel_no_driver
        ,count(DISTINCT CASE WHEN base.order_status = 'Cancelled' and cancel_reason = 'No driver' and cancel_by = 'User' THEN base.id ELSE NULL END ) AS cancel_no_driver_by_user
        ,count(DISTINCT CASE WHEN base.order_status = 'Quit' THEN base.id ELSE NULL END ) AS quit_orders       
        ,sum(CASE WHEN base.is_asap = 1 THEN base.lt_submit_to_cancel ELSE NULL END)/cast(count(DISTINCT CASE WHEN base.is_asap = 1 THEN base.id ELSE NULL END) AS double) AS avg_submit_to_cancel


FROM (SELECT *,CASE WHEN is_asap = 1 THEN created_date ELSE DATE(delivered_timestamp) END AS inflow_date FROM dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab) base 

WHERE 1 = 1 
AND order_type = 0
AND city_name NOT LIKE '%Test%'
GROUP BY 1,2,3,4
