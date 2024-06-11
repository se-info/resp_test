with order_raw AS 
(SELECT  
        ms.merchant_id,
        raw.sender_name AS "merchant_name",
        raw.city_id,
        raw.city_name,
        raw.id,
        raw.order_code,
        raw.created_date,
        CASE 
        WHEN raw.max_arrived_at_merchant_timestamp > raw.last_incharge_timestamp THEN 
        COALESCE(DATE_DIFF('second',raw.last_incharge_timestamp,raw.max_arrived_at_merchant_timestamp),0)/CAST(60 AS DECIMAL(10,2)) 
        ELSE 0 END AS "incharged_to_arrived",
        CASE 
        WHEN raw.picked_timestamp > raw.max_arrived_at_merchant_timestamp THEN 
             COALESCE(DATE_DIFF('second',raw.max_arrived_at_merchant_timestamp,raw.picked_timestamp),0)/CAST(60 AS DECIMAL(10,2)) 
        ELSE 0 END AS "arrived_to_picked",
        COALESCE(IF(ms.prepare_time_actual IS NOT NULL,ms.prepare_time_actual,
                    DATE_DIFF('second',osl.confirmed_time,osl.picked_time)),0)/CAST(60 AS DECIMAL(10,2)) AS "prepared_time",
        IF(raw.order_status = 'Delivered',1,0) AS is_del,
        COALESCE(d.turn_of_denied_long_prep,0) AS turn_of_denied_long_prep,
        CASE 
        WHEN COALESCE(d.turn_of_denied_long_prep,0) > 0 THEN 1 ELSE 0 END AS "is_have_denied_long_prep"


FROM driver_ops_raw_order_tab raw 

LEFT JOIN 
(SELECT
        delivery_id,
        order_type,
        COUNT(DISTINCT (shipper_id,created_ts)) AS "turn_of_denied_long_prep"


FROM driver_ops_deny_log_tab  
WHERE reASon_id = 2 
GROUP BY 1,2 ) d on d.delivery_id = raw.delivery_id and d.order_type = raw.order_type

LEFT JOIN
(SELECT 
        order_id,
        CAST(MAX(CASE when status = 13 then from_unixtime(create_time) else null end) AS TIMESTAMP) - interval '1' hour AS confirmed_time,
        CAST(MAX(CASE when status = 6 then from_unixtime(create_time) else null end) AS TIMESTAMP) - interval '1' hour AS picked_time

FROM shopeefood.foody_order_db__order_status_log_tab_di
GROUP BY 1 
) osl on osl.order_id = raw.id

LEFT JOIN shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live ms ON ms.id = raw.id

WHERE raw.order_type = 0
AND (raw.created_date = DATE'2024-03-03'
OR raw.created_date = DATE'2024-04-04'
OR raw.created_date = DATE'2024-05-05')
AND REGEXP_LIKE(COALESCE(raw.city_name,'n/a'),'n/a|Dien Bien|Test|test|stress') = false
)
,filter_tab as 
(SELECT
        created_date,
        city_id,
        city_name,
        COUNT(DISTINCT order_code)/CAST(COUNT(DISTINCT (merchant_id,created_date)) AS DECIMAL(10,2)) AS avg_gross_order,
        COUNT(DISTINCT CASE WHEN is_del = 1 THEN order_code ELSE NULL END)/CAST(COUNT(DISTINCT (merchant_id,created_date)) AS DECIMAL(10,2)) AS avg_net_order,
        AVG(CASE WHEN arrived_to_picked > 0 THEN arrived_to_picked ELSE NULL END) AS avg_waiting_time,
        AVG(CASE WHEN prepared_time > 0 THEN prepared_time ELSE NULL END) AS avg_prep_time,
        APPROX_PERCENTILE(CASE WHEN arrived_to_picked > 0 THEN arrived_to_picked ELSE NULL END,0.80) AS pct80th_waiting_time,
        APPROX_PERCENTILE(CASE WHEN prepared_time > 0 THEN prepared_time ELSE NULL END,0.80) AS pct80th_prepared_time

FROM order_raw 
GROUP BY 1,2,3 
)
,merchant_list as 
(SELECT 
        created_date,
        merchant_id,
        merchant_name,
        city_name,
        city_id,
        SUM(turn_of_denied_long_prep)/CAST(COUNT(DISTINCT created_date) AS DECIMAL(10,2)) AS "trung bình tổng số lượt từ chối long_prep",
        COUNT(DISTINCT CASE WHEN is_have_denied_long_prep > 0 THEN order_code ELSE NULL END)/CAST(COUNT(DISTINCT created_date) AS DECIMAL(10,2)) AS "trung bình số đơn bị từ chối ít nhất 1 lần",
        COUNT(DISTINCT order_code)/CAST(COUNT(DISTINCT created_date) AS DECIMAL(10,2)) AS "gross_order",
        COUNT(DISTINCT CASE WHEN is_del > 0 THEN order_code ELSE NULL END)/CAST(COUNT(DISTINCT created_date) AS DECIMAL(10,2)) AS "net_order",
        AVG(CASE WHEN arrived_to_picked > 0 THEN arrived_to_picked ELSE NULL END) AS avg_waiting_time,
        AVG(CASE WHEN prepared_time > 0 THEN prepared_time ELSE NULL END) AS avg_prep_time

FROM order_raw 
GROUP BY 1,2,3,4,5
)
SELECT  
      ml.*,
      sg.segment


FROM merchant_list ml

LEFT JOIN 
(SELECT 
        grass_month,
        merchant_id,
        if(is_food_merchant = 1, segment, 'Mart') AS segment

FROM vnfdbi_commercial.spf_dwd_mex_segment_monthly_vn

WHERE grass_month >= date'2024-03-01'
) sg on sg.grass_month = DATE_TRUNC('month',ml.created_date) AND sg.merchant_id = ml.merchant_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 

LEFT JOIN filter_tab ft ON ml.city_id = ft.city_id and ml.created_date = ft.created_date

WHERE ml.net_order > ft.avg_net_order
AND ml.avg_waiting_time > ft.pct80th_waiting_time
AND ml.avg_prep_time > ft.pct80th_prepared_time;
;
-- Merchant segment
select 
        grass_month,
        merchant_id,
        if(is_food_merchant = 1, segment, 'Mart') AS segment

from vnfdbi_commercial.spf_dwd_mex_segment_monthly_vn

WHERE grass_month = date'2024-05-01'








