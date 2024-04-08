WITH raw AS 
(SELECT
        -- oct.shipper_uid as shipper_id
        distinct oct.id as order_id
        ,oct.order_code as order_code
        ,oct.status
        ,oct.created_date
        ,oct.created_hour
        ,oct.is_asap
        ,oct.distance
        ,oct.sub_total
        ,case when oct.cancel_actor = 'Fraud' then 'System' 
        when oct.is_co_oos_all_item = 1 then 'Driver'
        else cancel_actor end as cancel_main_actor
        ,case when cancel_actor = 'Buyer' then 1 else 0 end as is_cancel_by_user
        ,case 
         when city_group IN ('HCM','HN','DN') THEN city_group 
         when regexp_like(lower(city_name),'dak lak|thanh hoa|binh thuan|binh dinh') = true THEN 'new cities' 
         ELSE city_tier END AS cities
        ,oct.city_name
        ,cancel_reason
        ,is_cancelled
        ,MONTH(created_date) AS month_

FROM vnfdbi_opsndrivers.shopeefood_vn_bnp_ops_order_detail_tab__vn_daily_s0_live oct


WHERE 1=1
AND oct.city_id NOT IN (0,238,468,469,470,471,472)
AND created_date BETWEEN DATE'2023-07-01' AND current_date - interval '1' day
AND foody_service_id = 1 

)
SELECT 
        -- CASE
        -- WHEN created_hour < 11 THEN 'early morning'
        -- WHEN created_hour < 14 THEN 'lunch'
        -- WHEN created_hour < 18 THEN 'off peak'
        -- WHEN created_hour < 21 THEN 'dinner'
        -- WHEN created_hour >= 21 THEN 'late night' END AS time_slot,
        -- CASE
        -- WHEN distance < 1 THEN '1. 0-1km'
        -- WHEN distance < 3 THEN '2. 1-3km'
        -- WHEN distance < 5 THEN '3. 3-5km'
        -- WHEN distance < 7 THEN '4. 5-7km'
        -- WHEN distance < 10 THEN '5. 7-10km'
        -- WHEN distance >= 10 THEN '6. 10km++' END AS distance_range,
        CASE 
        WHEN sub_total < 500000 THEN '1. 0-500k'
        WHEN sub_total < 1000000 THEN '2. 500-1000k'
        WHEN sub_total < 1500000 THEN '3. 1000-1500k'
        WHEN sub_total < 2000000 THEN '4. 1500-2000k'
        WHEN sub_total >= 2000000 THEN '5. 2000k++' END AS order_value, 
        -- cities,
        COUNT(DISTINCT CASE WHEN month_ = 7 THEN order_id ELSE NULL END)/CAST(COUNT(DISTINCT CASE WHEN month_ = 7 THEN created_date ELSE NULL END) AS DOUBLE) AS ado_jul,
        COUNT(DISTINCT CASE WHEN month_ = 8 THEN order_id ELSE NULL END)/CAST(COUNT(DISTINCT CASE WHEN month_ = 8 THEN created_date ELSE NULL END) AS DOUBLE) AS ado_aug,
        COUNT(DISTINCT CASE WHEN month_ = 7 AND is_cancelled = 1 AND cancel_reason = 'No driver' THEN order_id  ELSE NULL END)
        /CAST(COUNT(DISTINCT CASE WHEN month_ = 7 AND is_cancelled = 1 AND cancel_reason = 'No driver' THEN created_date ELSE NULL END) AS DOUBLE) AS cnd_jul,
        COUNT(DISTINCT CASE WHEN month_ = 8 AND is_cancelled = 1 AND cancel_reason = 'No driver' THEN order_id  ELSE NULL END)
        /CAST(COUNT(DISTINCT CASE WHEN month_ = 8 AND is_cancelled = 1 AND cancel_reason = 'No driver' THEN created_date ELSE NULL END) AS DOUBLE) AS cnd_aug

FROM raw 

GROUP BY 1

