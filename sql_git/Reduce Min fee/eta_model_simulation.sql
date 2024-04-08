WITH raw_order AS 
(SELECT 
         oct.id AS order_id
        ,oct.order_code
        ,oct.restaurant_id AS merchant_id 
        ,CASE 
            WHEN oct.status = 7 THEN '1. Delivered' 
            WHEN oct.status = 8 THEN '2. Cancelled' 
            WHEN oct.status = 9 THEN '3. Quit' 
            END AS order_status                                           
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_confirm.value') AS DOUBLE)/60 AS t_confirm
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_arrive_merchant.value') AS DOUBLE)/60 AS t_arrive_merchant
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_prep.value') AS DOUBLE)/60 AS t_prep
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_assign.value') AS DOUBLE)/60 AS t_assign
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_pickup.value') AS DOUBLE)/60 AS t_pickup
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_arrive_customer.value') AS DOUBLE)/60 AS t_arrive_customer
        ,CAST(JSON_EXTRACT(JSON_EXTRACT(oct.extra_data,'$.estimate_time_object'),'$.t_customer_wait.value') AS DOUBLE)/60 AS t_customer_wait
        ,DATE(FROM_UNIXTIME(oct.submit_time - 3600)) AS created_date
        ,FROM_UNIXTIME(oct.submit_time - 3600) AS submit_timestamp
        ,FROM_UNIXTIME(oct.estimated_delivered_time - 3600) AS estimated_delivered_time
        ,DATE_DIFF('second',FROM_UNIXTIME(oct.submit_time - 3600),FROM_UNIXTIME(oct.estimated_delivered_time - 3600))/CAST(60 AS DOUBLE) AS actual_eta
        ,DATE_DIFF('second',FROM_UNIXTIME(oct.submit_time - 3600),FROM_UNIXTIME(oct.final_delivered_time - 3600))/CAST(60 AS DOUBLE) AS lt_e2,
        raw.id,
        raw.group_id,
        raw.distance,
        raw.delivery_id

FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da oct

LEFT JOIN dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw 
    on raw.id = oct.id 
    and raw.order_type = 0    

WHERE date(dt) = current_date - interval '1' day
                                                                                    
AND oct.status = 7
)
,eta_value AS
(SELECT 
        ro.order_id,
        ro.order_code,
        ro.group_id,
        ro.distance,
        GREATEST(t_assign + t_pickup,t_confirm + t_prep) + (t_arrive_customer + t_customer_wait) AS eta_model,
        ro.actual_eta,
        ro.delivery_id

FROM raw_order ro

WHERE 1 = 1 
)
,eta_sum AS 
(SELECT 
        group_id,
        SUM(eta_model) AS eta_model,
        SUM(actual_eta) AS actual_eta,
        SUM(distance) AS distance, 
        SUM(distance)/CAST(SUM(eta_model) AS DOUBLE) AS vtb
FROM eta_value 
GROUP BY 1
)
,group_info_tab as 
(SELECT 
        id as group_id
        , CAST(JSON_EXTRACT(t.route,'$.order_id') AS INT) AS delivery_id
        , CAST(JSON_EXTRACT(t.route,'$.is_pick') AS VARCHAR) AS is_pick
        , group_code
        , t.route
        , ref_order_category
        , cast(json_extract(extra_data, '$.distance_matrix.data') as ARRAY(ARRAY(integer))) as distance_matrix
        , cast(json_extract(extra_data, '$.distance_matrix.mapping') as ARRAY(integer)) as mapping        
        , distance * 1.00 / 100 as group_distance
        , cast(json_extract(extra_data, '$.re') as double) AS re 
        , cast(json_extract(extra_data, '$.pick_city_id') as int) AS city_id
        , json_array_length(json_extract(extra_data, '$.distance_matrix.mapping')) / 2 as group_order_cnt
        , ship_fee * 1.00 / 100 / (json_array_length(json_extract(extra_data, '$.distance_matrix.mapping'))/2) as group_fee_per_order
        , extra_data
        , DATE(FROM_UNIXTIME(create_time - 3600)) AS created_date
        , json_array_length(json_extract(extra_data, '$.distance_matrix.mapping')) as leng_mapping
        , ROW_NUMBER()OVER(PARTITION  BY group_code) AS row_num

FROM shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live raw 

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.route') AS ARRAY<JSON>) ) AS t(route)

WHERE ref_order_category = 0
)
,summary AS 
(select 
         row_num
        ,delivery_id  
        ,is_pick
        ,group_id 
        ,group_code
        ,group_distance
        ,mapping
        ,distance_matrix
        ,ref_order_category
        ,created_date
        ,array_position(mapping,IF(is_pick = 'true',-1*delivery_id,delivery_id)) as check_index
        ,CASE WHEN ref_order_category = 0 THEN 'order_delivery' ELSE 'order_spxi' END AS source
        ,re
        ,group_order_cnt
        ,leng_mapping
        

from group_info_tab
WHERE created_date BETWEEN current_date - interval '7' day and current_date - interval '1' day
AND group_order_cnt > 1
AND delivery_id IS NOT NULL
)
,simulate_tab AS
(SELECT
        s1.group_code,
        s1.created_date,
        s1.group_id,
        s1.delivery_id,
        s2.delivery_id AS delivery_id_v2,
        s1.is_pick,
        s2.is_pick AS is_pick_v2,
        s1.row_num,
        s2.row_num as row_num_v2,
        s1.group_order_cnt,
        s1.group_distance,
        CASE 
        WHEN s2.row_num - s1.row_num <= 1 THEN 1 ELSE 0 END AS is_valid,
        ARRAY_POSITION(s1.mapping,IF(s1.is_pick = 'true',-1*s1.delivery_id,s1.delivery_id)) AS mapping1,
        ARRAY_POSITION(s2.mapping,IF(s2.is_pick = 'true',-1*s2.delivery_id,s2.delivery_id)) AS mapping2,
        s1.distance_matrix[ARRAY_POSITION(s1.mapping,IF(s1.is_pick = 'true',-1*s1.delivery_id,s1.delivery_id))]
                          [ARRAY_POSITION(s2.mapping,IF(s2.is_pick = 'true',-1*s2.delivery_id,s2.delivery_id))] 
                          AS distance_dump,
        ROW_NUMBER()OVER(PARTITION BY s1.delivery_id ORDER BY s1.row_num DESC) AS rank,
        SUM(s1.distance_matrix[ARRAY_POSITION(s1.mapping,IF(s1.is_pick = 'true',-1*s1.delivery_id,s1.delivery_id))]
                          [ARRAY_POSITION(s2.mapping,IF(s2.is_pick = 'true',-1*s2.delivery_id,s2.delivery_id))]
                )OVER(PARTITION BY s1.group_code ORDER BY s1.row_num) AS distance_cum_sum

FROM summary s1 

LEFT JOIN (select * from summary where row_num <= leng_mapping) s2 
    on s1.group_code = s2.group_code
    and (s1.row_num <  s2.row_num OR s2.row_num = s1.leng_mapping)

WHERE 1 = 1 
AND (CASE WHEN s2.row_num - s1.row_num <= 1 THEN 1 ELSE 0 END) = 1
AND s1.row_num <= s1.leng_mapping
-- AND s1.group_code = 'D64309149868'
)
SELECT
        single_distance_range,
        COUNT(DISTINCT group_code) AS cnt_group,
        COUNT(DISTINCT order_code) AS cnt_order,
        SUM(eta_extra)/COUNT(DISTINCT order_code) AS avg_extra_eta,
        (SUM(actual_eta)-SUM(eta_model))/COUNT(DISTINCT order_code) AS extra_current,
        SUM(actual_eta)/COUNT(DISTINCT order_code) AS avg_actual_eta

FROM 
(SELECT 
        s1.created_date,
        s1.group_id,
        s1.group_code,
        s1.group_distance,
        e1 .actual_eta,
        e1.order_code,
        e1.eta_model,
        s1.group_order_cnt,
        CASE 
        WHEN e1.distance <= 3 THEN '1. 0 - 3km'
        WHEN e1.distance <= 5 THEN '2. 3 - 5km'
        WHEN e1.distance <= 7 THEN '3. 5 - 7km' 
        WHEN e1.distance > 7 THEN '4. ++7km' END AS single_distance_range,
        MAX(eta.vtb) AS vtb,
        SUM(CASE WHEN s2.row_num < s1.row_num THEN s2.distance_dump ELSE NULL END)*1.0/1000 AS distance_simulation,
        (SUM(CASE WHEN s2.row_num < s1.row_num THEN s2.distance_dump ELSE NULL END)*1.0/1000)/CAST(MAX(eta.vtb) AS DOUBLE) AS eta_simulation,
        (SUM(CASE WHEN s2.row_num < s1.row_num THEN s2.distance_dump ELSE NULL END)*1.0/1000)/CAST(MAX(eta.vtb) AS DOUBLE) - e1.eta_model
        AS eta_extra 


FROM
(SELECT  
        s1.group_code,
        s1.group_id,
        s1.created_date,
        s1.delivery_id,
        s1.group_distance,
        MAX(s1.row_num)  AS row_num,
        MAX(group_order_cnt) AS group_order_cnt



FROM simulate_tab s1 
GROUP BY 1,2,3,4,5
) s1 
LEFT JOIN simulate_tab s2 
    on s1.group_code = s2.group_code

LEFT JOIN eta_value e1 
    on e1.delivery_id = s1.delivery_id

LEFT JOIN eta_sum eta 
    on eta.group_id = s1.group_id        

GROUP BY 1,2,3,4,5,6,7,8,9,e1.eta_model
)
WHERE eta_extra > 0 
GROUP BY 1 

