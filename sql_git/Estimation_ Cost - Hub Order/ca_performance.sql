WITH raw AS 
(SELECT 
         sa.ref_order_id
        ,sa.driver_id 
        ,sa.order_category
        ,sa.status
        ,sa.create_time 
        ,case when sa.experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted
        ,case when sa.experiment_group in (5,6,7,8) then 1 else 0 end as is_ca
        ,CAST(SPLIT(sa.location,',')[1] AS DOUBLE) AS assign_lat
        ,CAST(SPLIT(sa.location,',')[2] AS DOUBLE) AS assign_long
        ,dot.drop_latitude AS previous_drop_latitude
        ,dot.drop_longitude AS previous_drop_longitude
        ,dot.pick_latitude
        ,dot.pick_longitude
        ,hm.hub_type_x_start_time
        ,hm.start_shift_time
        ,hm.end_shift_time
        ,FROM_UNIXTIME(dot.real_drop_time - 3600) AS delivered_timestamp
        ,hi.hub_name AS order_hub_name
        ,hm.hub_locations AS driver_hub_name
        ,CASE WHEN doet.hub_id > 0 THEN 1 ELSE 0 END AS is_hub_order
        ,"great_circle_distance"(CAST(SPLIT(sa.location,',')[1] AS DOUBLE),CAST(SPLIT(sa.location,',')[2] AS DOUBLE),dot.pick_latitude,dot.pick_longitude) AS assign_to_pick        
        ,ROW_NUMBER()OVER(PARTITION BY sa.driver_id,DATE(sa.create_time) ORDER BY sa.create_time ASC) AS assign_rank
        ,ROW_NUMBER()OVER(PARTITION BY sa.ref_order_id ORDER BY sa.create_time ASC) AS order_rank



FROM dev_vnfdbi_opsndrivers.phong_temp_assign sa

LEFT JOIN (SELECT * FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics WHERE total_order > 0) hm
    on hm.uid = sa.driver_id
    and hm.date_ = date(sa.create_time)
    and sa.create_time between hm.start_shift_time and hm.end_shift_time

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = sa.ref_order_id 
    and dot.ref_order_category = sa.order_category

LEFT JOIN (select order_id,CAST(json_extract(order_data,'$.hub_id') as BIGINT) AS hub_id from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet 
    on dot.id = doet.order_id

LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi 
    on hi.id = doet.hub_id
    and hi.city_id = 217

WHERE date(sa.create_time) = date'2023-06-23'	
and sa.status in (3,4)
and sa.city_id = 217
and hm.uid is not null
and sa.order_status = 400
)
SELECT
         raw.ref_order_id AS ca_order
        ,raw.driver_id  
        ,raw.assign_lat
        ,raw.assign_long
        ,raw.order_hub_name
        ,raw.driver_hub_name
        ,f.is_hub AS first_ca_assign_hub
        ,raw.assign_to_pick
        ,"great_circle_distance"(MAX_BY(v2.previous_drop_latitude,v2.create_time),MAX_BY(v2.previous_drop_longitude,v2.create_time),raw.pick_latitude,raw.pick_longitude)*1000 AS drop1_to_pick_ca
        ,"great_circle_distance"(raw.assign_lat,raw.assign_long,MAX_BY(v2.previous_drop_latitude,v2.create_time),MAX_BY(v2.previous_drop_longitude,v2.create_time))*1000 AS assign_ca_to_drop1_distance
        ,MAX_BY(v2.ref_order_id,v2.create_time) AS previous_order
        ,MAX_BY(v2.previous_drop_latitude,v2.create_time) as previous_drop_latitude_v2
        ,MAX_BY(v2.previous_drop_longitude,v2.create_time) as previous_drop_longitude_v2


FROM raw

LEFT JOIN (SELECT 
                 sa.ref_order_id
                ,CASE WHEN sm.shipper_type_id = 12 THEN 1 ELSE 0 END AS is_hub 
                ,ROW_NUMBER()OVER(PARTITION BY sa.driver_id,DATE(sa.create_time) ORDER BY sa.create_time ASC) AS assign_rank
                ,ROW_NUMBER()OVER(PARTITION BY sa.ref_order_id ORDER BY sa.create_time ASC) AS order_rank
FROM dev_vnfdbi_opsndrivers.phong_temp_assign sa  
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = sa.driver_id 
    and try_cast(sm.grass_date as date) = date(sa.create_time)

WHERE 1 = 1 
AND date(sa.create_time) = date'2023-06-23'
AND sa.experiment_group in (5,6,7,8)
) f on f.ref_order_id = raw.ref_order_id AND f.order_rank = 1

LEFT JOIN raw v2 
    on v2.driver_id = raw.driver_id
    and date(v2.create_time) = date(raw.create_time)
    and v2.assign_rank < raw.assign_rank
    and v2.delivered_timestamp > raw.create_time


WHERE raw.is_ca = 1
AND v2.driver_id is not null     
GROUP BY 1,2,3,4,5,6,7,8,raw.pick_latitude,raw.pick_longitude