with raw as 
(select 
        ro.order_code,
        ro.source,
        ro.hub_id,
        ro.pick_hub_id,
        ro.drop_hub_id,
        ro.distance as user_distance,
        ro.district_id,
        dot.driver_distance,
        IF(ro.distance <= 3,'1. less than 3km','2. over 3km') as user_distance_group,
        -- IF(dot.driver_distance <= 3,'1. less than 3km','2. over 3km') as driver_distance_group,
        case 
        when dot.driver_distance <= 3.6 then '1. 0 - 3.6km' 
        when dot.driver_distance <= 3.8 then '2. 3.6 - 3.8km' 
        when dot.driver_distance <= 4 then '3. 3.8 - 4km' 
        when dot.driver_distance <= 4.2 then '4. 4 - 4.2km' 
        when dot.driver_distance <= 4.5 then '5. 4.2 - 4.5km'
        when dot.driver_distance > 4.5 then '6. ++4.5km' end as driver_distance_group,
        ARRAY_MIN(ARRAY_AGG("great_circle_distance"(ro.drop_latitude,ro.drop_longitude,
                                 CAST(hi.hub_lat AS DOUBLE),CAST(hi.hub_long AS DOUBLE))
                                 )) AS hub_edge_distance  

from driver_ops_raw_order_tab ro 

LEFT JOIN 
(SELECT 
       raw.id 
      ,raw.hub_name 
      ,CAST(t.points AS ARRAY<JSON>)[1] AS hub_lat
      ,CAST(t.points AS ARRAY<JSON>)[2] AS hub_long

FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live raw  

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.geo_data.points') AS ARRAY<JSON>)) AS t(points)
) hi on hi.id = CAST(ro.pick_hub_id AS BIGINT)


-- left join  dev_vnfdbi_opsndrivers.driver_ops_hub_polygon_tab t2
--     on ST_Within(ST_Point(pick_latitude,pick_longitude), ST_GeometryFromText(t2.hub_polygon))

-- left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi 
--    on hi.id = cast(t2.hub_id as bigint)

left join 
(select ref_order_id,ref_order_code,delivery_distance/1000.00 as driver_distance,ref_order_category

from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
where date(dt) = current_date - interval '1' day 
) dot on dot.ref_order_id = ro.id and dot.ref_order_category = ro.order_type

where ro.hub_id is null 
and ro.pick_hub_id > 0 
and ro.city_id = 217
and ro.order_type = 0 
and ro.created_date = date'2024-08-13'
and ro.order_status = 'Delivered'

group by 1,2,3,4,5,6,7,8,9,10
)
select 
        user_distance_group,
        driver_distance_group,
        border_distance_group,
        count(distinct order_code) as total_order

from
(select 
        user_distance_group,
        driver_distance_group,
        case 
        when hub_edge_distance <= 1.8 then '1. 0 - 1.8km'
        when hub_edge_distance <= 2 then '2. 1.8 - 2km'
        when hub_edge_distance <= 2.2 then '3. 2- 2.2km' 
        when hub_edge_distance > 2.2 then '4. ++2.2' 
        end as border_distance_group,
        order_code,
        pick_hub_id,
        drop_hub_id,
        driver_distance
        


from raw 

)

group by 1,2,3
LIMIT 100 




