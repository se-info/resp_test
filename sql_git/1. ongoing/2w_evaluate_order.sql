with raw as 
(select 
        ro.created_date,
        ro.order_code,
        ro.source,
        ro.hub_id,
        ro.pick_hub_id,
        ro.drop_hub_id,
        ro.distance as user_distance,
        ro.district_id,
        dot.driver_distance,
        -- IF(ro.distance <= 3,'1. less than 3km','2. over 3km') as user_distance_group,
        -- IF(dot.driver_distance <= 3,'1. less than 3km','2. over 3km') as driver_distance_group,
        case 
        when dot.driver_distance between 0 and 3 then '1. 0-3km'
        when dot.driver_distance between 3 and 4 then '2. 3-4km'
        when dot.driver_distance between 4 and 5 then '3. 4-5km'
        when dot.driver_distance > 5 then '4. 5km++'
        end as driver_distance_group,
        case 
        when ro.distance between 0 and 3 then '1. 0-3km'
        when ro.distance between 3 and 4 then '2. 3-4km'
        when ro.distance between 4 and 5 then '3. 4-5km'
        when ro.distance > 5 then '4. 5km++'
        end as user_distance_group,
        if(ho.ref_order_id is not null,1,0) as is_hub_delivered
        -- ARRAY_MIN(ARRAY_AGG("great_circle_distance"(ro.drop_latitude,ro.drop_longitude,
        --                          CAST(hi.hub_lat AS DOUBLE),CAST(hi.hub_long AS DOUBLE))
        --                          )) AS hub_edge_distance  
from (select raw.*,if(raw.order_type != 0,1,coalesce(is_foody_delivery,0)) as filter_delivery
from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 
left join (select id,is_foody_delivery 
           from shopeefood.shopeefood_mart_dwd_vn_order_completed_da 
           where date(dt) = current_date - interval '1' day) oct 
                on raw.id = oct.id
) ro 

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_daily_s0_live ho on ho.ref_order_id = ro.id and ho.ref_order_category = ro.order_type

-- LEFT JOIN 
-- (SELECT 
--        raw.id 
--       ,raw.hub_name 
--       ,CAST(t.points AS ARRAY<JSON>)[1] AS hub_lat
--       ,CAST(t.points AS ARRAY<JSON>)[2] AS hub_long

-- FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live raw  

-- CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.geo_data.points') AS ARRAY<JSON>)) AS t(points)
-- ) hi on hi.id = CAST(ro.pick_hub_id AS BIGINT)


-- left join  dev_vnfdbi_opsndrivers.driver_ops_hub_polygon_tab t2
--     on ST_Within(ST_Point(pick_latitude,pick_longitude), ST_GeometryFromText(t2.hub_polygon))

-- left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi 
--    on hi.id = cast(t2.hub_id as bigint)

left join 
(select ref_order_id,ref_order_code,delivery_distance/1000.00 as driver_distance,ref_order_category

from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
where date(dt) = current_date - interval '1' day 
) dot on dot.ref_order_id = ro.id and dot.ref_order_category = ro.order_type

where 1 = 1 
and ro.created_date = date'2024-08-12'
and ro.order_status = 'Delivered'
and ro.filter_delivery = 1 
-- group by 1,2,3,4,5,6,7,8,9,10
)
select 
        -- created_date,
        coalesce(driver_distance_group,'Grand Total') as driver_distance_group,
        coalesce(user_distance_group,'Grand Total') as user_distance_group,
        count(distinct order_code) as ado,
        count(distinct case when is_hub_delivered = 1 then order_code else null end) as hub_ado,
        count(distinct case when is_hub_delivered != 1 then order_code else null end) as non_hub_ado


from raw 

-- group by 1,grouping sets (user_distance_group,())
group by grouping sets (driver_distance_group,user_distance_group,(driver_distance_group,user_distance_group),())

