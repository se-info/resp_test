WITH
  raw AS (
   SELECT *
   FROM
     (
      SELECT
        ns.order_id
      , ns.order_type
      , ns.status
      , location
      , try_cast(split_part(location,',',1) as double) as incharged_lattitue
      , try_cast(split_part(location,',',2) as double) as incharged_longtitute
      , CASE 
            WHEN ns.order_type = 0 THEN 'Food/Market'
            WHEN ns.order_type = 4 THEN 'NowShip Instant' 
            WHEN ns.order_type = 5 THEN 'NowShip Food Mex' 
            WHEN ns.order_type = 6 THEN 'NowShip Shopee' 
            WHEN ns.order_type = 7 THEN 'NowShip Same Day' 
            WHEN ns.order_type = 8 THEN 'NowShip Multi Drop' 
            WHEN ns.order_type = 200 AND ogi.ref_order_category = 0 THEN 'Food/Market' 
            WHEN ns.order_type = 200 AND ogi.ref_order_category = 6 THEN 'NowShip Shopee' 
            WHEN ns.order_type = 200 AND ogi.ref_order_category = 7 THEN 'NowShip Same Day' 
            ELSE 'Others' END AS order_source
      , (CASE WHEN (ns.order_type <> 200) THEN ns.order_type ELSE ogi.ref_order_category END) order_category
      , CASE WHEN (ns.order_type = 200) THEN '1. Group Order' 
             WHEN COALESCE(dot.group_id, 0) > 0 THEN '2. Stack Order' 
             ELSE '3. Single Order' 
             END AS order_group_type
      , ns.assign_type       
      , ns.city_id
      , city.name_en city_name
      , CASE WHEN ns.city_id = 217 THEN 'HCM' WHEN ns.city_id = 218 THEN 'HN' WHEN ns.city_id = 219 THEN 'DN' ELSE 'OTH' END city_group
      , from_unixtime(ns.create_time - 3600) create_time
      , from_unixtime(ns.update_time - 3600) update_time
      , date(from_unixtime(ns.create_time - (60 * 60))) as date_
      , CASE
            WHEN ns.order_type = 200 AND ogi.ref_order_category = 0 THEN COALESCE(g.food_service, 'NA') 
            WHEN ns.order_type = 0 THEN COALESCE(s.food_service, 'NA') 
            ELSE 'NowShip' END AS food_service
      , CASE 
            WHEN ns.order_type <> 200 THEN 1 
            ELSE COALESCE(order_rank.total_order_in_group_at_start, 0) END AS total_order_in_group
      , CASE 
            WHEN ns.order_type <> 200 THEN 1 
            ELSE COALESCE(order_rank.total_order_in_group_actual_del, 0) END AS total_order_in_group_actual_del
      , ns.shipper_uid shipper_id
      FROM
        (
         SELECT
           order_id
         , order_type
         , create_time
         , assign_type
         , update_time
         , status
         , city_id
         , shipper_uid
         , location
         FROM
           shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
         WHERE 1 = 1 AND status IN (3, 4, 8, 9, 2, 14, 15, 17, 18)
UNION          SELECT
           order_id
         , order_type
         , create_time
         , assign_type
         , update_time
         , status
         , city_id
         , shipper_uid
         , location
         FROM
           shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
         WHERE 1 = 1 AND status IN (3, 4, 8, 9, 2, 14, 15, 17, 18)
      )  ns
LEFT JOIN (
         SELECT
           id
         , ref_order_category
         FROM
           shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live
         GROUP BY 1, 2
      )  ogi ON ogi.id > 0 AND ogi.id = (CASE WHEN ns.order_type = 200 THEN ns.order_id ELSE 0 END)
      LEFT JOIN (
         SELECT
           ogm.group_id
         , ogi.group_code
         , count(DISTINCT ogm.ref_order_id) total_order_in_group
         , count(DISTINCT (CASE WHEN (ogi.create_time = ogm.create_time) THEN ogm.ref_order_id ELSE null END)) total_order_in_group_at_start
         , count(DISTINCT (CASE WHEN ((ogi.create_time = ogm.create_time) AND (ogm.mapping_status = 11)) THEN ogm.ref_order_id ELSE null END)) total_order_in_group_actual_del
         FROM
           shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm
         LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi ON ogi.id = ogm.group_id
         WHERE 1 = 1 AND ogm.group_id IS NOT NULL
         GROUP BY 1, 2
      )  order_rank ON order_rank.group_id = (CASE WHEN (ns.order_type = 200) THEN ns.order_id ELSE 0 END)
      LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON (city.id = ns.city_id) AND (city.country_id = 86)
      LEFT JOIN (
         SELECT
           ref_order_id
         , ref_order_category
         , group_id
         FROM
           shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live
         WHERE (grass_schema = 'foody_partner_db')
         GROUP BY 1, 2, 3
      )  dot ON dot.ref_order_id = ns.order_id AND ns.order_type <> 200 AND ns.order_type = dot.ref_order_category
      LEFT JOIN (
         SELECT
           dot.ref_order_id
         , dot.ref_order_category
         , CASE WHEN (go.now_service_category_id = 1) THEN 'Food' 
                WHEN (go.now_service_category_id > 0) THEN 'Fresh/Market' 
                ELSE 'Others'
                END AS food_service
         FROM
           shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
         LEFT JOIN (
            SELECT
              id
            , now_service_category_id
            FROM
              shopeefood.foody_mart__fact_gross_order_join_detail
            WHERE (grass_region = 'VN')
            GROUP BY 1, 2
         )  go ON go.id = dot.ref_order_id AND dot.ref_order_category = 0
         WHERE 1 = 1 
         AND dot.ref_order_category = 0 
         AND go.now_service_category_id >= 0
         GROUP BY 1, 2, 3
      )  s ON s.ref_order_id = ns.order_id AND ns.order_type = 0 AND ns.order_type = dot.ref_order_category
      LEFT JOIN (
         SELECT
           ogm.group_id
         , ogm.ref_order_category
         , (CASE WHEN (go.now_service_category_id = 1) THEN 'Food' WHEN (go.now_service_category_id > 0) THEN 'Fresh/Market' ELSE 'Others' END) food_service
         FROM
           shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm
         LEFT JOIN (
            SELECT
              id
            , now_service_category_id
            FROM
              shopeefood.foody_mart__fact_gross_order_join_detail
            WHERE (grass_region = 'VN')
            GROUP BY 1, 2
         )  go ON (go.id = ogm.ref_order_id) AND (ogm.ref_order_category = 0)
         WHERE 1 = 1 AND (ogm.ref_order_category = 0) AND (COALESCE(ogm.group_id, 0) > 0) AND (go.now_service_category_id >= 0)
         GROUP BY 1, 2, 3
      )  g ON g.group_id = ns.order_id AND ns.order_type = 200 AND (CASE WHEN (ns.order_type <> 200) THEN ns.order_type ELSE ogi.ref_order_category END )= 0
      
      WHERE 1 = 1 
      AND date(from_unixtime(ns.create_time -3600)) BETWEEN current_date - interval '2' day and current_date - interval '2' day
      AND ns.city_id <> 238
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 ,18, 19, 20
     )
)
,ds_routing as 
(select 
         split_part(cast(order_id as varchar),'_',2) as order_id
       , split_part(cast(order_id as varchar),'_',1) as order_type  
       , shipper_id
       , driving_distance

from 

(select 
cast((json_extract(processing_info,'$.ds_response.results')) as json) as t1
        ,t.order_id
        ,json_extract(t.ext_info[1],'$.driving_distance') as driving_distance
        ,json_extract(t.ext_info[1],'$.shipper_id') as shipper_id

from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab__reg_daily_s0_live
cross join unnest (cast((json_extract(processing_info,'$.ds_response.results')) as map<varchar,array<json>>)) as t(order_id,ext_info)

where json_extract(processing_info,'$.ds_response.results') is not null

) a

where split_part(cast(order_id as varchar),'_',1) = 'do'

)
select  raw.order_id
       ,raw.shipper_id
       ,cast(ds.driving_distance as double)/cast(1000 as double) as driving_distance
       ,raw.location as incharged_latlong 
       ,cast(dot.pick_latitude as varchar)||','||cast(dot.pick_longitude as varchar) as pick_latlong 

from raw   

left join (select * 
from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = raw.order_id and dot.ref_order_category = raw.order_type

left join ds_routing ds on cast(ds.order_id as bigint) = raw.order_id and raw.order_type = 0 and raw.shipper_id = cast(ds.shipper_id as bigint) and order_group_type != '2. Stack Order'

where 1 = 1 
and raw.order_type = 0
and raw.location is not null 
and order_group_type not in ('2. Stack Order','1. Group Order')
and assign_type <> 5
group by 1,2,3,4,5

limit 500
;

WITH orig AS(
    SELECT 
        id
        , city_id
        , processing_time
        , grass_date
        , create_time
        , date_format(FROM_UNIXTIME(create_time, 7, 0), '%Y-%m-%d %H:%i:%s') AS date_str
        , date_format(FROM_UNIXTIME(create_time, 7, 0), '%Y-%m-%d') AS date_string
        
        , json_extract_scalar(processing_info, '$.ds_response["batch_id"]') AS batch_id
        , json_extract(processing_info, '$.ds_response["results"]') AS ds_response_results

        , json_extract(processing_info, '$.ds_request') AS ds_request
        , json_extract(processing_info, '$.ds_request["info"]') AS ds_request_info
        , json_extract(processing_info, '$.ds_request["order_shippers"]') AS ds_request_orders
        , json_extract(processing_info, '$.ds_request["shippers"]') AS ds_request_shippers
        , json_extract(processing_info, '$.ds_request["order_shippers"]') AS ds_request_order_shippers
        , json_extract(processing_info, '$.ds_request["vehicles"]') AS ds_request_vehicles
    FROM shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab__reg_daily_s0_live
    WHERE not json_extract(processing_info, '$.ds_request["info"]') is null
    order by create_time, id
),

response AS(
    SELECT 
        id
        , city_id
        , grass_date
        , create_time
        , date_string
        , batch_id
        , order_id
        , CAST(json_extract(value, '$.shipper_id') AS bigint) AS shipper_id
        , CAST(json_extract(value, '$.pred_accept_prob') AS double) AS pred_accept_prob
        , CAST(json_extract(value, '$.driving_distance') AS double) AS driving_distance
        , CAST(json_extract(value, '$.flying_distance') AS double) AS flying_distance
    FROM orig
    CROSS JOIN UNNEST(cast(ds_response_results as map<varchar,json>)) AS x(order_id, value_list)
    CROSS JOIN UNNEST(cast(value_list as array<json>)) AS a(value)
)

SELECT * FROM response 