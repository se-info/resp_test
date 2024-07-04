with base_dod as
(
SELECT
      dod.uid AS shipper_id
    , spp.shopee_uid  
    , city.name_en as pick_city_name 
    , ct.personal_email
    , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
    , dot.ref_order_id
    , dot.ref_order_code
    , case when dotet.hub_id > 0 then 1 else 0 end as is_hub_qualified 
    , CASE
        WHEN dot.ref_order_category = 0 THEN 'Food/Market'
        --WHEN dot.ref_order_category = 4 THEN 'NS Instant'
        --WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
        --WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
        --WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
        --WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
    ELSE 'SPXI' END AS order_source
    ,dot.ref_order_category
    , CASE
        WHEN dod.deny_type = 0 THEN 'NA'
        WHEN dod.deny_type = 1 THEN 'Driver_Fault'
        WHEN dod.deny_type = 10 THEN 'Order_Fault'
        WHEN dod.deny_type = 11 THEN 'Order_Pending'
        WHEN dod.deny_type = 20 THEN 'System_Fault'
    END AS deny_type
    , rea.content_en as deny_reason
    , case when sm.shipper_type_id = 12 
                and slot.uid is not null and (cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                else 'Non Hub' end as shipper_type
    , case when hub.uid is not null then 
    (--- KPI
            case when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
            and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
            and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/
            (date_diff('second',from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
                      ,from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/cast(3600 as double)) >= 0.9
            and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
            and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 2 then 1

            when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
            and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
            and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/
                        (date_diff('second',from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
                      ,from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/cast(3600 as double)) >= 0.9
            and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
            and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 2 then 1

            when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) <> 6
            and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
            and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/
                        (date_diff('second',from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
                      ,from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/cast(3600 as double)) >= 0.9
            and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
            and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 then 1

            when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) = 6
            and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
            and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/
                        (date_diff('second',from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
                      ,from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/cast(3600 as double)) >= 0.9
            and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
            -- and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 
            then 1

            when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and sm.city_id = 217
            and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
            and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/
                        (date_diff('second',from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
                      ,from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/cast(3600 as double)) >= 0.9
            and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
            --and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 then 1 

            when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and sm.city_id = 218
            and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
            and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/
                        (date_diff('second',from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
                      ,from_unixtime(cast(cast(json_extract(hub.extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/cast(3600 as double)) >= 0.9
            and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
            and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 then 1 
            else 0 end) else 0 end as is_kpi

    ,FROM_UNIXTIME(dod.create_time - 3600) as deny_timestamp
    ,coalesce(fa.last_incharge_timestamp,FROM_UNIXTIME(dod.create_time - 3600)) as incharged_time 
    ,date_diff('second',coalesce(fa.last_incharge_timestamp,FROM_UNIXTIME(dod.create_time - 3600)),FROM_UNIXTIME(dod.create_time - 3600))/cast(60 as double) as diff_incharged_denied
    ,date_format(FROM_UNIXTIME(dod.create_time - 3600),'%T') as hour_timestamp
    ,cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60 as hour_minute

    FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod

    left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id
    
    LEFT JOIN 
            (
            SELECT 
                    order_id
                    ,cast(json_extract(order_data,'$.shipper_policy.type') as bigint) AS driver_payment_policy 
                    ,cast(json_extract(order_data,'$.hub_id') as bigint) as hub_id 
                    ,cast(json_extract(order_data,'$.pick_hub_id') as bigint) as pick_hub_id 
                    ,cast(json_extract(order_data,'$.drop_hub_id') as bigint) as drop_hub_id 
                    ,cast(json_extract(order_data,'$.real_pick_hub_id') as bigint) as real_pick_hub_id 
                    ,cast(json_extract(order_data,'$.real_drop_hub_id') as bigint) as real_drop_hub_id 

            FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day)

            )dotet ON dot.id = dotet.order_id

    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

    LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dod.uid and try_cast(sm.grass_date as date) =  date(FROM_UNIXTIME(dod.create_time - 3600))

    LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = dod.uid 
    --HUB SHIFT CHECK 
    LEFT JOIN ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(start_time*1.0000/3600) as start_time 
                        ,(end_time*1.0000/3600) as end_time
                from 
                shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
              )slot on slot.uid = dod.uid and DATE(FROM_UNIXTIME(dod.create_time - 3600)) = date_ts 
    
    ---HUB KPI 
    
    left join (select * from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live )hub 
                on hub.uid = dod.uid and date(from_unixtime(hub.report_date - 3600)) = DATE(FROM_UNIXTIME(dod.create_time - 3600))

    left join shopeefood.foody_internal_db__shipper_info_contact_tab__reg_daily_s2_live ct on ct.uid = dod.uid 

    LEFT JOIN
                    (
                    SELECT   order_id 
                            , 0 as order_type
                            ,shipper_uid
                            ,from_unixtime(create_time - 3600) as last_incharge_timestamp
                            -- ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                            -- ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp  
                            -- ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp 
                            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                            where 1=1 
                            and status = 11 
                            and grass_schema = 'foody_order_db'
                    
                    UNION
                    
                    SELECT   ns.order_id
                            ,ns.order_type 
                            ,shipper_uid
                            ,from_unixtime(create_time - 3600) as last_incharge_timestamp
                            -- ,case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end as last_incharge_timestamp
                    FROM    

                            ( SELECT order_id, order_type , create_time , update_time, status, shipper_uid
                    
                             from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and status in (2,10,14,15,7,5)
                             and grass_schema = 'foody_partner_archive_db'   
                             UNION
                        
                             SELECT order_id, order_type, create_time , update_time, status, shipper_uid
                        
                             from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and status in (2,10,14,15,7,5)
                             and grass_schema = 'foody_partner_db'
                             )ns
                            --  where order_id = 27756166
                    )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type and fa.shipper_uid = dod.uid 
    WHERE 1=1

    and dot.ref_order_category = 6 --NSS service 
    
    order by dod.create_time desc
    )
,assign AS (
   SELECT *
   FROM
     (
      SELECT
        ns.order_id
      , ns.order_type
      , ns.status
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
      , CASE WHEN (ns.order_type = 200) THEN 'Group Order' 
             WHEN COALESCE(dot.group_id, 0) > 0 THEN 'Stack Order' 
             ELSE 'Single Order' 
             END AS order_group_type
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
      AND date(from_unixtime(ns.create_time -3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day
      AND ns.order_type = 200 
      AND ns.city_id <> 238
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
     )
)    
,metrics as 
(select a.*
          ,case when ogi.group_status is not null then 1 else 0 end as is_group
          ,ogi.group_id
          ,case when ogi.group_status is not null and assign.order_id is not null then assign.order_group_type 
                when ogi.group_status is not null and assign.order_id is null then 'Stack'
                else 'Single' end as order_group_type   
    
    from base_dod a 

    -- left join group_ b on regexp_like(a.ref_order_code,b.order_agg) = true

    LEFT JOIN 
    (
        select ogi.*,ogm.uid,ogm.group_status,ogi.create_time as order_create_time, ogm.create_time as group_create_time

    from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da 
    where date(dt) = current_date - interval '1' day) ogi 

    LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogm on ogm.id = ogi.group_id
    -- where ogi.group_id = 31938127
    ) ogi on ogi.ref_order_id = a.ref_order_id 
          and ogi.ref_order_category = a.ref_order_category
          and ogi.uid = a.shipper_id
          and ogi.mapping_status = 22

    LEFT JOIN assign    on assign.order_id = ogi.group_id 
                        and ogi.ref_order_category = assign.order_category
                        and ogi.uid = assign.shipper_id
                        and ogi.mapping_status = 22

    where 1 = 1
    
    and a.shipper_type = 'Hub Inshift'
    
    and a.deny_reason = 'Order out of working location (Hub Driver only)'

    and a.deny_date >= current_date - interval '30' day
)    
    
select 
        metrics.* 

from metrics

