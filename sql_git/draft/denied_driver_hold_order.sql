with base as
(SELECT
      dod.uid AS shipper_id
    , city.name_en as city_name 
    , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
    , dot.ref_order_id
    , dot.ref_order_code
    , CASE
        WHEN dot.ref_order_category = 0 THEN 'Food/Market'
        --WHEN dot.ref_order_category = 4 THEN 'NS Instant'
        --WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
        --WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
        --WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
        --WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
    ELSE 'SPXI' END AS order_source
    -- , CASE
    --     WHEN dod.deny_type = 0 THEN 'NA'
    --     WHEN dod.deny_type = 1 THEN 'Driver_Fault'
    --     WHEN dod.deny_type = 10 THEN 'Order_Fault'
    --     WHEN dod.deny_type = 11 THEN 'Order_Pending'
    --     WHEN dod.deny_type = 20 THEN 'System_Fault'
    -- END AS deny_type
    , rea.content_en as deny_reason
    , case when sm.shipper_type_id = 12 
                and slot.uid is not null and (cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                else 'Non Hub' end as shipper_type
    ,FROM_UNIXTIME(dod.create_time - 3600) as deny_timestamp
    -- ,date_format(FROM_UNIXTIME(dod.create_time - 3600),'%T') as hour_timestamp
    -- ,cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60 as hour_minute
    ,from_unixtime(sa.create_time - 3600) as assign_ts 
    ,date_diff('second',from_unixtime(sa.create_time - 3600),FROM_UNIXTIME(dod.create_time - 3600))*1.00/60 as diff_assign_denied
    ,slot.shift_hour
    ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
          else 0 end as extra_ship
    FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod

    left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id
    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86
    LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dod.uid and try_cast(sm.grass_date as date) =  date(FROM_UNIXTIME(dod.create_time - 3600))
    LEFT JOIN 
    (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type,shipper_uid

from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live

where status in (2,10,14,15)

UNION

SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type,shipper_uid

from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live

where status in (2,10,14,15)
) sa on sa.order_id = dot.ref_order_id and sa.order_type = dot.ref_order_category and dod.uid = sa.shipper_uid



    --HUB SHIFT CHECK 
    left  join ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(start_time*1.0000/3600) as start_time 
                        ,(end_time*1.0000/3600) as end_time
                        ,(end_time - start_time)/3600 as shift_hour
    from 
    shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

    ) slot 

    on slot.uid = dod.uid and DATE(FROM_UNIXTIME(dod.create_time - 3600)) = date_ts 


    --HUB Performance

    left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub on hub.uid = dod.uid and DATE(FROM_UNIXTIME(dod.create_time - 3600)) = DATE(FROM_UNIXTIME(hub.report_date - 3600))






    WHERE 1=1
    -- and dot.ref_order_id = 22357054

    and deny_type <> 1
    -- and dot.ref_order_category <> 0 --NSS service 
    --and reason_id = 118
    
    order by dod.create_time desc)



    select deny_date
          ,shipper_id
          ,city_name
          ,concat(cast(shift_hour as varchar),'-',' hour shift') as shift_type
          ,extra_ship
          ,shipper_type as order_type
          ,ref_order_code
          ,order_source
          ,deny_reason
          ,deny_timestamp
          ,assign_ts as assign_timestamp 
          ,diff_assign_denied as minutes_assign_to_denied



    from base 
    

    where 1 = 1 
    and diff_assign_denied >= 5
    and deny_date between current_date - interval '7' day and current_date - interval '1' day
    and extra_ship > 0
    and shipper_type = 'Hub Inshift'



order by 1,12    