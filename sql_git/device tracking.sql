with driver_device_all as 
    (
        select distinct a.request_id
            ,   if(sync_event_hit_result = 'B00', 1, 0) is_hit_rule
            ,   a.user_id shopee_uid
            ,   b.uid as now_shipper_id
            ,   concat('@',cast(json_extract(json_extract(a.entities, '$["DFPInfoSZ"]'),'$.SecurityDeviceID') as varchar)) device_id
            ,   if(regexp_like(coalesce(json_format(json_extract(json_extract(a.entities, '$["DFPInfoSZ"]'), '$.tags')),'na'), '%is_repack|is_repack_v2|is_repack_v3|is_repack_v4|is_repack_v5|is_gps_modified|is_wrong_pkg_name|is_suspicious_so%') = true, 1, 0) is_unofficial_app_tags     
            ,   if(regexp_like(coalesce(json_format(json_extract(json_extract(a.entities, '$["DFPInfoSZ"]'), '$.tags')),'na'), '%is_app_multi_open_system|is_app_multi_open_system_vmos|is_app_multi_open_app|is_system_multi_open_app%') = true, 1, 0) is_multi_open_app_tags 
            ,   json_format(json_extract(json_extract(a.entities, '$["DFPInfoSZ"]'), '$.tags')) as szsdk_risk_tags
            ,   try_cast(json_extract(a.attributes, '$.Latitude') as double) as login_latitude
            ,   try_cast(json_extract(a.attributes, '$.Longitude') as double) as login_longitude
            ,   date(from_unixtime(a.event_timestamp/1000000000 - 3600)) login_date
            ,   from_unixtime(a.event_timestamp/1000000000 - 3600) login_timestamp
            ,   a.event_timestamp/1000000000 - 3600 login_unixtime 
            ,   try_cast(json_extract(attributes, '$["Is Auto Login"]') as varchar) as is_auto_login
            ,   row_number() over(partition by shopee_uid, date(from_unixtime(a.event_timestamp/1000000000 - 3600)) order by a.event_timestamp/1000000000 - 3600 asc) rk
        from antifraud_region.dwd_evt_rule_engine_all_strategies_exec_log_hi__vn a 
        left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live b on a.user_id = b.shopee_uid
        where 1=1 
        and a.event_id = 16
        and cast(json_extract(json_extract(entities, '$["DFPInfoSZ"]'),'$.SecurityDeviceID') as varchar) != '' 
        and cast(json_extract(json_extract(entities, '$["DFPInfoSZ"]'),'$.SecurityDeviceID') as varchar) is not null
        and b.uid > 0 and a.user_id > 0
        and date(from_unixtime(a.event_timestamp/1000000000 - 3600)) between date'2022-11-16' and date'2022-11-30'
     --   and try_cast(json_extract(attributes, '$["Is Auto Login"]') as varchar) = 'false'
        and try_cast(json_extract(a.attributes, '$.Latitude') as double) > 0
        and try_cast(json_extract(a.attributes, '$.Longitude') as double) > 0
    )

    select * from driver_device_all 
