select 
        ps.mode_id,
        b.name as peak_mode_name,
        c.name_en as city_name,
        di.name_en as district_name,
        -- json_extract(ps.configurations,'$.instance_settings') as config
        try_cast(json_extract(t.info,'$.type') as bigint) as "type",
        case 
        when try_cast(json_extract(t.info,'$.type') as bigint) = 1 then 'MAX_DELIVERY_DISTANCE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 2 then 'MIN_SHIPPING_FEE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 3 then 'SHIPPING_FEE_RATE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 4 then 'PREPARE_TIME'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 5 then 'DRIVER_FIRST_MODE #Deprecated'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 6 then 'AUTO_CONFIRM_MAX_TIME'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 7 then 'MINIMUM_SHIPPING_FEE_BASE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 8 then 'AUTO_ASSIGN_TIMEOUT'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 9 then 'TASK_AUTO_UPDATE_CONFIRMED_FROM'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 10 then 'TASK_AUTO_UPDATE_CONFIRMED_TO'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 11 then 'DRIVER_MIN_SHIPPING_FEE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 12 then 'DRIVER_SHIPPING_FEE_RATE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 13 then 'NEW_DRIVER_FIRST_MODE_ENABLED'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 14 then 'NOW_SHIP_USER_MIN_SHIPPING_FEE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 15 then 'NOW_SHIP_USER_RATE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 16 then 'NOW_SHIP_USER_MAX_DISTANCE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 17 then 'NOW_SHIP_DRIVER_MIN_SHIPPING_FEE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 18 then 'NOW_SHIP_DRIVER_RATE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 19 then 'NOW_SHIP_DRIVER_MAX_DISTANCE'
        when try_cast(json_extract(t.info,'$.type') as bigint) = 20 then 'SEND_DRIVER_PN'
        end as setting_type,
        try_cast(json_extract(t.info,'$.value') as double) as "value"

from shopeefood.foody_delivery_admin_db__peak_mode_instance_tab__reg_daily_s0_live ps

left join shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live b on b.id = ps.mode_id

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live c on c.id = ps.city_id and c.country_id = 86

left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = ps.district_id and di.province_id = ps.city_id

cross join unnest (cast(json_extract(ps.configurations,'$.instance_settings') as array<json>)) as t(info)

where b.name in ('Normal Mode','Peak 1 Mode','Peak 2 Mode','Peak 3 Mode','Idle Mode')
