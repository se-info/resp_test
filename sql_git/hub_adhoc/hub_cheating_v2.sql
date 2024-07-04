with hub_income as
(
select 
    date(from_unixtime(hub.report_date - 3600)) as report_date
    ,uid as shipper_id
    ,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_category_name
    ,cast(json_extract(hub.extra_data,'$.total_order') as bigint) as total_order_inshift
    ,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
    else 0 end as extra_ship
    ,cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) as is_apply_fixed_amount -- check driver has order <= threshold and pass all kpi >> dieu kien de duoc bu min
    ,cast(json_extract(hub.extra_data,'$.total_income') as bigint) as total_income
    ,cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint) as calculated_shipping_shared
    ,hub.extra_data
    ,cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) as city_id
    ,case 
    	when cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) not in (217,218,220) then 999 
    	else cast(json_extract(hub.extra_data,'$.lasted_shipper_info.city_id') as bigint) end as dummy_city_id
    ,cast(json_extract(hub.extra_data,'$.hub_ids') as array<int>) as hub_id
from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
where date(from_unixtime(hub.report_date - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
,list_shipper_hub_id as
(
select 
        hub.report_date
        ,hub.shipper_id
        ,hub.hub_id as hub_array
        ,a.hub_id
from hub_income hub
cross join unnest (hub_id) a(hub_id)
    )

,final_hub_name_array as
(select
    b.report_date
    ,b.shipper_id
    ,hub_array
    ,array_agg(distinct inf.hub_name) as hub_name_array
from list_shipper_hub_id b
left join shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live inf on inf.id = b.hub_id
group by 1,2,3
)

,base as
(select 
    hub.*
    ,case when hub.total_order_inshift < cast(config.min_order as int) then 1 else 0 end as is_under_min
    ,case when is_apply_fixed_amount = 'true' and hub.total_order_inshift < cast(config.min_order as int) then 1 else 0 end is_compensated
    ,cast(config.min_order as int) as min_order
from hub_income hub
left join vnfdbi_opsndrivers.ingest_hub_config_min_order config
	on lower(hub.shift_category_name) = lower(config.shift_category_name) and hub.dummy_city_id = cast(config.city_id as bigint) and hub.report_date between cast (config.start_date as date)  and  coalesce(try_cast(config.end_date as date) ,current_date) 
)
select 
    b.*
    ,case when sip.working_status = 1 then 'WORKING' 
        when sip.working_status = 2 then 'OFF'
        when sip.working_status = 3 then 'WAITING'
        else null end as working_status 
    ,case when sm.shipper_type_id = 12 then 'hub' else 'non-hub' end as current_shipper_type
    ,hub_name.hub_name_array
from base b
left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live sip
    on sip.uid = b.shipper_id
left join shopeefood.foody_mart__profile_shipper_master sm
    on b.shipper_id = sm.shipper_id and sm.grass_date = 'current'
left join final_hub_name_array as hub_name
    on b.shipper_id = hub_name.shipper_id and b.report_date = hub_name.report_date
where is_compensated = 1