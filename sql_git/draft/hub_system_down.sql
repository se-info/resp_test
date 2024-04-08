with raw as 
(
SELECT 
 date(from_unixtime(hub.report_date - 3600)) as date_ 
,hub.uid 
,sm.shipper_name
,sm.city_name
,cast(json_extract(hub.extra_data,'$.total_order') as bigint) as total_order
,cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.000/3600 as online_ 
,cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 as online_peak
,coalesce(cast(json_extract(hub.extra_data,'$.total_bonus') as bigint),0) as daily_bonus
,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
      else 0 end as extra_ship
,cast(json_extract(hub.extra_data,'$.total_income') as bigint) as income_ 
,cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) as shift_ 
,case when array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 else 0 end as is_autoaccept
,case when ip.shipper_id is not null then 1 else 0 end as is_impact 
,dp.in_shift_peak_noon_online_time
,dp.in_shift_peak_night_online_time



FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub 


LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = hub.uid and try_cast(grass_date as date) = date(from_unixtime(hub.report_date - 3600))


LEFT JOIN dev_vnfdbi_opsndrivers.hub_list_impact_system_down ip on cast(ip.shipper_id as bigint) = hub.uid 


LEFT JOIN 
(
select *
from vnfdbi_opsndrivers.snp_foody_hub_driver_report_tab
where report_date = date '2022-04-15'
)dp on dp.shipper_id = hub.uid and dp.report_date =  date(from_unixtime(hub.report_date - 3600))

)

select * 


from raw 

where date_ = date'2022-04-15'
--and is_impact = 1 