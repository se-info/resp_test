
drop table if exists dev_vnfdbi_opsndrivers.navgigation_table;
create table if not exists dev_vnfdbi_opsndrivers.navgigation_table
as 
SELECT dot.ref_order_code
,dot.ref_order_id
,date(from_unixtime(dot.submitted_time - 3600)) as create_date
,date(from_unixtime(dot.real_drop_time - 3600)) as delivered_date
,dot.uid
,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as is_hub_driver
,case when cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift 
,from_unixtime(dot.submitted_time - 3600) create_timestamp
,from_unixtime(dot.real_drop_time - 3600) delivered_timestamp
,(dot.delivery_distance*1.00)/1000 as distance
,case when dot.pick_city_id = 217 then 'HCM'
when dot.pick_city_id = 218 then 'HN'
when dot.pick_city_id = 219 then 'DN'
when dot.pick_city_id = 220 then 'HP'
ELSE 'OTH' end as city_group
,hub.hub_name as pick_hub_name
,hubb.hub_name
,hubbb.hub_name as drop_hub_name
,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0) as real_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0) as real_pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_drop_hub_id') as BIGINT ),0) as real_drop_hub_id
,drop_latitude
,drop_longitude
,pick_longitude
,pick_latitude
FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet ON dot.id = dotet.order_id
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hub on hub.id = COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0)
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubb on hubb.id = COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0)
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubbb on hubbb.id = COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0)

LEFT JOIN (SELECT *
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date
from shopeefood.foody_mart__profile_shipper_master
where grass_region = 'VN')sm on sm.shipper_id = dot.uid and sm.report_date = date(from_unixtime(dot.submitted_time - 3600))
WHERE 1 = 1
and dot.ref_order_category = 0
and dot.order_status = 400

--and dot.pick_city_id in (217)
and date(from_unixtime(dot.submitted_time - 3600)) between date'2022-05-10' and date'2022-05-16'
--current_date - interval '30' day and current_date - interval '1' day