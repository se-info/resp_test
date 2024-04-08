SELECT a.report_date
,a.create_hour
--,a.tier
,a.city_group
,a.hub_location
,a.is_hub_qualified
,a.is_hub_driver
,count(a.ref_order_id) as total_order
from
(SELECT base.report_date
,base.uid
,base.create_hour
,blt.current_driver_tier as tier
,base.ref_order_id
,base.city_group
,base.pick_hub_id
,coalesce(base.hub_name,base.pick_hub_name)  as hub_location
,base.distance
,case when base.distance <= 1 then '1. 0.1km - 1km'
when base.distance <= 2 then '2. 1km - 2km'
when base.distance <=3 then '3. 2km - 3km'
when base.distance >3 then '4. >3km'
else null end as distance_range
,case when base.shipper_type_id = 12 then '1'
ELSE '0' end as is_hub_driver
--,count(case WHEN base.hub_id > 0 or (base.pick_hub_id >0 and base.distance <= 3 ) then base.ref_order_id else null end) as total_hub_qualified
,case WHEN base.hub_id > 0 then 1
      WHEN base.pick_hub_id > 0 and base.distance <= 2 then 1
      else 0 end as is_hub_qualified
FROM
(SELECT dot.ref_order_id,date(from_unixtime(dot.submitted_time - 3600)) as report_date
,dot.uid
,extract(hour from from_unixtime(dot.submitted_time - 3600)) as create_hour
,(dot.delivery_distance*1.00)/1000 as distance
,case when dot.pick_city_id = 217 then 'HCM'
when dot.pick_city_id = 218 then 'HN'
when dot.pick_city_id = 219 then 'DN'
ELSE 'OTH' end as city_group
,sm.shipper_type_id
,hub.hub_name as pick_hub_name
,hubb.hub_name
,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet ON dot.id = dotet.order_id
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hub on hub.id = COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0)
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubb on hubb.id = COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0)
LEFT JOIN (SELECT *
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date
from shopeefood.foody_mart__profile_shipper_master)sm on sm.shipper_id = dot.uid and sm.report_date = date(from_unixtime(dot.submitted_time - 3600))
WHERE 1 = 1
and (COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0)>0 or COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) >0 )
and dot.ref_order_category = 0 
and dot.pick_city_id = 217
and dot.ref_order_status = 7
--and extract(hour from from_unixtime(dot.submitted_time - 3600)) between 10 and 20
)base
LEFT JOIN (select blt.uid
,cast(FROM_unixtime(blt.report_date -3600)as DATE) as report_date
,blt.total_point
,case when blt.tier in (1,6,11) then 'T1'
when blt.tier in (2,7,12) then 'T2'
when blt.tier in (3,8,13) then 'T3'
when blt.tier in (4,9,14) then 'T4'
when blt.tier in (5,10,15) then 'T5'
else null
end as current_driver_tier
FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live blt )blt on blt.uid = base.uid and blt.report_date = base.report_date
WHERE base.report_date >= date((current_date)-interval '14' day)
AND base.report_date < date(current_date)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12 )a
where 1=1
--and a.is_hub_qualified = '1'
group by 1,2,3,4,5,6
