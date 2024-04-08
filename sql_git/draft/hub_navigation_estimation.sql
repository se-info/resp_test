SELECT a.report_date
,year(a.report_date)*100+week(a.report_date) as created_year_week
,a.city_group
,create_hour
,hub_order_est
,pick_drop_rule
,distance_range
,count(a.ref_order_id) as total_order

from
(SELECT base.report_date
,base.uid
,base.create_hour
,base.ref_order_id
,base.city_group
,coalesce(base.hub_name,base.pick_hub_name) as hub_location
,case when base.distance < 1 then '1. 0 - 1km'
when base.distance < 2 then '2. 1 - 2km'
when base.distance < 2.5 then '3. 2 - 2.5km'
when base.distance < 3 then '4. 2.5 - 3km'
when base.distance < 4 then '5. 3 - 4km'
when base.distance <= 5 then '6. 4 - 5km'
else '7. > 5km' end as distance_range
,case when base.shipper_type_id = 12 then '1'
ELSE '0' end as is_hub_driver
,case WHEN base.hub_id > 0 and base.create_hour between 8 and 19 then 1
WHEN base.pick_hub_id > 0 and base.distance <= 2 and base.create_hour between 8 and 19 then 1
else 0 end as is_hub_qualified_2km
,case WHEN base.hub_id > 0 and base.create_hour between 8 and 19 then 1
WHEN base.pick_hub_id > 0 and base.distance <= 2.3 and base.create_hour between 8 and 19 then 1
else 0 end as is_hub_qualified_2km3
,case WHEN base.hub_id > 0 and base.create_hour between 8 and 19 then 1
WHEN base.pick_hub_id > 0 and base.distance <= 2.7 and base.create_hour between 8 and 19 then 1
else 0 end as is_hub_qualified_2km7
,case WHEN base.hub_id > 0 and base.create_hour between 8 and 21 then 1
WHEN base.pick_hub_id > 0 and base.distance <= 3 and base.create_hour between 8 and 21 then 1
else 0 end as is_hub_qualified_3km
,case when base.hub_id > 0 and pick_hub_id = drop_hub_id and base.create_hour between 8 and 21 then '1. Pick and Drop in Hub'
when base.hub_id > 0 and pick_hub_id <> drop_hub_id and base.create_hour between 8 and 21 then '2. Pick in Hub and Drop in another Hub ' /* < 2.5km */ 
--when base.drop_hub_id > 0 and pick_hub_id = 0 and distance < 2.5 and base.create_hour between 8 and 21  then '3. Drop in Hub and not pick in Hub ' /* non overlap with (2) */
when base.drop_hub_id > 0 and pick_hub_id = 0 and distance < 3.5 and base.create_hour between 8 and 21 then '4. Drop in Hub and not pick in Hub and less than 3km' /* non overlap with (2) */
else 'Non Qualified' end as pick_drop_rule
,case when base.hub_id > 0 and base.create_hour between 8 and 21 then 'Hub qualified current'
      when base.hub_id = 0 and base.pick_hub_id > 0 and base.distance >= 2.5 and distance < 3.5 and base.create_hour between 8 and 21 then 'Hub qualified increase drop off to 3km'
      else 'Non Qualified' end as hub_order_est

FROM
(SELECT dot.ref_order_id,date(from_unixtime(dot.submitted_time - 3600)) as report_date
,dot.uid
,extract(hour from from_unixtime(dot.submitted_time - 3600)) as create_hour
,(dot.delivery_distance*1.00)/1000 as distance
,case when dot.pick_city_id = 217 then 'HCM'
when dot.pick_city_id = 218 then 'HN'
when dot.pick_city_id = 219 then 'DN'
when dot.pick_city_id = 220 then 'HP'
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
from shopeefood.foody_mart__profile_shipper_master
where grass_region = 'VN')sm on sm.shipper_id = dot.uid and sm.report_date = date(from_unixtime(dot.submitted_time - 3600))
WHERE 1 = 1
and dot.ref_order_category = 0
and dot.order_status = 400
and dot.pick_city_id in (217,218)
)base

)a
where 1=1
and report_date between current_date - interval '7' day and current_date - interval '1' day
group by 1,2,3,4,5,6,7