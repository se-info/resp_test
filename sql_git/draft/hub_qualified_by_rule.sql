SELECT a.report_date
,a.city_group
,1 as no
,pick_drop_rule
,hub_location
,count(a.ref_order_id) as total_order
,sum(distance)*1.000000/count(a.ref_order_id) as avg_distance
,approx_percentile(distance,0.9) as pct90_distance
,approx_percentile(distance,0.95) as pct95_distance


from
(SELECT base.report_date
,base.uid
,base.distance
,base.ref_order_id
,base.city_group
,coalesce(base.hub_name,base.pick_hub_name)  as hub_location
,case when base.distance < 1 then '1. 0 - 1km'
      when base.distance < 2 then '2. 1 - 2km'
      when base.distance < 2.5 then '3. 2 - 2.5km'
      when base.distance < 3 then '4. 2.5 - 3km'
      when base.distance < 4 then '5. 3 - 4km'
      when base.distance <= 5 then '6. 4 - 5km'
      else '7. > 5km' end as distance_range   
,case when base.shipper_type_id = 12 then '1'
ELSE '0' end as is_hub_driver
,case WHEN base.hub_id > 0 and base.report_date < date'2022-03-09' and base.city_group = 'HCM' and base.create_hour between 8 and 19 then 1
 	  WHEN base.hub_id > 0 and base.report_date >= date'2022-03-09' and base.city_group = 'HCM'  and base.create_hour between 8 and 21 then 1
      WHEN base.hub_id > 0 and base.report_date < date'2022-03-17' and base.city_group = 'HN' and base.create_hour between 8 and 19 then 1
 	  WHEN base.hub_id > 0 and base.report_date >= date'2022-03-17' and base.city_group = 'HN'  and base.create_hour between 8 and 20 then 1
      WHEN base.hub_id > 0 and base.city_group = 'HP'  and base.create_hour between 10 and 19 then 1
  	  WHEN base.hub_id > 0 and base.create_hour between 8 and 19 then 1
 	  --WHEN base.pick_hub_id > 0 and base.create_hour between 8 and 19 and base.distance < 2.3 then 1	
      else 0 end as is_hub_qualified
,case when (base.pick_hub_id > 0  and pick_hub_id = drop_hub_id  or base.real_pick_hub_id > 0  and real_pick_hub_id = real_drop_hub_id )
            then '1. Pick and Drop in Hub'
      when (base.pick_hub_id > 0  and pick_hub_id <> drop_hub_id or base.real_pick_hub_id > 0  and real_pick_hub_id <> real_drop_hub_id) 
            then  '2. Pick in Hub and Drop out Hub '
      else null end as pick_drop_rule

FROM
(
SELECT dot.ref_order_id,date(from_unixtime(dot.submitted_time - 3600)) as report_date
,dot.uid
,extract(hour from from_unixtime(dot.submitted_time - 3600)) as create_hour
,(dot.delivery_distance*1.00)/1000 as distance
,case when dot.pick_city_id = 217 then 'HCM'
when dot.pick_city_id = 218 then 'HN'
when dot.pick_city_id = 219 then 'DN'
when dot.pick_city_id = 220 then 'HP' 
ELSE 'OTH' end as city_group
,case when dot.ref_order_category = 0 then 'order-delivery' else 'ship-order' end as service
,sm.shipper_type_id
,hub.hub_name as pick_hub_name
,hubb.hub_name
,hubbb.hub_name as real_pick_hub_name
,hubbbb.hub_name as real_hub_name
,dotet.order_data
,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0) as real_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0) as real_pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_drop_hub_id') as BIGINT ),0) as real_drop_hub_id


FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet ON dot.id = dotet.order_id
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hub on hub.id = COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0)
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubb on hubb.id = COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0)
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubbb on hubbb.id = COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0)
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubbbb on hubbbb.id = COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0)
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


GROUP BY 1,2,3,4,5,6,7,8,9,10
)a
where 1=1
and is_hub_qualified = 1 
and report_date between current_date - interval '30' day and current_date - interval '1' day
group by 1,2,3,4,5











