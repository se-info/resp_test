SELECT a.report_date
,case when a.report_date between date('2022-01-01') and date('2022-01-02') then 202152
      else year(a.report_date)*100+week(a.report_date) end as created_year_week

,a.city_group
,count(a.ref_order_id) as total_order
,count(case when a.hub_location is not null then a.ref_order_id else null end) as total_order_open_hub
,count(case when a.is_hub_qualified = 1 then a.ref_order_id else null end) as total_hub_qualified
,count(distinct case when is_hub_delivered_inshift = 1 then uid else null end) as total_active_hub

from
(SELECT base.report_date
,base.uid
,base.create_hour
,base.ref_order_id
,base.city_group
,coalesce(base.hub_name,base.pick_hub_name)  as hub_location
,case when base.shipper_type_id = 12 and policy = 2 then 1 else 0 end as is_hub_delivered_inshift
,case WHEN base.hub_id > 0 and base.report_date < date'2022-03-09' and base.city_group = 'HCM' and base.create_hour between 8 and 19 then 1
 	  WHEN base.hub_id > 0 and base.report_date <= date'2022-05-09' and base.city_group = 'HCM'  and base.create_hour between 8 and 21 then 1
      WHEN base.hub_id > 0 and base.report_date >= date'2022-05-10' and base.city_group = 'HCM'  and base.create_hour between 6 and 21 then 1
      WHEN base.hub_id > 0 and base.report_date < date'2022-03-17' and base.city_group = 'HN' and base.create_hour between 8 and 19 then 1
 	  WHEN base.hub_id > 0 and base.report_date < date'2022-05-19' and base.city_group = 'HN'  and base.create_hour between 8 and 20 then 1
      WHEN base.hub_id > 0 and base.report_date >= date'2022-05-19' and base.city_group = 'HN'  and base.create_hour between 8 and 22 then 1
  	  WHEN base.hub_id > 0 and base.create_hour between 8 and 19 then 1
 	  --WHEN base.pick_hub_id > 0 and base.create_hour between 8 and 19 and base.distance < 2.3 then 1	
      else 0 end as is_hub_qualified
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
,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as policy
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
)base


GROUP BY 1,2,3,4,5,6,7,8 )a
where 1=1
and report_date between current_date - interval '1' day and current_date - interval '1' day
group by 1,2,3
