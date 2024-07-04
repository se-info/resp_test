SELECT 'All' as city
,a.app_ver
,pick_drop_rule
,distance_range
,count(a.ref_order_id)/cast(count(distinct a.report_date) as double) as total_order
,count(distinct case when is_hub_qualified = 1 then a.ref_order_id else null end)/cast(count(distinct case when is_hub_qualified =1 then a.report_date else null end) as double) as total_order_hub
,sum(distance)/cast(count(a.ref_order_id) as double) as avg_distance
,approx_percentile(distance,0.9) as pct90_distance
,approx_percentile(distance,0.95) as pct95_distance


from
(SELECT base.report_date
,base.uid
,base.distance
,base.ref_order_id
,base.app_ver
,base.city_group
-- ,coalesce(base.hub_name,base.pick_hub_name)  as hub_location
,case when base.distance < 3 then '1. 0 - 3km'
      when base.distance < 5 then '2. 3 - 5km'
      when base.distance < 7 then '3. 5 - 7km'
      when base.distance < 10 then '4. 7 - 10km'
      when base.distance > 10 then '5. > 10km' 
      end as distance_range   
,case when base.shipper_type_id = 12 then '1'
ELSE '0' end as is_hub_driver
,case WHEN base.hub_id > 0 then 1 else 0 end as is_hub_qualified
,case when (base.pick_hub_id > 0  and pick_hub_id = drop_hub_id  or base.real_pick_hub_id > 0  and real_pick_hub_id = real_drop_hub_id )
            then '1. Pick and Drop in Hub'
      when (base.pick_hub_id > 0  and pick_hub_id <> drop_hub_id or base.real_pick_hub_id > 0  and real_pick_hub_id <> real_drop_hub_id) 
            then  '2. Pick in Hub and Drop out Hub '
      else null end as pick_drop_rule

FROM
(
SELECT dot.ref_order_id
,date(from_unixtime(dot.submitted_time - 3600)) as report_date
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
,case   when mgo.app_type_id in (50,51) then 'Shopee'
        when mgo.app_type_id in (1,2,3,4,10,11,20,21,26,27,28) then 'SPF' --Foody
        else 'SPF' end as app_ver 
-- ,hub.hub_name as pick_hub_name
-- ,hubb.hub_name
-- ,hubbb.hub_name as real_pick_hub_name
-- ,hubbbb.hub_name as real_hub_name
,dotet.order_data
,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0) as real_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0) as real_pick_hub_id
,COALESCE(cast(json_extract(dotet.order_data,'$.real_drop_hub_id') as BIGINT ),0) as real_drop_hub_id


FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet ON dot.id = dotet.order_id
left join shopeefood.foody_mart__fact_gross_order_join_detail mgo on dot.ref_order_id = mgo.id and dot.ref_order_category = 0 
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hub on hub.id = COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0)
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubb on hubb.id = COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0)
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubbb on hubbb.id = COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0)
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hubbbb on hubbbb.id = COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0)
LEFT JOIN (SELECT *
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date
from shopeefood.foody_mart__profile_shipper_master
          where grass_region = 'VN')sm on sm.shipper_id = dot.uid and sm.report_date = date(from_unixtime(dot.submitted_time - 3600))
WHERE 1 = 1
and dot.ref_order_category = 0 
and dot.order_status = 400
-- and dot.pick_city_id in (217,218)
)base


GROUP BY 1,2,3,4,5,6,7,8,9,10
)a
where 1=1
-- and is_hub_qualified = 1 
and report_date between current_date - interval '7' day and current_date - interval '1' day
group by 1,2,3,4









