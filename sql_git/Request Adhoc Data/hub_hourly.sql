with historical_data as 
(SELECT 
        dot.ref_order_id
        ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,dot.uid
        ,extract(hour from from_unixtime(dot.real_drop_time - 3600)) as create_hour
        ,(dot.delivery_distance*1.00)/1000 as distance
        ,case when dot.pick_city_id = 217 then 'HCM'
        when dot.pick_city_id = 218 then 'HN'
        when dot.pick_city_id = 219 then 'DN'
        when dot.pick_city_id = 220 then 'HP' 
        ELSE 'OTH' end as city_group
        ,case when dot.ref_order_category = 0 then 'order-delivery' else 'ship-order' end as service
        ,sm.shipper_type_id
        ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 1 else 0 end as  is_group_order 
        ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1 else 0 end as  is_stack_order         
        -- ,case when mgo.app_type_id in (50,51) then 'Shopee'
        --       when mgo.app_type_id in (1,2,3,4,10,11,20,21,26,27,28) then 'SPF' --Foody
        --       else 'SPF' end as app_ver 
        ,dotet.order_data
        ,COALESCE(cast(json_extract(dotet.order_data,'$.shipper_policy.type') as BIGINT ),0) as policy_type
        ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0) as real_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0) as real_pick_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_drop_hub_id') as BIGINT ),0) as real_drop_hub_id


FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet ON dot.id = dotet.order_id
-- left join shopeefood.foody_mart__fact_gross_order_join_detail mgo on dot.ref_order_id = mgo.id and dot.ref_order_category = 0 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dot.uid and try_cast(sm.grass_date as date) = date(from_unixtime(dot.submitted_time - 3600))

-- Group order
LEFT JOIN 
(SELECT 
a.order_id
,a.order_type
,case when a.order_type <> 200 then order_type else ogi.ref_order_category end as order_category 
,case when a.assign_type = 1 then '1. Single Assign'
when a.assign_type in (2,4) then '2. Multi Assign'
when a.assign_type = 3 then '3. Well-Stack Assign'
when a.assign_type = 5 then '4. Free Pick'
when a.assign_type = 6 then '5. Manual'
when a.assign_type in (7,8) then '6. New Stack Assign'
else null end as assign_type
,from_unixtime(a.create_time - 60*60) as create_time
,from_unixtime(a.update_time - 60*60) as update_time
,date(from_unixtime(a.create_time - 60*60)) as date_
,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
,ogm.total_order_in_group
                            
from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type
                                
from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge
                                
 UNION
                                    
SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type
                                
from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge
)a
                                
-- take last incharge
LEFT JOIN 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
                                
from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge
                                
 UNION
                                    
SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
                                
from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
where status in (3,4) -- shipper incharge
)a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time
LEFT JOIN
(select 
group_id
,count(distinct ref_order_code) as total_order_in_group
                               
                               
from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)
group by 1 
)  ogm on ogm.group_id =  (case when a.order_type = 200 then a.order_id else 0 end)     
                                
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
                            
where 1=1
and a_filter.order_id is null -- take last incharge
and a.order_type = 200
                            
GROUP BY 1,2,3,4,5,6,7,8,9
                            
)group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category 

WHERE 1 = 1
and dot.order_status = 400
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '7' day and current_date - interval '1' day
)
,realtime_data as 
(SELECT 
        dot.ref_order_id
        ,date(from_unixtime(dot.real_drop_time - 3600)) as report_date
        ,dot.uid
        ,extract(hour from from_unixtime(dot.real_drop_time - 3600)) as create_hour
        ,(dot.delivery_distance*1.00)/1000 as distance
        ,case when dot.pick_city_id = 217 then 'HCM'
        when dot.pick_city_id = 218 then 'HN'
        when dot.pick_city_id = 219 then 'DN'
        when dot.pick_city_id = 220 then 'HP' 
        ELSE 'OTH' end as city_group
        ,case when dot.ref_order_category = 0 then 'order-delivery' else 'ship-order' end as service
        -- ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 1 else 0 end as  is_group_order 
        -- ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1 else 0 end as  is_stack_order       
        ,case when dot.group_id > 0 then 1 else 0 end as is_stack_order
        ,0 as is_group_order  
        -- ,case when mgo.app_type_id in (50,51) then 'Shopee'
        --       when mgo.app_type_id in (1,2,3,4,10,11,20,21,26,27,28) then 'SPF' --Foody
        --       else 'SPF' end as app_ver 
        ,dotet.order_data
        ,COALESCE(cast(json_extract(dotet.order_data,'$.shipper_policy.type') as BIGINT ),0) as policy_type
        ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_hub_id') as BIGINT ),0) as real_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_pick_hub_id') as BIGINT ),0) as real_pick_hub_id
        ,COALESCE(cast(json_extract(dotet.order_data,'$.real_drop_hub_id') as BIGINT ),0) as real_drop_hub_id


FROM (
select * 
from shopeefood.foody_partner_db__driver_order_tab__reg_continuous_s0_live where date(from_unixtime(submitted_time - 3600)) = current_date 
) dot
LEFT JOIN shopeefood.foody_partner_db__driver_order_extra_tab__reg_continuous_s0_live dotet ON dot.id = dotet.order_id
)

select * from realtime_data
