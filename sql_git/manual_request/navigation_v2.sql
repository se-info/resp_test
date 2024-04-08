with raw as 
(SELECT base.delivered_date
    ,date(first_auto_assign_timestamp) as create_date
    --,delivered_timestamp
    --,create_timestamp
    ,hour(delivered_timestamp)*100 + minute(delivered_timestamp) as delivered_hour_min
    --,hour(create_timestamp)*100 + minute(create_timestamp) as create_hour_min
    ,hour(fa.first_auto_assign_timestamp)*100 + minute(first_auto_assign_timestamp) as create_hour_min 
    ,base.city_group
    ,coalesce(base.hub_name,base.pick_hub_name) as hub_location
    ,base.pick_hub_id
    ,base.drop_hub_id
    ,base.uid
    ,case when base.hub_id > 0 and base.drop_hub_id > 0 and base.pick_hub_id <> base.drop_hub_id then 'Current - Navigation'
          when base.hub_id = 0 and base.pick_hub_id > 0 and base.drop_hub_id > 0 and base.pick_hub_id <> base.drop_hub_id and base.distance < 3.2 then 'Navigation - Pick in Hub'  
          when base.hub_id = 0 and base.pick_hub_id = 0 and base.drop_hub_id > 0 and base.distance < 3.2 then 'Navigation - Pick non Hub'
          when base.hub_id > 0 and base.pick_hub_id = base.drop_hub_id then 'Current - Pick and drop in Hub'
        --when base.hub_id > 0 then 'Current - Qualified '
        else 'Other - Non Qualified' end as order_route
    ,case when hour(first_auto_assign_timestamp) between 6 and 21 and city_group = 'HCM' then 1 
          when hour(first_auto_assign_timestamp) between 8 and 20 and city_group = 'HN'  then 1
          else 0 end as active_hub_time
    ,base.ref_order_code
    ,base.is_hub_driver
    ,base.is_inshift
    ,base.drop_latitude
    ,base.drop_longitude
    ,base.pick_latitude
    ,base.pick_longitude
    ,base.pick_hub_name
    -- ,base.drop_hub_name

    FROM dev_vnfdbi_opsndrivers.navgigation_table base

    left join 
    (SELECT order_id , 0 as order_type
    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
    where 1=1
    and grass_schema = 'foody_order_db'
    group by 1,2) fa on fa.order_id = base.ref_order_id 

    where  base.delivered_date between date'2022-05-10' and date'2022-05-16'
    -- 
    and city_group in ('HCM','HN')
    --and base.hub_id > 0 
    )

,est_tab as (
    select delivered_date
          ,city_group 
          ,order_route
        --   ,array_agg(pick_hub_name order by drop_hub_name asc) as et
        --   ,case when drop_hub_name <> pick_hub_name then concat(pick_hub_name,',',drop_hub_name)
        --         when drop_hub_name is null then concat(pick_hub_name,',','non hub')
        --         when pick_hub_name is null then concat('non hub',',',drop_hub_name)  
        --         else null end as new_hub
          ,pick_latitude
          ,pick_longitude
          ,drop_latitude
          ,drop_longitude
          ,pick_hub_name as current_hub
          ,case when order_route != 'Current - Pick and drop in Hub' then new.new_hub_name else null end as new_hub_name 
          ,case when new.new_hub_name is not null and order_route != 'Current - Pick and drop in Hub' then 1 else 0 end as is_new_hub
          ,ref_order_code 






from raw a 
left join dev_vnfdbi_opsndrivers.hub_overlapping_raw new on cast(new.hub_id as bigint) = a.pick_hub_id

where a.order_route not in  ('Other - Non Qualified')
-- a.order_route in ('Navigation - Pick non Hub','Navigation - Pick in Hub','Current - Navigation')
    and a.active_hub_time = 1 


    -- group by 1,2,3,4,5
    )

select   

-- array_sort(split(new_hub_name, ',')) as tsst
-- *

 city_group
-- ,new_hub_name
,order_route
,count(distinct ref_order_code)*1.00/count(distinct delivered_date) as total_order_drop_out_hub  
,count(distinct case when new_hub_name is not null and is_new_hub = 1 then new_hub_name else null end) as total_hub_can_overlapping
from est_tab 

group by 1,2
