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
          when base.hub_id = 0 and base.pick_hub_id > 0 and base.drop_hub_id > 0 and base.pick_hub_id <> base.drop_hub_id and base.distance < 3.5 then 'Navigation - Pick in Hub'  
          when base.hub_id = 0 and base.pick_hub_id = 0 and base.drop_hub_id > 0 and base.distance < 3.5 then 'Navigation - Pick non Hub'
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
    and city_group in ('HCM','HN')
    --and base.hub_id > 0 

    )
,est as 
(
    select   a.delivered_date 
            ,a.city_group
            ,a.delivered_hour_min
            ,a.ref_order_code
            ,a.pick_hub_id
            ,a.drop_hub_id
            --,a.hub_location
            --,a.is_inshift
            --,a.is_hub_driver
            --Calculate order assign radius
            ,"great_circle_distance"(a.drop_latitude, a.drop_longitude, b.pick_latitude, b.pick_longitude)*1.22*1000 delivery_distance_meter
            ,b.ref_order_code as ability_navigation
            ,b.create_hour_min as first_auto_assign_hour_min
            ,b.pick_hub_id as navigation_pick
            ,b.drop_hub_id as navigation_drop

    from raw a 


    left join (select * from raw where order_route in ('Navigation - Pick non Hub','Navigation - Pick in Hub','Current - Navigation')
                                 and active_hub_time = 1) b
            on a.city_group = b.city_group  and a.delivered_date = b.create_date
                                            and a.delivered_hour_min between b.create_hour_min and b.create_hour_min + 2 
                                            and a.drop_hub_id = b.pick_hub_id 
                                            and a.pick_hub_id = b.drop_hub_id


    where a.order_route in ('Navigation - Pick non Hub','Navigation - Pick in Hub','Current - Navigation')
    and a.active_hub_time = 1 
    --group by 1,2,3,4,5,6,7,8,9
)
    
,est_v2 as 
(
    select   a.delivered_date 
            ,a.city_group
            ,a.delivered_hour_min
            ,a.ref_order_code
            ,a.pick_hub_id
            ,a.drop_hub_id
            --,a.hub_location
            --,a.is_inshift
            --,a.is_hub_driver
            --Calculate order assign radius
            ,"great_circle_distance"(a.drop_latitude, a.drop_longitude, b.pick_latitude, b.pick_longitude)*1.22*1000 delivery_distance_meter
            ,b.ref_order_code as ability_navigation
            ,b.create_hour_min as first_auto_assign_hour_min
            ,b.pick_hub_id as navigation_pick
            ,b.drop_hub_id as navigation_drop

    from raw a 


    left join (select * from raw where order_route in ('Navigation - Pick non Hub','Navigation - Pick in Hub','Current - Navigation')
                                 and active_hub_time = 1) b
            on a.city_group = b.city_group  and a.delivered_date = b.create_date
                                            and a.delivered_hour_min between b.create_hour_min - 2 and b.create_hour_min + 2 
                                            and a.drop_hub_id = b.pick_hub_id 
                                            and a.pick_hub_id = b.drop_hub_id


    where a.order_route in ('Navigation - Pick non Hub','Navigation - Pick in Hub','Current - Navigation')
    and a.active_hub_time = 1 
    --group by 1,2,3,4,5,6,7,8,9
)
    select city_group
        ,'1. Without CA' as metrics
        ,count(distinct ref_order_code)*1.0000/count(distinct delivered_date) as total_drop_out_hub_order
        ,count(distinct case when ability_navigation is not null and delivery_distance_meter <= 1000 then ability_navigation else null end)*1.0000/count(distinct delivered_date) as total_order_can_back_to_hub
        ,sum(case when ability_navigation is not null and delivery_distance_meter <= 1000 then 1 else null end)*1.0000/count(distinct delivered_date) as ability_assign_back_to_hub


    from est 

    group by 1,2

UNION ALL 

        select city_group
        ,'2. With CA' as metrics
        ,count(distinct ref_order_code)*1.0000/count(distinct delivered_date) as total_drop_out_hub_order
        ,count(distinct case when ability_navigation is not null and delivery_distance_meter <= 1000 then ability_navigation else null end)*1.0000/count(distinct delivered_date) as total_order_can_back_to_hub
        ,sum(case when ability_navigation is not null and delivery_distance_meter <= 1000 then 1 else null end)*1.0000/count(distinct delivered_date) as ability_assign_back_to_hub


    from est_v2

    group by 1,2 



    
