with raw as 
(SELECT 
        order_id
        , create_uid
        , shipper_uid
        , create_time as auto_assign_unixtime
        , FROM_UNIXTIME(create_time - 3600) auto_assign_timestamp

from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

where 1=1 and status = 21 and create_uid > 0 
and date( FROM_UNIXTIME(create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
-- and order_id = 381409414

)
-- ,location_agg as
-- (
-- select  
--          t.*
--          ,try_cast(split_part(t.location,',',1) as double) as lat_
--          ,try_cast(split_part(t.location,',',2) as double) as long_
         
-- from shopeefood.foody_partner_db__order_shipper_status_log_tab__reg_daily_s0_live t 
-- WHERE date(from_unixtime(create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day

-- )
,withdraw_raw as 
(select *

from dev_vnfdbi_opsndrivers.snp_foody_sf_chat_new 
where issue_reason like '%Wrong assigned rule for HUB Driver%'
and date(start_at) between current_date - interval '30' day and current_date - interval '1' day
)
,metrics as 
(select 
       date(a.auto_assign_timestamp) as report_date 
      ,a.order_id
      ,a.auto_assign_timestamp
      ,a.shipper_uid
      ,case when sm.shipper_type_id = 12 then 1 else 0 end as is_hub_driver 
      ,case when dot.hub_id > 0 then 1 else 0 end as is_hub_qualified
      ,case when (dot.pick_hub_id > 0  and pick_hub_id = drop_hub_id  or dot.real_pick_hub_id > 0  and real_pick_hub_id = real_drop_hub_id )
            then '1. Pick and Drop in Hub'
            when (dot.pick_hub_id > 0  and pick_hub_id <> drop_hub_id or dot.real_pick_hub_id > 0  and real_pick_hub_id <> real_drop_hub_id) 
            then  '2. Pick in Hub and Drop out Hub '
            else null end as pick_drop_rule
      ,dot.distance      
    --   ,b.lat_ 
    --   ,b.long_
    --   ,mm.merchant_latitude
    --   ,mm.merchant_longtitude
    --   ,great_circle_distance(b.lat_,b.long_,mm.merchant_latitude,mm.merchant_longtitude) as pickup_distance
    --   ,oct.restaurant_id
    --   ,wr.
      ,array_agg(wr.now_uid) as driver_request_sf  
    --   ,map_agg(cast(wr.now_uid as varchar),issue_reason) as sale_force_check


from raw a 

-- left join location_agg b on b.order_id = a.order_id
--                          and b.shipper_uid = a.shipper_uid   
--                          and b.status = 11 
--                          and b.order_type = 0     
                                                                                         


-- left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = a.order_id

-- left join shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = oct.restaurant_id and try_cast(mm.grass_date as date) = date(a.auto_assign_timestamp)

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_uid and try_cast(sm.grass_date as date) = date(a.auto_assign_timestamp)

--check hub order
left join 
(SELECT 
         id
        ,ref_order_id
        ,ref_order_code
        ,ref_order_category 
        ,delivery_distance/cast(1000 as double) as distance
        ,hub_id 
        ,pick_hub_id
        ,drop_hub_id
        ,real_pick_hub_id
        ,real_drop_hub_id
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 

left join 
(
SELECT 
         order_id
        ,cast(json_extract(order_data,'$.shipper_policy.type') as bigint) AS driver_payment_policy 
        ,cast(json_extract(order_data,'$.hub_id') as bigint) as hub_id 
        ,cast(json_extract(order_data,'$.pick_hub_id') as bigint) as pick_hub_id 
        ,cast(json_extract(order_data,'$.drop_hub_id') as bigint) as drop_hub_id 
        ,cast(json_extract(order_data,'$.real_pick_hub_id') as bigint) as real_pick_hub_id 
        ,cast(json_extract(order_data,'$.real_drop_hub_id') as bigint) as real_drop_hub_id 

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day)
)dotet ON dot.id = dotet.order_id
WHERE date(from_unixtime(submitted_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
) dot ON a.order_id = dot.ref_order_id AND dot.ref_order_category = 0
left join withdraw_raw wr on wr.ref_id = dot.ref_order_code

where 1 = 1 
-- and a.order_id = 381409414

group by 1,2,3,4,5,6,7,8
)

select 
        report_date
       ,is_hub_driver
       ,is_hub_qualified
       ,pick_drop_rule
       ,cardinality(driver_request_sf) as total_driver_request_sf        
       ,cardinality(array_agg(shipper_uid)) as total_driver_withdrawn        
       ,sum(distance)/cast(cardinality(driver_request_sf) as double) as total_distance
    --    ,sale_force_check
    --    ,array_agg(shipper_uid) as check_driver_withdrawn 


from metrics 

group by 1,2,3,4,5
