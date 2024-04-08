with assign_raw as
(select 
        date_  
    --    ,hour_
       ,case when hour_ <=  2 then 0
            when hour_ <=  4 then 2
            when hour_ <=  6 then 4
            when hour_ <=  8 then 6
            when hour_ <=  10 then 8
            when hour_ <=  12 then 10
            when hour_ <=  14 then 12
            when hour_ <=  16 then 14
            when hour_ <=  18 then 16
            when hour_ <=  20 then 18
            when hour_ <=  22 then 20
            when hour_ <=  24 then 22
             end as hour_range    
       ,city_name 
       ,shipper_id
       ,shipper_type
       ,coalesce(sum(total_orders),0) total_order
       ,coalesce(sum(total_assign_excl_fp),0) total_assign_excl_fp
       ,coalesce(sum(total_deny),0) total_deny        


from dev_vnfdbi_opsndrivers.phong_assignment_tracker

where date_ != date'2022-10-20'
group by 1,2,3,4,5
)
,metrics as 
(select 
        date_ 
       ,shipper_id
       ,city_name
       ,shipper_type 
       ,hour_range
       ,total_order
       ,pct_assign
       ,case when pct_assign < 0.5 then 1 else 0 end as is_impacted


from
(select a.* ,case when total_assign_excl_fp > 0 then 1 - (coalesce(sum(total_deny),0)/cast(coalesce(sum(total_assign_excl_fp),0) as double))
             when total_assign_excl_fp = 0 and total_deny > 0 then 0
             when total_assign_excl_fp = 0 and total_deny = 0 then 1
             end as pct_assign    


from assign_raw a 

-- where city_name = 'HCM City'
group by 1,2,3,4,5,6,7,8
)  

group by 1,2,3,4,5,6,7
)
select   
        date_ 
    --    ,shipper_id 
       ,city_name
       ,shipper_type
       ,hour_range 
       ,hour_impacted
       ,sum(total_order) as total_order
       ,coalesce(sum(case when is_impacted = 1 then orders_impacted else null end),null) as total_order_impacted
       ,coalesce(count(distinct case when is_impacted = 1 then shipper_id else null end),null) as total_driver_impacted

from
(select 
        a.date_ 
       ,a.shipper_id
       ,a.city_name
       ,a.shipper_type
       ,a.hour_range
       ,a.total_order
       ,a.pct_assign
       ,a.is_impacted
       ,coalesce(b.total_order,null) as orders_impacted
       ,coalesce(b.hour_range,null) as hour_impacted
    --    ,sum(case when a.is_impacted = 1 then b.total_order else null end ) as orders_impacted

from metrics a 

left join metrics b on a.date_ = b.date_ 
                    and a.shipper_id = b.shipper_id 
                    and a.hour_range + 2 = b.hour_range 

where 1 = 1
-- and a.date_ = current_date - interval '2' day
-- and a.shipper_id = 18209315
   
)

group by 1,2,3,4,5
