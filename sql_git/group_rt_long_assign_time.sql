with filter_id as 
(select distinct ass.delivery_id,r.order_code,r.order_type
        -- from_unixtime(try_assign_time - 3600),
        -- from_unixtime(create_time - 3600),
        -- *
from shopeefood.shopeefood_mart_dwd_vn_assignment_driver_filtered_reason_di ass 

left join driver_ops_raw_order_tab r on r.delivery_id = ass.delivery_id

where ass.assign_type like '%CFAgroup%' 
and date(from_unixtime(try_assign_time - 3600)) = date'2024-10-01'
and date_diff('second',from_unixtime(create_time - 3600),from_unixtime(try_assign_time - 3600))*1.0000/60 >= 1
) 
,sa as 
(select 
        order_code,
        order_category,
        min(create_time) as min_ts,
        max(create_time) as max_ts,
        min_by(assign_type,create_time) as first_assign_type,
        max_by(assign_type,create_time) as last_assign_type,
        max_by(order_type,create_time) as last_order_type,
        count(distinct (order_code,driver_id,create_time)) as assigning_count

from driver_ops_order_assign_log_tab
where 1 = 1 
and date(create_time) >= date'2024-09-23'
and order_category = 0
group by 1,2 
)
,s as 
(select 
        r.created_date,
        r.order_code as code,
        date_diff('second',first_auto_assign_timestamp,last_incharge_timestamp)*1.00/60 as assignment_time,
        if(r.hub_id >0,1,0) as is_hub_order,
        if(r.driver_policy=2,1,0) as is_hub_delivered,
        hour(r.created_timestamp) as "hour",
        case 
        when (sa.last_assign_type = '6. New Stack Assign' or sa.last_order_type = 'Group') then 'stack|group'
        else 'single' end as assign_type,
        group_id,
        -- if(r.group_id >0,'stack|group','single') as assign_type,
        r.city_group,
        r.delivery_id,
        sa.assigning_count

from driver_ops_raw_order_tab r 

left join sa on sa.order_code = r.order_code and sa.order_category = r.order_type


where 1 = 1 
and r.order_type = 0
and r.created_date >= date'2024-09-23'
and r.order_status = 'Delivered'
and r.shipper_id > 0 
and r.city_id = 217
)
select s.* 
from s 
inner join filter_id f on f.delivery_id = s.delivery_id
