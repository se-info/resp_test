with raw as 
(select 
        r.order_code,
        r.shipper_id,
        r.sender_name,
        r.city_name,
        r.district_id,
        date_diff('second',first_auto_assign_timestamp,last_incharge_timestamp)*1.00/60 as first_to_last,
        date_diff('second',first_auto_assign_timestamp,first_incharge_timestamp)*1.00/60 as first_to_first,
        r.hub_id,
        r.pick_hub_id,
        r.drop_hub_id,
        r.driver_distance,
        r.delivered_timestamp,
        r.group_id,
        r.is_asap
        -- if(sm.shipper_type_id=12,'hub','non-hub') as working_group

from driver_ops_raw_order_tab r 

where date(delivered_timestamp) = date'2024-09-23'
and r.order_status = 'Delivered'
and r.city_id = 218
and r.order_type = 0
-- and is_asap = 1 

order by 12 desc 
)
select 
        date(delivered_timestamp) as created_date,
        city_name,
        hour(delivered_timestamp) as "hour",
        avg(case when is_asap = 1 then first_to_first else null end) as "avg first auto to first incharged overall",
        avg(case when is_asap = 1 then first_to_last else null end) as "avg first auto to last incharged overall",
        avg(case when is_asap = 1 and group_id = 0 then first_to_first else null end) as "avg first auto to first incharged single",
        avg(case when is_asap = 1 and group_id = 0 then first_to_last else null end) as "avg first auto to last incharged single",
        avg(case when is_asap = 1 and group_id > 0 then first_to_first else null end) as "avg first auto to first incharged stack",
        avg(case when is_asap = 1 and group_id > 0 then first_to_last else null end) as "avg first auto to last incharged stack",
        count(distinct order_code) as cnt_order,
        count(distinct case when group_id > 0 then order_code else null end)*1.00/count(distinct order_code) as percent_stack,
        count(distinct shipper_id) as "active driver"

from raw 

group by 1,2,3

