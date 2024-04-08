with raw as 
(select 
        created_date,
        order_code,
        case
        when raw.group_id > 0 and order_assign_type = 'Group' then 'group'
        when raw.group_id > 0 and order_assign_type != 'Group' then 'stack'
        else 'single' end as is_stack_group,
        date_diff('second',created_timestamp,delivered_timestamp)*1.0000/60 as ata,
        raw.group_id,
        ogi.min_group_created,
        ogi.max_group_delivered,
        case 
        when raw.group_id > 0 then date_diff('second',min_group_created,max_group_delivered)*1.0000/60/total_order_in_group
        else date_diff('second',last_incharge_timestamp,delivered_timestamp)*1.0000/60 end as lt_completed_adj,
        case 
        when dp.shipper_tier = 'Hub' then 'Level 1'
        else dp.shipper_tier end as tier


from driver_ops_raw_order_tab raw   

left join (select report_date,shipper_id,shipper_tier from driver_ops_driver_performance_tab ) dp 
    on dp.shipper_id = raw.shipper_id
    and dp.report_date = raw.created_date

left join 
(select 
        group_id,
        count(id) as total_order_in_group,
        min(last_incharge_timestamp) as min_group_created,
        max(delivered_timestamp) as max_group_delivered,
        array_agg(id) as id_ext
from driver_ops_raw_order_tab
where group_id > 0 
and order_type != 0 
group by 1 
) ogi on ogi.group_id = (case when raw.group_id > 0 then raw.group_id else 0 end)

where order_status = 'Delivered'
and order_type != 0
and created_date between date'2023-10-01' and date'2023-10-31'
-- and distance <= 6
)
select  
        date_trunc('month',created_date) as month_,
        tier,
        coalesce(is_stack_group,'overall') as type_,
        sum(ata)/cast(count(distinct order_code) as double) as ata,
        sum(lt_completed_adj)/cast(count(distinct order_code) as double) as ata_adj

from raw


group by 1,2, grouping sets (is_stack_group,())


