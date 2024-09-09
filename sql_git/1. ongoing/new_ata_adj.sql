with raw as 
(select 
        raw.order_code,
        raw.city_name,
        raw.shipper_id,
        date(delivered_timestamp) as report_date,
        hour(delivered_timestamp) as hour_,
        raw.group_id,
        ogi.min_group_created,
        ogi.max_group_delivered,
        raw.created_timestamp,
        raw.last_incharge_timestamp,
        raw.delivered_timestamp,
        ogi.total_order_in_group,
        if(order_type=0,'delivery','spxi') as source

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 


left join 
(select 
        group_id,
        count(id) as total_order_in_group,
        min(last_incharge_timestamp) as min_group_created,
        max(delivered_timestamp) as max_group_delivered
from driver_ops_raw_order_tab
where group_id > 0 
group by 1 
) ogi on ogi.group_id = (case when raw.group_id > 0 then raw.group_id else 0 end)

where 1 = 1 
and date(delivered_timestamp) >= date'2023-10-01'
and raw.order_type = 0 
and raw.order_status = 'Delivered'
and raw.shipper_id > 0 
and raw.is_asap = 1 
)
select
        -- case 
        -- when group_id > 0 then 1
        -- else 0 end as is_stack_group,
        if(city_name in ('HCM City','Ha Noi City','Da Nang City'),city_name,'Other') as city_group,
        -- source,
        hour_,
        sum(lt_completed_adj)/count(distinct order_code) as ata_adj,
        count(distinct order_code) ado,
        count(distinct case when group_id > 0 then order_code else null end) stack_ado,
        sum(case when group_id > 0 then lt_completed_adj else null end)
                /count(distinct case when group_id > 0 then order_code else null end) as stack_ata_adj

from
(select  
        source,
        order_code,
        group_id,
        report_date,
        hour_,
        city_name,
        case 
        when group_id > 0 then date_diff('second',min_group_created,max_group_delivered)*1.0000/60/total_order_in_group
        else date_diff('second',last_incharge_timestamp,delivered_timestamp)*1.0000/60 end as lt_completed_adj

from raw )

where report_date = date'2024-08-08'
group by 1,2