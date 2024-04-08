with raw as 
(select 
        raw.order_code,
        raw.city_name,
        raw.shipper_id,
        13500 as current_shipping_fee,
        case 
        when raw.distance <= 2 then 12000
        when raw.distance > 2 then 13500
        end as new_shipping_fee,
        raw.driver_policy,
        (hi.shift_end_time - hi.shift_start_time)/3600 as shift_hour,
        cast(json_extract(hi.extra_data,'$.is_apply_fixed_amount') as varchar) as apply_extra_fee,
        hub.slot_id,
        date(delivered_timestamp) as report_date,
        hour(delivered_timestamp) as hour_,
        raw.group_id,
        ogi.min_group_created,
        ogi.max_group_delivered,
        raw.created_timestamp,
        raw.delivered_timestamp,
        ogi.total_order_in_group

from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = raw.id
    and hub.ref_order_category = raw.order_type

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hi 
    on hi.id = hub.autopay_report_id

left join 
(select 
        group_id,
        count(id) as total_order_in_group,
        min(created_timestamp) as min_group_created,
        max(delivered_timestamp) as max_group_delivered
from driver_ops_raw_order_tab
where group_id > 0 
group by 1 
) ogi on ogi.group_id = (case when raw.group_id > 0 then raw.group_id else 0 end)

where driver_policy = 2 
and date(delivered_timestamp) >= date'2023-10-01'
and raw.order_type = 0 
and raw.order_status = 'Delivered'
and raw.shipper_id > 0 
and raw.is_asap = 1 
and raw.city_id in (217,218)
)
select 
        -- hub_segment,
        -- case 
        -- when hour_ >= 1 and hour_ <= 5 then '1 - 5AM'
        -- when hour_ >= 6 and hour_ <= 10 then '6 - 10AM'
        -- when hour_ >= 11 and hour_ <= 13 then '11 - 13PM'
        -- when hour_ >= 14 and hour_ <= 17 then '14 - 17PM'
        -- when hour_ >= 17 and hour_ <= 20 then '17 - 20PM'
        -- else '21 -0PM' end as hour_range,
        -- count(distinct order_code)/cast(count(distinct report_date) as double) ado
        hub_segment,
        case 
        when group_id > 0 then 1
        else 0 end as is_stack_group,
        sum(lt_completed_adj)/count(distinct order_code) as ata_adj,
        count(distinct order_code)/cast(count(distinct report_date) as double) ado
from
(select  
        case 
        when shift_hour > 5 then 'long'
        when shift_hour = 5 then 'mid'
        when shift_hour = 3 then 'short' end as hub_segment,
        order_code,
        group_id,
        report_date,
        hour_,
        case 
        when group_id > 0 then date_diff('second',min_group_created,max_group_delivered)*1.0000/60/total_order_in_group
        else date_diff('second',created_timestamp,delivered_timestamp)*1.0000/60 end as lt_completed_adj


from raw 
)
where hub_segment is not null
group by 1,2 
