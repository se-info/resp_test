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
        extra_data


from dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

left join shopeefood.foody_partner_archive_db__shipper_hub_order_tab__reg_continuous_s0_live hub 
    on hub.ref_order_id = raw.id
    and hub.ref_order_category = raw.order_type

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hi 
    on hi.id = hub.autopay_report_id

where driver_policy = 2 
and date(delivered_timestamp) between date'2023-10-01' and date'2023-10-24'
and raw.order_type = 0 
and raw.order_status = 'Delivered'
and raw.shipper_id > 0 
/*
Hub refresh lại việc 12k cho 0-2km, 2-3.xx: 13.5k và + extra fee if higher, cho chị lại impact của hub theo Total Hub/Long Shift/Mid Shift/Short Shift
*/
)
,metrics as 
(select
        raw.report_date,
        raw.city_name,  
        raw.shipper_id,
        raw.shift_hour,
        raw.apply_extra_fee,
        do.kpi,
        do.daily_bonus,
        do.extra_ship,
        count(distinct raw.order_code) as total_order,
        sum(raw.current_shipping_fee) as total_shipping_fee,
        sum(raw.new_shipping_fee) as total_new_shipping_fee

from raw 

left join driver_ops_hub_driver_performance_tab do 
    on do.uid = raw.shipper_id
    and do.date_ = raw.report_date
    and do.total_order > 0 
    and do.slot_id = raw.slot_id

group by 1,2,3,4,5,6,7,8
)
select  
        *
from
(select 
        date_trunc('month',report_date) as month_,
        -- report_date,
        -- shipper_id,
        city_name,
        case 
        when shift_hour > 5 then 'long'
        when shift_hour = 5 then 'mid'
        when shift_hour = 3 then 'short' end as hub_segment,
        sum(total_order) as total_order,
        sum(total_shipping_fee) as current_ship,
        sum(daily_bonus) as current_bonus,
        sum(extra_ship) as current_extra,
        sum(total_new_shipping_fee) as new_ship,
        sum(new_extra) as new_extra,
        count(distinct report_date) as days

from 
(select  
        *,
        case 
        when extra_ship > 0 and shift_hour = 10 then (13500*30)-total_new_shipping_fee
        when extra_ship > 0 and shift_hour = 8 then (13500*25)-total_new_shipping_fee
        else 0 end as new_extra

from metrics )
group by 1,2,3
)
where hub_segment is not null 